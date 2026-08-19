"""
Aufwach-Proxy fuer den Speakr-ASR-Pod auf RunPod.

Speakr spricht diesen Proxy an. Der Proxy startet bei Bedarf den Pod,
baut den SSH-Tunnel auf, startet den ASR-Dienst, reicht die Anfrage
durch und stoppt den Pod nach einer Leerlaufzeit wieder.

Nutzt die RunPod REST-API v2 (api.runpod.io/v2). Die v1-API
(rest.runpod.io/v1) wird am 15.11.2026 abgeschaltet (410 Gone).
"""
import asyncio
import json
import logging
import os
import subprocess
import time

import httpx
from fastapi import FastAPI, Request, Response

API = "https://api.runpod.io/v2"
POD_ID = os.environ.get("RUNPOD_POD_ID", "")
# Datei hat Vorrang vor der Umgebungsvariable, damit sich der Pod wechseln laesst,
# ohne den Stack (und damit den API-Key) anzufassen.
POD_REF_DATEI = os.environ.get("POD_REF_DATEI", "/app/pod.txt")
VOLUME_ID = os.environ.get("RUNPOD_VOLUME_ID", "")
# Liegt eine Pod-Spezifikation vor, wird bei Bedarf ein neuer Pod erstellt und
# im Leerlauf terminiert, statt einen bestehenden zu starten und zu stoppen.
# Grund: Ein gestoppter Pod haengt an einer Host-Maschine und laesst sich oft
# nicht mehr starten ("not enough free GPUs on the host machine").
POD_SPEC_DATEI = os.environ.get("POD_SPEC_DATEI", "/app/pod_spec.json")
# Zusaetzliche Umgebungsvariablen fuer den Pod, getrennt von der Spezifikation,
# damit Geheimnisse (z.B. HF_TOKEN) nicht in einer versionierten oder
# ueberschriebenen Datei landen. Format: KEY=VALUE, eine Zeile pro Eintrag.
POD_ENV_DATEI = os.environ.get("POD_ENV_DATEI", "/app/pod_env")
API_KEY = os.environ.get("RUNPOD_API_KEY", "")
SSH_KEY = os.environ.get("SSH_KEY", "/keys/id_ed25519")
TUNNEL_PORT = int(os.environ.get("TUNNEL_PORT", "19000"))
REMOTE_PORT = int(os.environ.get("REMOTE_PORT", "9000"))
START_CMD = os.environ.get("START_CMD", "bash /workspace/start_asr.sh")


def _konfig_datei(pfad: str = "/app/proxy_env") -> dict:
    werte: dict[str, str] = {}
    try:
        with open(pfad) as f:
            for zeile in f:
                zeile = zeile.strip()
                if zeile and not zeile.startswith("#") and "=" in zeile:
                    k, _, v = zeile.partition("=")
                    werte[k.strip()] = v.strip()
    except OSError:
        pass
    return werte


_KONFIG = _konfig_datei()


def _einstellung(name: str, standard: str) -> str:
    """Datei schlaegt Umgebungsvariable, damit sich Werte aendern lassen,
    ohne den Stack (und damit den API-Key) anzufassen."""
    return _KONFIG.get(name, os.environ.get(name, standard))


IDLE_SECONDS = float(_einstellung("IDLE_MINUTES", "10")) * 60
# Wie lange auf einen benutzbaren Pod gewartet wird. RunPod braucht gelegentlich
# deutlich mehr als zehn Minuten, bis der Pod RUNNING ist.
BOOT_TIMEOUT = float(_einstellung("BOOT_TIMEOUT", "1500"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("asr-proxy")

# Eigene Doku-Routen abschalten: sonst verdecken /docs, /redoc und
# /openapi.json die gleichnamigen Pfade des ASR-Dienstes dahinter.
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
_lock = asyncio.Lock()
_tunnel: subprocess.Popen | None = None
_last_used = time.time()
_inflight = 0


def _headers():
    return {"Authorization": f"Bearer {API_KEY}"}


def _negiert(text: str):
    """Sortierschluessel, der Zeichenketten absteigend sortiert."""
    return tuple(-ord(c) for c in text)


_pod_id_cache: str | None = None


def _pod_referenz() -> str:
    """Liest die Pod-Referenz, Datei vor Umgebungsvariable."""
    try:
        with open(POD_REF_DATEI) as f:
            wert = f.read().strip()
            if wert:
                return wert
    except OSError:
        pass
    return POD_ID


def _volume_id_von(pod: dict) -> str | None:
    netz = (pod.get("mounts") or {}).get("network") or []
    return netz[0].get("volumeId") if netz else None


async def pod_id_aufloesen(client: httpx.AsyncClient, erzwingen: bool = False) -> str:
    """Ermittelt die aktuelle Pod-ID.

    RunPod vergibt bei einer Pod-Migration eine neue ID, die alte wird ungueltig.
    Deshalb wird die Referenz (ID, Name oder Volume-ID) gegen die Pod-Liste
    aufgeloest und das Ergebnis gemerkt.
    """
    global _pod_id_cache
    if _pod_id_cache and not erzwingen:
        return _pod_id_cache

    ref = _pod_referenz()
    if not ref and not VOLUME_ID:
        raise RuntimeError("Weder Pod-Referenz noch Volume-ID konfiguriert")

    # 1. Direkter Treffer ueber die ID
    if ref:
        r = await client.get(f"{API}/pods/{ref}", headers=_headers(), timeout=30)
        if r.status_code == 200:
            _pod_id_cache = ref
            return ref

    # 2. Ueber die Pod-Liste nach Name oder Netzwerk-Volume suchen
    r = await client.get(f"{API}/pods", headers=_headers(), timeout=30)
    r.raise_for_status()
    pods = r.json().get("pods", [])

    def passt(pod: dict) -> bool:
        vol = _volume_id_von(pod)
        if ref and (pod.get("name") == ref or vol == ref):
            return True
        return bool(VOLUME_ID) and vol == VOLUME_ID

    treffer = [p for p in pods if passt(p)]
    # Laufende zuerst, danach der zuletzt gestartete
    treffer.sort(key=lambda p: (p.get("status") != "RUNNING",
                                _negiert(p.get("startedAt") or p.get("createdAt") or "")))
    if not treffer:
        raise RuntimeError(f"Kein Pod gefunden fuer Referenz '{ref}' oder Volume '{VOLUME_ID}'")

    neu = treffer[0]["id"]
    if neu != _pod_id_cache:
        log.info("Pod aufgeloest: %s (Name: %s, Status: %s)",
                 neu, treffer[0].get("name"), treffer[0].get("status"))
    _pod_id_cache = neu
    return neu


async def pod_info(client: httpx.AsyncClient) -> dict:
    pid = await pod_id_aufloesen(client)
    r = await client.get(f"{API}/pods/{pid}", headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


async def pod_aktion(client: httpx.AsyncClient, aktion: str,
                      versuche: int = 12, pause: float = 20.0) -> None:
    """Loest eine Statusaenderung aus (start/stop/restart). RunPod antwortet
    sporadisch mit einem Serverfehler, wenn auf der Host-Maschine gerade
    keine GPU frei ist. Das ist meist voruebergehend, deshalb wird mehrfach
    versucht."""
    letzter = ""
    for i in range(versuche):
        pid = await pod_id_aufloesen(client)
        r = await client.post(f"{API}/pods/{pid}/action", headers=_headers(),
                              json={"action": aktion}, timeout=60)
        if r.status_code < 400:
            return
        letzter = f"{r.status_code} {r.text[:200]}"
        log.warning("Pod-Aktion '%s' Versuch %d/%d fehlgeschlagen: %s",
                    aktion, i + 1, versuche, letzter)
        if aktion == "start":
            # Falls ein paralleler Versuch bereits Erfolg hatte
            info = await pod_info(client)
            if info.get("status") == "RUNNING":
                return
        await asyncio.sleep(pause)
    raise RuntimeError(f"Pod-Aktion '{aktion}' nach {versuche} Versuchen fehlgeschlagen: {letzter}")


def _pod_zusatz_env() -> dict:
    """Liest zusaetzliche Pod-Variablen aus einer KEY=VALUE-Datei."""
    werte: dict[str, str] = {}
    try:
        with open(POD_ENV_DATEI) as f:
            for zeile in f:
                zeile = zeile.strip()
                if not zeile or zeile.startswith("#") or "=" not in zeile:
                    continue
                schluessel, _, wert = zeile.partition("=")
                werte[schluessel.strip()] = wert.strip().strip('"').strip("'")
    except OSError:
        pass
    return werte


def _pod_spec() -> dict | None:
    """Liest die Pod-Spezifikation. Fehlt sie, bleibt es beim Start/Stop-Betrieb."""
    try:
        with open(POD_SPEC_DATEI) as f:
            spec = json.load(f)
    except (OSError, ValueError):
        return None
    zusatz = _pod_zusatz_env()
    if zusatz:
        spec.setdefault("env", {}).update(zusatz)
    return spec


async def pod_erstellen(client: httpx.AsyncClient, spec: dict) -> str:
    """Erstellt einen neuen Pod. Die v2-API platziert pro Aufruf genau EINEN
    GPU-Typ und sucht anders als v1 nicht selbststaendig ueber mehrere
    Kandidaten. Deshalb wird 'gpuTypeIds' (unsere eigene, nicht von der API
    verstandene Erweiterung der Spezifikation) hier selbst der Reihe nach
    durchprobiert. Fehlerbehandlung gemaess RunPod-Doku:
    422 = Anfrage fehlerhaft (nie wiederholen), 402 = Guthaben leer (abbrechen),
    400/403 = dieser Kandidat scheidet aus (naechster), 429 = Rate-Limit
    (warten, gleicher Kandidat), 5xx = voruebergehend (kurz warten, gleicher
    Kandidat)."""
    spec = dict(spec)
    kandidaten = spec.pop("gpuTypeIds", None)
    if not kandidaten:
        raise RuntimeError("Keine GPU-Kandidaten in der Pod-Spezifikation (gpuTypeIds fehlt)")
    anzahl = spec.pop("gpuCount", 1)

    letzter = ""
    for gpu_id in kandidaten:
        body = {**spec, "gpu": {"id": gpu_id, "count": anzahl}}
        for _ in range(3):
            r = await client.post(f"{API}/pods", headers=_headers(), json=body, timeout=120)
            if r.status_code == 201:
                daten = r.json()
                pid = daten["id"]
                log.info("Neuer Pod erstellt: %s (GPU: %s, %s $/h)",
                         pid, gpu_id, daten.get("cost"))
                return pid
            letzter = f"{r.status_code} {r.text[:300]}"
            if r.status_code == 422:
                raise RuntimeError(f"Pod-Spezifikation ungueltig (422), Abbruch: {letzter}")
            if r.status_code == 402:
                raise RuntimeError(f"Guthaben nicht ausreichend (402), Abbruch: {letzter}")
            if r.status_code == 429:
                pause = float(r.headers.get("Retry-After", "5"))
                log.warning("Rate-Limit bei GPU %s, warte %ss", gpu_id, pause)
                await asyncio.sleep(pause)
                continue
            if r.status_code >= 500:
                log.warning("GPU %s: Serverfehler %s, erneuter Versuch", gpu_id, letzter)
                await asyncio.sleep(3)
                continue
            # 400 oder 403: keine Kapazitaet oder kein Zugriff, naechster Kandidat
            log.info("GPU %s nicht verfuegbar (%s)", gpu_id, letzter)
            break
    raise RuntimeError(
        f"Pod-Erstellung bei allen {len(kandidaten)} GPU-Kandidaten fehlgeschlagen: {letzter}")


async def pod_terminieren(client: httpx.AsyncClient, pid: str) -> None:
    r = await client.delete(f"{API}/pods/{pid}", headers=_headers(), timeout=60)
    log.info("Pod %s terminiert (HTTP %s)", pid, r.status_code)


def _tunnel_alive() -> bool:
    return _tunnel is not None and _tunnel.poll() is None


def _kill_tunnel() -> None:
    global _tunnel
    if _tunnel is not None:
        try:
            _tunnel.terminate()
            _tunnel.wait(timeout=10)
        except Exception:
            try:
                _tunnel.kill()
            except Exception:
                pass
        _tunnel = None


def _open_tunnel(user: str, ip: str, port: int) -> None:
    global _tunnel
    _kill_tunnel()
    cmd = [
        "ssh", "-N",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-i", SSH_KEY,
        "-p", str(port),
        "-L", f"127.0.0.1:{TUNNEL_PORT}:localhost:{REMOTE_PORT}",
        f"{user}@{ip}",
    ]
    log.info("Oeffne SSH-Tunnel nach %s@%s:%s", user, ip, port)
    _tunnel = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ssh_run(user: str, ip: str, port: int, command: str, timeout: float = 60.0) -> int:
    """Fuehrt einen Befehl per SSH aus. BatchMode verhindert, dass ssh auf eine
    Passworteingabe wartet, wenn die Schluesselanmeldung scheitert."""
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-i", SSH_KEY,
        "-p", str(port),
        f"{user}@{ip}",
        command,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
        if r.returncode != 0:
            log.warning("SSH-Befehl fehlgeschlagen (rc=%s): %s",
                        r.returncode, r.stderr.decode(errors="replace")[:200].strip())
        return r.returncode
    except subprocess.TimeoutExpired:
        log.warning("SSH-Befehl lief in einen Timeout nach %ss", timeout)
        return 255


async def _service_up(client: httpx.AsyncClient) -> bool:
    try:
        r = await client.get(f"http://127.0.0.1:{TUNNEL_PORT}/docs", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


async def _aufraeumen_nach_fehlschlag() -> None:
    """Raeumt einen selbst erstellten, aber unbrauchbaren Pod weg, sonst
    laeuft er als Waise weiter und kostet Geld."""
    if not _pod_spec():
        return
    pid = _pod_id_cache
    if not pid:
        return
    try:
        async with httpx.AsyncClient() as client:
            log.warning("Aufwecken fehlgeschlagen, terminiere Pod %s", pid)
            await pod_terminieren(client, pid)
    except Exception as exc:
        log.warning("Aufraeumen fehlgeschlagen: %s", exc)
    finally:
        globals()["_pod_id_cache"] = None
        _kill_tunnel()


async def ensure_ready() -> None:
    global _pod_id_cache
    if not (_pod_referenz() or VOLUME_ID) or not API_KEY:
        raise RuntimeError("Pod-Referenz oder RUNPOD_API_KEY ist nicht gesetzt")

    deadline = time.time() + BOOT_TIMEOUT
    async with httpx.AsyncClient() as client:
        spec = _pod_spec()
        info = None
        try:
            info = await pod_info(client)
        except Exception as exc:
            if not spec:
                raise
            log.info("Kein bestehender Pod gefunden (%s), erstelle einen neuen", exc)

        if info is not None and info.get("status") != "RUNNING":
            if spec:
                # Start versuchen; scheitert er (Host belegt, Aktion nicht
                # erlaubt), den Pod wegwerfen und einen neuen auf freier
                # Hardware erstellen.
                try:
                    if "start" not in (info.get("actions") or []):
                        raise RuntimeError(f"Aktion 'start' im Status {info.get('status')} nicht erlaubt")
                    log.info("Pod steht (%s), versuche Start", info.get("status"))
                    await pod_aktion(client, "start", versuche=2, pause=10.0)
                except Exception as exc:
                    log.warning("Start nicht moeglich (%s), erstelle stattdessen einen neuen Pod", exc)
                    try:
                        await pod_terminieren(client, info["id"])
                    except Exception as e2:
                        log.warning("Alten Pod konnte nicht terminiert werden: %s", e2)
                    _pod_id_cache = await pod_erstellen(client, spec)
                    info = None
            else:
                log.info("Pod steht (%s), starte ihn", info.get("status"))
                await pod_aktion(client, "start")
            _kill_tunnel()
        elif info is None and spec:
            _pod_id_cache = await pod_erstellen(client, spec)
            _kill_tunnel()

        # Auf RUNNING und die direkte SSH-Verbindung warten. Anders als v1
        # liefert v2 Host und Port fertig aufgeloest unter ssh.direct, das
        # fehleranfaellige Raten des TCP-Ports (v1 lieferte dort oft den
        # UDP-Port) entfaellt damit.
        ssh = None
        while time.time() < deadline:
            info = await pod_info(client)
            ssh = (info.get("ssh") or {}).get("direct")
            if info.get("status") == "RUNNING" and ssh:
                break
            await asyncio.sleep(5)
        if not ssh:
            raise RuntimeError("Pod hat nach dem Start keine direkte SSH-Verbindung (ssh.direct) geliefert")
        ip = ssh["host"]
        ssh_port = int(ssh["port"])
        ssh_user = ssh.get("username") or "root"
        log.info("Pod bereit: ssh=%s@%s:%s", ssh_user, ip, ssh_port)

        # Auf SSH-Anmeldung warten und Tunnel aufbauen
        if not _tunnel_alive():
            while time.time() < deadline:
                if _ssh_run(ssh_user, ip, ssh_port, "true") == 0:
                    break
                await asyncio.sleep(5)
            _open_tunnel(ssh_user, ip, ssh_port)
            await asyncio.sleep(3)

        # ASR-Dienst pruefen und noetigenfalls starten
        if not await _service_up(client):
            log.info("ASR-Dienst laeuft nicht, starte ihn auf dem Pod")
            _ssh_run(ssh_user, ip, ssh_port, f"nohup {START_CMD} > /workspace/asr.log 2>&1 &")
            while time.time() < deadline:
                if not _tunnel_alive():
                    _open_tunnel(ssh_user, ip, ssh_port)
                    await asyncio.sleep(3)
                if await _service_up(client):
                    break
                await asyncio.sleep(5)

        if not await _service_up(client):
            raise RuntimeError("ASR-Dienst wurde nicht rechtzeitig bereit")
        log.info("ASR-Dienst ist bereit")


async def idle_watcher() -> None:
    while True:
        await asyncio.sleep(60)
        if _inflight > 0:
            continue
        if time.time() - _last_used < IDLE_SECONDS:
            continue
        if not API_KEY:
            continue
        try:
            async with httpx.AsyncClient() as client:
                info = await pod_info(client)
                if info.get("status") == "RUNNING":
                    _kill_tunnel()
                    if _pod_spec():
                        log.info("Leerlauf erreicht, terminiere Pod")
                        await pod_terminieren(client, info["id"])
                        globals()["_pod_id_cache"] = None
                    else:
                        log.info("Leerlauf erreicht, stoppe Pod")
                        await pod_aktion(client, "stop", versuche=1)
        except Exception as exc:
            log.warning("Leerlauf-Pruefung fehlgeschlagen: %s", exc)


@app.on_event("startup")
async def _startup() -> None:
    if not (_pod_referenz() or VOLUME_ID) or not API_KEY:
        log.error("Pod-Referenz oder RUNPOD_API_KEY fehlt. Der Proxy laeuft, "
                  "kann den Pod aber nicht steuern.")
    asyncio.create_task(idle_watcher())


@app.get("/healthz")
async def healthz() -> dict:
    out = {
        "ok": True,
        "pod": _pod_id_cache or _pod_referenz() or None,
        "modus": "erstellen/terminieren" if _pod_spec() else "start/stop",
        "pod_zusatz_variablen": sorted(_pod_zusatz_env().keys()),
        "tunnel": _tunnel_alive(),
        "inflight": _inflight,
        "sekunden_seit_letzter_nutzung": round(time.time() - _last_used),
    }
    if not (_pod_referenz() or VOLUME_ID) or not API_KEY:
        out["ok"] = False
        out["fehler"] = "Pod-Referenz oder RUNPOD_API_KEY nicht gesetzt"
        return out
    try:
        async with httpx.AsyncClient() as client:
            try:
                info = await pod_info(client)
                out["api"] = "erreichbar"
                out["pod_status"] = info.get("status")
                out["gpu"] = (info.get("gpu") or {}).get("id")
            except Exception as exc:
                if _pod_spec() and "Kein Pod gefunden" in str(exc):
                    # Im Erstellen-Modus ist "kein Pod vorhanden" der Normalzustand
                    # im Leerlauf und kein Fehler.
                    await client.get(f"{API}/pods", headers=_headers(), timeout=30)
                    out["api"] = "erreichbar"
                    out["pod_status"] = "kein Pod (Leerlauf)"
                else:
                    raise
    except Exception as exc:
        out["ok"] = False
        out["api"] = "Fehler"
        out["fehler"] = str(exc)[:200]
    return out


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def forward(path: str, request: Request) -> Response:
    global _last_used, _inflight
    # Sofort als belegt markieren, sonst stoppt der Leerlauf-Waechter den Pod
    # mitten im Aufwecken.
    _inflight += 1
    _last_used = time.time()
    try:
        try:
            await asyncio.wait_for(_lock.acquire(), timeout=BOOT_TIMEOUT + 120)
        except asyncio.TimeoutError:
            return Response(content=b"Proxy ist belegt: eine andere Anfrage haelt den Aufweck-Vorgang.",
                            status_code=503)
        try:
            try:
                await ensure_ready()
            except Exception:
                await _aufraeumen_nach_fehlschlag()
                raise
        finally:
            _lock.release()
    except Exception:
        _inflight -= 1
        _last_used = time.time()
        raise

    try:
        body = await request.body()
        skip = {"host", "content-length", "connection"}
        headers = {k: v for k, v in request.headers.items() if k.lower() not in skip}
        async with httpx.AsyncClient(timeout=httpx.Timeout(3600.0)) as client:
            resp = await client.request(
                request.method,
                f"http://127.0.0.1:{TUNNEL_PORT}/{path}",
                params=dict(request.query_params),
                content=body,
                headers=headers,
            )
        drop = {"content-length", "transfer-encoding", "content-encoding", "connection"}
        out = {k: v for k, v in resp.headers.items() if k.lower() not in drop}
        return Response(content=resp.content, status_code=resp.status_code, headers=out)
    finally:
        _inflight -= 1
        _last_used = time.time()
