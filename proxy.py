"""
Aufwach-Proxy fuer den Speakr-ASR-Pod auf RunPod.

Speakr spricht diesen Proxy an. Der Proxy startet bei Bedarf den Pod,
baut den SSH-Tunnel auf, startet den ASR-Dienst, reicht die Anfrage
durch und stoppt den Pod nach einer Leerlaufzeit wieder.
"""
import asyncio
import logging
import os
import socket
import subprocess
import time

import httpx
from fastapi import FastAPI, Request, Response

API = "https://rest.runpod.io/v1"
POD_ID = os.environ.get("RUNPOD_POD_ID", "")
# Datei hat Vorrang vor der Umgebungsvariable, damit sich der Pod wechseln laesst,
# ohne den Stack (und damit den API-Key) anzufassen.
POD_REF_DATEI = os.environ.get("POD_REF_DATEI", "/app/pod.txt")
VOLUME_ID = os.environ.get("RUNPOD_VOLUME_ID", "")
API_KEY = os.environ.get("RUNPOD_API_KEY", "")
IDLE_SECONDS = float(os.environ.get("IDLE_MINUTES", "10")) * 60
SSH_KEY = os.environ.get("SSH_KEY", "/keys/id_ed25519")
TUNNEL_PORT = int(os.environ.get("TUNNEL_PORT", "19000"))
REMOTE_PORT = int(os.environ.get("REMOTE_PORT", "9000"))
START_CMD = os.environ.get("START_CMD", "bash /workspace/start_asr.sh")
BOOT_TIMEOUT = float(os.environ.get("BOOT_TIMEOUT", "600"))

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

    # 2. Ueber die Pod-Liste nach Name oder Volume suchen
    r = await client.get(f"{API}/pods", headers=_headers(),
                         params={"includeNetworkVolume": "true"}, timeout=30)
    r.raise_for_status()
    pods = r.json()
    if isinstance(pods, dict):
        pods = pods.get("data") or pods.get("pods") or []

    def passt(pod: dict) -> bool:
        if ref and pod.get("name") == ref:
            return True
        vol = (pod.get("networkVolume") or {}).get("id")
        return bool(VOLUME_ID) and vol == VOLUME_ID

    treffer = [p for p in pods if passt(p)]
    # Laufende bevorzugen, danach die zuletzt gestarteten
    treffer.sort(key=lambda p: (p.get("desiredStatus") != "RUNNING",
                                p.get("lastStartedAt") or ""), reverse=False)
    if not treffer:
        raise RuntimeError(f"Kein Pod gefunden fuer Referenz '{ref}' oder Volume '{VOLUME_ID}'")

    neu = treffer[0]["id"]
    if neu != _pod_id_cache:
        log.info("Pod aufgeloest: %s (Name: %s, Status: %s)",
                 neu, treffer[0].get("name"), treffer[0].get("desiredStatus"))
    _pod_id_cache = neu
    return neu


async def pod_info(client: httpx.AsyncClient) -> dict:
    pid = await pod_id_aufloesen(client)
    r = await client.get(f"{API}/pods/{pid}", headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


async def pod_start(client: httpx.AsyncClient, versuche: int = 12, pause: float = 20.0) -> None:
    """Startet den Pod. RunPod antwortet sporadisch mit 500, wenn auf der
    Host-Maschine gerade keine GPU frei ist. Das ist meist voruebergehend,
    deshalb wird mehrfach versucht."""
    letzter = ""
    for i in range(versuche):
        pid = await pod_id_aufloesen(client)
        r = await client.post(f"{API}/pods/{pid}/start", headers=_headers(), timeout=60)
        if r.status_code < 400:
            return
        letzter = f"{r.status_code} {r.text[:200]}"
        log.warning("Pod-Start Versuch %d/%d fehlgeschlagen: %s", i + 1, versuche, letzter)
        # Falls ein paralleler Versuch bereits Erfolg hatte
        info = await pod_info(client)
        if info.get("desiredStatus") == "RUNNING":
            return
        await asyncio.sleep(pause)
    raise RuntimeError(f"Pod-Start nach {versuche} Versuchen fehlgeschlagen: {letzter}")


async def pod_stop(client: httpx.AsyncClient) -> None:
    pid = await pod_id_aufloesen(client)
    r = await client.post(f"{API}/pods/{pid}/stop", headers=_headers(), timeout=60)
    log.info("Pod gestoppt (HTTP %s)", r.status_code)


def _tcp_offen(ip: str, port: int, timeout: float = 3.0) -> bool:
    """Prueft, ob auf ip:port eine TCP-Verbindung angenommen wird."""
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except OSError:
        return False


def _ssh_port_finden(ip: str, gemeldet: int) -> int | None:
    """RunPods REST-API liefert unter portMappings["22"] den UDP-Port, waehrend
    SSH auf einem anderen TCP-Port lauscht (beobachtet: TCP = UDP - 1). Da das
    nirgends dokumentiert ist, werden die Kandidaten durchprobiert und der
    genommen, der tatsaechlich eine TCP-Verbindung annimmt."""
    for kandidat in (gemeldet - 1, gemeldet, gemeldet + 1):
        if kandidat > 0 and _tcp_offen(ip, kandidat):
            if kandidat != gemeldet:
                log.info("SSH-Port %s statt gemeldetem %s (API liefert den UDP-Port)",
                         kandidat, gemeldet)
            return kandidat
    return None


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


def _open_tunnel(ip: str, port: int) -> None:
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
        f"root@{ip}",
    ]
    log.info("Oeffne SSH-Tunnel nach %s:%s", ip, port)
    _tunnel = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ssh_run(ip: str, port: int, command: str, timeout: float = 60.0) -> int:
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
        f"root@{ip}",
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


async def ensure_ready() -> None:
    if not (_pod_referenz() or VOLUME_ID) or not API_KEY:
        raise RuntimeError("Pod-Referenz oder RUNPOD_API_KEY ist nicht gesetzt")

    deadline = time.time() + BOOT_TIMEOUT
    async with httpx.AsyncClient() as client:
        info = await pod_info(client)
        if info.get("desiredStatus") != "RUNNING":
            log.info("Pod steht (%s), starte ihn", info.get("desiredStatus"))
            await pod_start(client)
            _kill_tunnel()

        # Auf IP und Portmapping warten
        ip, ssh_port = None, None
        while time.time() < deadline:
            info = await pod_info(client)
            ip = info.get("publicIp")
            ssh_port = (info.get("portMappings") or {}).get("22")
            if info.get("desiredStatus") == "RUNNING" and ip and ssh_port:
                break
            await asyncio.sleep(5)
        if not (ip and ssh_port):
            raise RuntimeError("Pod hat nach dem Start keine IP oder kein SSH-Portmapping geliefert")
        log.info("Pod bereit: ip=%s ssh-port=%s portMappings=%s ports=%s",
                 ip, ssh_port, info.get("portMappings"), info.get("ports"))

        # TCP-Port fuer SSH ermitteln und auf Erreichbarkeit warten
        tcp_port = None
        while time.time() < deadline:
            tcp_port = _ssh_port_finden(ip, int(ssh_port))
            if tcp_port is not None:
                break
            await asyncio.sleep(5)
        if tcp_port is None:
            raise RuntimeError(
                f"Auf {ip} nimmt weder Port {int(ssh_port) - 1}, {ssh_port} noch "
                f"{int(ssh_port) + 1} eine TCP-Verbindung an")
        ssh_port = tcp_port

        # Tunnel aufbauen und auf SSH-Anmeldung warten
        if not _tunnel_alive():
            while time.time() < deadline:
                if _ssh_run(ip, int(ssh_port), "true") == 0:
                    break
                await asyncio.sleep(5)
            _open_tunnel(ip, int(ssh_port))
            await asyncio.sleep(3)

        # ASR-Dienst pruefen und noetigenfalls starten
        if not await _service_up(client):
            log.info("ASR-Dienst laeuft nicht, starte ihn auf dem Pod")
            _ssh_run(ip, int(ssh_port), f"nohup {START_CMD} > /workspace/asr.log 2>&1 &")
            while time.time() < deadline:
                if not _tunnel_alive():
                    _open_tunnel(ip, int(ssh_port))
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
                if info.get("desiredStatus") == "RUNNING":
                    log.info("Leerlauf erreicht, stoppe Pod")
                    _kill_tunnel()
                    await pod_stop(client)
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
            info = await pod_info(client)
        out["api"] = "erreichbar"
        out["pod_status"] = info.get("desiredStatus")
        out["gpu"] = (info.get("gpu") or {}).get("id")
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
            await ensure_ready()
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
