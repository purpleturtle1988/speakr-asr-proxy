"""
Aufwach-Proxy fuer den Speakr-ASR-Pod auf RunPod.

Speakr spricht diesen Proxy an. Der Proxy startet bei Bedarf den Pod,
baut den SSH-Tunnel auf, startet den ASR-Dienst, reicht die Anfrage
durch und stoppt den Pod nach einer Leerlaufzeit wieder.
"""
import asyncio
import logging
import os
import subprocess
import time

import httpx
from fastapi import FastAPI, Request, Response

API = "https://rest.runpod.io/v1"
POD_ID = os.environ.get("RUNPOD_POD_ID", "")
API_KEY = os.environ.get("RUNPOD_API_KEY", "")
IDLE_SECONDS = float(os.environ.get("IDLE_MINUTES", "10")) * 60
SSH_KEY = os.environ.get("SSH_KEY", "/keys/id_ed25519")
TUNNEL_PORT = int(os.environ.get("TUNNEL_PORT", "19000"))
REMOTE_PORT = int(os.environ.get("REMOTE_PORT", "9000"))
START_CMD = os.environ.get("START_CMD", "bash /workspace/start_asr.sh")
BOOT_TIMEOUT = float(os.environ.get("BOOT_TIMEOUT", "600"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("asr-proxy")

app = FastAPI()
_lock = asyncio.Lock()
_tunnel: subprocess.Popen | None = None
_last_used = time.time()
_inflight = 0


def _headers():
    return {"Authorization": f"Bearer {API_KEY}"}


async def pod_info(client: httpx.AsyncClient) -> dict:
    r = await client.get(f"{API}/pods/{POD_ID}", headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


async def pod_start(client: httpx.AsyncClient) -> None:
    r = await client.post(f"{API}/pods/{POD_ID}/start", headers=_headers(), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Pod-Start fehlgeschlagen: {r.status_code} {r.text[:300]}")


async def pod_stop(client: httpx.AsyncClient) -> None:
    r = await client.post(f"{API}/pods/{POD_ID}/stop", headers=_headers(), timeout=60)
    log.info("Pod gestoppt (HTTP %s)", r.status_code)


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
        "-i", SSH_KEY,
        "-p", str(port),
        "-L", f"127.0.0.1:{TUNNEL_PORT}:localhost:{REMOTE_PORT}",
        f"root@{ip}",
    ]
    log.info("Oeffne SSH-Tunnel nach %s:%s", ip, port)
    _tunnel = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ssh_run(ip: str, port: int, command: str) -> int:
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-i", SSH_KEY,
        "-p", str(port),
        f"root@{ip}",
        command,
    ]
    return subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def _service_up(client: httpx.AsyncClient) -> bool:
    try:
        r = await client.get(f"http://127.0.0.1:{TUNNEL_PORT}/docs", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


async def ensure_ready() -> None:
    if not POD_ID or not API_KEY:
        raise RuntimeError("RUNPOD_POD_ID oder RUNPOD_API_KEY ist nicht gesetzt")

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

        # Tunnel aufbauen und auf SSH warten
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
        if not (POD_ID and API_KEY):
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
    if not POD_ID or not API_KEY:
        log.error("RUNPOD_POD_ID oder RUNPOD_API_KEY fehlt. Der Proxy laeuft, "
                  "kann den Pod aber nicht steuern.")
    asyncio.create_task(idle_watcher())


@app.get("/healthz")
async def healthz() -> dict:
    out = {
        "ok": True,
        "pod": POD_ID or None,
        "tunnel": _tunnel_alive(),
        "inflight": _inflight,
        "sekunden_seit_letzter_nutzung": round(time.time() - _last_used),
    }
    if not POD_ID or not API_KEY:
        out["ok"] = False
        out["fehler"] = "RUNPOD_POD_ID oder RUNPOD_API_KEY nicht gesetzt"
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
    async with _lock:
        await ensure_ready()

    _inflight += 1
    _last_used = time.time()
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
