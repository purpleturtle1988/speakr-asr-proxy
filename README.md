# speakr-asr-proxy

Aufwach-Proxy zwischen [Speakr](https://github.com/murtaza-nasir/speakr) und einem
ASR-Dienst auf einem RunPod-Pod.

Speakr spricht diesen Proxy als `ASR_BASE_URL` an. Der Proxy:

1. startet den Pod über die RunPod-API, falls er gestoppt ist,
2. wartet auf IP und SSH-Portmapping (die sich bei jedem Start ändern),
3. baut den SSH-Tunnel auf,
4. startet den ASR-Dienst auf dem Pod, falls er nicht läuft,
5. reicht die Anfrage durch und gibt die Antwort zurück,
6. stoppt den Pod nach einer konfigurierbaren Leerlaufzeit.

## Konfiguration

| Variable | Pflicht | Default | Bedeutung |
|---|---|---|---|
| `RUNPOD_POD_ID` | ja | | ID des Pods |
| `RUNPOD_API_KEY` | ja | | RunPod-API-Key mit Schreibrechten |
| `IDLE_MINUTES` | nein | `10` | Leerlauf bis zum Stoppen des Pods |
| `TUNNEL_PORT` | nein | `19000` | lokaler Port des SSH-Tunnels |
| `REMOTE_PORT` | nein | `9000` | Port des ASR-Dienstes auf dem Pod |
| `START_CMD` | nein | `bash /workspace/start_asr.sh` | Startbefehl auf dem Pod |
| `BOOT_TIMEOUT` | nein | `600` | Sekunden bis zum Abbruch beim Hochfahren |
| `SSH_KEY` | nein | `/keys/id_ed25519` | privater Schlüssel im Container |

## Status

`GET /healthz` zeigt Tunnelzustand, laufende Anfragen und Leerlaufzeit.

Alle anderen Pfade werden an den ASR-Dienst durchgereicht.
