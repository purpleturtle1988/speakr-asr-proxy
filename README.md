# speakr-asr-proxy

Aufwach-Proxy zwischen [Speakr](https://github.com/murtaza-nasir/speakr) und einem
ASR-Dienst auf einem RunPod-Pod.

Speakr spricht diesen Proxy als `ASR_BASE_URL` an. Der Proxy:

1. erstellt den Pod über die RunPod-API, falls er nicht existiert (oder startet ihn,
   im Start/Stop-Betrieb ohne `pod_spec.json`),
2. wartet auf `RUNNING` und die direkte SSH-Verbindung (`ssh.direct` aus der API),
3. baut den SSH-Tunnel auf,
4. startet den ASR-Dienst auf dem Pod, falls er nicht läuft,
5. reicht die Anfrage durch und gibt die Antwort zurück,
6. terminiert (bzw. stoppt im Start/Stop-Betrieb) den Pod nach einer konfigurierbaren Leerlaufzeit.

Nutzt die RunPod REST-API **v2** (`api.runpod.io/v2`). Die v1-API
(`rest.runpod.io/v1`) wird am **15.11.2026** abgeschaltet (410 Gone),
siehe [Migrationsguide](https://docs.runpod.io/api-reference-v2/migrate-from-v1).

## Konfiguration

| Variable | Pflicht | Default | Bedeutung |
|---|---|---|---|
| `RUNPOD_POD_ID` | ja* | | ID/Name des Pods (Fallback, falls `pod.txt` fehlt) |
| `RUNPOD_API_KEY` | ja | | RunPod-API-Key mit Schreibrechten |
| `IDLE_MINUTES` | nein | `10` | Leerlauf bis zum Stoppen/Terminieren des Pods |
| `TUNNEL_PORT` | nein | `19000` | lokaler Port des SSH-Tunnels |
| `REMOTE_PORT` | nein | `9000` | Port des ASR-Dienstes auf dem Pod |
| `START_CMD` | nein | `bash /workspace/start_asr.sh` | Startbefehl auf dem Pod |
| `BOOT_TIMEOUT` | nein | `1500` | Sekunden bis zum Abbruch beim Hochfahren |
| `SSH_KEY` | nein | `/keys/id_ed25519` | privater Schlüssel im Container |

\* Im Erstellen/Terminieren-Betrieb (`pod_spec.json` vorhanden) übernimmt
`pod.txt` meist die Referenz (z. B. die Network-Volume-ID), `RUNPOD_POD_ID`
ist dann nur der Fallback.

## Pod-Erstellung (`pod_spec.json`)

Liegt `/app/pod_spec.json` vor, erstellt der Proxy bei Bedarf einen neuen Pod
und terminiert ihn im Leerlauf, statt einen bestehenden zu starten/stoppen
(robuster gegen "not enough free GPUs on the host machine"). Das Feld
`gpuTypeIds` ist eine reine Proxy-Erweiterung: die RunPod-v2-API platziert
pro Aufruf nur einen GPU-Typ, der Proxy probiert die Liste selbst der Reihe
nach durch. Siehe `pod_spec.example.json`.

## Status

`GET /healthz` zeigt Tunnelzustand, laufende Anfragen und Leerlaufzeit.

Alle anderen Pfade werden an den ASR-Dienst durchgereicht.
