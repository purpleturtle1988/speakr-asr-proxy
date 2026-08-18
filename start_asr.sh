#!/bin/bash
# Startet den Flix-ASR-Dienst vom Netzwerk-Volume.
# Alle ASR_-Variablen lassen sich ueber die Pod-Umgebung ueberschreiben,
# damit fuer eine Aenderung nicht das Volume angefasst werden muss.
export PATH=/workspace/bin:$PATH
command -v ffmpeg >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq ffmpeg)

# Eine SSH-Sitzung erbt die Container-Umgebung nicht. Die vom Pod gesetzten
# Variablen (u.a. HF_TOKEN) stehen aber in der Umgebung von PID 1.
if [ -r /proc/1/environ ]; then
  while IFS= read -r -d '' eintrag; do
    case "$eintrag" in
      HF_TOKEN=*|HF_HOME=*|ASR_*=*|MODEL_IDLE_TIMEOUT=*) export "$eintrag" ;;
    esac
  done < /proc/1/environ
fi

. /workspace/venv/bin/activate
cd /workspace/whisper-asr-webservice

# Modell-Cache auf das Volume legen. Sonst laedt WhisperX die pyannote-Modelle
# bei jedem Pod-Start neu, weil die Container Disk geloescht wird.
export HF_HOME=${HF_HOME:-/workspace/.cache/huggingface}

export ASR_ENGINE=${ASR_ENGINE:-whisperx}
export ASR_MODEL=${ASR_MODEL:-/workspace/flix-ct2}
export ASR_DEVICE=${ASR_DEVICE:-cuda}
export ASR_QUANTIZATION=${ASR_QUANTIZATION:-float16}
export MODEL_IDLE_TIMEOUT=${MODEL_IDLE_TIMEOUT:-0}

echo "ASR_ENGINE=$ASR_ENGINE  ASR_MODEL=$ASR_MODEL  HF_HOME=$HF_HOME"
echo "HF_TOKEN gesetzt: $([ -n "$HF_TOKEN" ] && echo ja || echo NEIN)"

exec uvicorn app.webservice:app --host 0.0.0.0 --port 9000
