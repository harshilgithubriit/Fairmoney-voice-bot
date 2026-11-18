"""
Centralized Configuration Loader for FairMoney Voicebot.
Loads settings from appsetting.json (or appsettings.json) with fallback to .env and environment variables.
Resolves all relative paths against BASE_DIR to eliminate path issues across different working directories.
"""

from pathlib import Path
import json
import logging
import os
import sys
from dotenv import load_dotenv

# Project root directory (fairmoney_voicebotmain)
BASE_DIR = Path(__file__).resolve().parent.parent

# Ensure project root is at front of sys.path for clean imports
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Optional fallback .env loader
load_dotenv(BASE_DIR / ".env")


def _find_settings_file() -> Path | None:
    env_path = os.environ.get("APPSETTINGS_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p.resolve()

    candidates = [
        BASE_DIR / "appsetting.json",
        BASE_DIR / "appsettings.json",
        BASE_DIR.parent / "appsetting.json",
        BASE_DIR.parent / "appsettings.json",
        Path.cwd() / "appsetting.json",
        Path.cwd() / "appsettings.json",
    ]
    existing = []
    for c in candidates:
        if c:
            p = Path(c).resolve()
            if p.is_file() and p not in existing:
                existing.append(p)
    if not existing:
        return None
    # Pick the most recently modified file so user edits in root or subfolder are always prioritized
    return max(existing, key=lambda f: f.stat().st_mtime)


SETTINGS_FILE = _find_settings_file()
SETTINGS: dict = {}

if SETTINGS_FILE and SETTINGS_FILE.is_file():
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            SETTINGS = json.load(f)
    except Exception as e:
        print(f"[config.settings] WARNING: Failed to parse {SETTINGS_FILE}: {e}")
        SETTINGS = {}
else:
    print("[config.settings] WARNING: appsetting.json not found, using environment defaults.")


def _to_bool(val, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _get(keys: tuple[str, ...], env_var: str | None = None, default=None):
    """Retrieve setting from nested JSON dict, falling back to env var, then default."""
    curr = SETTINGS
    for k in keys:
        if isinstance(curr, dict) and k in curr:
            curr = curr[k]
        else:
            curr = None
            break

    if curr is not None and curr != "":
        return curr

    if env_var:
        env_val = os.environ.get(env_var)
        if env_val is not None and env_val != "":
            return env_val

    return default


def _resolve_path(raw_path: str, default: str) -> str:
    path_val = raw_path or default
    p = Path(path_val)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    return str(p)


# -----------------------------------------------------------------------------
# 1. Server Configuration
# -----------------------------------------------------------------------------
FASTAGI_HOST: str = str(_get(("Server", "FastAgiHost"), "FAIRMONEY_FASTAGI_HOST", "0.0.0.0"))
FASTAGI_PORT: int = int(_get(("Server", "FastAgiPort"), "FAIRMONEY_FASTAGI_PORT", 4573))

AUDIOSOCKET_HOST: str = str(_get(("Server", "AudioSocketHost"), "FAIRMONEY_AUDIOSOCKET_HOST", "0.0.0.0"))
AUDIOSOCKET_PORT: int = int(_get(("Server", "AudioSocketPort"), "FAIRMONEY_AUDIOSOCKET_PORT", 9092))
AUDIOSOCKET_ADVERTISED_HOST: str = str(_get(("Server", "AudioSocketAdvertisedHost"), "FAIRMONEY_AUDIOSOCKET_ADVERTISED_HOST", "192.168.200.148"))

MAX_CONCURRENT_CALLS: int = int(_get(("Server", "MaxConcurrentCalls"), "FAIRMONEY_MAX_CONCURRENT_CALLS", 100))

# -----------------------------------------------------------------------------
# 2. ApiEndpoints Configuration
# -----------------------------------------------------------------------------
DISPOSITION_API_URL: str = str(_get(("ApiEndpoints", "DispositionApiUrl"), "DISPOSITION_API_URL", "http://localhost:9002/api/api/CallBOTInput"))
PAYMENT_LINK_BASE_URL: str = str(_get(("ApiEndpoints", "PaymentLinkBaseUrl"), "PAYMENT_LINK_BASE_URL", "https://pay.fairmoney.ng/mock/"))
CRM_TIMEOUT_SECS: float = float(_get(("ApiEndpoints", "CrmTimeoutSecs"), "FAIRMONEY_CRM_TIMEOUT_SECS", 3.0))
ENABLE_CRM_PUSH: bool = _to_bool(_get(("ApiEndpoints", "EnableCrmPush"), "FAIRMONEY_ENABLE_CRM_PUSH", True), default=True)

# -----------------------------------------------------------------------------
# 3. AiServices Configuration
# -----------------------------------------------------------------------------
DEEPGRAM_API_KEY: str = str(_get(("AiServices", "Deepgram", "ApiKey"), "DEEPGRAM_API_KEY", "") or "")

GROQ_API_KEY: str = str(_get(("AiServices", "Groq", "ApiKey"), "GROQ_API_KEY", "") or "")
GROQ_LLM_MODEL: str = str(_get(("AiServices", "Groq", "Model"), "FAIRMONEY_GROQ_LLM_MODEL", "openai/gpt-oss-120b"))
GROQ_TEMPERATURE: float = float(_get(("AiServices", "Groq", "Temperature"), "FAIRMONEY_GROQ_TEMPERATURE", 0.2))
GROQ_MAX_TOKENS: int = int(_get(("AiServices", "Groq", "MaxTokens"), "FAIRMONEY_GROQ_MAX_TOKENS", 250))

TTS_ENGINE: str = str(_get(("AiServices", "TtsEngine"), "FAIRMONEY_TTS_ENGINE", "azure")).strip().lower()

AZURE_SPEECH_KEY: str = str(_get(("AiServices", "Azure", "SpeechKey"), "AZURE_SPEECH_KEY", "") or "")
AZURE_SPEECH_REGION: str = str(_get(("AiServices", "Azure", "SpeechRegion"), "AZURE_SPEECH_REGION", "centralindia"))
AZURE_VOICE_NAME: str = str(_get(("AiServices", "Azure", "VoiceName"), "AZURE_VOICE_NAME", "en-NG-EzinneNeural"))

CARTESIA_API_KEY: str = str(_get(("AiServices", "Cartesia", "ApiKey"), "CARTESIA_API_KEY", "") or "")

# -----------------------------------------------------------------------------
# 4. BusinessRules Configuration
# -----------------------------------------------------------------------------
MAX_CALL_DURATION_SECS: float = float(_get(("BusinessRules", "MaxCallDurationSecs"), "FAIRMONEY_MAX_CALL_DURATION_SECS", 300.0))
MAX_PAYMENT_DATE_DAYS: int = int(_get(("BusinessRules", "MaxPaymentDateDays"), "FAIRMONEY_MAX_PAYMENT_DATE_DAYS", 4))
DAYS_WINDOW_MODE: str = str(_get(("BusinessRules", "DaysWindowMode"), "FAIRMONEY_DAYS_WINDOW_MODE", "calendar"))
TRANSFER_QUEUE: str = str(_get(("BusinessRules", "TransferQueue"), "FAIRMONEY_TRANSFER_QUEUE", "agent"))
BUSY_PLAYBACK_AUDIO: str = str(_get(("BusinessRules", "BusyPlaybackAudio"), "FAIRMONEY_BUSY_PLAYBACK", "/usr/TECKINFO/BUSY"))
POST_WAIT_SECS: int = int(_get(("BusinessRules", "PostWaitSecs"), "FAIRMONEY_POST_WAIT_SECS", 2))
USE_MOCK_LOAN_DATA: bool = _to_bool(_get(("BusinessRules", "UseMockLoanData"), "FAIRMONEY_USE_MOCK_LOAN_DATA", False), default=False)

# -----------------------------------------------------------------------------
# 5. Audio Configuration
# -----------------------------------------------------------------------------
SAMPLE_RATE: int = int(_get(("Audio", "SampleRate"), "FAIRMONEY_SAMPLE_RATE", 8000))
AUDIO_SOURCE: str = str(_get(("Audio", "AudioSource"), "FAIRMONEY_AUDIO_SOURCE", "socket")).strip().lower()
AUDIO_QUEUE_MAX: int = int(_get(("Audio", "AudioQueueMax"), "FAIRMONEY_AUDIO_QUEUE_MAX", 2840))
GRACEFUL_SHUTDOWN_SECS: float = float(_get(("Audio", "GracefulShutdownSecs"), "FAIRMONEY_GRACEFUL_SHUTDOWN_SECS", 2.0))

# -----------------------------------------------------------------------------
# 6. Storage Configuration
# -----------------------------------------------------------------------------
DB_PATH: str = _resolve_path(str(_get(("Storage", "DatabasePath"), "FAIRMONEY_DB_PATH", "fairmoney_calls.db")), "fairmoney_calls.db")
DB_TIMEOUT_SECS: float = float(_get(("Storage", "DatabaseTimeoutSecs"), "FAIRMONEY_DB_TIMEOUT_SECS", 30.0))
LOGS_DIR: str = _resolve_path(str(_get(("Storage", "LogsDir"), "FAIRMONEY_LOGS_DIR", "logs")), "logs")
TRANSCRIPTS_DIR: str = _resolve_path(str(_get(("Storage", "TranscriptsDir"), "FAIRMONEY_TRANSCRIPT_DIR", "transcripts")), "transcripts")
DISPOSITIONS_DIR: str = _resolve_path(str(_get(("Storage", "DispositionsDir"), "FAIRMONEY_DISPOSITION_DIR", "dispositions")), "dispositions")

# Backwards compatibility aliases
TRANSCRIPT_DIR: str = TRANSCRIPTS_DIR
DISPOSITION_DIR: str = DISPOSITIONS_DIR

# Ensure storage directories exist
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)
os.makedirs(DISPOSITIONS_DIR, exist_ok=True)
if os.path.dirname(DB_PATH):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# -----------------------------------------------------------------------------
# 7. Logging Configuration
# -----------------------------------------------------------------------------
LOG_LEVEL_STR: str = str(_get(("Logging", "Level"), "FAIRMONEY_LOG_LEVEL", "INFO")).upper()
LOG_LEVEL: int = getattr(logging, LOG_LEVEL_STR, logging.INFO)
LOG_RETENTION_DAYS: int = int(_get(("Logging", "RetentionDays"), "FAIRMONEY_LOG_RETENTION_DAYS", 30))
DISABLE_QUICK_EDIT: bool = _to_bool(_get(("Logging", "DisableQuickEdit"), "FAIRMONEY_DISABLE_QUICK_EDIT", True), default=True)

# -----------------------------------------------------------------------------
# 8. Asterisk AMI Configuration
# -----------------------------------------------------------------------------
AMI_HOST: str = str(_get(("AsteriskAmi", "Host"), "AMI_HOST", "192.168.200.11"))
AMI_PORT: int = int(_get(("AsteriskAmi", "Port"), "AMI_PORT", 5038))
AMI_USERNAME: str = str(_get(("AsteriskAmi", "Username"), "AMI_USERNAME", "lite"))
AMI_SECRET: str = str(_get(("AsteriskAmi", "Secret"), "AMI_SECRET", "4003"))

# -----------------------------------------------------------------------------
# Sync to os.environ so all external libraries automatically inherit them
# -----------------------------------------------------------------------------
_ENV_SYNCS = {
    "DEEPGRAM_API_KEY": DEEPGRAM_API_KEY,
    "GROQ_API_KEY": GROQ_API_KEY,
    "FAIRMONEY_GROQ_LLM_MODEL": GROQ_LLM_MODEL,
    "CARTESIA_API_KEY": CARTESIA_API_KEY,
    "AZURE_SPEECH_KEY": AZURE_SPEECH_KEY,
    "AZURE_SPEECH_REGION": AZURE_SPEECH_REGION,
    "AZURE_VOICE_NAME": AZURE_VOICE_NAME,
    "FAIRMONEY_TTS_ENGINE": TTS_ENGINE,
    "AMI_HOST": AMI_HOST,
    "AMI_PORT": str(AMI_PORT),
    "AMI_USERNAME": AMI_USERNAME,
    "AMI_SECRET": AMI_SECRET,
    "FAIRMONEY_TRANSFER_QUEUE": TRANSFER_QUEUE,
    "DISPOSITION_API_URL": DISPOSITION_API_URL,
    "PAYMENT_LINK_BASE_URL": PAYMENT_LINK_BASE_URL,
    "FAIRMONEY_DB_PATH": DB_PATH,
    "FAIRMONEY_LOGS_DIR": LOGS_DIR,
    "FAIRMONEY_TRANSCRIPT_DIR": TRANSCRIPTS_DIR,
    "FAIRMONEY_DISPOSITION_DIR": DISPOSITIONS_DIR,
}

for k, v in _ENV_SYNCS.items():
    if v is not None:
        os.environ[k] = str(v)
