"""
Decides which STT backend a call should use:
  - Known language up front (IVR selection, DID routing, account setting):
      call get_stt_service(language=<code>) directly and skip detection.
  - Unknown language: start on Flux auto-detect in background.
"""
import asyncio

from services.stt_opt import STTService, AUDIO_SOURCE
from services.stt_nova import NovaSTTService, NOVA_SUPPORTED_LANGUAGES

FLUX_SUPPORTED_LANGUAGES = {"en", "es", "fr", "de", "hi", "ru", "pt", "ja", "it", "nl"}


async def get_stt_service(language: str | None = None):
    """
    language: BCP-47-ish code like "en", "hi", "ta", etc, or None to
    auto-detect via Flux's first turn before deciding.
    """
    if language is not None:
        if language in FLUX_SUPPORTED_LANGUAGES:
            svc = STTService()
            asyncio.create_task(svc.start())
            return svc
        if language in NOVA_SUPPORTED_LANGUAGES:
            svc = NovaSTTService(language=language)
            await svc.start()
            return svc
        raise ValueError(
            f"[STT router] Language {language!r} is not supported by either "
            f"Flux ({sorted(FLUX_SUPPORTED_LANGUAGES)}) or Nova-3 "
            f"({sorted(NOVA_SUPPORTED_LANGUAGES)})."
        )

    # Unknown language: start on Flux in background
    svc = STTService()
    asyncio.create_task(svc.start())
    return svc


async def swap_to_nova(flux_svc: STTService, detected_language: str) -> NovaSTTService:
    """
    Call this once you've read `languages[0]` from Flux's first TurnInfo
    event and it's NOT in FLUX_SUPPORTED_LANGUAGES (e.g. "ta", "te", "kn").
    """
    if detected_language not in NOVA_SUPPORTED_LANGUAGES:
        raise ValueError(
            f"[STT router] Detected language {detected_language!r} isn't "
            f"supported by Nova-3 either — no fallback available."
        )
    await flux_svc.close()
    nova_svc = NovaSTTService(language=detected_language)
    await nova_svc.start()
    return nova_svc
