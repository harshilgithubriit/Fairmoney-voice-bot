import asyncio
import re
import os
import queue
import struct
import threading
import time
from typing import AsyncIterator

import azure.cognitiveservices.speech as speechsdk
from dotenv import load_dotenv

from services.latency import LatencyTracker
from services.stt_opt import AUDIO_SOURCE, SAMPLE_RATE

# ── Speaker output (mic mode only) ────────────────────────────────────────────
if AUDIO_SOURCE == "mic":
    import sounddevice as sd

load_dotenv()

# ── Azure TTS configuration ───────────────────────────────────────────────────
# SOW item 6: Languages = Nigerian English.
# Voice: en-NG-EzinneNeural (female, Nigerian English accent).
try:
    from config.settings import (
        AZURE_SPEECH_KEY,
        AZURE_SPEECH_REGION,
        AZURE_VOICE_NAME,
    )
    _AZURE_SPEECH_KEY    = AZURE_SPEECH_KEY or os.getenv("AZURE_SPEECH_KEY")
    _AZURE_SPEECH_REGION = AZURE_SPEECH_REGION or os.getenv("AZURE_SPEECH_REGION")
    _DEFAULT_VOICE       = AZURE_VOICE_NAME or os.getenv("AZURE_VOICE_NAME", "en-NG-EzinneNeural")
except Exception:
    _AZURE_SPEECH_KEY    = os.getenv("AZURE_SPEECH_KEY")
    _AZURE_SPEECH_REGION = os.getenv("AZURE_SPEECH_REGION")
    _DEFAULT_VOICE       = os.getenv("AZURE_VOICE_NAME", "en-NG-EzinneNeural")

_VOICE_MAP = {
    "en-NG": {"voice": "en-NG-EzinneNeural",  "lang": "en-NG"},
    "en":    {"voice": "en-NG-EzinneNeural",  "lang": "en-NG"},
    # fallback for any stray Hindi-tagged text (unlikely in this flow)
    "hi-IN": {"voice": "hi-IN-SwaraNeural",   "lang": "hi-IN"},
    "hi":    {"voice": "hi-IN-SwaraNeural",   "lang": "hi-IN"},
}

_SAMPLE_RATE = SAMPLE_RATE

FRAME_BYTES    = 320 if _SAMPLE_RATE == 8000 else 640
FRAME_INTERVAL = 0.020

_FRAME_KIND = {
    8000:  0x10,
    16000: 0x12,
}.get(_SAMPLE_RATE)

if _FRAME_KIND is None:
    raise RuntimeError(
        f"Unsupported _SAMPLE_RATE={_SAMPLE_RATE}. "
        f"Must be 8000 (slin, 0x10) or 16000 (slin16, 0x12)."
    )

_AZURE_AUDIO_FORMAT = (
    speechsdk.SpeechSynthesisOutputFormat.Raw8Khz16BitMonoPcm
    if _SAMPLE_RATE == 8000
    else speechsdk.SpeechSynthesisOutputFormat.Raw16Khz16BitMonoPcm
)

print(f"[TTS] Engine: Azure (streaming) | Voice: {_DEFAULT_VOICE} (Nigerian English) | "
      f"Sample rate: {_SAMPLE_RATE} Hz | Frame: {FRAME_BYTES} bytes | "
      f"Kind: 0x{_FRAME_KIND:02x}")

# ── Pre-warmed shared SpeechConfig ──────────────────────────────────────────
_shared_speech_config = speechsdk.SpeechConfig(
    subscription=_AZURE_SPEECH_KEY,
    region=_AZURE_SPEECH_REGION,
)
_shared_speech_config.set_speech_synthesis_output_format(_AZURE_AUDIO_FORMAT)

def _prewarm_azure() -> None:
    """Pre-warm the Azure Speech TLS connection in the background on startup."""
    try:
        synth = speechsdk.SpeechSynthesizer(speech_config=_shared_speech_config, audio_config=None)
        # Synthesize a silent space to open and warm the TLS tunnel to Azure
        synth.speak_ssml_async('<speak version="1.0" xml:lang="en-NG"><voice name="en-NG-EzinneNeural"> </voice></speak>').get()
        print("[TTS] Azure Speech connection pre-warmed and ready.")
    except Exception as e:
        print(f"[TTS] Azure pre-warm notice: {e}")

threading.Thread(target=_prewarm_azure, daemon=True).start()

# ── Language tag detection (legacy support — flow normally sends no tags) ────
_LANG_TAG_RE  = re.compile(r"^\[(LANG_EN|LANG_HI)\]\s*")
_LANG_TAG_ANY = re.compile(r"\[(LANG_EN|LANG_HI)\]")

def _detect_lang_from_tag(tag: str) -> tuple[str, str]:
    if tag == "LANG_EN":
        return _VOICE_MAP["en-NG"]["voice"], _VOICE_MAP["en-NG"]["lang"]
    return _VOICE_MAP["hi-IN"]["voice"], _VOICE_MAP["hi-IN"]["lang"]

def _strip_lang_tags(text: str) -> str:
    return _LANG_TAG_ANY.sub("", text)


# ── AudioSocket framing ───────────────────────────────────────────────────────

def _make_audio_frame(pcm_bytes: bytes) -> bytes:
    header = bytes([_FRAME_KIND]) + struct.pack(">H", len(pcm_bytes))
    return header + pcm_bytes


# ── Paced sender ──────────────────────────────────────────────────────────────

async def _pace_and_send(
    writer: asyncio.StreamWriter,
    pcm_buffer_ref: list,
    deadline_ref: list,
    interrupted_check,
) -> None:
    loop = asyncio.get_event_loop()

    while len(pcm_buffer_ref[0]) >= FRAME_BYTES:
        if interrupted_check():
            return

        chunk               = pcm_buffer_ref[0][:FRAME_BYTES]
        pcm_buffer_ref[0]   = pcm_buffer_ref[0][FRAME_BYTES:]

        writer.write(_make_audio_frame(chunk))
        await writer.drain()

        deadline_ref[0] += FRAME_INTERVAL
        sleep_for = deadline_ref[0] - loop.time()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)


# ── Azure SSML helper ─────────────────────────────────────────────────────────

def _speech_rate(text: str) -> str:
    """
    Pick a TTS prosody rate based on sentence content/length.
    Short sentences (greetings, confirmations, questions) can be spoken a bit
    faster; very long sentences read more naturally at a slower pace.
    """
    n = len(text.strip())
    if n <= 40:
        return "+20%"          # short: greeting, "is that correct?", etc.
    if n <= 90:
        return "+10%"          # medium
    return "0%"                # long: the intro/overdue script reads naturally

def _build_ssml(text: str, voice: str, lang: str) -> str:
    safe = (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    rate = _speech_rate(safe)
    return (
        f'<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" '
        f'xml:lang="{lang}">'
        f'<voice name="{voice}"><prosody rate="{rate}">{safe}</prosody></voice>'
        f'</speak>'
    )


# ── Sentence splitting ────────────────────────────────────────────────────────
_SENTENCE_END_RE = re.compile(r"(?<=[.!?।])(?![.!?।])")

def _split_ready_sentences(buffer: str) -> tuple[list[str], str]:
    """
    Given accumulated text, return (complete_sentences, remainder).
    Splits immediately on sentence-ending punctuation marks to minimize latency.
    """
    if not buffer:
        return [], ""

    parts = _SENTENCE_END_RE.split(buffer)
    if len(parts) == 1:
        return [], buffer

    *ready, remainder = parts
    ready = [s.strip() for s in ready if s.strip()]
    return ready, remainder


# ── Core synth worker (runs in a thread) ──────────────────────────────────────

def _synth_worker(
    text_queue: "queue.Queue[str | None]",
    pcm_queue: "queue.Queue[bytes | None]",
    interrupted_flag: threading.Event,
    voice: str,
    lang: str,
) -> None:
    synthesizer = speechsdk.SpeechSynthesizer(speech_config=_shared_speech_config, audio_config=None)

    READ_SIZE = FRAME_BYTES * 4

    try:
        while True:
            if interrupted_flag.is_set():
                break

            try:
                text_chunk = text_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if text_chunk is None:          # sentinel: no more text coming
                break
            if interrupted_flag.is_set():
                break

            ssml   = _build_ssml(text_chunk, voice, lang)
            result = synthesizer.speak_ssml_async(ssml).get()

            if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
                audio_bytes = result.audio_data
                offset = 0
                while offset < len(audio_bytes):
                    if interrupted_flag.is_set():
                        break
                    chunk = audio_bytes[offset:offset + READ_SIZE]
                    pcm_queue.put(chunk)
                    offset += READ_SIZE

            elif result.reason == speechsdk.ResultReason.Canceled:
                details = result.cancellation_details
                if not interrupted_flag.is_set():
                    print(f"[TTS] Azure cancelled: {details.reason} – {details.error_details}")

            if interrupted_flag.is_set():
                break

    except Exception as e:
        if not interrupted_flag.is_set():
            print(f"[TTS] Azure synthesis error: {e}")
    finally:
        pcm_queue.put(None)   # sentinel: no more audio coming


# ── Main TTS Handler ──────────────────────────────────────────────────────────

async def stream_tts(
    text_stream: AsyncIterator[str],
    tracker: LatencyTracker | None = None,
    interrupt_event: asyncio.Event | None = None,
    writer: asyncio.StreamWriter | None = None,
    language: str = "en",
) -> bool:
    """Signature-compatible with services.tts.stream_tts (Cartesia).

    `language` is accepted and ignored: this engine always speaks the
    configured Nigerian English voice unless a legacy [LANG_*] tag appears.
    """
    interrupted      = False
    interrupted_flag = threading.Event()

    voice_name  = _VOICE_MAP["en-NG"]["voice"]   # default: Nigerian English
    xml_lang    = _VOICE_MAP["en-NG"]["lang"]

    async def interrupt_watcher():
        nonlocal interrupted
        if interrupt_event is None:
            return
        await interrupt_event.wait()
        interrupted = True
        interrupted_flag.set()

    watcher_task = asyncio.create_task(interrupt_watcher())

    if tracker:
        tracker.tts_start = time.perf_counter()

    text_queue: "queue.Queue[str | None]" = queue.Queue()
    pcm_queue:  "queue.Queue[bytes | None]" = queue.Queue()

    synth_thread: threading.Thread | None = None
    text_buffer = ""

    def _start_synth_thread_if_needed():
        nonlocal synth_thread
        if synth_thread is None:
            synth_thread = threading.Thread(
                target=_synth_worker,
                args=(text_queue, pcm_queue, interrupted_flag, voice_name, xml_lang),
                daemon=True,
            )
            synth_thread.start()

    async def _produce_text():
        nonlocal interrupted, text_buffer

        async for chunk in text_stream:
            if interrupted:
                return

            cleaned = _strip_lang_tags(chunk)
            if not cleaned:
                continue

            text_buffer += cleaned

            ready_sentences, remainder = _split_ready_sentences(text_buffer)
            text_buffer = remainder

            for sentence in ready_sentences:
                _start_synth_thread_if_needed()
                text_queue.put(sentence)
                await asyncio.sleep(0.06)  # pause between sentences for natural pacing

        tail = text_buffer.strip()
        if tail:
            _start_synth_thread_if_needed()
            text_queue.put(tail)

        text_queue.put(None)

    producer_task = asyncio.create_task(_produce_text())

    # ── Mic mode ──────────────────────────────────────────────────────────────
    if AUDIO_SOURCE == "mic":
        playback_stream = sd.RawOutputStream(
            samplerate=_SAMPLE_RATE, channels=1, dtype="int16"
        )
        playback_stream.start()

        try:
            while True:
                if interrupted:
                    break

                try:
                    chunk = await asyncio.to_thread(pcm_queue.get, timeout=0.1)
                except queue.Empty:
                    if producer_task.done() and synth_thread is None:
                        break
                    continue

                if chunk is None:
                    break

                await asyncio.to_thread(playback_stream.write, chunk)

        finally:
            if interrupted:
                playback_stream.abort()
            else:
                playback_stream.stop()
            playback_stream.close()

            interrupted_flag.set()
            if synth_thread is not None:
                synth_thread.join(timeout=2)

            producer_task.cancel()
            try:
                await producer_task
            except (asyncio.CancelledError, Exception):
                pass

        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass

        if tracker and not interrupted:
            tracker.tts_end = time.perf_counter()

        return not interrupted

    # ── AudioSocket mode ──────────────────────────────────────────────────────
    if writer is None:
        interrupted_flag.set()
        producer_task.cancel()
        try:
            await producer_task
        except (asyncio.CancelledError, Exception):
            pass
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        return True

    pcm_buffer  = [b""]
    deadline    = [0.0]
    first_audio = True
    loop        = asyncio.get_event_loop()

    try:
        while True:
            if interrupted:
                break

            try:
                chunk = await asyncio.to_thread(pcm_queue.get, timeout=0.05)
            except queue.Empty:
                if producer_task.done() and synth_thread is None:
                    break
                continue

            if chunk is None:
                if not interrupted:
                    remainder = pcm_buffer[0]
                    if len(remainder) >= FRAME_BYTES // 2:
                        pad           = FRAME_BYTES - len(remainder)
                        padded        = remainder + b"\x00" * pad
                        pcm_buffer[0] = padded
                        await _pace_and_send(writer, pcm_buffer, deadline, lambda: interrupted)
                break

            if first_audio:
                if tracker:
                    tracker.tts_first_audio = time.perf_counter()
                first_audio  = False
                deadline[0]  = loop.time()

            pcm_buffer[0] += chunk
            await _pace_and_send(writer, pcm_buffer, deadline, lambda: interrupted)

    except Exception as e:
        if not interrupted:
            print(f"[TTS] audio consumer error: {e}")

    finally:
        interrupted_flag.set()
        if synth_thread is not None:
            synth_thread.join(timeout=2)

        producer_task.cancel()
        try:
            await producer_task
        except (asyncio.CancelledError, Exception):
            pass

        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass

    if tracker and not interrupted:
        tracker.tts_end = time.perf_counter()

    return not interrupted
