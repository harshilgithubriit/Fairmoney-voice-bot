"""
Async speech-to-text service backed by Deepgram Nova-3 (v1 /listen WebSocket).

WHY THIS EXISTS
────────────────
Flux Multilingual (`flux-general-multi`) only supports 10 languages:
en, es, fr, de, hi, ru, pt, ja, it, nl. It does NOT support Tamil, Telugu,
Kannada, Bengali, Marathi, or Gujarati — so calls in those languages must
go through Nova-3 instead (`model=nova-3`, `language=<code>`).

KEY DIFFERENCE FROM STTService (stt.py, Flux-based)
──────────────────────────────────────────────────
Flux gives you model-integrated turn detection: StartOfTurn, EagerEndOfTurn,
TurnResumed, EndOfTurn events tell you exactly when the caller started/
finished speaking. Nova-3 gives you none of that — it's a raw transcription
stream (interim + final results per utterance). So this class reimplements
manual endpointing: RMS-based silence detection over the incoming audio
queue, the same approach your old threaded version used before the Flux
rewrite. There's no EagerEndOfTurn equivalent, so responses will feel a
little less snappy than the Flux path — that's an inherent tradeoff of
Nova-3 not being conversation-aware, not a bug in this implementation.

Tune SILENCE_RMS_THRESHOLD / SILENCE_TIMEOUT_SECS against real 8kHz
Asterisk-sourced audio for your target languages before trusting this in
production. If you later swap the crude RMS gate for Silero VAD, only
`_listen_loop_socket` needs to change — the rest of the class (queueing,
lifecycle, interrupt handling) stays the same.
"""

import asyncio
import os
import time
import numpy as np
from dotenv import load_dotenv

from deepgram import AsyncDeepgramClient
from deepgram.core.events import EventType

from services.latency import LatencyTracker

SAMPLE_RATE = 8000 

# Languages confirmed supported by Nova-3 as of this writing, restricted to
# the set Cartesia also supports on the TTS side. VERIFY Punjabi ("pa") and
# Malayalam ("ml") in the Deepgram Playground before adding them here —
# they were not found in Deepgram's language-support changelog.
NOVA_SUPPORTED_LANGUAGES = {
    "hi": "hi",  # Hindi
    "bn": "bn",  # Bengali
    "mr": "mr",  # Marathi
    "ta": "ta",  # Tamil
    "te": "te",  # Telugu
    "kn": "kn",  # Kannada
    "gu": "gu",  # Gujarati
}

load_dotenv()


class NovaSTTService:
    """
    Mirrors STTService's public interface (feed_audio, listen_turn, start,
    close, abort_current_listen, was_aborted) so callers/audiosocket_server.py
    can treat both classes interchangeably. Each instance owns its own
    connection/queue — same per-call isolation guarantee as the Flux class.
    """

    def __init__(self, language: str):
        if language not in NOVA_SUPPORTED_LANGUAGES:
            raise ValueError(
                f"[NovaSTT] Unsupported language {language!r}. "
                f"Supported: {sorted(NOVA_SUPPORTED_LANGUAGES)}"
            )
        self.language = NOVA_SUPPORTED_LANGUAGES[language]
        self.deepgram = AsyncDeepgramClient(api_key=os.getenv("DEEPGRAM_API_KEY"))

        self.speech_finished = asyncio.Event()
        self.abort_event = asyncio.Event()
        self.transcript_parts: list[str] = []

        self.tracker: LatencyTracker | None = None
        self.interrupt_event: asyncio.Event | None = None
        self.interrupt_signalled = False
        self._was_aborted = False

        self.bot_speaking = asyncio.Event()
        self._active = False

        self._audio_queue: asyncio.Queue = asyncio.Queue()

        # Deepgram's Results messages carry no turn identifier, so we can't
        # tag-and-drop stale ones by ID. Instead: only trust a `speech_final`
        # (or use it to stamp tracker.stt_end) once we've actually forwarded
        # at least one audio chunk in the CURRENT turn. Without this, a
        # straggling final result for audio sent during the PREVIOUS turn
        # (e.g. the greeting, or leftover pre-flush audio) can arrive right
        # after a new listen_turn() starts and instantly finalize it before
        # the caller has said anything — this was producing the negative/
        # matching-positive latency numbers you saw.
        self._audio_sent_this_turn = False
        self._turn_id = 0  # for logging only

    # =========================================================
    # AUDIO INJECTION (same contract as STTService.feed_audio)
    # =========================================================
    async def feed_audio(self, raw: bytes) -> None:
        self._audio_queue.put_nowait(raw)

    def audio_queue_depth(self) -> int:
        return self._audio_queue.qsize()

    def flush_audio_queue(self) -> int:
        discarded = 0
        while not self._audio_queue.empty():
            try:
                self._audio_queue.get_nowait()
                discarded += 1
            except asyncio.QueueEmpty:
                break
        if discarded:
            print(f"[NovaSTT] [{id(self)}] Flushed {discarded} stale audio frames.")
        return discarded

    # =========================================================
    # DEEPGRAM MESSAGE HANDLING
    # =========================================================
    async def _handle_message(self, message) -> None:
        if not self._active:
            return
        try:
            # The v1 listen client hands back typed response objects
            # (e.g. LiveResultResponse), NOT plain dicts — unlike Flux's v2
            # client. Support both shapes defensively: try dict-style
            # first, fall back to attribute access. Previously this only
            # checked isinstance(message, dict), which is False for every
            # v1 message — so every single message was silently dropped
            # right here, before ever reaching the type/Results check.
            def _get(obj, key, default=None):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            msg_type = _get(message, "type", "")

            # Some SDK versions don't set `type` on Results objects at all —
            # if there's no type but there IS a channel/alternatives shape,
            # treat it as a Results message.
            if not msg_type:
                if _get(message, "channel") is not None:
                    msg_type = "Results"

            if msg_type != "Results":
                print(f"[NovaSTT] [{id(self)}] Non-Results message "
                      f"(type={msg_type!r}): {message!r}")
                return

            if not self._audio_sent_this_turn:
                # We haven't forwarded any audio in the CURRENT turn yet,
                # so this can only be a straggler describing audio from the
                # previous turn. Drop it — don't let it populate
                # transcript_parts or stamp tracker times for the new turn.
                print(f"[NovaSTT] [{id(self)}] Dropping pre-turn stale Results "
                      f"message (no audio sent yet this turn).")
                return

            channel = _get(message, "channel")
            alternatives = _get(channel, "alternatives", [{}]) if channel is not None else [{}]
            alt = alternatives[0] if alternatives else {}
            sentence = (_get(alt, "transcript") or "").strip()
            is_final = bool(_get(message, "is_final", False))
            speech_final = bool(_get(message, "speech_final", False))

            if not sentence:
                print(f"[NovaSTT] [{id(self)}] Results msg with EMPTY transcript "
                      f"(is_final={is_final}, speech_final={speech_final}, "
                      f"confidence={_get(alt, 'confidence')})")
                return

            # Signal interrupt as soon as any speech is detected — same
            # semantics as the Flux service's early-interrupt behavior.
            if self.interrupt_event is not None:
                should_signal = (
                    not self.interrupt_signalled and not self.interrupt_event.is_set()
                )
                if should_signal:
                    self.interrupt_signalled = True
                    self.interrupt_event.set()

            if is_final:
                self.transcript_parts.append(sentence)
                if self.tracker:
                    self.tracker.stt_end = time.perf_counter()
                print(f"[NovaSTT] [{id(self)}] final={sentence!r} speech_final={speech_final}")

                # `speech_final` fires when Deepgram's own endpointing
                # (VAD + silence) decides the utterance is done. Combined
                # with our own RMS timeout in _listen_loop_socket, this
                # gives two independent signals for end-of-turn.
                if speech_final and not self.speech_finished.is_set():
                    self.speech_finished.set()
            else:
                print(f"[NovaSTT] [{id(self)}] interim={sentence!r}")

        except Exception as e:
            print(f"[NovaSTT] [{id(self)}] Callback error: {e}")

    # =========================================================
    # LISTEN FOR ONE USER TURN
    # =========================================================
    async def listen_turn(
        self,
        tracker: LatencyTracker | None = None,
        interrupt_event: asyncio.Event | None = None,
        loop=None,
    ) -> str:
        self.flush_audio_queue()
        self.transcript_parts.clear()
        self.interrupt_signalled = False
        self.speech_finished.clear()
        self.abort_event.clear()
        self._was_aborted = False
        self._turn_id += 1
        my_turn = self._turn_id
        self._audio_sent_this_turn = False

        self.tracker = tracker
        self.interrupt_event = interrupt_event

        print(f"\n[NovaSTT] [{id(self)}] Listening (lang={self.language}, turn={my_turn})...")

        if self.tracker:
            self.tracker.stt_start = time.perf_counter()

        await self._listen_loop_socket()

        if self.abort_event.is_set() and not self.speech_finished.is_set():
            self._was_aborted = True

        return " ".join(self.transcript_parts).strip()

    # =========================================================
    # SOCKET AUDIO LOOP — manual RMS-based endpointing
    # =========================================================
    async def _listen_loop_socket(self) -> None:
        MAX_LISTEN_SECS = 90
        SILENCE_TIMEOUT_SECS = 1.2  # gap of silence that ends a turn
        SILENCE_RMS_THRESHOLD = 300  # TUNE against real 8kHz call audio
        POLL_TIMEOUT = 0.5

        started_at = time.perf_counter()
        silence_start = None
        chunks_sent = 0

        try:
            while not self.speech_finished.is_set() and not self.abort_event.is_set():
                if time.perf_counter() - started_at > MAX_LISTEN_SECS:
                    print(f"[NovaSTT] [{id(self)}] Max listen duration reached.")
                    self.speech_finished.set()
                    break

                try:
                    raw = await asyncio.wait_for(self._audio_queue.get(), timeout=POLL_TIMEOUT)
                except asyncio.TimeoutError:
                    if chunks_sent == 0 and time.perf_counter() - started_at > 5.0:
                        # DIAGNOSTIC: if we've been waiting 5+ seconds and
                        # have never received a single audio chunk on the
                        # queue, feed_audio() almost certainly isn't being
                        # called for this instance — check the RTP/AudioSocket
                        # read loop is calling state.stt.feed_audio(), and
                        # that `state.stt` actually points at this NovaSTT
                        # instance (not a stale STTService reference).
                        print(f"[NovaSTT] [{id(self)}] WARNING: no audio "
                              f"received on queue after 5s. Is feed_audio() "
                              f"being called for this instance?")
                    if not self.bot_speaking.is_set() and self.transcript_parts:
                        # We've already got some final transcript and now
                        # got dead air on the queue itself — treat as end.
                        if silence_start is None:
                            silence_start = time.perf_counter()
                        elif time.perf_counter() - silence_start > SILENCE_TIMEOUT_SECS:
                            print(f"[NovaSTT] [{id(self)}] Silence timeout (queue empty).")
                            self.speech_finished.set()
                            break
                    continue

                await self.connection.send_media(raw)
                self._audio_sent_this_turn = True
                chunks_sent += 1

                arr = np.frombuffer(raw, dtype=np.int16)
                rms = float(np.sqrt(np.mean(arr.astype(np.float64) ** 2)))

                if self.bot_speaking.is_set() or rms > SILENCE_RMS_THRESHOLD:
                    silence_start = None
                elif self.transcript_parts:
                    # Only start counting silence toward end-of-turn once
                    # we've captured at least one final transcript chunk,
                    # so we don't cut off before the caller has said anything.
                    if silence_start is None:
                        silence_start = time.perf_counter()
                    elif time.perf_counter() - silence_start > SILENCE_TIMEOUT_SECS:
                        print(f"[NovaSTT] [{id(self)}] Silence timeout (RMS={rms:.1f}).")
                        self.speech_finished.set()
                        break

                if chunks_sent % 500 == 0:
                    elapsed = chunks_sent * 16 / 1000
                    print(f"[NovaSTT] [{id(self)}] {chunks_sent} chunks (~{elapsed:.1f}s)")

        except Exception as e:
            print(f"[NovaSTT] [{id(self)}] Socket stream error: {e}")
            self.speech_finished.set()

    # =========================================================
    # CONNECTION LIFECYCLE
    # =========================================================
    async def start(self) -> None:
        print(f"[NovaSTT] [{id(self)}] Establishing Nova-3 connection "
              f"(lang={self.language}, {SAMPLE_RATE}Hz)...")
        print(f"[NovaSTT] [{id(self)}] Connect params: model=nova-3, "
              f"language={self.language!r}, encoding=linear16, "
              f"sample_rate={SAMPLE_RATE}, endpointing=300, "
              f"utterance_end_ms=1000")

        self._active = True

        try:
            self._connect_ctx = self.deepgram.listen.v1.connect(
                model="nova-3",
                language=self.language,
                encoding="linear16",
                sample_rate=SAMPLE_RATE,
                interim_results=True,
                endpointing=300,   # ms of silence Deepgram itself waits before speech_final
                utterance_end_ms=1000,
                smart_format=True,
            )
            self.connection = await self._connect_ctx.__aenter__()
        except Exception as e:
            # DIAGNOSTIC: previously a bad/rejected param here (or an auth
            # issue specific to this model/language combo) would either
            # raise somewhere uncaught inside asyncio.create_task later, or
            # get swallowed — either way, invisible. Surface it explicitly.
            import traceback
            print(f"[NovaSTT] [{id(self)}] FAILED to establish connection "
                  f"(lang={self.language}): {e!r}")
            traceback.print_exc()
            self._active = False
            raise

        self.connection.on(EventType.MESSAGE, self._on_message_sync_bridge)
        self.connection.on(EventType.ERROR, lambda error: print(f"[NovaSTT] [{id(self)}] WS error: {error}"))
        self.connection.on(EventType.OPEN, lambda _: print(f"[NovaSTT] [{id(self)}] WebSocket opened."))
        self.connection.on(EventType.CLOSE, lambda _: print(f"[NovaSTT] [{id(self)}] WebSocket closed."))

        self._receive_task = asyncio.create_task(
            self._safe_start_listening(), name=f"nova-stt-recv-{id(self)}"
        )

    async def _safe_start_listening(self) -> None:
        try:
            await self.connection.start_listening()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._active:
                import traceback
                print(f"[NovaSTT] [{id(self)}] start_listening() ended with error: {e!r}")
                traceback.print_exc()

    def _on_message_sync_bridge(self, message, **kwargs) -> None:
        asyncio.create_task(self._handle_message(message))

    # =========================================================
    # INTERRUPTION HELPERS
    # =========================================================
    def abort_current_listen(self) -> None:
        self.abort_event.set()

    def was_aborted(self) -> bool:
        return self._was_aborted

    # =========================================================
    # CLEAN SHUTDOWN
    # =========================================================
    async def close(self) -> None:
        print(f"[NovaSTT] [{id(self)}] Closing Nova-3 connection...")

        self._active = False
        self.abort_current_listen()

        while not self._audio_queue.empty():
            try:
                self._audio_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        if getattr(self, "_receive_task", None) and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except (asyncio.CancelledError, Exception):
                pass

        if hasattr(self, "connection") and self.connection:
            try:
                await self.connection.send_close_stream()
            except Exception:
                print(f"[NovaSTT] [{id(self)}] Error sending close stream.")

        if getattr(self, "_connect_ctx", None):
            try:
                await self._connect_ctx.__aexit__(None, None, None)
            except Exception:
                print(f"[NovaSTT] [{id(self)}] Error closing context manager.")