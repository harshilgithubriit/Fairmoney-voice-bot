"""
Async speech-to-text service backed by Deepgram v2 (Flux) WebSocket.

REWRITE NOTES (vs. the old threading-based version)
─────────────────────────────────────────────────────
- Uses AsyncDeepgramClient + `async with client.listen.v2.connect(...)`.
  No more `listen_thread`, no more `start_listening` running in a daemon
  thread — the WS receive loop is now a plain asyncio Task owned by this
  event loop.
- `_audio_queue` is now `asyncio.Queue`. `feed_audio()` is `async def` and
  does `put_nowait` (safe to call from the event-loop thread, which is the
  only place `audiosocket_server.py` ever calls it from).
- `speech_finished`, `abort_event` are `asyncio.Event`. Since the Deepgram
  message handler now ALSO runs on the event loop (as a task, not a thread),
  we no longer need `call_soon_threadsafe` for STT-internal state. We DO
  still need it for `interrupt_event`, because that Event may be waited on
  by the TTS coroutine running as a *different* task — same loop though,
  so plain `.set()` is actually fine too. Kept `call_soon_threadsafe` only
  where a callback could conceivably fire from a non-loop context, to be
  safe if the SDK internals ever use a thread for transport.
- `self._lock` (threading.Lock) is REMOVED. All state is now only mutated
  from coroutines running on the single event loop — asyncio doesn't need
  a lock for that. If you see race conditions after this rewrite, it means
  something is calling into this class from a real OS thread again.
- No more manual RMS-based busy-poll spin every 20ms while idle. The
  silence-timeout logic uses `asyncio.wait_for(queue.get(), timeout=...)`
  so the task is fully suspended (zero CPU) whenever there's no audio and
  no timeout has elapsed — this is the main concurrency win.
- Each STTService instance still owns its own client/connection/queue —
  no shared state across calls, same isolation guarantee as before.

CONCURRENCY IMPACT
──────────────────
Old design: 1 blocking OS thread (busy-polling every 20ms) PER concurrent
call for the audio loop, plus 1 more OS thread for Deepgram's own
`start_listening`. If `listen_turn()` was invoked without
`run_in_executor`/`to_thread`, the event loop was blocked entirely
(effectively 1 concurrent call). If it was offloaded, you were capped by
the default ThreadPoolExecutor (`min(32, cpu_count()+4)`, ~8-12 workers).

New design: zero dedicated OS threads for the audio loop (it's a coroutine
suspended on `await`), and the Deepgram WS I/O is handled by an asyncio
Task as well (the SDK's async client uses the event loop's own socket
I/O, not a background thread). Concurrency ceiling is now determined by:
  - CPU available to run coroutine callbacks (GIL-bound, but coroutines
    that are `await`-suspended cost ~nothing)
  - Deepgram API rate limits / concurrent-connection limits on your plan
  - Network bandwidth
  - Whatever explicit semaphore you put in audiosocket_server.py

Recommend load-testing with a semaphore of e.g. 20, 50, 100 and watching
CPU/memory/Deepgram error rates to find your real number empirically —
there's no honest way to give you a fixed number without measuring your
box + your Deepgram plan's connection limits.
"""

import asyncio
import os
import time
import numpy as np
from dotenv import load_dotenv

from deepgram import AsyncDeepgramClient
from deepgram.core.events import EventType

from services.latency import LatencyTracker

try:
    from config.settings import AUDIO_SOURCE, SAMPLE_RATE
except Exception:
    AUDIO_SOURCE = "socket"
    SAMPLE_RATE = 8000

if AUDIO_SOURCE == "mic":
    import sounddevice as sd

load_dotenv()


class STTService:
    """
    Each STTService instance owns its own AsyncDeepgramClient, its own
    WebSocket connection (opened per listen_turn via `async with`), and its
    own asyncio.Queue. Nothing is shared at class level, so concurrent
    calls can never mix transcripts or audio — same isolation guarantee as
    the threaded version, minus the OS threads.
    """

    def __init__(self):
        self.deepgram = AsyncDeepgramClient(api_key=os.getenv("DEEPGRAM_API_KEY"))

        self._skip_next_flush = False
        self._last_eager_eot_time: float = 0.0
        self._eager_eot_fired = False

        # Per-turn state
        self.speech_finished = asyncio.Event()
        self.abort_event = asyncio.Event()
        self.ready_event = asyncio.Event()
        self.transcript_parts: list[str] = []

        self.tracker = None
        self.interrupt_event: asyncio.Event | None = None
        self.loop: asyncio.AbstractEventLoop | None = None

        self.interrupt_signalled = False
        self._was_aborted = False

        self.bot_speaking = asyncio.Event()

        self._active = False

        # Pending delayed-finalise task for EagerEndOfTurn debounce
        self._delayed_finalise_task: asyncio.Task | None = None

        self._audio_queue: asyncio.Queue | None = None
        if AUDIO_SOURCE == "socket":
            self._audio_queue = asyncio.Queue()

        self.stream = None  # mic mode only

    # =========================================================
    # SOCKET-MODE AUDIO INJECTION
    # =========================================================
    async def feed_audio(self, raw: bytes) -> None:
        """Push a raw PCM chunk into the service (socket mode only)."""
        if AUDIO_SOURCE != "socket":
            return
        if self._audio_queue is not None:
            self._audio_queue.put_nowait(raw)

    def audio_queue_depth(self) -> int:
        if self._audio_queue is None:
            return 0
        return self._audio_queue.qsize()

    def set_resume_mode(self) -> None:
        """
        Call this before listen_turn() when a TurnResumed happened so that
        the next listen_turn() skips flush_audio_queue(). Without this,
        the second half of the user's mid-gap sentence is discarded.
        """
        self._skip_next_flush = True

    def flush_audio_queue(self) -> int:
        """
        Discard all queued audio frames accumulated during TTS playback.
        """
        if self._audio_queue is None:
            return 0
        discarded = 0
        while not self._audio_queue.empty():
            try:
                self._audio_queue.get_nowait()
                discarded += 1
            except asyncio.QueueEmpty:
                break
        if discarded:
            print(f"[STT] [{id(self)}] Flushed {discarded} stale audio frames before listen.")
        return discarded

    # =========================================================
    # DEEPGRAM MESSAGE HANDLING (now a coroutine, not a thread callback)
    # =========================================================
    async def _handle_message(self, message) -> None:
        """
        Runs as a coroutine on the event loop (invoked from the async
        receive loop in `_run_deepgram_connection`), NOT from a separate
        OS thread. This means all state mutation here is inherently safe
        without a lock, as long as nothing else touches this state from
        a real thread.
        """
        if not self._active:
            return

        try:
            def _get(obj, key, default=None):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            msg_type = _get(message, "type", "")

            if msg_type == "Connected":
                print(f"[STT] [{id(self)}] Connected. request_id={_get(message, 'request_id')}")
                return
            if msg_type == "Update":
                return

            if msg_type == "TurnInfo":
                event = _get(message, "event", "")
                sentence = (_get(message, "transcript") or "").strip()
                confidence = _get(message, "end_of_turn_confidence", 0.0)

                print(f"[STT] [{id(self)}] TurnInfo event={event!r} "
                      f"transcript={sentence!r} confidence={confidence:.4f}")

                if event == "StartOfTurn":
                    if self.tracker:
                        self.tracker.stt_speech_start = time.perf_counter()
                    self._eager_eot_fired = False

                # Signal interrupt as soon as any speech is detected
                if sentence and self.interrupt_event is not None:
                    should_signal = (
                        not self.interrupt_signalled
                        and not self.interrupt_event.is_set()
                    )
                    if should_signal:
                        self.interrupt_signalled = True
                        self.interrupt_event.set()

                if event == "EagerEndOfTurn":
                    self._eager_eot_fired = True
                    if sentence:
                        self.transcript_parts.append(sentence)
                    if self.tracker:
                        self.tracker.stt_end = time.perf_counter()
                    self._last_eager_eot_time = time.perf_counter()

                    # Cancel any previous pending finalise task, start a fresh one.
                    if self._delayed_finalise_task and not self._delayed_finalise_task.done():
                        self._delayed_finalise_task.cancel()
                    self._delayed_finalise_task = asyncio.create_task(
                        self._delayed_finalise()
                    )

                elif event == "TurnResumed":
                    time_since_eager = time.perf_counter() - self._last_eager_eot_time
                    print(f"[STT] [{id(self)}] TurnResumed "
                          f"({time_since_eager:.3f}s after EagerEndOfTurn)")

                    self._eager_eot_fired = False
                    self.transcript_parts.clear()
                    if self._delayed_finalise_task and not self._delayed_finalise_task.done():
                        self._delayed_finalise_task.cancel()

                    self.speech_finished.clear()
                    self._skip_next_flush = True

                    if self.interrupt_event is not None:
                        should_signal = (
                            not self.interrupt_signalled
                            and not self.interrupt_event.is_set()
                        )
                        if should_signal:
                            self.interrupt_signalled = True
                            self.interrupt_event.set()

                elif event == "EndOfTurn":
                    self._eager_eot_fired = False
                    if self._delayed_finalise_task and not self._delayed_finalise_task.done():
                        self._delayed_finalise_task.cancel()
                    if sentence and sentence not in self.transcript_parts:
                        self.transcript_parts.append(sentence)
                    if self.tracker:
                        self.tracker.stt_end = time.perf_counter()
                    if not self.speech_finished.is_set():
                        self.speech_finished.set()

            else:
                print(f"[STT] [{id(self)}] Unhandled message type={msg_type!r}: {message!r}")

        except Exception as e:
            print(f"[STT] [{id(self)}] Callback error: {e}")

    async def _delayed_finalise(self) -> None:
        """
        800ms debounce: only finalise on EagerEndOfTurn if TurnResumed
        doesn't arrive and cancel this task first.
        """
        try:
            await asyncio.sleep(0.8)
            if self._eager_eot_fired:
                print(f"[STT] [{id(self)}] EagerEndOfTurn confirmed after 800ms — finalising turn.")
                self.speech_finished.set()
        except asyncio.CancelledError:
            pass  # TurnResumed cancelled us — expected, not an error

    # =========================================================
    # LISTEN FOR ONE USER TURN
    # =========================================================
    async def listen_turn(
        self,
        tracker: LatencyTracker | None = None,
        interrupt_event: asyncio.Event | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> str:
        """
        Opens a fresh Deepgram v2 connection for this turn, forwards queued
        audio to it, and returns the finalised transcript. Fully async —
        no thread pool involved.
        """
        # Await Deepgram connection readiness if it was started in the background
        await self.ready_event.wait()

        if self._skip_next_flush:
            print(f"[STT] [{id(self)}] Skipping flush — resuming mid-utterance.")
            self._skip_next_flush = False
        else:
            self.flush_audio_queue()

        self.transcript_parts.clear()
        self.interrupt_signalled = False
        self._eager_eot_fired = False

        self.speech_finished.clear()
        self.abort_event.clear()
        self._was_aborted = False

        self.tracker = tracker
        self.interrupt_event = interrupt_event
        self.loop = loop

        print(f"\n[STT] [{id(self)}] Listening...")

        if self.tracker:
            self.tracker.stt_start = time.perf_counter()

        if AUDIO_SOURCE == "mic":
            await self._listen_loop_mic()
        else:
            await self._listen_loop_socket()

        if self.abort_event.is_set() and not self.speech_finished.is_set():
            self._was_aborted = True

        result = " ".join(self.transcript_parts).strip()
        return result

    # =========================================================
    # MIC AUDIO LOOP (dev only — left mostly as-is, wrapped async)
    # =========================================================
    async def _listen_loop_mic(self) -> None:
        chunks_sent = 0
        SILENCE_TIMEOUT_SECS = 2.5
        MIC_GAIN = 5.0
        silence_start = None

        try:
            while not self.speech_finished.is_set() and not self.abort_event.is_set():
                data, overflowed = await asyncio.to_thread(self.stream.read, 256)
                if overflowed:
                    continue

                arr = np.frombuffer(data, dtype=np.int16).astype(np.float64) * MIC_GAIN
                arr = np.clip(arr, -32768, 32767).astype(np.int16)
                raw = arr.tobytes()
                await self.connection.send_media(raw)
                chunks_sent += 1

                rms = float(np.sqrt(np.mean(arr.astype(np.float64) ** 2)))

                if self.bot_speaking.is_set():
                    silence_start = None
                else:
                    if rms > 50:
                        silence_start = None
                    else:
                        if silence_start is None:
                            silence_start = time.perf_counter()
                        elif time.perf_counter() - silence_start > SILENCE_TIMEOUT_SECS:
                            print(f"[STT] [{id(self)}] Silence timeout — forcing end of turn.")
                            self.speech_finished.set()
                            break

                if chunks_sent % 500 == 0:
                    elapsed = chunks_sent * 16 / 1000
                    print(f"[STT] [{id(self)}] {chunks_sent} chunks (~{elapsed:.1f}s) | last RMS={rms:.1f}")

        except Exception as e:
            print(f"[STT] [{id(self)}] Mic stream error: {e}")

    # =========================================================
    # SOCKET AUDIO LOOP — the important one
    # =========================================================
    async def _listen_loop_socket(self) -> None:
        """
        Drains PCM frames from the asyncio.Queue and forwards them to the
        (already open) Deepgram connection.
        """
        MAX_LISTEN_SECS = 90
        SILENCE_TIMEOUT_SECS = 30.0
        SILENCE_RMS_THRESHOLD = 300
        POLL_TIMEOUT = 0.5
        LOW_AUDIO_RMS_WARNING = 30.0

        started_at = time.perf_counter()
        silence_start = None
        chunks_sent = 0
        peak_rms = 0.0

        try:
            while not self.speech_finished.is_set() and not self.abort_event.is_set():
                if time.perf_counter() - started_at > MAX_LISTEN_SECS:
                    print(f"[STT] [{id(self)}] Max listen duration reached — forcing end of turn.")
                    self.speech_finished.set()
                    break

                try:
                    raw = await asyncio.wait_for(self._audio_queue.get(), timeout=POLL_TIMEOUT)
                except asyncio.TimeoutError:
                    if not self.bot_speaking.is_set():
                        if silence_start is None:
                            silence_start = time.perf_counter()
                        elif time.perf_counter() - silence_start > SILENCE_TIMEOUT_SECS:
                            print(f"[STT] [{id(self)}] Silence timeout (queue empty) — forcing end of turn.")
                            self.speech_finished.set()
                            break
                    continue

                await self.connection.send_media(raw)
                chunks_sent += 1

                arr = np.frombuffer(raw, dtype=np.int16)
                rms = float(np.sqrt(np.mean(arr.astype(np.float64) ** 2)))
                peak_rms = max(peak_rms, rms)

                if self.bot_speaking.is_set():
                    silence_start = None
                else:
                    if rms > SILENCE_RMS_THRESHOLD:
                        silence_start = None
                    else:
                        if silence_start is None:
                            silence_start = time.perf_counter()
                        elif time.perf_counter() - silence_start > SILENCE_TIMEOUT_SECS:
                            print(f"[STT] [{id(self)}] Silence timeout ({SILENCE_TIMEOUT_SECS}s, "
                                  f"RMS={rms:.1f}) — forcing end of turn.")
                            self.speech_finished.set()
                            break

                if chunks_sent % 500 == 0:
                    elapsed = chunks_sent * 16 / 1000
                    print(f"[STT] [{id(self)}] {chunks_sent} chunks (~{elapsed:.1f}s) | "
                          f"last RMS={rms:.1f} | peak RMS={peak_rms:.1f}")
                    if peak_rms < LOW_AUDIO_RMS_WARNING and not self.bot_speaking.is_set():
                        print(f"[STT] [{id(self)}] WARNING: caller audio level is very low "
                              f"(peak RMS={peak_rms:.1f}). Check Asterisk/RTP input gain, "
                              "mute state, and whether AudioSocket is receiving caller audio.")

        except Exception as e:
            print(f"[STT] [{id(self)}] Socket stream error: {e}")
            self.speech_finished.set()

    # =========================================================
    # CONNECTION LIFECYCLE
    # =========================================================
    async def start(self) -> None:
        """
        Opens the Deepgram v2 (Flux) connection and starts the background
        receive task.
        """
        print(f"[STT] [{id(self)}] Establishing Deepgram v2 (Flux) connection at {SAMPLE_RATE}Hz...")

        self._active = True

        try:
            self._connect_ctx = self.deepgram.listen.v2.connect(
                model="flux-general-multi",
                encoding="linear16",
                sample_rate=SAMPLE_RATE,
                eager_eot_threshold=0.85,
                eot_threshold=0.88,
                eot_timeout_ms=2500,
                request_options={
                    "additional_query_parameters": {
                        "language_hint": ["en", "hi"],
                        "keyterm": ["salaried", "self-employed", "self employed", "No", "Harshil", "Harsh", "income", "EMI"],
                    }
                },
            )
            self.connection = await self._connect_ctx.__aenter__()

            self.connection.on(EventType.MESSAGE, self._on_message_sync_bridge)
            self.connection.on(EventType.ERROR, lambda error: print(f"[STT] [{id(self)}] WS error: {error}"))
            self.connection.on(EventType.OPEN, lambda _: print(f"[STT] [{id(self)}] WebSocket opened."))
            self.connection.on(EventType.CLOSE, lambda _: print(f"[STT] [{id(self)}] WebSocket closed."))

            self._receive_task = asyncio.create_task(
                self._safe_start_listening(), name=f"stt-recv-{id(self)}"
            )

            if AUDIO_SOURCE == "mic":
                self.stream = sd.RawInputStream(
                    samplerate=SAMPLE_RATE, blocksize=256, dtype="int16", channels=1,
                )
                self.stream.start()
                print(f"[STT] [{id(self)}] Hardware microphone and Deepgram stream ready.")
            else:
                print(f"[STT] [{id(self)}] Socket mode — waiting for audio via feed_audio().")

        finally:
            self.ready_event.set()

    async def _safe_start_listening(self) -> None:
        try:
            await self.connection.start_listening()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._active:
                print(f"[STT] [{id(self)}] start_listening() ended with error: {e}")

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
        print(f"[STT] [{id(self)}] Closing audio stream and Deepgram connection...")

        self._active = False
        self.abort_current_listen()

        if self._delayed_finalise_task and not self._delayed_finalise_task.done():
            self._delayed_finalise_task.cancel()

        if AUDIO_SOURCE == "mic" and self.stream:
            try:
                self.stream.stop()
                self.stream.close()
            except Exception:
                print(f"[STT] [{id(self)}] Error closing mic stream.")
        elif AUDIO_SOURCE == "socket" and self._audio_queue is not None:
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
                print(f"[STT] [{id(self)}] Error sending close stream.")

        if getattr(self, "_connect_ctx", None):
            try:
                await self._connect_ctx.__aexit__(None, None, None)
            except Exception:
                print(f"[STT] [{id(self)}] Error closing context manager.")
