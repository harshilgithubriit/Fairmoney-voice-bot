
# import asyncio
# import os
# import struct
# import time
# from typing import AsyncIterator

# from cartesia import AsyncCartesia
# from dotenv import load_dotenv

# from services.latency import LatencyTracker
# #from services.stt import AUDIO_SOURCE, SAMPLE_RATE
# from services.stt_opt import AUDIO_SOURCE,SAMPLE_RATE

# # ── Speaker output (mic mode only) ────────────────────────────────────────────
# if AUDIO_SOURCE == "mic":
#     import sounddevice as sd

# load_dotenv()

# # ── NO module-level _cartesia_client singleton. ────────────────────────────────
# # Previously a single AsyncCartesia instance was shared across all concurrent
# # calls, causing audio from one call to bleed into another.  We now create a
# # fresh client (and therefore a fresh websocket) inside every stream_tts()
# # call so each call is fully isolated.
# # ──────────────────────────────────────────────────────────────────────────────

# _CARTESIA_API_KEY = os.getenv("CARTESIA_API_KEY")

# _MODEL_ID = "sonic-3.5"
# _VOICE_ID = "faf0731e-dfb9-4cfc-8119-259a79b27e12"
# #"faf0731e-dfb9-4cfc-8119-259a79b27e12"
# #"910fb75e-1d20-4840-ac63-ac6b26a71bdc" -kevin
# #95d51f79-c397-46f9-b49a-23763d3eaa2d - arushi

# # Must match STT sample rate — confirmed 8kHz by loopback test
# _SAMPLE_RATE = SAMPLE_RATE

# FRAME_BYTES    = 320 if _SAMPLE_RATE == 8000 else 640   # bytes per 20 ms frame
# FRAME_INTERVAL = 0.020                                    # 20 ms in seconds

# _FRAME_KIND = {
#     8000:  0x10,
#     16000: 0x12,
# }.get(_SAMPLE_RATE)

# if _FRAME_KIND is None:
#     raise RuntimeError(
#         f"Unsupported _SAMPLE_RATE={_SAMPLE_RATE}. "
#         f"Must be 8000 (slin, 0x10) or 16000 (slin16, 0x12)."
#     )

# print(f"[TTS] Sample rate: {_SAMPLE_RATE} Hz | Frame: {FRAME_BYTES} bytes | "
#       f"Kind: 0x{_FRAME_KIND:02x}")


# def _make_audio_frame(pcm_bytes: bytes) -> bytes:
#     header = bytes([_FRAME_KIND]) + struct.pack(">H", len(pcm_bytes))
#     return header + pcm_bytes


# async def _pace_and_send(
#     writer: asyncio.StreamWriter,
#     pcm_buffer_ref: list,          # single-element list so we can mutate from caller
#     deadline_ref: list,            # single-element list holding the float deadline
#     interrupted_check,             # callable → bool
# ) -> None:
#     """
#     Drain FRAME_BYTES-sized chunks from pcm_buffer_ref and send them to
#     writer on a strict wall-clock schedule (one frame every FRAME_INTERVAL).

#     Using a deadline accumulator instead of sleep(FRAME_INTERVAL) prevents
#     drift: if drain() or the event loop takes longer than expected for one
#     frame, the next frame is sent sooner to compensate, keeping the long-run
#     average exactly right.
#     """
#     loop = asyncio.get_event_loop()

#     while len(pcm_buffer_ref[0]) >= FRAME_BYTES:
#         if interrupted_check():
#             return

#         chunk               = pcm_buffer_ref[0][:FRAME_BYTES]
#         pcm_buffer_ref[0]   = pcm_buffer_ref[0][FRAME_BYTES:]

#         writer.write(_make_audio_frame(chunk))
#         await writer.drain()

#         # Advance deadline by one frame interval, then sleep only the
#         # remaining time.  If we're already past the deadline (slow drain),
#         # sleep(0) yields to the event loop without over-sleeping.
#         deadline_ref[0] += FRAME_INTERVAL
#         sleep_for = deadline_ref[0] - loop.time()
#         if sleep_for > 0:
#             await asyncio.sleep(sleep_for)


# async def stream_tts(
#     text_stream: AsyncIterator[str],
#     tracker: LatencyTracker | None = None,
#     interrupt_event: asyncio.Event | None = None,
#     writer: asyncio.StreamWriter | None = None,
# ) -> bool:
#     """
#     Streams TTS audio to the caller.

#     A brand-new AsyncCartesia client (and therefore a brand-new websocket
#     connection) is created for every invocation.  This guarantees that
#     concurrent calls are fully isolated — audio for call A can never be
#     written to call B's writer.

#     Returns:
#         True  → completed normally
#         False → interrupted
#     """

#     interrupted = False

#     if AUDIO_SOURCE == "mic":
#         playback_stream = sd.RawOutputStream(
#             samplerate=_SAMPLE_RATE,
#             channels=1,
#             dtype="int16",
#         )
#         playback_stream.start()
#     else:
#         playback_stream = None

#     async def interrupt_watcher():
#         nonlocal interrupted

#         if interrupt_event is None:
#             return

#         await interrupt_event.wait()
#         interrupted = True

#         if playback_stream is not None:
#             try:
#                 playback_stream.abort()
#             except Exception:
#                 pass

#     # ── Per-call Cartesia client ───────────────────────────────────────────────
#     # This is the key fix: each call to stream_tts() gets its own client and
#     # its own websocket, so concurrent calls never share an audio channel.
#     cartesia_client = AsyncCartesia(api_key=_CARTESIA_API_KEY)
#     # ─────────────────────────────────────────────────────────────────────────

#     async with cartesia_client.tts.websocket_connect() as ws:

#         ctx = ws.context(
#             model_id=_MODEL_ID,
#             voice={"mode": "id", "id": _VOICE_ID},
#             output_format={
#                 "container": "raw",
#                 "encoding": "pcm_s16le",
#                 "sample_rate": _SAMPLE_RATE,
#             },
#         )

#         tts_started = False

#         async def text_producer():
#             nonlocal tts_started

#             try:
#                 async for text_chunk in text_stream:
#                     if interrupted:
#                         break

#                     if not tts_started:
#                         if tracker:
#                             tracker.tts_start = time.perf_counter()
#                         tts_started = True

#                     await ctx.push(text_chunk)

#                 await ctx.no_more_inputs()

#             except Exception as e:
#                 if not interrupted:
#                     print(f"\n[TTS text_producer error]: {e}")

#         async def audio_consumer():
#             first_audio   = True
#             pcm_buffer    = [b""]          # mutable ref for _pace_and_send
#             loop          = asyncio.get_event_loop()

#             # Deadline is initialised on the first frame so we don't start
#             # counting until audio actually arrives from Cartesia.
#             deadline      = [0.0]
#             deadline_set  = False

#             try:
#                 async for response in ctx.receive():

#                     if interrupted:
#                         return

#                     if response.type == "chunk" and response.audio:

#                         if first_audio:
#                             if tracker:
#                                 tracker.tts_first_audio = time.perf_counter()
#                             first_audio = False

#                         if AUDIO_SOURCE == "mic":
#                             await asyncio.to_thread(
#                                 playback_stream.write,
#                                 response.audio,
#                             )

#                         elif writer is not None:
#                             pcm_buffer[0] += response.audio

#                             # Set the deadline anchor once, on the first chunk.
#                             if not deadline_set:
#                                 deadline[0]  = loop.time()
#                                 deadline_set = True

#                             # Send all complete frames now, paced to wall clock.
#                             await _pace_and_send(
#                                 writer,
#                                 pcm_buffer,
#                                 deadline,
#                                 lambda: interrupted,
#                             )

#                     if interrupted:
#                         return

#                 # ── All Cartesia chunks received ──────────────────────────
#                 # Send any leftover bytes.  If the remainder is at least half
#                 # a frame, pad and send it; otherwise discard — a very short
#                 # fragment causes a click worse than silence.
#                 if (
#                     AUDIO_SOURCE != "mic"
#                     and writer is not None
#                     and not interrupted
#                 ):
#                     remainder = pcm_buffer[0]
#                     if len(remainder) >= FRAME_BYTES // 2:
#                         pad           = FRAME_BYTES - len(remainder)
#                         padded        = remainder + b"\x00" * pad
#                         pcm_buffer[0] = padded
#                         await _pace_and_send(
#                             writer,
#                             pcm_buffer,
#                             deadline,
#                             lambda: interrupted,
#                         )
#                     # else: too short — discard silently

#                 if tracker and not interrupted:
#                     tracker.tts_end = time.perf_counter()

#             except asyncio.CancelledError:
#                 raise

#             except Exception as e:
#                 if not interrupted:
#                     print(f"\n[TTS audio_consumer error]: {e}")

#         watcher_task  = asyncio.create_task(interrupt_watcher())
#         producer_task = asyncio.create_task(text_producer())
#         consumer_task = asyncio.create_task(audio_consumer())

#         try:
#             await asyncio.gather(producer_task, consumer_task)

#         except asyncio.CancelledError:
#             producer_task.cancel()
#             consumer_task.cancel()

#             for task in (producer_task, consumer_task):
#                 try:
#                     await task
#                 except (asyncio.CancelledError, Exception):
#                     pass

#             raise

#         except Exception:
#             pass

#         finally:
#             watcher_task.cancel()
#             try:
#                 await watcher_task
#             except asyncio.CancelledError:
#                 pass

#         if interrupted:
#             for task in (producer_task, consumer_task):
#                 if not task.done():
#                     task.cancel()
#                     try:
#                         await task
#                     except (asyncio.CancelledError, Exception):
#                         pass

#     # ── Explicit client close ─────────────────────────────────────────────────
#     # Ensures the underlying httpx client is released and doesn't linger.
#     try:
#         await cartesia_client.close()
#     except Exception:
#         pass
#     # ─────────────────────────────────────────────────────────────────────────

#     if playback_stream is not None:
#         try:
#             if interrupted:
#                 playback_stream.abort()
#             else:
#                 playback_stream.stop()
#         except Exception:
#             pass

#         try:
#             playback_stream.close()
#         except Exception:
#             pass


#     return not interrupted




import asyncio
import os
import struct
import time
from typing import AsyncIterator

from cartesia import AsyncCartesia
from dotenv import load_dotenv

from services.latency import LatencyTracker
from services.stt_opt import AUDIO_SOURCE, SAMPLE_RATE

# ── Speaker output (mic mode only) ────────────────────────────────────────────
if AUDIO_SOURCE == "mic":
    import sounddevice as sd

load_dotenv()

# ── NO module-level _cartesia_client singleton. ────────────────────────────────
# Previously a single AsyncCartesia instance was shared across all concurrent
# calls, causing audio from one call to bleed into another. We now create a
# fresh client (and therefore a fresh websocket) inside every stream_tts()
# call so each call is fully isolated.
# ──────────────────────────────────────────────────────────────────────────────

_CARTESIA_API_KEY = os.getenv("CARTESIA_API_KEY")

_MODEL_ID = "sonic-3.5"

# ── Per-language voice map ───────────────────────────────────────────────────
# REPLACE every "REPLACE_..." value below with a real voice ID from
# https://play.cartesia.ai (filter by language, copy the voice's UUID).
# Fake/placeholder IDs WILL fail at runtime with a Cartesia API error —
# don't leave them unset before testing a given language.
_VOICE_BY_LANGUAGE = {
    # Human-sounding voices picked via extra/list_cartesia_voices.py.
    "en": "faf0731e-dfb9-4cfc-8119-259a79b27e12",  # previous female voice (user preference)
    "hi": "faf0731e-dfb9-4cfc-8119-259a79b27e12",
    "ta": "80e4e2b3-ec54-4930-97ac-667eba950352",
    "te": "76961778-5ce4-4aa9-9cdf-66a029d61a8f",
    "bn": "2ba861ea-7cdc-43d1-8608-4045b5a41de5", #male
    "mr": "5c32dce6-936a-4892-b131-bafe474afe5f",
    "gu": "4590a461-bc68-4a50-8d14-ac04f5923d22",
    "kn": "7c6219d2-e8d2-462c-89d8-7ecba7c75d65",
    # "pa" / "ml": add once Deepgram STT support is confirmed for them —
    # Cartesia itself does support both on the TTS side.
}
_DEFAULT_LANGUAGE = "en"


def _resolve_voice_id(language: str) -> str:
    """
    Falls back to English's voice if the requested language has no voice
    configured yet, rather than crashing the call — logs loudly so it's
    obvious in call logs that a language is unconfigured.
    """
    voice_id = _VOICE_BY_LANGUAGE.get(language)
    if not voice_id or voice_id.startswith("REPLACE_"):
        print(f"[TTS] WARNING: no voice configured for language={language!r} — "
              f"falling back to {_DEFAULT_LANGUAGE}. Set a real voice ID in "
              f"_VOICE_BY_LANGUAGE before relying on this language in production.")
        return _VOICE_BY_LANGUAGE[_DEFAULT_LANGUAGE]
    return voice_id


# Must match STT sample rate — confirmed 8kHz by loopback test
_SAMPLE_RATE = SAMPLE_RATE

FRAME_BYTES    = 320 if _SAMPLE_RATE == 8000 else 640   # bytes per 20 ms frame
FRAME_INTERVAL = 0.020                                    # 20 ms in seconds

_FRAME_KIND = {
    8000:  0x10,
    16000: 0x12,
}.get(_SAMPLE_RATE)

if _FRAME_KIND is None:
    raise RuntimeError(
        f"Unsupported _SAMPLE_RATE={_SAMPLE_RATE}. "
        f"Must be 8000 (slin, 0x10) or 16000 (slin16, 0x12)."
    )

print(f"[TTS] Sample rate: {_SAMPLE_RATE} Hz | Frame: {FRAME_BYTES} bytes | "
      f"Kind: 0x{_FRAME_KIND:02x}")


def _make_audio_frame(pcm_bytes: bytes) -> bytes:
    header = bytes([_FRAME_KIND]) + struct.pack(">H", len(pcm_bytes))
    return header + pcm_bytes


async def _pace_and_send(
    writer: asyncio.StreamWriter,
    pcm_buffer_ref: list,          # single-element list so we can mutate from caller
    deadline_ref: list,            # single-element list holding the float deadline
    interrupted_check,             # callable → bool
) -> None:
    """
    Drain FRAME_BYTES-sized chunks from pcm_buffer_ref and send them to
    writer on a strict wall-clock schedule (one frame every FRAME_INTERVAL).

    Using a deadline accumulator instead of sleep(FRAME_INTERVAL) prevents
    drift: if drain() or the event loop takes longer than expected for one
    frame, the next frame is sent sooner to compensate, keeping the long-run
    average exactly right.
    """
    loop = asyncio.get_event_loop()

    while len(pcm_buffer_ref[0]) >= FRAME_BYTES:
        if interrupted_check():
            return

        chunk               = pcm_buffer_ref[0][:FRAME_BYTES]
        pcm_buffer_ref[0]   = pcm_buffer_ref[0][FRAME_BYTES:]

        writer.write(_make_audio_frame(chunk))
        await writer.drain()

        # Advance deadline by one frame interval, then sleep only the
        # remaining time. If we're already past the deadline (slow drain),
        # sleep(0) yields to the event loop without over-sleeping.
        deadline_ref[0] += FRAME_INTERVAL
        sleep_for = deadline_ref[0] - loop.time()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)


async def stream_tts(
    text_stream: AsyncIterator[str],
    tracker: LatencyTracker | None = None,
    interrupt_event: asyncio.Event | None = None,
    writer: asyncio.StreamWriter | None = None,
    language: str = _DEFAULT_LANGUAGE,
) -> bool:
    """
    Streams TTS audio to the caller.

    A brand-new AsyncCartesia client (and therefore a brand-new websocket
    connection) is created for every invocation. This guarantees that
    concurrent calls are fully isolated — audio for call A can never be
    written to call B's writer.

    `language` should be the same code you passed to get_stt_service() for
    this call (e.g. "hi", "ta", "mr") so STT and TTS agree on the language
    for the whole session. Determines both the Cartesia `language` field
    (affects pronunciation/prosody) and which voice is used.

    Returns:
        True  → completed normally
        False → interrupted
    """

    interrupted = False
    voice_id = _resolve_voice_id(language)

    if AUDIO_SOURCE == "mic":
        playback_stream = sd.RawOutputStream(
            samplerate=_SAMPLE_RATE,
            channels=1,
            dtype="int16",
        )
        playback_stream.start()
    else:
        playback_stream = None

    async def interrupt_watcher():
        nonlocal interrupted

        if interrupt_event is None:
            return

        await interrupt_event.wait()
        interrupted = True

        if playback_stream is not None:
            try:
                playback_stream.abort()
            except Exception:
                pass

    # ── Per-call Cartesia client ───────────────────────────────────────────────
    # This is the key fix: each call to stream_tts() gets its own client and
    # its own websocket, so concurrent calls never share an audio channel.
    cartesia_client = AsyncCartesia(api_key=_CARTESIA_API_KEY)
    # ─────────────────────────────────────────────────────────────────────────

    async with cartesia_client.tts.websocket_connect() as ws:

        ctx = ws.context(
            model_id=_MODEL_ID,
            voice={"mode": "id", "id": voice_id},
            language=language,
            #generation_config={"speed": 1.50},
            output_format={
                "container": "raw",
                "encoding": "pcm_s16le",
                "sample_rate": _SAMPLE_RATE,
            },
        )

        tts_started = False

        async def text_producer():
            nonlocal tts_started

            try:
                async for text_chunk in text_stream:
                    if interrupted:
                        break

                    if not tts_started:
                        if tracker:
                            tracker.tts_start = time.perf_counter()
                        tts_started = True

                    await ctx.push(text_chunk)

                await ctx.no_more_inputs()

            except Exception as e:
                if not interrupted:
                    print(f"\n[TTS text_producer error]: {e}")

        async def audio_consumer():
            first_audio   = True
            pcm_buffer    = [b""]          # mutable ref for _pace_and_send
            loop          = asyncio.get_event_loop()

            # Deadline is initialised on the first frame so we don't start
            # counting until audio actually arrives from Cartesia.
            deadline      = [0.0]
            deadline_set  = False

            try:
                async for response in ctx.receive():

                    if interrupted:
                        return

                    if response.type == "chunk" and response.audio:

                        if first_audio:
                            if tracker:
                                tracker.tts_first_audio = time.perf_counter()
                            first_audio = False

                        if AUDIO_SOURCE == "mic":
                            await asyncio.to_thread(
                                playback_stream.write,
                                response.audio,
                            )

                        elif writer is not None:
                            pcm_buffer[0] += response.audio

                            # Set the deadline anchor once, on the first chunk.
                            if not deadline_set:
                                deadline[0]  = loop.time()
                                deadline_set = True

                            # Send all complete frames now, paced to wall clock.
                            await _pace_and_send(
                                writer,
                                pcm_buffer,
                                deadline,
                                lambda: interrupted,
                            )

                    if interrupted:
                        return

                # ── All Cartesia chunks received ──────────────────────────
                # Send any leftover bytes. If the remainder is at least half
                # a frame, pad and send it; otherwise discard — a very short
                # fragment causes a click worse than silence.
                if (
                    AUDIO_SOURCE != "mic"
                    and writer is not None
                    and not interrupted
                ):
                    remainder = pcm_buffer[0]
                    if len(remainder) >= FRAME_BYTES // 2:
                        pad           = FRAME_BYTES - len(remainder)
                        padded        = remainder + b"\x00" * pad
                        pcm_buffer[0] = padded
                        await _pace_and_send(
                            writer,
                            pcm_buffer,
                            deadline,
                            lambda: interrupted,
                        )
                    # else: too short — discard silently

                if tracker and not interrupted:
                    tracker.tts_end = time.perf_counter()

            except asyncio.CancelledError:
                raise

            except Exception as e:
                if not interrupted:
                    print(f"\n[TTS audio_consumer error]: {e}")

        watcher_task  = asyncio.create_task(interrupt_watcher())
        producer_task = asyncio.create_task(text_producer())
        consumer_task = asyncio.create_task(audio_consumer())

        try:
            await asyncio.gather(producer_task, consumer_task)

        except asyncio.CancelledError:
            producer_task.cancel()
            consumer_task.cancel()

            for task in (producer_task, consumer_task):
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

            raise

        except Exception:
            pass

        finally:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

        if interrupted:
            for task in (producer_task, consumer_task):
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass

    # ── Explicit client close ─────────────────────────────────────────────────
    # Ensures the underlying httpx client is released and doesn't linger.
    try:
        await cartesia_client.close()
    except Exception:
        pass
    # ─────────────────────────────────────────────────────────────────────────

    if playback_stream is not None:
        try:
            if interrupted:
                playback_stream.abort()
            else:
                playback_stream.stop()
        except Exception:
            pass

        try:
            playback_stream.close()
        except Exception:
            pass

    return not interrupted