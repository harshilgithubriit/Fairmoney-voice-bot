"""
load_test_conversation.py

Extends load_test_audiosocket.py's single-shot test into a repeated,
turn-aware conversation per simulated call — closer to a real 5-minute
call than one WAV played once.

HOW TURN-TAKING WORKS HERE
────────────────────────────
After sending one "utterance" (a chunk of your sample WAV), the simulated
caller waits until the bot has FINISHED talking back (detected as: audio
was received, then a gap of `SILENCE_GAP_SECS` with no audio) before
sending its next utterance. This mimics real turn-taking instead of
barging over the bot on a fixed timer, which would trigger your interrupt
logic on every single turn — a different (also valid, but different) test.

This does NOT understand what the bot said — it's still just replaying
your sample clip on each turn. That's an accepted limitation: full
semantic back-and-forth would require running real STT on YOUR test
client too. This tests infrastructure survival (memory, task count,
connection stability, STT/TTS/LLM pipeline health) under sustained
multi-turn load — not conversational correctness. Use real test calls
for correctness; use this for "does it fall over after 5 minutes at 75
concurrent calls."

USAGE
─────
  python load_test_conversation.py --wav sample.wav --calls 75 --ramp 8 \
      --call-duration 300 --turn-gap 6
"""

import argparse
import asyncio
import struct
import time
import uuid
import wave
import statistics
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("loadtest-conv")

KIND_UUID = 0x01
KIND_AUDIO_8 = 0x10
KIND_AUDIO_16 = 0x12
KIND_HANGUP = 0x00

FRAME_INTERVAL = 0.020  # 20ms


def make_frame(kind: int, payload: bytes) -> bytes:
    return bytes([kind]) + struct.pack(">H", len(payload)) + payload


def load_wav_frames(path: str, frame_bytes: int) -> tuple[list[bytes], int]:
    with wave.open(path, "rb") as wf:
        sample_rate = wf.getframerate()
        sampwidth = wf.getsampwidth()
        channels = wf.getnchannels()
        pcm = wf.readframes(wf.getnframes())

    if sampwidth != 2 or channels != 1:
        raise ValueError(
            f"WAV must be 16-bit mono PCM. Got sampwidth={sampwidth} channels={channels}."
        )

    frames = []
    for i in range(0, len(pcm) - frame_bytes + 1, frame_bytes):
        frames.append(pcm[i:i + frame_bytes])

    return frames, sample_rate


class CallResult:
    def __init__(self, call_id: str):
        self.call_id = call_id
        self.connected = False
        self.turns_completed = 0
        self.total_bot_audio_bytes = 0
        self.turn_latencies: list[float] = []   # time from end-of-our-speech to first bot audio byte
        self.error: str | None = None
        self.start_time = time.perf_counter()
        self.end_time: float | None = None

    def duration(self) -> float:
        return (self.end_time or time.perf_counter()) - self.start_time


async def simulate_conversation(
    host: str,
    port: int,
    wav_frames: list[bytes],
    frame_kind: int,
    call_duration: float,
    turn_gap: float,
    initial_greeting_wait: float,
    result: CallResult,
    max_turn_wait: float = 45.0,
) -> None:
    reader = None
    writer = None

    # Shared mutable state between the reader task and the main loop.
    last_bot_audio_at = [None]     # perf_counter() timestamp of last bot audio byte received
    bot_currently_speaking = [False]
    pending_first_byte_ts = [None]  # set when we start waiting for a reply

    async def reader_task():
        try:
            while True:
                header = await reader.readexactly(3)
                kind = header[0]
                length = struct.unpack(">H", header[1:3])[0]
                payload = await reader.readexactly(length) if length else b""

                if kind in (KIND_AUDIO_8, KIND_AUDIO_16):
                    now = time.perf_counter()
                    if pending_first_byte_ts[0] is not None:
                        result.turn_latencies.append(now - pending_first_byte_ts[0])
                        pending_first_byte_ts[0] = None
                    last_bot_audio_at[0] = now
                    bot_currently_speaking[0] = True
                    result.total_bot_audio_bytes += len(payload)
                elif kind == KIND_HANGUP:
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except asyncio.CancelledError:
            pass

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=5.0
        )
        result.connected = True

        call_uuid = uuid.uuid4().bytes
        writer.write(make_frame(KIND_UUID, call_uuid))
        await writer.drain()

        read_bg = asyncio.create_task(reader_task())

        # Let the bot's opening greeting play out before our first turn.
        await asyncio.sleep(initial_greeting_wait)

        call_deadline = time.perf_counter() + call_duration

        while time.perf_counter() < call_deadline:
            # ── Send one "utterance" (our sample clip) ──────────────────
            pending_first_byte_ts[0] = time.perf_counter()
            deadline = time.perf_counter()
            for chunk in wav_frames:
                if time.perf_counter() >= call_deadline:
                    break
                writer.write(make_frame(frame_kind, chunk))
                await writer.drain()
                deadline += FRAME_INTERVAL
                sleep_for = deadline - time.perf_counter()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)

            result.turns_completed += 1

            # ── Wait for the bot to reply AND finish replying ───────────
            # "Finished" = no bot audio for `turn_gap` seconds straight.
            wait_start = time.perf_counter()
            while time.perf_counter() < call_deadline:
                await asyncio.sleep(0.5)
                if (
                    last_bot_audio_at[0] is not None
                    and time.perf_counter() - last_bot_audio_at[0] > turn_gap
                ):
                    break
                if time.perf_counter() - wait_start > max_turn_wait:
                    # Safety valve: don't wait forever for a bot reply
                    # that may never come. IMPORTANT: this must be set
                    # LONGER than the server's own forced-silence timeout
                    # (30s for Flux, per SILENCE_TIMEOUT_SECS in stt.py) —
                    # otherwise this client can resend audio right as the
                    # server's timeout is about to fire, resetting the
                    # server's silence clock and preventing the turn from
                    # EVER finalizing. That produces an ever-growing,
                    # never-finalizing transcript exactly like you'd see
                    # from a stuck listen_turn() call.
                    log.warning(f"[{result.call_id}] No bot reply detected "
                                f"within {max_turn_wait:.0f}s of a turn — moving on.")
                    break
            last_bot_audio_at[0] = None

        # ── Hangup ────────────────────────────────────────────────────
        writer.write(make_frame(KIND_HANGUP, b""))
        await writer.drain()

        read_bg.cancel()
        try:
            await read_bg
        except asyncio.CancelledError:
            pass

    except Exception as e:
        result.error = f"{type(e).__name__}: {e}"

    finally:
        result.end_time = time.perf_counter()
        if writer is not None:
            try:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
            except Exception:
                pass


async def run_load_test(
    host: str,
    port: int,
    wav_path: str,
    num_calls: int,
    ramp_per_sec: float,
    call_duration: float,
    turn_gap: float,
    initial_greeting_wait: float,
    max_turn_wait: float,
) -> None:
    frame_bytes = 320
    wav_frames, sample_rate = load_wav_frames(wav_path, frame_bytes)
    frame_kind = KIND_AUDIO_8 if sample_rate == 8000 else KIND_AUDIO_16

    log.info(f"Loaded WAV: {len(wav_frames)} frames (~{len(wav_frames) * 0.02:.1f}s per turn)")
    log.info(f"Starting conversation load test: {num_calls} calls, ramp={ramp_per_sec}/sec, "
             f"call_duration={call_duration}s, target={host}:{port}")

    results: list[CallResult] = []
    tasks = []

    for i in range(num_calls):
        result = CallResult(call_id=f"loadtest-{i}")
        results.append(result)
        task = asyncio.create_task(
            simulate_conversation(
                host, port, wav_frames, frame_kind,
                call_duration, turn_gap, initial_greeting_wait, result,
                max_turn_wait=max_turn_wait,
            )
        )
        tasks.append(task)

        if ramp_per_sec > 0 and i < num_calls - 1:
            await asyncio.sleep(1.0 / ramp_per_sec)

    log.info(f"All {num_calls} calls launched — running for up to {call_duration:.0f}s each...")
    await asyncio.gather(*tasks, return_exceptions=True)

    connected = sum(1 for r in results if r.connected)
    errors = [r for r in results if r.error]
    total_turns = sum(r.turns_completed for r in results)
    all_latencies = [lat for r in results for lat in r.turn_latencies]

    print("\n" + "=" * 60)
    print("CONVERSATION LOAD TEST RESULTS")
    print("=" * 60)
    print(f"Calls attempted:            {num_calls}")
    print(f"Connected successfully:     {connected}/{num_calls}")
    print(f"Errors:                     {len(errors)}/{num_calls}")
    print(f"Total turns completed:      {total_turns}")
    print(f"Avg turns per call:         {total_turns / max(connected, 1):.1f}")

    if all_latencies:
        print(f"\nPer-turn latency (our speech end -> first bot audio byte):")
        print(f"  min:    {min(all_latencies):.2f}s")
        print(f"  median: {statistics.median(all_latencies):.2f}s")
        print(f"  max:    {max(all_latencies):.2f}s")
        if len(all_latencies) > 1:
            print(f"  stdev:  {statistics.stdev(all_latencies):.2f}s")
        print(f"  ({len(all_latencies)} turns measured)")

    if errors:
        print(f"\nFirst {min(5, len(errors))} errors seen:")
        for r in errors[:5]:
            print(f"  [{r.call_id}] {r.error}")

    print("\n" + "-" * 60)
    print("Watch server-side CPU/memory (Task Manager) DURING this run, not just")
    print("after — a slow memory climb or rising per-turn latency across the")
    print("run's own duration is the real signal, not just pass/fail counts.")
    print("Also check Deepgram/Cartesia dashboards for rate-limit/connection")
    print("errors that won't show up as Python exceptions here.")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Multi-turn conversation load test against audiosocket_server.py"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9092)
    parser.add_argument("--wav", required=True, help="Path to 8kHz 16-bit mono PCM WAV file")
    parser.add_argument("--calls", type=int, default=10)
    parser.add_argument("--ramp", type=float, default=5.0,
                         help="Calls started per second (0 = launch instantly)")
    parser.add_argument("--call-duration", type=float, default=300.0,
                         help="Total simulated call length in seconds (default: 300 = 5 min)")
    parser.add_argument("--turn-gap", type=float, default=6.0,
                         help="Seconds of bot silence that mark 'bot finished replying' "
                              "before we send the next turn (default: 6.0 — tune to your "
                              "TTS reply length; too short and you'll barge in mid-reply)")
    parser.add_argument("--greeting-wait", type=float, default=6.0,
                         help="Seconds to wait after connecting before first turn, so we "
                              "don't barge in on the bot's opening greeting (default: 6.0)")
    parser.add_argument("--max-turn-wait", type=float, default=45.0,
                         help="Seconds to wait for a bot reply before giving up and moving "
                              "to the next turn (default: 45.0). MUST be longer than your "
                              "STT service's own forced-silence timeout (30s for Flux) or "
                              "you'll race it and turns may never finalize.")
    args = parser.parse_args()

    asyncio.run(
        run_load_test(
            args.host, args.port, args.wav, args.calls, args.ramp,
            args.call_duration, args.turn_gap, args.greeting_wait,
            args.max_turn_wait,
        )
    )


if __name__ == "__main__":
    main()