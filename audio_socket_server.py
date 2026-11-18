from logging_config import setup_logging
setup_logging()

import asyncio
import struct
import uuid
import time

import logging
#from services.stt import AUDIO_SOURCE
from services.stt_opt import AUDIO_SOURCE
# Fairmoney project: this import line is the ONLY bot switch point.
# Old project pointed at bajaj_top_up_main; this project points at fairmoney_main.
from fairmoney_main import session_manager, handle_call, pending_customer_info

from config.settings import (
    AUDIOSOCKET_HOST as HOST,
    AUDIOSOCKET_PORT as PORT,
    MAX_CONCURRENT_CALLS,
    AUDIO_QUEUE_MAX,
    GRACEFUL_SHUTDOWN_SECS as GRACEFUL_SHUTDOWN_GRACE_SECS,
)

KIND_UUID = 0x01
KIND_AUDIO_8 = 0x10
KIND_AUDIO_16 = 0x12
KIND_HANGUP = 0x00

AUDIO_KINDS = {KIND_AUDIO_8, KIND_AUDIO_16}
_call_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CALLS)
_active_calls = 0
_active_calls_lock = asyncio.Lock()


# logging.basicConfig(
#     level=logging.INFO,
#     format="[%(asctime)s] %(levelname)s %(message)s",
#     datefmt="%H:%M:%S",
# )
log = logging.getLogger("audiosocket")


# =============================================================================
# FRAME READER
# =============================================================================
async def read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes] | None:
    try:
        header = await reader.readexactly(3)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None

    kind = header[0]
    length = struct.unpack(">H", header[1:3])[0]

    if length == 0:
        log.warning("no payload recieved")
        return kind, b""

    try:
        payload = await reader.readexactly(length)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None

    return kind, payload


# =============================================================================
# PER-CALL HANDLER
# =============================================================================

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    global _active_calls

    peer = writer.get_extra_info("peername")
    asterisk_id = None
    call_task = None
    loop = asyncio.get_running_loop()
    frames_fed = 0
    frames_dropped = 0
    acquired_semaphore = False

    log.info(f"New connection from {peer}")

    try:
        # ── Step 1: read UUID frame ───────────────────────────────────────
        while True:
            frame = await read_frame(reader)
            if frame is None:
                log.warning("Connection closed before UUID frame received.")
                return

            kind, payload = frame

            if kind == KIND_UUID:
                if len(payload) != 16:
                    log.error("asterisk id could not be captured")
                    return
                try:
                    asterisk_id = str(uuid.UUID(bytes=payload))
                except (ValueError, struct.error) as e:
                    log.error(f"Could not parse UUID from Asterisk: {e} — closing connection.")
                    return

                log.info(f"Asterisk call UUID: {asterisk_id}")
                break

            elif kind == KIND_HANGUP:
                log.info("Hangup before UUID — ignoring.")
                return
            else:
                log.debug(f"Early frame kind=0x{kind:02x} ({len(payload)} bytes) — discarded.")

        # ── Step 1.5: concurrency gate ─────────────────────────────────────
        # If we're at capacity, reject cleanly rather than accepting a call
        # we can't actually serve well. Asterisk dialplan should play a
        # busy message / fall back to a queue on connection failure.
        if _call_semaphore.locked():
            log.warning(
                f"[{asterisk_id}] At capacity ({MAX_CONCURRENT_CALLS} concurrent) — "
                f"rejecting call."
            )
            return
        await _call_semaphore.acquire()
        acquired_semaphore = True

        async with _active_calls_lock:
            _active_calls += 1
            log.info(f"[{asterisk_id}] Active concurrent calls: {_active_calls}/{MAX_CONCURRENT_CALLS}")

        # Pull customer data staged by fastagi_server (from CUSTOMER_* channel
        # vars) and pass it into handle_call so the bot uses the real dialer
        # customer instead of the dev fallback. Retries up to 250ms to prevent
        # race condition if AudioSocket connects before FastAGI stages data.
        cust_info = pending_customer_info.pop(asterisk_id, None)
        if cust_info is None:
            for _ in range(5):
                await asyncio.sleep(0.05)
                cust_info = pending_customer_info.pop(asterisk_id, None)
                if cust_info is not None:
                    break
        if cust_info is None:
            cust_info = {}

        call_task = asyncio.create_task(
            handle_call(
                asterisk_id,
                loop,
                writer=writer,
                customer_name=cust_info.get("customer_name", ""),
                customer_email=cust_info.get("customer_email", ""),
                customer_number=cust_info.get("customer_number", ""),
                due_amount=cust_info.get("due_amount", ""),
                lead_id=cust_info.get("lead_id", "0"),
                serviceid=cust_info.get("serviceid", "0"),
            ),
            name=f"call-{asterisk_id}",
        )
        log.info(
            f"[{asterisk_id}] Conversation task started (customer={cust_info.get('customer_name', '')!r})."
        )

        # ── Step 3: audio forwarding loop (Asterisk → STT) ───────────────
        while True:
            frame = await read_frame(reader)

            if frame is None:
                log.info(f"[{asterisk_id}] TCP connection dropped.")

                if call_task and not call_task.done():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(call_task),
                            timeout=GRACEFUL_SHUTDOWN_GRACE_SECS,
                        )
                        log.info(f"[{asterisk_id}] call_task finished gracefully after connection drop.")
                    except asyncio.TimeoutError:
                        log.warning(
                            f"[{asterisk_id}] call_task did not finish within "
                            f"{GRACEFUL_SHUTDOWN_GRACE_SECS}s — cancelling."
                        )
                        call_task.cancel()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.warning(f"[{asterisk_id}] call_task raised while waiting: {e}")
                break

            kind, payload = frame

            if kind == KIND_HANGUP:
                log.info(f"[{asterisk_id}] Hangup frame received — ending call.")
                if call_task and not call_task.done():
                    call_task.cancel()
                break

            elif kind in AUDIO_KINDS:
                state = session_manager.get_by_asterisk_id(asterisk_id)
                if state is None:
                    continue

                queue_depth = state.stt.audio_queue_depth()
                if queue_depth >= AUDIO_QUEUE_MAX:
                    frames_dropped += 1
                    if frames_dropped % 50 == 1:
                        log.warning(
                            f"[{asterisk_id}] Audio queue full "
                            f"(depth={queue_depth}); dropped so far: {frames_dropped}"
                        )
                else:
                    # feed_audio is now async (asyncio.Queue backed)
                    await state.stt.feed_audio(payload)
                    frames_fed += 1

            else:
                log.debug(f"[{asterisk_id}] Unhandled frame kind=0x{kind:02x} ({len(payload)} bytes).")

    except Exception as e:
        log.exception(f"[{asterisk_id or 'unknown'}] Error in handle_client: {e}")

    finally:
        log.info(f"[{asterisk_id or 'unknown'}] Audio stats — fed: {frames_fed}, dropped: {frames_dropped}")

        if call_task and not call_task.done():
            call_task.cancel()
            try:
                await call_task
            except asyncio.CancelledError:
                pass
            log.info(f"[{asterisk_id}] Conversation task cancelled.")

        if acquired_semaphore:
            _call_semaphore.release()
            async with _active_calls_lock:
                _active_calls -= 1
                log.info(f"[{asterisk_id}] Active concurrent calls: {_active_calls}/{MAX_CONCURRENT_CALLS}")

        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception:
            pass

        log.info(f"[{asterisk_id or 'unknown'}] Connection closed.")


# =============================================================================
# SERVER ENTRY POINT
# =============================================================================

async def serve() -> None:
    if AUDIO_SOURCE != "socket":
        raise RuntimeError(
            "AUDIO_SOURCE is set to 'mic' in stt.py.\n"
            "Change it to 'socket' before running the AudioSocket server."
        )

    server = await asyncio.start_server(handle_client, HOST, PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info(f"AudioSocket server listening on {addrs}")
    log.info(f"Max concurrent calls: {MAX_CONCURRENT_CALLS}")
    log.info("Waiting for Asterisk connections...")
    log.info("─" * 50)

    async with server:
        await server.serve_forever()



