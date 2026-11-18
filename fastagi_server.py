import asyncio
import json
import logging
import sqlite3
import uuid as uuid_lib

from logging_config import setup_logging
setup_logging()

# Fairmoney project: shared state lives in fairmoney_main.py
from fairmoney_main import (
    transfer_requests,
    pending_customer_info,
    disposition_requests,
)
from config.settings import (
    DB_PATH,
    FASTAGI_HOST as HOST,
    FASTAGI_PORT as PORT,
    AUDIOSOCKET_ADVERTISED_HOST as AUDIOSOCKET_HOST,
    AUDIOSOCKET_PORT,
    TRANSFER_QUEUE,
    BUSY_PLAYBACK_AUDIO as BUSY_PLAYBACK,
    POST_WAIT_SECS,
)

log = logging.getLogger("fastagi")


# =============================================================================
# LOW-LEVEL AGI PROTOCOL HELPERS
# =============================================================================

class AGIHangup(Exception):
    """Raised when Asterisk reports the channel is gone (HANGUP result)."""


async def read_agi_env(reader: asyncio.StreamReader) -> dict:
    env = {}
    while True:
        line = await reader.readline()
        if not line:
            raise AGIHangup("Connection closed before AGI env completed.")
        line = line.decode(errors="replace").rstrip("\r\n")
        if line == "":
            break
        if ":" in line:
            key, _, value = line.partition(":")
            env[key.strip()] = value.strip()
    return env


async def agi_command(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: str,
) -> tuple[int, str]:
    writer.write((command + "\n").encode())
    await writer.drain()

    line = await reader.readline()
    if not line:
        raise AGIHangup(f"Connection closed while awaiting reply to: {command}")

    text = line.decode(errors="replace").strip()
    log.debug(f"AGI << {text}")

    if text.upper().startswith("HANGUP"):
        raise AGIHangup("Asterisk reported HANGUP.")

    code = 200
    if text.startswith("200"):
        code = 200
    else:
        try:
            code = int(text.split(" ", 1)[0])
        except (ValueError, IndexError):
            code = 0

    return code, text


async def agi_answer(reader, writer) -> None:
    await agi_command(reader, writer, "ANSWER")


async def agi_set_channel_var(reader, writer, var: str, value: str) -> None:
    await agi_command(reader, writer, f'SET VARIABLE "{var}" "{value}"')


async def agi_get_variable(reader, writer, var: str) -> str | None:
    code, text = await agi_command(reader, writer, f'GET VARIABLE "{var}"')
    if "result=0" in text:
        return None
    start = text.find("(")
    end = text.rfind(")")
    if start != -1 and end != -1 and end > start:
        return text[start + 1 : end]
    return None


async def agi_exec(reader, writer, application: str, options: str = "") -> tuple[int, str]:
    cmd = f'EXEC {application} "{options}"' if options else f"EXEC {application}"
    return await agi_command(reader, writer, cmd)


async def agi_playback(reader, writer, filename: str) -> None:
    await agi_exec(reader, writer, "Playback", filename)


async def agi_verbose(reader, writer, message: str, level: int = 1) -> None:
    await agi_command(reader, writer, f'VERBOSE "{message}" {level}')


async def agi_wait(reader, writer, seconds: int) -> None:
    await agi_exec(reader, writer, "Wait", str(seconds))


async def agi_queue(reader, writer, queue_name: str) -> None:
    await agi_exec(reader, writer, "Queue", queue_name)


async def agi_hangup(reader, writer) -> None:
    await agi_command(reader, writer, "HANGUP")


def get_sqlite_call_fallback(audiosocket_id: str) -> dict | None:
    """Fetch the latest interim snapshot from local SQLite if memory dictionary was empty."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM calls WHERE asterisk_id = ?", (audiosocket_id,)).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try:
                if isinstance(d.get("interaction_notes"), str):
                    d["interaction_notes"] = json.loads(d["interaction_notes"])
            except Exception:
                pass
            try:
                if isinstance(d.get("conversation_log"), str):
                    d["conversation_log"] = json.loads(d["conversation_log"])
            except Exception:
                pass
            return d
    except Exception as e:
        log.warning(f"SQLite fallback lookup error: {e}")
    return None


# =============================================================================
# PER-CALL HANDLER
# =============================================================================

async def handle_agi_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    log.info(f"New AGI connection from {peer}")

    call_id = "unknown"

    try:
        env = await read_agi_env(reader)
        call_id = env.get("agi_uniqueid", "unknown")
        channel = env.get("agi_channel", "unknown")
        callerid = env.get("agi_callerid", "unknown")

        log.info(f"[{call_id}] channel={channel} callerid={callerid}")

        # ── Step 1: Answer + set audio formats ──────────────────────────────
        await agi_answer(reader, writer)
        await agi_set_channel_var(reader, writer, "CHANNEL(audioreadformat)", "slin16")
        await agi_set_channel_var(reader, writer, "CHANNEL(audiowriteformat)", "slin16")

        # ── Step 2: generate the AudioSocket UUID ───────────────────────────
        audiosocket_id = str(uuid_lib.uuid4())
        await agi_set_channel_var(reader, writer, "AUDIOSOCKET_ID", audiosocket_id)

        # ── Step 2b: capture customer data from channel variables ──────────
        # ── Step 2b: capture customer data from channel variables ──────────
        customer_name = (
            await agi_get_variable(reader, writer, "CUSTOMER_NAME")
            or await agi_get_variable(reader, writer, "customer_name")
            or await agi_get_variable(reader, writer, "nCustomerName")
            or await agi_get_variable(reader, writer, "CustomerName")
            or ""
        )
        customer_email = (
            await agi_get_variable(reader, writer, "CUSTOMER_EMAIL")
            or await agi_get_variable(reader, writer, "customer_email")
            or await agi_get_variable(reader, writer, "nCustomerEmail")
            or ""
        )
        due_amount = (
            await agi_get_variable(reader, writer, "DUE_AMOUNT")
            or await agi_get_variable(reader, writer, "due_amount")
            or await agi_get_variable(reader, writer, "LOAN_AMOUNT")
            or await agi_get_variable(reader, writer, "loan_amount")
            or await agi_get_variable(reader, writer, "AMOUNT")
            or await agi_get_variable(reader, writer, "amount")
            or ""
        )

        # Strictly capture Teckinfo call number from IDGCALL:
        idgcall = (
            await agi_get_variable(reader, writer, "IDGCALL")
            or await agi_get_variable(reader, writer, "idgcall")
            or await agi_get_variable(reader, writer, "CUSTOMER_NUMBER")
            or callerid
            or ""
        )
        customer_number = idgcall  # Keep both variables defined and equal

        lead_id = (
            await agi_get_variable(reader, writer, "IDGLEAD")
            or await agi_get_variable(reader, writer, "LEAD_ID")
            or await agi_get_variable(reader, writer, "lead_id")
            or "0"
        )
        serviceid = (
            await agi_get_variable(reader, writer, "IDGSERVICE")
            or await agi_get_variable(reader, writer, "SERVICE_ID")
            or await agi_get_variable(reader, writer, "serviceid")
            or "0"
        )

        pending_customer_info[audiosocket_id] = {
            "customer_name": customer_name,
            "customer_email": customer_email,
            "customer_number": customer_number,
            "due_amount": due_amount,
            "idgcall": idgcall,
            "lead_id": lead_id,
            "serviceid": serviceid,
        }
        log.info(
            f"[{call_id}] Customer capture: name={customer_name!r} due_amount={due_amount!r} "
            f"idgcall={idgcall!r} lead_id={lead_id!r} serviceid={serviceid!r}"
        )

        # ── Step 2c: Pre-stamp baseline variables while channel is 100% ALIVE ─
        # Guarantees that ALL Call Master fields are NEVER BLANK, even on instant hangup.
        await agi_set_channel_var(reader, writer, "HumanEscalation", "N")
        await agi_set_channel_var(reader, writer, "IDGLEAD", str(lead_id))
        await agi_set_channel_var(reader, writer, "IDGSERVICE", str(serviceid))
        await agi_set_channel_var(reader, writer, "IDGCALL", str(idgcall))
        await agi_set_channel_var(reader, writer, "LEAD_ID", str(lead_id))
        await agi_set_channel_var(reader, writer, "SERVICE_ID", str(serviceid))
        await agi_set_channel_var(reader, writer, "CallDataAsteriskId", audiosocket_id)
        await agi_set_channel_var(reader, writer, "CallDataCustomerName", customer_name or "Customer")
        await agi_set_channel_var(reader, writer, "CallDataCallbackNumber", "")
        await agi_set_channel_var(reader, writer, "CallDataAlternateNumber", "")
        await agi_set_channel_var(reader, writer, "CallDataCallOutcome", "ABORTED")
        await agi_set_channel_var(reader, writer, "CallDataCallStatus", "ABORTED")
        await agi_set_channel_var(reader, writer, "CallDataRpcStatus", "RPC_NOT_VERIFIED")
        await agi_set_channel_var(reader, writer, "CallDataPaymentIntent", "NONE")
        await agi_set_channel_var(reader, writer, "CallDataPaymentStatus", "NONE")
        await agi_set_channel_var(reader, writer, "CallDataPtpAmount", "")
        await agi_set_channel_var(reader, writer, "CallDataPtpDate", "")
        await agi_set_channel_var(reader, writer, "CallDataTransferReason", "")
        await agi_set_channel_var(reader, writer, "CallDataRefusalOutcome", "")
        await agi_set_channel_var(reader, writer, "CallDataCallbackDetails", "")
        await agi_set_channel_var(reader, writer, "CallDataInteractionNotes", "[]")
        await agi_set_channel_var(reader, writer, "CallDataConversationLog", "[]")
        log.info(f"[{call_id}] Baseline CallData variables pre-stamped onto active channel.")

        log.info(f"[{call_id}] Starting AudioSocket bridge uuid={audiosocket_id} "
                 f"→ {AUDIOSOCKET_HOST}:{AUDIOSOCKET_PORT}")

        # ── Step 3: run AudioSocket (blocks for the whole bot conversation) ─
        result_code, result_text = await agi_exec(
            reader, writer,
            "AudioSocket",
            f"{audiosocket_id},{AUDIOSOCKET_HOST}:{AUDIOSOCKET_PORT}",
        )
        log.info(f"[{call_id}] AudioSocket EXEC returned: {result_text}")

        # ── Step 4: dynamic post-call logic ─────────────────────────────────
        queue_name = transfer_requests.pop(audiosocket_id, None)
        human_escalation = "Y" if queue_name else "N"

        # Safely attempt to stamp HumanEscalation without letting an early hangup abort Step 5
        try:
            await agi_set_channel_var(reader, writer, "HumanEscalation", human_escalation)
            log.info(
                f"[{call_id}] Post-call decision: HumanEscalation={human_escalation} "
                f"(queue_name={queue_name!r})"
            )
        except AGIHangup:
            log.info(f"[{call_id}] Channel hung up; HumanEscalation remains at baseline.")
        except Exception as e:
            log.warning(f"[{call_id}] Could not set HumanEscalation: {e}")

        # ── Step 5: stamp the full call record onto the channel ─────────────
        disposition = None
        for _ in range(25):
            disposition = disposition_requests.pop(audiosocket_id, None)
            if disposition is not None:
                break
            await asyncio.sleep(0.04)

        # Fallback to local SQLite snapshot if memory dictionary wasn't populated in time
        if disposition is None:
            disposition = get_sqlite_call_fallback(audiosocket_id)
            if disposition:
                log.info(f"[{call_id}] Retrieved disposition from SQLite fallback (outcome={disposition.get('call_outcome')}).")

        if disposition is not None:
            async def safe_set(var_name: str, var_val: str) -> bool:
                try:
                    await agi_set_channel_var(reader, writer, var_name, str(var_val))
                    return True
                except AGIHangup:
                    return False
                except Exception as ex:
                    log.debug(f"[{call_id}] Error setting {var_name}: {ex}")
                    return True

            call_outcome_val = str(disposition.get("call_outcome", "") or "")
            payment_intent_val = str(disposition.get("payment_intent", "") or "")
            log.info(f"[{call_id}] Stamping final CallData onto channel (outcome={call_outcome_val}, intent={payment_intent_val})...")

            # CRITICAL ORDER: High-priority business fields stamped FIRST within the first 20ms
            fields_to_stamp = [
                # 1. OUTCOME & INTENT (stamped under both names so dialplan & Call Master always match!)
                ("CallDataCallOutcome", call_outcome_val),
                ("CallDataCallStatus", call_outcome_val),
                ("CallDataPaymentIntent", payment_intent_val),
                ("CallDataPaymentStatus", payment_intent_val),

                # 2. PROMISE TO PAY DATA
                ("CallDataPtpAmount", str(disposition.get("ptp_amount", "") or "")),
                ("CallDataPtpDate", str(disposition.get("ptp_date", "") or "")),

                # 3. ESCALATION & REFUSAL OUTCOMES
                ("CallDataTransferReason", str(disposition.get("transfer_reason", "") or "")),
                ("CallDataRefusalOutcome", str(disposition.get("refusal_outcome", "") or "")),

                # 4. CALLBACK & NUMBERS
                ("CallDataCallbackNumber", str(disposition.get("callback_number", "") or "")),
                ("CallDataAlternateNumber", str(disposition.get("alternate_number", "") or disposition.get("callback_number", "") or "")),
                ("CallDataCallbackDetails", str(disposition.get("callback_details", "") or "")),
                ("CallDataCallbackRequired", str(disposition.get("callback_required", "") or "")),
                ("CallDataCallbackTime", str(disposition.get("callback_time", "") or "")),

                # 5. METADATA
                ("IDGLEAD", str(disposition.get("lead_id", "") or lead_id or "0")),
                ("IDGSERVICE", str(disposition.get("serviceid", "") or serviceid or "0")),
                ("IDGCALL", str(disposition.get("idgcall", "") or disposition.get("customer_number", "") or idgcall or "")),
                ("LEAD_ID", str(disposition.get("lead_id", "") or lead_id or "0")),
                ("SERVICE_ID", str(disposition.get("serviceid", "") or serviceid or "0")),
                ("CallDataRpcStatus", str(disposition.get("rpc_status", "") or "")),
                ("CallDataCustomerName", str(disposition.get("customer_name", "") or "")),
                ("CallDataAsteriskId", str(disposition.get("asterisk_id", "") or "")),
                ("CallDataTimestamp", str(disposition.get("timestamp", "") or "")),

                # 6. JSON PAYLOADS (Stamped last so massive strings don't block critical fields)
                ("CallDataInteractionNotes", json.dumps(disposition.get("interaction_notes", []), ensure_ascii=False, default=str)),
                ("CallDataConversationLog", json.dumps(disposition.get("conversation_log", []), ensure_ascii=False, default=str)),
                ("CallData", json.dumps(disposition, ensure_ascii=False, default=str)),
            ]

            stamped_count = 0
            for v_name, v_val in fields_to_stamp:
                ok = await safe_set(v_name, v_val)
                if not ok:
                    log.info(f"[{call_id}] Channel socket closed after stamping {stamped_count} fields.")
                    break
                stamped_count += 1

            log.info(f"[{call_id}] Stamped {stamped_count} CallData channel variables successfully.")
        else:
            log.warning(f"[{call_id}] No disposition record found to stamp CallData.")

        # ── Step 6: Explicitly hang up channel if no agent transfer is needed ──
        if human_escalation == "N":
            log.info(f"[{call_id}] HumanEscalation=N — Hanging up Asterisk channel via FastAGI.")
            try:
                await agi_hangup(reader, writer)
            except AGIHangup:
                log.info(f"[{call_id}] Channel already hung up.")
            except Exception as e:
                log.info(f"[{call_id}] Channel hangup note: {e}")
        else:
            log.info(f"[{call_id}] HumanEscalation=Y — Returning control to dialplan for queue/transfer.")

    except AGIHangup as e:
        log.info(f"[{call_id}] Caller hung up / channel gone: {e}")

    except Exception as e:
        log.exception(f"[{call_id}] Error in handle_agi_call: {e}")

    finally:
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception:
            pass
        log.info(f"[{call_id}] AGI connection closed.")


# =============================================================================
# SERVER ENTRY POINT
# =============================================================================

async def serve() -> None:
    server = await asyncio.start_server(handle_agi_call, HOST, PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info(f"FastAGI server listening on {addrs}")
    log.info("─" * 50)
    log.info("Minimal dialplan required:")
    log.info(f"  exten => _X.,1,AGI(agi://{HOST if HOST != '0.0.0.0' else 'YOUR_SERVER_IP'}:{PORT}/call)")
    log.info("  exten => _X.,n,Hangup()")
    log.info("─" * 50)
    log.info(f"AudioSocket bridge target: {AUDIOSOCKET_HOST}:{AUDIOSOCKET_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        log.info("FastAGI server shut down.")
