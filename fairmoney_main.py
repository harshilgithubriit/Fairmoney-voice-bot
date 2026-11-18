import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, date, timedelta
import httpx
from langchain_core.messages import AIMessage, HumanMessage

from db import upsert_call
from dotenv import load_dotenv
from logging_config import setup_logging

setup_logging()
load_dotenv()

from config.disposition import (
    RPC_STATUS,
    PAYMENT_INTENT,
    CALL_OUTCOME,
)
from config.settings import (
    PAYMENT_LINK_BASE_URL,
    DISPOSITION_DIR,
    TRANSCRIPT_DIR,
    MAX_CALL_DURATION_SECS,
    DISPOSITION_API_URL,
    CRM_TIMEOUT_SECS,
    ENABLE_CRM_PUSH,
)
from config.transfer import TRANSFER_QUEUE
from data.loan_lookup import get_loan, get_customer_fallback
from prompts.fairmoney_prompt import (
    INITIAL_SYSTEM_PROMPT,
    FULL_SYSTEM_PROMPT,
    ALL_MARKERS,
    RPC_VERIFIED_MARKER,
    RPC_NOT_VERIFIED_MARKER,
    RPC_CALLBACK_MARKER,
    INTENT_WILL_PAY_MARKER,
    INTENT_NEEDS_TIME_MARKER,
    INTENT_TRANSFER_MARKER,
    INTENT_REFUSES_MARKER,
    PTP_CAPTURED_MARKER,
    DATE_NEGOTIATED_MARKER,
    PAYMENT_LINK_MARKER,
    DISCONNECT_MARKER,
)
from session.session_manager import SessionManager
from config.settings import TTS_ENGINE as _TTS_ENGINE

if _TTS_ENGINE == "azure":
    from services.azure_tts import stream_tts
else:
    from services.tts import stream_tts

# ── Safe Tool Imports with Fallbacks (Guarantees zero startup crashes) ───────
from tools.fairmoney_tools import (
    validate_payment_date,
    normalize_phone_number,
)

try:
    from tools.fairmoney_tools import (
        normalize_ptp_date,
        normalize_ptp_amount,
        normalize_callback_time,
    )
except ImportError:
    def normalize_ptp_date(raw_date: str) -> str:
        """Convert spoken dates ('today', 'tomorrow', 'Friday', etc.) to standard SQL DATETIME 'YYYY-MM-DD 18:00:00'."""
        if not raw_date:
            return datetime.now().strftime("%Y-%m-%d 18:00:00")
        val = str(raw_date).strip().lower()
        now = datetime.now()
        if "today" in val:
            return now.strftime("%Y-%m-%d 18:00:00")
        if "tomorrow" in val:
            return (now + timedelta(days=1)).strftime("%Y-%m-%d 18:00:00")
        m = re.search(r"\b(\d{4}[-/]\d{1,2}[-/]\d{1,2})\b", val)
        if m:
            clean_d = m.group(1).replace("/", "-")
            return f"{clean_d} 18:00:00" if len(clean_d) == 10 else f"{clean_d}"
        return (now + timedelta(days=1)).strftime("%Y-%m-%d 18:00:00")

    def normalize_ptp_amount(raw_amt: str, fallback_balance: str = "50000") -> str:
        """Normalize spoken amounts like 'twenty five thousand' or '50k' to numeric string like '25000'."""
        if not raw_amt:
            return fallback_balance
        text = str(raw_amt).lower().replace(",", "")
        digits = re.sub(r"[^\d.]", "", text)
        if digits:
            try:
                val = float(digits)
                if "k" in text:
                    val *= 1000
                elif "m" in text:
                    val *= 1000000
                return str(int(val))
            except Exception:
                pass
        spoken_map = {
            "one hundred thousand": "100000",
            "hundred thousand": "100000",
            "fifty thousand": "50000",
            "twenty five thousand": "25000",
            "twenty-five thousand": "25000",
            "thirty thousand": "30000",
            "forty thousand": "40000",
            "ten thousand": "10000",
            "twenty thousand": "20000",
            "sixty thousand": "60000",
            "seventy thousand": "70000",
            "eighty thousand": "80000",
            "ninety thousand": "90000",
            "fifteen thousand": "15000",
            "five thousand": "5000",
        }
        for k, v in spoken_map.items():
            if k in text:
                return v
        return fallback_balance

    def normalize_callback_time(val: str | None) -> str:
        """Convert spoken or informal callback time to exact SQL DATETIME 'YYYY-MM-DD HH:MM:SS'."""
        if not val:
            return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d 10:00:00")
        text = str(val).strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$", text):
            return text.replace("T", " ") if len(text) >= 19 else text.replace("T", " ") + ":00"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
            return f"{text} 10:00:00"
        now = datetime.now()
        target_date = now.date() + timedelta(days=1)
        target_hour = 10
        target_minute = 0
        lower = text.lower()
        if "today" in lower:
            target_date = now.date()
        elif "tomorrow" in lower:
            target_date = now.date() + timedelta(days=1)
        m_time = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", lower)
        if m_time:
            hr = int(m_time.group(1))
            mn = int(m_time.group(2) or 0)
            mer = m_time.group(3)
            if mer == "pm" and hr < 12:
                hr += 12
            elif mer == "am" and hr == 12:
                hr = 0
            if 0 <= hr <= 23:
                target_hour = hr
                target_minute = mn
        res_dt = datetime(target_date.year, target_date.month, target_date.day, target_hour, target_minute, 0)
        return res_dt.strftime("%Y-%m-%d %H:%M:%S")

log = logging.getLogger("fairmoney")
session_manager = SessionManager()

# ── Persistent Async HTTP Client for CRM API pushes ────────────────────────
_crm_http_client: httpx.AsyncClient | None = None

def get_crm_http_client() -> httpx.AsyncClient:
    global _crm_http_client
    if _crm_http_client is None or _crm_http_client.is_closed:
        _crm_http_client = httpx.AsyncClient(timeout=CRM_TIMEOUT_SECS)
    return _crm_http_client

# Shared transfer, customer, and disposition state:
transfer_requests: dict[str, str] = {}
pending_customer_info: dict[str, dict] = {}
disposition_requests: dict[str, dict] = {}

# AudioSocket disconnect frame
HANGUP_FRAME = b"\x00\x00\x00"

async def send_hangup(writer: asyncio.StreamWriter, call_id: str) -> None:
    """Send the AudioSocket HANGUP_FRAME for a clean, expected disconnect."""
    if writer is None:
        return
    if writer.is_closing():
        print(f"[{call_id}] Writer already closing — skipping hangup frame.")
        return
    try:
        writer.write(HANGUP_FRAME)
        await writer.drain()
        print(f"[{call_id}] Hangup frame sent to Asterisk.")
    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        print(f"[{call_id}] Could not send hangup frame (connection already gone): {e}")

# Turn-loop safety limits
MAX_TURNS = 15
MAX_EMPTY_RETRIES = 3
RPC_MAX_GATE_ATTEMPTS = 2
MIN_BOT_SPEAK_SECS = 0.4

def _amount_to_words(amount) -> str:
    """Convert a naira amount to spoken words."""
    try:
        total = int(round(float(amount)))
    except (TypeError, ValueError):
        return "the amount due"
    if total <= 0:
        return "zero"

    ones = [
        "", "one", "two", "three", "four", "five", "six", "seven", "eight",
        "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
        "sixteen", "seventeen", "eighteen", "nineteen",
    ]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy",
            "eighty", "ninety"]
    scales = ["", "thousand", "million", "billion", "trillion"]

    def under_1000(n: int) -> str:
        parts = []
        hundreds = n // 100
        if hundreds:
            parts.append(f"{ones[hundreds]} hundred")
        n %= 100
        if 0 < n < 20:
            parts.append(ones[n])
        elif n >= 20:
            parts.append(tens[n // 10])
            if n % 10:
                parts.append(ones[n % 10])
        return " ".join(parts)

    chunks = []
    i = 0
    while total > 0:
        chunk = total % 1000
        if chunk:
            chunks.append((under_1000(chunk), scales[i]))
        total //= 1000
        i += 1

    words = []
    for value, scale in reversed(chunks):
        if value:
            words.append(value)
            if scale:
                words.append(scale)
    return " ".join(words)

_NAME_FILLERS = {
    "me", "myself", "him", "her", "he", "she", "it", "this", "that",
    "correct", "yes", "yeah", "yep", "right", "sure", "ok", "okay", "fine",
    "true", "indeed", "ji", "haan", "na", "speaking", "same", "main",
}

def _extract_stated_name(t: str):
    """Return a name the caller explicitly stated."""
    m = re.search(r"\b(?:this is|my name is|i am|i,m|maine|naam hai)\s+([a-z]+)", t)
    if not m:
        m = re.search(r"\b([a-z]+)\s+speaking\b", t)
    if not m:
        return None
    word = m.group(1)
    if word in _NAME_FILLERS:
        return None
    return word

def _is_clear_positive(text: str, customer_name: str) -> bool:
    """True when the person clearly confirms they are the customer."""
    if not text or not text.strip():
        return False
    t = text.strip().lower()

    if re.search(r"\b(no|nahi|na|n't|not|never|wrong number|galti)\b", t):
        return False

    name = (customer_name or "").strip().lower()
    stated = _extract_stated_name(t)
    if stated is not None:
        return bool(name) and stated == name
    if name and name in t:
        return True

    if re.search(
        r"\b(pay(?:ing|ment)?|amount|half|full|loan|balance|outstanding|"
        r"overdue|due|money|date|tomorrow|today|plan|emi|installment|"
        r"instalment|transfer|agents?|callback|incorrect|wrong|differs|mistake)\b",
        t,
    ):
        return False

    if re.search(
        r"\b(yes|yeah|yep|yup|haan|han|ji|speaking|correct|right|sure|myself)\b", t
    ):
        return True

    if re.search(r"\b(this is me|i am|it's me|it is me|i,m|main)\b", t):
        return True

    return False

def _is_clear_negative(text: str, customer_name: str) -> bool:
    """True when the person clearly denies being the customer."""
    if not text or not text.strip():
        return False
    t = text.strip().lower()

    if re.search(
        r"\b(no|no no|nahi|nhi|naheen|never|wrong number|wrong no|galti|galat)\b", t
    ):
        return True
    if re.search(r"\b(not me|i,m not|main nahi|maine nahi|nahi main|ji nahi)\b", t):
        return True

    name = (customer_name or "").strip().lower()
    if name:
        if re.search(rf"\b(?:i am not|i,m not|not|nahi|nhi)\s*{re.escape(name)}\b", t):
            return True
        if re.search(rf"\b{re.escape(name)}\s*(?:is\s+)?(?:not|nahi|nhi)\b", t):
            return True

    return False

def _is_no_alternate_number(text: str) -> bool:
    """True when the person clearly says they have no alternate/callback number."""
    if not text or not text.strip():
        return False
    t = text.strip().lower()
    alt = r"(?:alternate|alternative|another|other|second|new|calling)"
    patterns = [
        r"\bwrong\s+number\b",
        r"\bnot\s+(?:his|her|my)\s+number\b",
        r"\bdon't\s+know|dont\s+know|do\s+not\s+know\b",
        r"\bno\s+idea\b",
        r"\bnot\s+him\b|\bnot\s+her\b",
        rf"\bno\s+(?:{alt}\s+)?number\b",
        rf"\bno\s+{alt}\b",
        r"\bno callback\b",
        r"\bdon't have|dont have|do not have\b",
        r"\b(?:koi|any)\s+number\b",
        rf"\b{alt}\s+number\b[^.]*\b(?:nahi|nhi|na hai)\b",
        rf"\bnumber\b[^.]*\b(?:nahi|nhi)\b",
        rf"\b(?:nahi|nhi)\b[^.]*\bnumber\b",
        r"\b(?:not interested|interested nahi|nahi chahiye|nahi chahie|nahi chahunga)\b",
        r"\bno need\b",
        r"\bnone\b",
    ]
    return any(re.search(p, t) for p in patterns)


def _is_agent_transfer_request(text: str) -> bool:
    """True when the customer asks for a human agent, representative, or refuses to talk to bot."""
    if not text or not text.strip():
        return False
    t = text.strip().lower()
    patterns = [
        r"\b(?:transfer|connect|put|pass)\s+(?:me\s+)?(?:to|through\s+to)\s+(?:an?\s+)?(?:agent|human|person|specialist|officer|rep|representative|operator|manager|supervisor)\b",
        r"\b(?:speak|talk)\s+(?:to|with)\s+(?:an?\s+)?(?:agent|human|person|specialist|officer|rep|representative|operator|someone|somebody|manager)\b",
        r"\b(?:agent|human|specialist|officer|representative|customer\s+care|helpdesk)\b",
        r"\bi\s+(?:don't|dont|do\s+not)\s+want\s+to\s+talk\b",
        r"\bstop\s+talking\s+to\s+me\b",
        r"\bnot\s+talking\s+to\s+(?:a\s+)?(?:robot|machine|bot|computer)\b",
        r"\bagent\s+se\s+baat\b|\btransfer\s+kar\b",
    ]
    return any(re.search(p, t) for p in patterns)


def _get_time_greeting(use_lagos_time: bool = True) -> str:
    """Return 'Good morning', 'Good afternoon', or 'Good evening' based on current time."""
    from datetime import datetime, timezone, timedelta

    if use_lagos_time:
        # Nigeria / Lagos Time (WAT = UTC+1)
        lagos_tz = timezone(timedelta(hours=1))
        hour = datetime.now(lagos_tz).hour
    else:
        # Server Local Time
        hour = datetime.now().hour

    if 5 <= hour < 12:
        return "Good morning"
    elif 12 <= hour < 17:
        return "Good afternoon"
    else:
        return "Good evening"


def _build_step2_script(state, loan) -> str:
    """Deterministic STEP 2 loan script."""
    name = state.customer_name or "the account holder"
    amount_words = _amount_to_words((loan or {}).get("amount"))
    dpd_days = int((loan or {}).get("dpd_days") or 0)

    script = (
        f"{name}, this is Taiwo calling from FairMoney Microfinance Bank. "
        "I am calling regarding your Personal Loan FlexiCredit account. "
        "Please be aware that for quality purposes, this call is being "
        "recorded. "
    )
    if dpd_days <= 0:
        script += (
            f"This is to follow up on your account, which is due today, "
            f"with an outstanding balance of {amount_words} naira. Please "
            "confirm the payment status at your earliest convenience. "
        )
    else:
        script += (
            f"This is to follow up on your account, which is currently "
            f"{dpd_days} days overdue, with an outstanding balance of "
            f"{amount_words} naira. FairMoney demands full payment today. "
            f"The minimum due today is {amount_words} naira. "
        )
    script += (
        "Please confirm, will you be able to make this payment today, "
        "or do you need us to work out a plan with you?"
    )
    return script

def strip_markers(text: str) -> str:
    """Remove every registered control marker before TTS."""
    for marker in ALL_MARKERS:
        text = text.replace(marker, "")
    return text.strip()

def _clean_for_goodbye_match(s: str) -> str:
    """Normalize Unicode hyphens and dashes to standard ASCII hyphen for reliable matching."""
    return s.lower().replace("\u2011", "-").replace("‑", "-").replace("–", "-").replace("—", "-")

def is_hallucinated_response(text: str) -> bool:
    """Return True if the text contains placeholder patterns, LLM CoT, or raw JSON."""
    stripped = text.strip()
    # Drop stutter loops, repetitive dots/ellipses, and orphan brackets from LLM
    if "…" in text or ("..." in text and len(re.findall(r"[.…]{2,}", text)) >= 2):
        return True
    if stripped.startswith("(") and stripped.endswith(")"):
        return True
    if re.search(r"\breminder\s+reminder\b", text, re.IGNORECASE):
        return True
    if len(re.sub(r"[\s\.\…\(\)\-]", "", text)) < 4:
        return True
		
    # Drop raw JSON objects, curly braces, code blocks, or dictionary leaks from LLM
    if stripped.startswith("{") or stripped.endswith("}"):
        return True
    if stripped.startswith("```") or stripped.endswith("```"):
        return True

    # ── NAYA FILTER: Drop leaked function calls or tool calls from LLM ──
    if re.search(r'^\s*[a-zA-Z0-9_]+\s*\(.*?\)\s*$', stripped):
        return True
    if any(stripped.startswith(fn) for fn in [
        "record_payment_mode", "capture_ptp", "negotiate_payment_date",
        "request_callback", "share_payment_link", "transfer_call", "save_call_data"
    ]):
        return True

    if any(k in stripped for k in [
        '"call_outcome"', '"rpc_status"', '"payment_intent"', '"ptp_amount"',
        '"ptp_date"', '"customer_name"', '"payment_mode"', '"callback_required"',
        '"callback_details"', '"transfer_reason"', '"refusal_outcome"'
    ]):
        return True
    if re.search(r'^\s*["\']?[a-zA-Z0-9_]+["\']?\s*:\s*', stripped):
        return True

    stripped_dot = stripped.rstrip(".")
    if re.match(r'^\d{1,4}$', stripped_dot):
        return True
    placeholders = re.findall(r'\[[^\]]*\]', text)
    allowed = {'Title', 'Full Name', 'Pronoun', 'Possessive'}
    for ph in placeholders:
        inner = ph.strip('[]').strip()
        if inner not in allowed:
            return True
    if re.search(r'\(Note:', text, re.IGNORECASE):
        return True
    if re.search(r'wait for the customer', text, re.IGNORECASE):
        return True
    if re.search(r'wait for user', text, re.IGNORECASE):
        return True
    if re.search(r'should not output', text, re.IGNORECASE):
        return True
    if re.search(r'assistant should', text, re.IGNORECASE):
        return True
    if re.search(r'system expects', text, re.IGNORECASE):
        return True
    if re.search(r'we will capture your promise', text, re.IGNORECASE):
        return True
    if re.search(r'the system will', text, re.IGNORECASE):
        return True
    if re.search(r'we have captured', text, re.IGNORECASE):
        return True
    if re.search(r'we are capturing', text, re.IGNORECASE):
        return True
    if re.search(r'^We (need|await|are waiting|have already)', text.strip()):
        return True
    if re.search(r'We need to (continue|wait|get|capture|proceed)', text, re.IGNORECASE):
        return True
    if re.search(r'We (are )?waiting for (your|the|user)', text, re.IGNORECASE):
        return True
    if re.search(r'We (must|should) wait', text, re.IGNORECASE):
        return True
    return False

def build_disposition(state, is_final: bool = False) -> dict:
    """Assemble the structured disposition record for the dialer/CRM."""
    alt_num = state.callback_number or getattr(state, "alternate_number", None)
    
    # Determine real-time lifecycle status
    if state.disposition:
        current_outcome = state.disposition
    elif not is_final:
        current_outcome = CALL_OUTCOME.get("in_progress", "IN_PROGRESS")
    else:
        current_outcome = CALL_OUTCOME["aborted"]

    # Exact standard SQL DATETIME for callback_time
    cb_time = ""
    if state.callback_required or getattr(state, "callback_time", None) or alt_num:
        raw_cb = getattr(state, "callback_time", None) or "Anytime"
        cb_time = normalize_callback_time(raw_cb)

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "asterisk_id": state.asterisk_id,
        "idgcall": getattr(state, "idgcall", "") or getattr(state, "customer_number", "") or "",
        "customer_number": getattr(state, "customer_number", "") or "",
        "lead_id": getattr(state, "lead_id", "0") or "0",
        "serviceid": getattr(state, "serviceid", "0") or "0",
        "customer_name": state.customer_name,
        "customer_name_source": state.customer_name_source,
        "call_outcome": current_outcome,
        "rpc_status": state.rpc_status or RPC_STATUS["not_verified"],
        "payment_intent": state.payment_intent or "NONE",
        "ptp_amount": state.ptp_amount or "",
        "ptp_date": state.ptp_date or "",
        "payment_mode": getattr(state, "payment_mode", "") or "",
        "transfer_reason": state.transfer_reason or "",
        "refusal_outcome": state.refusal_outcome or "",
        "callback_required": state.callback_required,
        "callback_number": alt_num or "",
        "alternate_number": alt_num or "",
        "callback_time": cb_time,
        "callback_details": state.callback_details or (f"Alternate number: {alt_num} @ {cb_time}" if alt_num else ""),
        "interaction_notes": state.interaction_notes,
        "conversation_log": state.conversation_log,
    }

def build_string_api_payload(record: dict) -> dict:
    """Ensure all parameters match both the new 20-field API schema and Teckinfo C# Proxy_IA aliases."""
    def to_str(val) -> str:
        if val is None:
            return ""
        if isinstance(val, bool):
            return "true" if val else "false"
        if isinstance(val, (list, dict)):
            return json.dumps(val, ensure_ascii=False)
        return str(val).strip()

    # 1. Teckinfo Call Number: Prioritize 3-digit dialer ID (e.g. '897' / '898')
    teckinfo_phone = to_str(
        record.get("idgcall") 
        or record.get("callnumber")
        or record.get("customer_number")
    )

    lead_id_val = to_str(record.get("lead_id", "0"))
    service_id_val = to_str(record.get("serviceid", "0"))

    # 2. Callback Number ONLY (blank if no callback was requested)
    callback_phone = to_str(record.get("callback_number") or record.get("alternate_number") or "")

    inner = {
        # ── Primary identifier = Teckinfo 3-digit Call Number ─────────────────
        "callnumber": teckinfo_phone,

        # ── Teckinfo Dialer field names ───────────────────────────────────────
        "IDGCALL": teckinfo_phone,
        "IDGLEAD": lead_id_val,
        "IDGSERVICE": service_id_val,
        "idgcall": teckinfo_phone,
        "idglead": lead_id_val,
        "idgservice": service_id_val,

        # ── Standard field names ──────────────────────────────────────────────
        "lead_id": lead_id_val,
        "LeadId": lead_id_val,
        "serviceid": service_id_val,
        "ServiceId": service_id_val,
        "CallDataLeadId": lead_id_val,
        "CallDataServiceId": service_id_val,

        # ── NEW 20-Field API Schema (camelCase) ──────────────────────────────
        "callDataTimestamp": to_str(record.get("timestamp")),
        "callDataAsteriskId": to_str(record.get("asterisk_id")),
        "callDataCustomerName": to_str(record.get("customer_name")),
        "callDataMediaSource": to_str(record.get("customer_name_source")),
        "callDataCallStatus": to_str(record.get("call_outcome")),
        "callDataRpcStatus": to_str(record.get("rpc_status")),
        "callDataPaymentStatus": to_str(record.get("payment_intent")),
        "callDataPtpAmount": to_str(record.get("ptp_amount")),
        "callDataPtpDate": to_str(record.get("ptp_date")),
        "callDataPaymentMode": to_str(record.get("payment_mode")),
        "callDataTransferReason": to_str(record.get("transfer_reason")),
        "callDataRefusalOutcome": to_str(record.get("refusal_outcome")),
        "callDataCallbackRequired": to_str(record.get("callback_required")),
        "callDataInteractionNotes": to_str(record.get("interaction_notes")),
        "callDataConversationLog": to_str(record.get("conversation_log")),
        "callDataCallbackNumber": callback_phone,
        "callDataCallbackTime": to_str(record.get("callback_time")),

        # ── C# / .NET PascalCase Fallbacks ───────────────────────────────────
        "CallDataTimestamp": to_str(record.get("timestamp")),
        "CallDataAsteriskId": to_str(record.get("asterisk_id")),
        "CallDataCustomerName": to_str(record.get("customer_name")),
        "CallDataMediaSource": to_str(record.get("customer_name_source")),
        "CallDataCallStatus": to_str(record.get("call_outcome")),
        "CallDataRpcStatus": to_str(record.get("rpc_status")),
        "CallDataPaymentStatus": to_str(record.get("payment_intent")),
        "CallDataPtpAmount": to_str(record.get("ptp_amount")),
        "CallDataPtpDate": to_str(record.get("ptp_date")),
        "CallDataPaymentMode": to_str(record.get("payment_mode")),
        "CallDataTransferReason": to_str(record.get("transfer_reason")),
        "CallDataRefusalOutcome": to_str(record.get("refusal_outcome")),
        "CallDataCallbackRequired": to_str(record.get("callback_required")),
        "CallDataCallbackNumber": callback_phone,
        "CallDataCallbackTime": to_str(record.get("callback_time")),
        "CallDataInteractionNotes": to_str(record.get("interaction_notes")),
        "CallDataConversationLog": to_str(record.get("conversation_log")),
    }
    return {"requestMessage": inner}


async def post_disposition_api_async(record: dict) -> None:
    if not DISPOSITION_API_URL or not ENABLE_CRM_PUSH:
        return
    payload = build_string_api_payload(record)
    call_id = record.get("asterisk_id")
    try:
        client = get_crm_http_client()
        # Teckinfo C# API strictly requires {"requestMessage": { ... }} wrapped payload
        response = await client.post(DISPOSITION_API_URL, json=payload)
        res_data = {}
        try:
            res_data = response.json()
        except Exception:
            pass

        if response.status_code == 200 and res_data.get("result") is True:
            log.info(
                f"[{call_id}] CRM API update SUCCESS ({DISPOSITION_API_URL}) "
                f"outcome={record.get('call_outcome')} intent={record.get('payment_intent')}"
            )
        else:
            log.warning(
                f"[{call_id}] CRM API update REJECTED (HTTP {response.status_code}): {response.text}"
            )
    except Exception as e:
        log.warning(
            f"[{call_id}] Could not post update to CRM API: {type(e).__name__} - {e}"
        )
def dispatch_api_post(record: dict) -> None:
    """Safely spawn non-blocking background task for the CRM API push."""
    try:
        asyncio.create_task(post_disposition_api_async(record))
    except RuntimeError:
        pass

def post_disposition_api(record: dict) -> None:
    """Synchronous/wrapper compatibility for post_disposition_api."""
    dispatch_api_post(record)

def persist_interim(state, is_final: bool = False) -> None:
    """Upsert snapshot to local SQLite, stage FastAGI memory, and push to CRM API."""
    snapshot = build_disposition(state, is_final=is_final)
    upsert_call(snapshot)
    disposition_requests[state.asterisk_id] = snapshot
    
    # Non-blocking, instant CRM update at this exact step!
    dispatch_api_post(snapshot)

    print(
        f"[{state.asterisk_id}] [CRM SYNC] Step update pushed to API & DB. "
        f"outcome={snapshot.get('call_outcome')} "
        f"rpc={snapshot.get('rpc_status')} "
        f"intent={snapshot.get('payment_intent')} "
        f"ptp={snapshot.get('ptp_amount')}@{snapshot.get('ptp_date')} "
        f"mode={snapshot.get('payment_mode')} "
        f"cb={snapshot.get('callback_number')}@{snapshot.get('callback_time')} "
        f"turns={len(snapshot.get('conversation_log', []))}"
    )

def save_disposition(state) -> None:
    """Save final disposition JSONL and text transcript to disk."""
    record = build_disposition(state, is_final=True)

    os.makedirs(DISPOSITION_DIR, exist_ok=True)
    path = os.path.join(DISPOSITION_DIR, f"{datetime.now():%Y-%m-%d}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
    tpath = os.path.join(TRANSCRIPT_DIR, f"{state.asterisk_id}.txt")
    with open(tpath, "w", encoding="utf-8") as f:
        f.write("\n".join(state.conversation_log))

    log.info(
        f"[{state.asterisk_id}] Disposition saved → {path} | transcript → {tpath}"
    )


def _detect_and_set_payment_mode(state, text: str) -> None:
    """Auto-detect payment mode from customer words (including interrupts)."""
    if not text:
        return
    t = text.lower()
    if "email" in t:
        state.payment_mode = "payment_link_email"
    elif "sms" in t or "phone" in t or "message" in t or "text" in t:
        state.payment_mode = "payment_link_sms"
    elif "link" in t:
        if not getattr(state, "payment_mode", ""):
            state.payment_mode = "payment_link"
    elif "app" in t or "fairmoney" in t:
        state.payment_mode = "fairmoney_app"
    elif "bank" in t or "ussd" in t or ("transfer" in t and not any(w in t for w in ["call", "agent", "human", "person", "representative", "operator", "someone", "line"])):
        state.payment_mode = "bank_transfer"


async def handle_call(
    asterisk_id: str,
    loop: asyncio.AbstractEventLoop,
    writer: asyncio.StreamWriter,
    customer_name: str = "",
    customer_email: str = "",
    customer_number: str = "",
    due_amount: str = "",
    lead_id: str = "0",
    serviceid: str = "0",
) -> None:
    """Fairmoney call entry point."""
    log_prefix = f"[{asterisk_id}]"
    print(f"{log_prefix} Fairmoney call connected.")

    customer_name_source = "dialer" if (customer_name or customer_number) else "fallback"

    loan = get_loan(customer_number)
    if not loan:
        fallback = get_customer_fallback()
        if fallback:
            customer_name = customer_name or fallback.get("name", "")
            customer_number = customer_number or fallback.get("customer_number", "")
            loan = fallback
            customer_name_source = "fallback"
            log.warning(
                f"{log_prefix} No dialer data — using dev fallback customer "
                f"'{customer_name}'"
            )
    loan_block = ""
    if due_amount and str(due_amount).strip():
        try:
            amt_val = float(str(due_amount).replace(",", "").strip())
        except Exception:
            amt_val = 0.0
        # CRITICAL: loan dictionary populate karein taaki _build_step2_script amount ko words me convert kar sake
        if not loan:
            loan = {}
        loan["amount"] = amt_val
        loan["currency"] = "NGN"
        loan["due_date"] = "today"
        loan_block = f"Loan Amount: NGN {amt_val:,.2f}\nLoan Due Date: today"
    elif loan:
        loan_block = (
            f"Loan Amount: {loan.get('currency', 'NGN')} {loan.get('amount', 0):,.2f}\n"
            f"Loan Due Date: {loan.get('due_date', 'N/A')}"
        )
    else:
        log.warning(f"{log_prefix} No loan record for number '{customer_number}'.")

    if not customer_name:
        customer_name = "the account holder"

    state = await session_manager.create_session(
        asterisk_id,
        INITIAL_SYSTEM_PROMPT,
        customer_name=customer_name,
        customer_email=customer_email,
        customer_number=customer_number,
        customer_name_source=customer_name_source,
        dpd_days=loan.get("dpd_days", 0) if loan else 0,
        title=loan.get("title", "Mr.") if loan else "Mr.",
        pronoun=loan.get("pronoun", "he") if loan else "he",
        possessive=loan.get("possessive", "his") if loan else "his",
    )
    # Explicitly stamp Teckinfo parameters on session state:
    state.idgcall = customer_number
    state.lead_id = lead_id
    state.serviceid = serviceid

    full_prompt = session_manager.personalize(
        FULL_SYSTEM_PROMPT, customer_name, customer_email, customer_number,
        extra_notes=loan_block,
        dpd_days=loan.get("dpd_days", 0) if loan else 0,
        title=loan.get("title", "Mr.") if loan else "Mr.",
        pronoun=loan.get("pronoun", "he") if loan else "he",
        possessive=loan.get("possessive", "his") if loan else "his",
    )

    end_call = False
    callback_booked = False
    wrong_number_only = False
    _stop_streaming = [False]
    rpc_gate_verifying_phone = ""


    rpc_gate_attempts = 0
    rpc_gate_routed = False

    async def on_tool_call(tool_name: str, tool_args: dict) -> None:
        nonlocal end_call, callback_booked
        print(f"{log_prefix} [Tool] {tool_name} → {tool_args}")

        if tool_name == "disconnect_call":
            clean_log = " ".join(state.conversation_log).lower()
            goodbye_said = any(w in clean_log for w in ["goodbye", "good-bye", "good bye", "bye"])
            if _stop_streaming[0] or not goodbye_said:
                print(f"{log_prefix} [Tool] disconnect_call BLOCKED — bot asked question or goodbye not spoken.")
                return
            end_call = True


        elif tool_name == "transfer_call":
            if callback_booked:
                print(f"{log_prefix} [Tool] transfer_call IGNORED — callback already booked.")
                return
            queue = tool_args.get("queue") or tool_args.get("destination") or TRANSFER_QUEUE
            state.transfer_reason = tool_args.get("reason", "")
            state.payment_intent = state.payment_intent or PAYMENT_INTENT["transfer"]
            state.disposition = CALL_OUTCOME.get("transfer", "TRANSFER_TO_AGENT")
            transfer_requests[asterisk_id] = queue
            end_call = True
            persist_interim(state, is_final=True)
            print(
                f"{log_prefix} [Tool] transfer_call → queue='{queue}' "
                f"reason='{state.transfer_reason}'"
            )

        elif tool_name == "capture_ptp":
            raw_amt = str(tool_args.get("amount", "")).strip()
            raw_dt = str(tool_args.get("date", "")).strip()
            if not raw_dt:
                raw_dt = "today"

            # Exact Total Due Amount from FastAGI / Dialer:
            try:
                full_val = float((loan or {}).get("amount", 0) or 0)
            except Exception:
                full_val = 0.0

            # Poori conversation history check karein:
            all_cust_text = " ".join([m.lower() for m in state.conversation_log if m.startswith("Customer")]).lower()
            comb_text = f"{raw_amt.lower()} {all_cust_text}"

            # 1. Agar customer ne HALF bola hai (50% calculate karein):
            if any(k in comb_text for k in ("half", "50%", "half amount", "part payment")):
                calculated_amt = str(int(full_val / 2)) if full_val > 0 else "25000"
                print(f"{log_prefix} [Tool] PTP Half Amount calculated: {calculated_amt}")

            # 2. Agar customer ne FULL bola hai ya amount loan balance ke equal hai:
            elif any(k in comb_text for k in ("full", "complete", "total", "outstanding", "balance", "entire")) and full_val > 0:
                calculated_amt = str(int(full_val))
                print(f"{log_prefix} [Tool] PTP Full Amount (exact due_amount): {calculated_amt}")

            # 3. Agar specific amount bola hai:
            else:
                fallback_num = str(int(full_val)) if full_val > 0 else "50000"
                calculated_amt = normalize_ptp_amount(raw_amt or fallback_num, fallback_balance=fallback_num)

            # ── RULE: Amount total due se zyada nahi ho sakta (Max Capping) ──
            try:
                if full_val > 0 and float(calculated_amt) > full_val:
                    print(f"{log_prefix} [Tool] Agreed amount {calculated_amt} exceeds total due {full_val}. Capped to {int(full_val)}")
                    calculated_amt = str(int(full_val))
            except Exception:
                pass

            if not raw_dt:
                state.interaction_notes.append("PTP not captured: payment date was not supplied")
                persist_interim(state)
                return

            normalized_date = normalize_ptp_date(raw_dt)
            date_status, date_reason = validate_payment_date(normalized_date[:10])
            if date_status != "accepted":
                state.interaction_notes.append(f"PTP not captured: payment date rejected ({date_reason})")
                persist_interim(state)
                return

            state.payment_intent = PAYMENT_INTENT["will_pay"]
            state.ptp_amount = calculated_amt
            state.ptp_date = normalized_date
            state.ptp_captured = True
            print(f"{log_prefix} [Tool] PTP captured: {state.ptp_amount} by {state.ptp_date}")

        elif tool_name == "negotiate_payment_date":
            proposed = tool_args.get("proposed_date", "")
            state.payment_intent = PAYMENT_INTENT["needs_time"]
            status, reason = validate_payment_date(proposed)
            if status == "accepted":
                state.ptp_date = normalize_ptp_date(proposed)
                state.interaction_notes.append(f"Payment date agreed: {state.ptp_date}")
                print(f"{log_prefix} [Tool] Payment date accepted: {state.ptp_date}")
            else:
                state.interaction_notes.append(f"Payment date rejected ({reason}): {proposed}")
                print(f"{log_prefix} [Tool] Payment date rejected: {reason}")

        elif tool_name == "capture_refusal":
            if callback_booked:
                print(f"{log_prefix} [Tool] capture_refusal IGNORED — callback already booked.")
                return
            state.payment_intent = PAYMENT_INTENT["refuses"]
            state.refusal_outcome = tool_args.get("reason", "")
            state.disposition = CALL_OUTCOME["completed"]
            transfer_requests[asterisk_id] = TRANSFER_QUEUE
            state.transfer_reason = "refused"
            end_call = True
            print(f"{log_prefix} [Tool] Refusal captured: {state.refusal_outcome} → transfer")

        elif tool_name == "request_callback":
            raw_number = str(tool_args.get("callback_number", "")).strip()
            digits_only = re.sub(r"\D", "", raw_number)
            if len(digits_only) == 13 and digits_only.startswith("234"):
                digits_only = "0" + digits_only[3:]
            elif len(digits_only) == 10 and digits_only[0] in "7891":
                digits_only = "0" + digits_only
            if len(digits_only) == 11 and digits_only.startswith("0"):
                state.callback_required = True
                state.callback_number = digits_only
                state.alternate_number = digits_only
                pref_time = str(tool_args.get("preferred_time", "")).strip()
                if not pref_time or pref_time.lower() in ("", "[time]", "[number]", "none", "null", "undefined"):
                    pref_time = "Anytime"
                state.callback_time = normalize_callback_time(pref_time)
                state.callback_details = f"Alternate number: {digits_only} @ {state.callback_time}"
                state.rpc_status = RPC_STATUS["callback"]
                callback_booked = True
                print(f"{log_prefix} [Tool] Callback booked: {digits_only} @ {state.callback_time}")
            else:
                print(f"{log_prefix} [Tool] Callback REJECTED — invalid number: '{raw_number}'")

        elif tool_name == "share_payment_link":
            channel = tool_args.get("channel", "sms")
            state.payment_link_channel = channel
            state.payment_mode = "payment_link_email" if "email" in str(channel).lower() else "payment_link_sms"
            link = f"{PAYMENT_LINK_BASE_URL}{asterisk_id}"
            state.interaction_notes.append(f"Payment link shared via {channel}: {link}")
            state.disposition = CALL_OUTCOME["completed"]
            print(f"{log_prefix} [Tool] Payment link shared via {channel} (mock)")

        elif tool_name == "record_payment_mode":
            mode = tool_args.get("mode", "")
            state.payment_mode = mode
            state.interaction_notes.append(f"Payment mode: {mode}")
            print(f"{log_prefix} [Tool] Payment mode recorded: {mode}")

        elif tool_name == "save_call_data":
            # FIX: LLM jo data arguments me bhej raha hai usse state update karein
            if tool_args.get("payment_intent"):
                state.payment_intent = tool_args.get("payment_intent")
            if tool_args.get("payment_mode"):
                state.payment_mode = tool_args.get("payment_mode")
            if tool_args.get("ptp_amount") and not state.ptp_amount:
                full_val = float((loan or {}).get("amount", 0) or 0)
                state.ptp_amount = normalize_ptp_amount(tool_args.get("ptp_amount"), fallback_balance=str(int(full_val)))
                state.ptp_date = normalize_ptp_date(tool_args.get("ptp_date"))
            elif not state.ptp_date and state.payment_intent in ("WILL PAY", "will_pay"):
                state.ptp_date = normalize_ptp_date("today")

            state.saved_call_data = build_disposition(state, is_final=True)
            state.disposition = CALL_OUTCOME["completed"]
            clean_log = " ".join(state.conversation_log).lower()
            goodbye_said = any(w in clean_log for w in ["goodbye", "good-bye", "good bye", "bye"])
            if not _stop_streaming[0] and goodbye_said:
                end_call = True
            print(f"{log_prefix} [Tool] save_call_data → {state.saved_call_data}")

        persist_interim(state)

    def apply_marker_side_effects(text: str) -> None:
        nonlocal end_call, wrong_number_only
        clean_t = _clean_for_goodbye_match(text)
        if callback_booked:
            if DISCONNECT_MARKER in text or any(w in clean_t for w in ["good-bye", "goodbye", "good bye", "bye"]):
                end_call = True
            return
        if RPC_VERIFIED_MARKER in text:
            state.rpc_verified = True
            state.rpc_status = RPC_STATUS["verified"]
        if RPC_NOT_VERIFIED_MARKER in text:
            wrong_number_only = True
            state.rpc_status = RPC_STATUS["not_verified"]
        if RPC_CALLBACK_MARKER in text:
            state.rpc_status = RPC_STATUS["callback"]
        if INTENT_WILL_PAY_MARKER in text:
            state.payment_intent = PAYMENT_INTENT["will_pay"]
        if INTENT_NEEDS_TIME_MARKER in text:
            state.payment_intent = PAYMENT_INTENT["needs_time"]
        if INTENT_TRANSFER_MARKER in text:
            state.payment_intent = PAYMENT_INTENT["transfer"]
            state.transfer_reason = state.transfer_reason or "customer_request"
            state.disposition = CALL_OUTCOME.get("transfer", "TRANSFER_TO_AGENT")
            transfer_requests[asterisk_id] = TRANSFER_QUEUE
            end_call = True
            persist_interim(state, is_final=True)
        if INTENT_REFUSES_MARKER in text:
            state.payment_intent = PAYMENT_INTENT["refuses"]
        if PTP_CAPTURED_MARKER in text:
            state.interaction_notes.append("PTP confirmed by customer")
            if any(w in clean_t for w in ["disclose", "goodbye", "good-bye", "good bye", "bye", "thank you"]):
                end_call = True
        if DATE_NEGOTIATED_MARKER in text:
            state.interaction_notes.append("Payment date agreed by customer")
        if PAYMENT_LINK_MARKER in text:
            state.interaction_notes.append("Payment link offered")
            if any(w in clean_t for w in ["disclose", "goodbye", "good-bye", "good bye", "bye", "thank you"]):
                end_call = True
        if DISCONNECT_MARKER in text or any(w in clean_t for w in ["good-bye", "goodbye", "good bye", "bye"]):
            end_call = True

    async def deterministic_close(reason: str) -> None:
        """Deterministic RPC-not-verified close."""
        nonlocal wrong_number_only
        closing_line = "No problem. I understand. Thank you for your time. Goodbye."
        print(f"{log_prefix} [RPC GATE] Deterministic close ({reason}).")
        state.conversation_log.append(f"Bot: {closing_line}")

        async def closing_stream():
            yield closing_line

        await stream_tts(closing_stream(), writer=writer)
        wrong_number_only = True
        state.rpc_status = RPC_STATUS["not_verified"]

    try:
        async def run_bot_turn(user_text: str) -> str | None:
            """Streams LLM -> TTS while listening in the background."""
            nonlocal end_call
            spoke = [False]
            any_sentence_produced = [False]
            interrupt_event = asyncio.Event()
            tts_started = asyncio.Event()

            state.ptp_captured = False
            state.payment_link_channel = None

            _SENTENCE_CAP = 12
            _yielded_count = [0]
            _stop_streaming[0] = False
            first_audio_time: list[float] = [0.0]


            async def echoing_stream():
                _yielded_count[0] = 0
                _stop_streaming[0] = False
                buffer = ""
                last_yielded = ""
                pending_title = ""
                seen_sentences_in_turn = set()

                async for chunk in state.llm.ask_llm_stream(
                    user_text,
                    sys_prompt=full_prompt,
                    use_history=True,
                    tool_callback=on_tool_call,
                    run_name=f"fairmoney-{asterisk_id}",
                ):
                    if _stop_streaming[0]:
                        continue
                    buffer += chunk
                    if not any(buffer.endswith(p) for p in (".", "?", "!", "\n", ";")):
                        continue
                    apply_marker_side_effects(buffer)
                    sentence = strip_markers(buffer).rstrip(";").strip()
                    buffer = ""
                    if sentence:
                        any_sentence_produced[0] = True
                        if is_hallucinated_response(sentence):
                            print(f"{log_prefix} [HALLUCINATION FILTER] Dropped: {sentence}")
                            state.conversation_log.append(f"[FILTERED] {sentence}")
                            continue
							 # ── REPEAT GUARD: NEVER re-ask for amount/date once PTP is captured ──
                        if (state.ptp_captured or state.ptp_amount) and any(k in sentence.lower() for k in [
                            "exact amount", "amount you will be paying", "make the payment today", "which date will you", "paying today, is that correct"
                        ]):
                            print(f"{log_prefix} [REPEAT GUARD] Suppressed duplicate PTP question after PTP already captured: {sentence}")
                            state.conversation_log.append(f"[REPEAT GUARD] {sentence}")
                            continue
                        if pending_title:
                            sentence = pending_title + " " + sentence
                            pending_title = ""
                        if sentence.rstrip().endswith(("Mr.", "Mrs.", "Dr.", "Ms.", "Prof.")):
                            pending_title = sentence.rstrip()
                            continue
                        clean_norm = re.sub(r"[^a-zA-Z0-9 ]", "", sentence.lower()).strip()
                        if clean_norm in seen_sentences_in_turn or sentence.strip().rstrip() == last_yielded.strip().rstrip():
                            print(f"{log_prefix} [DEDUP FILTER] Dropped duplicate/repeat: {sentence}")
                            state.conversation_log.append(f"[DEDUP] {sentence}")
                            continue
                        seen_sentences_in_turn.add(clean_norm)
                        if _yielded_count[0] >= _SENTENCE_CAP:
                            print(f"{log_prefix} [SENTENCE CAP] Dropped ({_yielded_count[0]}/{_SENTENCE_CAP}): {sentence}")
                            state.conversation_log.append(f"[CAP] {sentence}")
                            continue
                        _yielded_count[0] += 1
                        last_yielded = sentence
                        spoke[0] = True
                        print(f"{log_prefix} Bot: {sentence}")
                        state.conversation_log.append(f"Bot: {sentence}")
                        if first_audio_time[0] == 0.0:
                            first_audio_time[0] = time.perf_counter()
                        yield sentence
                        tts_started.set()

                        # Immediate disconnect trigger on goodbye (handles Unicode non-breaking hyphens \u2011)
                        norm_sent = _clean_for_goodbye_match(sentence)
                        if any(w in norm_sent for w in ["good-bye", "goodbye", "good bye", "bye"]):
                            print(f"{log_prefix} [GOODBYE DETECTED] Ending call immediately after speaking.")
                            end_call = True
                            state.disposition = CALL_OUTCOME["completed"]
                            _stop_streaming[0] = True

                        if sentence.rstrip().endswith("?"):
                            _stop_streaming[0] = True

                if pending_title and not _stop_streaming[0]:
                    spoke[0] = True
                    print(f"{log_prefix} Bot: {pending_title}")
                    state.conversation_log.append(f"Bot: {pending_title}")
                    yield pending_title
                    tts_started.set()
                if buffer and not _stop_streaming[0]:
                    apply_marker_side_effects(buffer)
                    sentence = strip_markers(buffer)
                    if sentence:
                        any_sentence_produced[0] = True
                        if is_hallucinated_response(sentence):
                            print(f"{log_prefix} [HALLUCINATION FILTER] Dropped: {sentence}")
                            state.conversation_log.append(f"[FILTERED] {sentence}")
                        elif sentence.strip().rstrip() == last_yielded.strip().rstrip():
                            print(f"{log_prefix} [DEDUP FILTER] Dropped duplicate: {sentence}")
                            state.conversation_log.append(f"[DEDUP] {sentence}")
                        elif _yielded_count[0] < _SENTENCE_CAP:
                            spoke[0] = True
                            print(f"{log_prefix} Bot: {sentence}")
                            state.conversation_log.append(f"Bot: {sentence}")
                            yield sentence

                            norm_sent = _clean_for_goodbye_match(sentence)
                            if any(w in norm_sent for w in ["good-bye", "goodbye", "good bye", "bye"]):
                                print(f"{log_prefix} [GOODBYE DETECTED] Ending call immediately after speaking.")
                                end_call = True
                                state.disposition = CALL_OUTCOME["completed"]
                                _stop_streaming[0] = True

            async def speak() -> bool:
                state.stt.bot_speaking.set()
                try:
                    return bool(await stream_tts(
                        echoing_stream(), writer=writer, interrupt_event=interrupt_event
                    ))
                finally:
                    state.stt.bot_speaking.clear()
                    tts_started.set()

            async def background_listen():
                await tts_started.wait()
                interrupt_event.clear()
                return await state.stt.listen_turn(loop=loop, interrupt_event=interrupt_event)

            speak_task = asyncio.create_task(speak())
            listen_task = asyncio.create_task(background_listen())

            done, _ = await asyncio.wait(
                [speak_task, listen_task], return_when=asyncio.FIRST_COMPLETED
            )

            was_interrupted = False
            interrupt_text = ""
            deferred_reply = ""

            if speak_task in done:
                exc = speak_task.exception()
                if exc:
                    state.stt.abort_current_listen()
                    listen_task.cancel()
                    raise exc
                if not speak_task.result():
                    was_interrupted = True
                
                # CRITICAL: If goodbye was spoken, do NOT sit listening to dead air!
                # BUT if bot just asked a question (_stop_streaming[0]), NEVER cancel listen!
                if end_call and not _stop_streaming[0]:
                    state.stt.abort_current_listen()
                    listen_task.cancel()
                    try:
                        await listen_task
                    except (asyncio.CancelledError, Exception):
                        pass

            else:
                try:
                    interrupt_text = (await listen_task).strip()
                except (asyncio.CancelledError, Exception):
                    interrupt_text = ""

                if interrupt_text:
                    elapsed = (
                        time.perf_counter() - first_audio_time[0]
                        if first_audio_time[0] > 0
                        else 0.0
                    )
                    if elapsed >= MIN_BOT_SPEAK_SECS:
                        was_interrupted = True
                        speak_task.cancel()
                    else:
                        print(
                            f"{log_prefix} [InterruptGuard] Transcript "
                            f"{interrupt_text!r} arrived after only "
                            f"{elapsed:.2f}s of bot audio — deferring as next user turn."
                        )
                        deferred_reply = interrupt_text
                        interrupt_text = ""
                        try:
                            await speak_task
                        except asyncio.CancelledError:
                            pass
                        except Exception as exc:
                            raise exc
                        was_interrupted = False
                else:
                    was_interrupted = False

            if was_interrupted:
                if not interrupt_text:
                    try:
                        interrupt_text = (await listen_task).strip()
                    except (asyncio.CancelledError, Exception):
                        interrupt_text = ""
                if interrupt_text:
                    speak_task.cancel()
                else:
                    was_interrupted = False

            if not was_interrupted:
                if not speak_task.done():
                    try:
                        await speak_task
                    except asyncio.CancelledError:
                        pass
                if not listen_task.done():
                    if _stop_streaming[0]:
                        end_call = False
                        try:
                            customer_reply = (await listen_task).strip()
                        except (asyncio.CancelledError, Exception):
                            customer_reply = ""
                        if customer_reply:
                            return customer_reply
                        return None
                    else:
                        state.stt.abort_current_listen()
                        listen_task.cancel()
                        try:
                            await listen_task
                        except (asyncio.CancelledError, Exception):
                            pass

            # ── Check if goodbye was already spoken anywhere in conversation ─────
            def already_said_goodbye() -> bool:
                return any(
                    any(w in _clean_for_goodbye_match(str(log)) for w in ["good-bye", "goodbye", "good bye", "bye"])
                    for log in state.conversation_log
                )

            # Fallback for empty turns:
            if not spoke[0] and not any_sentence_produced[0] and not was_interrupted and (end_call or state.callback_required or state.payment_link_channel or state.ptp_captured or getattr(state, "payment_mode", "")):
                if state.refusal_outcome:
                    fallback = (
                        "I understand your situation. I am transferring your case "
                        "to an agent who will assist you further. Thank you for your time."
                    )
                elif state.transfer_reason:
                    fallback = (
                        "I understand. I am connecting you to an agent who will "
                        "help you. Please stay on the line."
                    )
                elif state.callback_required:
                    fallback = (
                        "Thank you. We will call you back at the time you preferred."
                    )
                elif state.payment_link_channel:
                    dest = "email address" if "email" in str(state.payment_link_channel).lower() else "phone number"
                    fallback = (
                        f"I have now sent the payment link to your {dest}. "
                        "For ease of payment, you can pay via the FairMoney app or "
                        "transfer directly to your FairMoney account. Do not disclose "
                        "your BVN, log-in details, or OTP to anyone. "
                        f"Thank you for choosing FairMoney Microfinance Bank, {state.customer_name}. Good-bye."
                    )
                    state.payment_link_channel = None
                    end_call = True
                elif getattr(state, "payment_mode", ""):

                    fallback = (
                        "For ease of payment, you can pay via the FairMoney app or "
                        "transfer directly to your FairMoney account. Do not disclose "
                        "your BVN, log-in details, or OTP to anyone. "
                        f"Thank you for choosing FairMoney Microfinance Bank, {state.customer_name}. Good-bye."
                    )
                elif state.ptp_captured:
                    fallback = (
                        "Great. How would you like to make the payment — via the "
                        "FairMoney app, bank transfer, or would you like me to send "
                        "you a payment link?"
                    )
                    state.ptp_captured = False
                else:
                    fallback = "Thank you for your time. Goodbye."

                print(f"{log_prefix} Bot: {fallback}")
                state.conversation_log.append(f"Bot: {fallback}")

                async def fallback_stream():
                    yield fallback

                await stream_tts(fallback_stream(), writer=writer)
                if state.payment_link_channel or state.callback_required or state.refusal_outcome or state.transfer_reason or getattr(state, "payment_mode", "") or "goodbye" in fallback.lower():
                    end_call = True

            # ── CLOSING SAFETY NET ──────────────────────────────────────────
            # Only trigger if terminal action occurred AND goodbye was genuinely never spoken (NEVER on transfer or callback)
            is_transfer_or_cb = bool(asterisk_id in transfer_requests or state.transfer_reason or state.callback_required or callback_booked or state.payment_intent == PAYMENT_INTENT.get("transfer"))
            if not _stop_streaming[0] and not is_transfer_or_cb and (getattr(state, "payment_mode", "") or state.payment_link_channel or end_call) and not already_said_goodbye():
                closing_disclaimer = (
                    "For ease of payment, you can pay via the FairMoney app or transfer directly to your FairMoney account. "
                    "Do not disclose your BVN, log-in details, or OTP to anyone. "
                    f"Thank you for choosing FairMoney Microfinance Bank, {state.customer_name}. Good-bye."
                )
                print(f"{log_prefix} Bot (closing auto-completion): {closing_disclaimer}")
                state.conversation_log.append(f"Bot: {closing_disclaimer}")

                async def closing_stream():
                    yield closing_disclaimer

                await stream_tts(closing_stream(), writer=writer)
                state.disposition = CALL_OUTCOME["completed"]
                end_call = True

            return deferred_reply or (interrupt_text if was_interrupted else None)

        call_started = time.perf_counter()
        greeting_salutation = _get_time_greeting()
        greeting_line = f"{greeting_salutation}, is this {state.customer_name}?"
        print(f"{log_prefix} Bot (instant greeting): {greeting_line}")
        state.conversation_log.append(f"Bot: {greeting_line}")
        state.llm.conversation_history.append(AIMessage(content=greeting_line))

        async def speak_text_with_bargein(text: str) -> str | None:
            interrupt_event = asyncio.Event()
            tts_started = asyncio.Event()

            async def text_stream():
                yield text

            async def speak():
                state.stt.bot_speaking.set()
                try:
                    return bool(await stream_tts(text_stream(), writer=writer, interrupt_event=interrupt_event))
                finally:
                    state.stt.bot_speaking.clear()
                    tts_started.set()

            async def background_listen():
                return await state.stt.listen_turn(loop=loop, interrupt_event=interrupt_event)

            state.stt.set_resume_mode()
            speak_task = asyncio.create_task(speak())
            listen_task = asyncio.create_task(background_listen())

            done, _ = await asyncio.wait([speak_task, listen_task], return_when=asyncio.FIRST_COMPLETED)

            if speak_task in done and speak_task.result():
                if not listen_task.done():
                    state.stt.abort_current_listen()
                    listen_task.cancel()
                    try:
                        await listen_task
                    except (asyncio.CancelledError, Exception):
                        pass
                return None
            else:
                speak_task.cancel()
                try:
                    return (await listen_task).strip()
                except Exception:
                    return None

        async def play_instant_greeting_with_bargein() -> str:
            return await speak_text_with_bargein(greeting_line) or ""

        greeting_reply = await play_instant_greeting_with_bargein()
        if not greeting_reply and not writer.is_closing():
            print(f"{log_prefix} Bot finished greeting — listening for customer reply...")
            try:
                greeting_reply = (await state.stt.listen_turn(loop=loop)).strip()
            except Exception as err:
                print(f"{log_prefix} Error listening for greeting reply: {err}")
                greeting_reply = ""
        if greeting_reply:
            state.llm.conversation_history.append(HumanMessage(content=greeting_reply))
        persist_interim(state)

        def loan_script_spoken() -> bool:
            return any(
                m.startswith("Bot:")
                and re.search(
                    r"(outstanding|overdue|balance|due on|loan)",
                    m,
                )
                for m in state.conversation_log
            )

        for turn_no in range(1, MAX_TURNS + 1):
            if end_call or writer.is_closing() or (hasattr(writer, "transport") and writer.transport is not None and writer.transport.is_closing()):
                if end_call:
                    await send_hangup(writer, asterisk_id)
                print(f"{log_prefix} Call loop ending (end_call={end_call}, closing={writer.is_closing()}).")
                break

            elapsed = time.perf_counter() - call_started
            if elapsed > MAX_CALL_DURATION_SECS:
                print(
                    f"{log_prefix} Max call duration ({MAX_CALL_DURATION_SECS:.0f}s) "
                    f"reached — closing gracefully."
                )
                async def timeout_bye_stream():
                    yield "Thank you for your time. Goodbye."
                await stream_tts(timeout_bye_stream(), writer=writer)
                await send_hangup(writer, asterisk_id)
                break

            if turn_no == 1 and greeting_reply:
                user_text = greeting_reply
                greeting_reply = ""
                print(f"{log_prefix} Customer (greeting reply): {user_text}")
            elif turn_no == 1:
                print(f"{log_prefix} Greeting silence — starting check-in")
                user_text = ""
            else:
                print(f"{log_prefix} Turn {turn_no}: listening...")
                user_text = await state.stt.listen_turn(loop=loop)
            clean = user_text.strip()

            if not clean:
                state.empty_count += 1
                print(f"{log_prefix} Empty transcript ({state.empty_count}/{MAX_EMPTY_RETRIES}).")
                if state.empty_count >= MAX_EMPTY_RETRIES:
                    async def empty_disconnect_stream():
                        yield "I understand you may be busy. Please call us back when you are available. Goodbye."
                    await stream_tts(empty_disconnect_stream(), writer=writer)
                    state.conversation_log.append("Bot: I understand you may be busy. Please call us back when you are available. Goodbye.")
                    final_listen = await state.stt.listen_turn(loop=loop)
                    final_clean = final_listen.strip()
                    if final_clean:
                        state.conversation_log.append(f"Customer: {final_clean}")
                        print(f"{log_prefix} Customer (post-goodbye): {final_clean}")
                        pending_input = final_clean
                        while pending_input and not end_call:
                            interrupted_by = await run_bot_turn(pending_input)
                            persist_interim(state)
                            if interrupted_by:
                                print(f"{log_prefix} Customer (interrupt): {interrupted_by}")
                                state.conversation_log.append(f"Customer (interrupt): {interrupted_by}")
                                _detect_and_set_payment_mode(state, interrupted_by)
                                pending_input = interrupted_by
                            else:
                                break
                    await send_hangup(writer, asterisk_id)
                    break
                if state.empty_count == 1:
                    async def ask_still_there_stream():
                        yield "Are you still there?"
                    await stream_tts(ask_still_there_stream(), writer=writer)
                    state.conversation_log.append("Bot: Are you still there?")
                elif state.empty_count == 2:
                    async def ask_need_help_stream():
                        yield "I am still here if you need help. Please let me know if you would like to continue."
                    await stream_tts(ask_need_help_stream(), writer=writer)
                    state.conversation_log.append("Bot: I am still here if you need help. Please let me know if you would like to continue.")
                continue

            state.empty_count = 0
            state.conversation_log.append(f"Customer: {clean}")
            print(f"{log_prefix} Customer: {clean}")

            # Auto-detect payment mode from customer words:
            _detect_and_set_payment_mode(state, clean)
            persist_interim(state)  # <── IMMEDIATE CRM UPDATE ON USER INPUT

            pending_input = clean
            while pending_input and not end_call:
                # ── IMMEDIATE AGENT TRANSFER (Nigerian English Collections Standard) ──
                if not end_call and _is_agent_transfer_request(pending_input):
                    print(f"{log_prefix} [AGENT TRANSFER] Customer requested agent/refused bot: {pending_input!r}")
                    transfer_phrase = (
                        "I understand. I am transferring you to one of our collections specialists right away. "
                        "Please hold the line."
                    )
                    state.conversation_log.append(f"Bot: {transfer_phrase}")
                    async def transfer_stream():
                        yield transfer_phrase
                    await stream_tts(transfer_stream(), writer=writer)
                    state.transfer_reason = "customer_requested_agent"
                    state.payment_intent = PAYMENT_INTENT["transfer"]
                    state.disposition = CALL_OUTCOME.get("transfer", "TRANSFER_TO_AGENT")
                    transfer_requests[asterisk_id] = TRANSFER_QUEUE
                    end_call = True
                    persist_interim(state, is_final=True)
                    break

                # ── STEP 2: POSITIVE RPC CONFIRMATION (Loan Reminder) ──
                if (
                    not state.rpc_verified
                    and not wrong_number_only
                    and not callback_booked
                    and not rpc_gate_routed
                    and not loan_script_spoken()
                    and _is_clear_positive(pending_input, state.customer_name)
                ):
                    state.rpc_verified = True
                    state.rpc_status = RPC_STATUS["verified"]
                    persist_interim(state)  # <── IMMEDIATE CRM UPDATE ON IDENTITY CONFIRMATION
                    step2_script = _build_step2_script(state, loan)
                    print(
                        f"{log_prefix} [STEP 2] Deterministic script after positive RPC confirmation."
                    )
                    state.conversation_log.append(f"Bot: {step2_script}")
                    state.llm.conversation_history.append(AIMessage(content=step2_script))
                    interrupted_step2 = await speak_text_with_bargein(step2_script)
                    if interrupted_step2:
                        print(f"{log_prefix} Customer (interrupt during Step 2): {interrupted_step2}")
                        state.conversation_log.append(f"Customer (interrupt): {interrupted_step2}")
                        pending_input = interrupted_step2
                        continue
                    break

                # ── RPC GATE: If customer is in verification of the 11-digit number ──
                if (
                    not state.rpc_verified
                    and not wrong_number_only
                    and not callback_booked
                    and rpc_gate_routed
                    and rpc_gate_verifying_phone
                ):
                    # Customer confirms YES to the read-back number:
                    if _is_clear_positive(pending_input, state.customer_name):
                        state.alternate_number = rpc_gate_verifying_phone
                        state.callback_number = rpc_gate_verifying_phone
                        state.callback_required = True
                        rpc_gate_verifying_phone = ""
                        print(f"{log_prefix} [RPC GATE] Alternate number confirmed: {state.alternate_number}")

                        # Move to NEXT step: Ask for preferred callback time
                        ask_time_line = (
                            "Thank you. What time would be convenient for us to call back on this number — in the morning or in the evening?"
                        )
                        state.conversation_log.append(f"Bot: {ask_time_line}")
                        interrupted_time = await speak_text_with_bargein(ask_time_line)
                        user_time = interrupted_time.strip() if interrupted_time else (await state.stt.listen_turn(loop=loop)).strip()

                        if user_time:
                            state.conversation_log.append(f"Customer: {user_time}")
                            state.callback_time = normalize_callback_time(user_time)
                        else:
                            state.callback_time = normalize_callback_time("Anytime")

                        state.callback_details = f"Alternate number: {state.alternate_number} @ {state.callback_time}"
                        closing_line = (
                            f"Thank you. We have noted that and will call back on {state.callback_time}. Have a good day."
                            if user_time
                            else "Thank you for providing the alternative number. Have a good day."
                        )
                        state.conversation_log.append(f"Bot: {closing_line}")
                        async def close_stream():
                            yield closing_line
                        await stream_tts(close_stream(), writer=writer)
                        wrong_number_only = True
                        state.rpc_status = RPC_STATUS["callback"]
                        break

                    # If customer says NO / provides a different number:
                    else:
                        norm_phone = normalize_phone_number(pending_input)
                        if len(norm_phone) >= 10 and norm_phone.isdigit():
                            rpc_gate_verifying_phone = norm_phone
                            spaced_digits = " ".join(norm_phone)
                            confirm_line = f"Let me confirm, the number is {spaced_digits}, is that correct?"
                            state.conversation_log.append(f"Bot: {confirm_line}")
                            interrupted_conf = await speak_text_with_bargein(confirm_line)
                            if interrupted_conf:
                                pending_input = interrupted_conf
                                continue
                            break
                        elif _is_no_alternate_number(pending_input):
                            await deterministic_close("customer refused alternate number")
                            break
                        else:
                            retry_line = "My apologies, could you please repeat the 11-digit contact number?"
                            state.conversation_log.append(f"Bot: {retry_line}")
                            async def retry_stream():
                                yield retry_line
                            await stream_tts(retry_stream(), writer=writer)
                            break

                # ── RPC GATE: Customer provides the 11-digit alternative number ──
                if (
                    not state.rpc_verified
                    and not wrong_number_only
                    and not callback_booked
                    and not loan_script_spoken()
                    and rpc_gate_routed
                    and not rpc_gate_verifying_phone
                ):
                    norm_phone = normalize_phone_number(pending_input)
                    if len(norm_phone) >= 10 and norm_phone.isdigit():
                        rpc_gate_verifying_phone = norm_phone
                        spaced_digits = " ".join(norm_phone)
                        # READ-BACK VERIFICATION TURN:
                        confirm_line = f"Let me confirm, the number is {spaced_digits}, is that correct?"
                        print(f"{log_prefix} [RPC GATE] Verifying captured number: {norm_phone}")
                        state.conversation_log.append(f"Bot: {confirm_line}")
                        interrupted_conf = await speak_text_with_bargein(confirm_line)
                        if interrupted_conf:
                            pending_input = interrupted_conf
                            continue
                        break
                    elif _is_no_alternate_number(pending_input):
                        await deterministic_close("no alternate number provided")
                        break
                    else:
                        await deterministic_close("no valid alternate number after routing")
                        break

                # ── RPC GATE: Initial denial trigger (Customer denies being the person) ──
                if (
                    not state.rpc_verified
                    and not wrong_number_only
                    and not callback_booked
                    and not rpc_gate_routed
                    and not loan_script_spoken()
                    and _is_clear_negative(pending_input, state.customer_name)
                ):
                    rpc_gate_routed = True
                    state.rpc_status = RPC_STATUS["not_verified"]
                    no_alt_now = _is_no_alternate_number(pending_input)
                    if no_alt_now:
                        gate_script = (
                            f"I apologize for the disturbance. We have been trying to reach {state.customer_name}. Have a good day."
                        )
                    else:
                        # Clean 11-digit request:
                        gate_script = (
                            "I apologize for the disturbance. We have been trying to reach "
                            f"{state.customer_name}. Could you please provide an alternative "
                            "11-digit contact number where they can be reached?"
                        )
                    print(
                        f"{log_prefix} [RPC GATE] Denial detected. Asking for 11-digit alternative number: {pending_input!r}"
                    )
                    state.conversation_log.append(f"Bot: {gate_script}")
                    state.llm.conversation_history.append(AIMessage(content=gate_script))

                    interrupted_gate = await speak_text_with_bargein(gate_script)
                    if no_alt_now:
                        await deterministic_close("denial includes no alternate number")
                        break
                    if interrupted_gate:
                        pending_input = interrupted_gate
                        continue
                    break

                if (
                    not state.rpc_verified
                    and not wrong_number_only
                    and not callback_booked
                    and not rpc_gate_routed
                ):
                    rpc_gate_attempts += 1
                    if rpc_gate_attempts >= RPC_MAX_GATE_ATTEMPTS + 1:
                        rpc_gate_routed = True
                        state.rpc_status = RPC_STATUS["not_verified"]
                        gate_script = (
                            "Please be aware that for quality purposes this call "
                            "is being recorded. My name is Taiwo calling from "
                            "Lagos. Sir, we have been trying to reach "
                            f"{state.customer_name} on this line, may we have an "
                            f"alternative number {state.pronoun} can be reached on?"
                        )
                        print(
                            f"{log_prefix} [RPC GATE] Deterministic third-party "
                            f"routing after {rpc_gate_attempts} identity attempts."
                        )
                        state.conversation_log.append(f"Bot: {gate_script}")
                        state.llm.conversation_history.append(AIMessage(content=gate_script))

                        interrupted_gate = await speak_text_with_bargein(gate_script)
                        if interrupted_gate:
                            print(f"{log_prefix} Customer (interrupt during Gate): {interrupted_gate}")
                            state.conversation_log.append(f"Customer (interrupt): {interrupted_gate}")
                            pending_input = interrupted_gate
                            continue
                        break
                log_len_before = len(state.conversation_log)
                interrupted_by = await run_bot_turn(pending_input)
                persist_interim(state)

                # ── CRITICAL FIX: Goodbye bolte hi turant loop break aur hangup karein ──
                if end_call:
                    print(f"{log_prefix} [GOODBYE DETECTED] Ending call immediately without listening further.")
                    await send_hangup(writer, asterisk_id)
                    try:
                        writer.close()
                    except Exception:
                        pass
                    break

                if interrupted_by:
                    print(f"{log_prefix} Customer (interrupt): {interrupted_by}")
                    state.conversation_log.append(f"Customer (interrupt): {interrupted_by}")
                    _detect_and_set_payment_mode(state, interrupted_by)
                    pending_input = interrupted_by
                else:
                    added = state.conversation_log[log_len_before:]
                    if (
                        not end_call
                        and not wrong_number_only
                        and not callback_booked
                        and state.rpc_verified
                        and not state.ptp_captured
                        and state.payment_intent in (None, PAYMENT_INTENT["will_pay"])
                        and re.search(
                            r"\b(pay(?:ing|ment)?|amount|half|full|today|tomorrow|"
                            r"money|installment|monthly|\d)\b",
                            pending_input.lower(),
                        )
                        and any(m.startswith("Bot:") for m in added)
                        and not any("?" in m for m in added)
                    ):
                        step3_refill = (
                            "Great. And could you confirm the exact amount you "
                            "would like to pay, and the date you plan to pay it?"
                        )
                        print(
                            f"{log_prefix} [STEP 3] Deterministic PTP capture refill."
                        )
                        state.conversation_log.append(f"Bot: {step3_refill}")

                        async def step3_refill_stream():
                            yield step3_refill

                        await stream_tts(step3_refill_stream(), writer=writer)
                    break

            if callback_booked or wrong_number_only or end_call or (asterisk_id in transfer_requests):
                state.disposition = state.disposition or CALL_OUTCOME["completed"]
                print(
                    f"{log_prefix} Callback flow complete / transfer requested "
                    f"(booked={callback_booked}, declined={wrong_number_only}, end_call={end_call}, transfer={asterisk_id in transfer_requests}) — closing call."
                )
                await send_hangup(writer, asterisk_id)
                try:
                    writer.close()
                except Exception:
                    pass
                break

        state.disposition = state.disposition or CALL_OUTCOME["completed"]
    except asyncio.CancelledError:
        log.warning(f"{log_prefix} Call task cancelled (caller hung up / channel dropped).")
        if not state.disposition:
            state.disposition = CALL_OUTCOME["aborted"]
        persist_interim(state, is_final=True)
        raise
    except Exception as e:
        log.exception(f"{log_prefix} Unhandled error in call flow: {e}")
        state.disposition = CALL_OUTCOME["failed"]
        persist_interim(state, is_final=True)
    finally:
        # Final status check if not already stamped
        if not state.disposition:
            state.disposition = CALL_OUTCOME["completed"] if state.rpc_verified else CALL_OUTCOME["aborted"]

        final_dispo = build_disposition(state, is_final=True)
        disposition_requests[asterisk_id] = final_dispo
        if state.saved_call_data is None:
            state.saved_call_data = final_dispo

# Save to local disk (.jsonl & transcript)
        save_disposition(state)
        # ── ALWAYS PUSH FINAL DEFINITIVE DISPOSITION TO CRM API & LOCAL DB ──
        upsert_call(final_dispo)
        try:
            await post_disposition_api_async(final_dispo)
            log.info(
                f"[{asterisk_id}] Final CRM disposition successfully pushed: "
                f"outcome={final_dispo.get('call_outcome')} "
                f"intent={final_dispo.get('payment_intent')} "
                f"mode={final_dispo.get('payment_mode')}"
            )
        except Exception as api_err:
            log.warning(f"[{asterisk_id}] Final CRM API push error: {api_err}")
        if hasattr(state, "stt") and state.stt:
            try:
                await state.stt.close()
            except Exception as stt_err:
                log.warning(f"{log_prefix} Error closing STT service: {stt_err}")
        session_manager.remove(asterisk_id)
        print(f"{log_prefix} Call ended.")