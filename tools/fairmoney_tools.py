from datetime import date, datetime, timedelta
import re

from langchain_core.tools import tool

from config.disposition import MAX_PAYMENT_DATE_DAYS, DAYS_WINDOW_MODE


WORD_MAP = {
    "zero": "0", "oh": "0", "o": "0",
    "one": "1",
    "two": "2", "to": "2", "too": "2",
    "three": "3",
    "four": "4", "for": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8", "ate": "8",
    "nine": "9",
}

def normalize_phone_number(phone_raw: str) -> str:
    """Normalize phone numbers from raw digits or spoken words (e.g. 'nine one four double one...')."""
    if not phone_raw:
        return ""

    text = str(phone_raw).lower()
    tokens = re.findall(r"[a-z0-9]+", text)
    out = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        # Handle "double one" -> "11", "triple five" -> "555"
        if t in ("double", "triple") and i + 1 < len(tokens):
            multiplier = 2 if t == "double" else 3
            nxt = tokens[i + 1]
            digit = WORD_MAP.get(nxt, nxt if nxt.isdigit() else None)
            if digit:
                out.append(digit * multiplier)
                i += 2
                continue
        if t in WORD_MAP:
            out.append(WORD_MAP[t])
        elif t.isdigit():
            out.append(t)
        i += 1

    digits = "".join(out)

    # Standard Nigerian mobile normalization:
    if len(digits) == 13 and digits.startswith("234"):
        digits = "0" + digits[3:]
    elif len(digits) == 10 and digits[0] in "7891":
        digits = "0" + digits
    elif len(digits) == 9 and digits[0] in "7891":
        digits = "0" + digits

    return digits


def normalize_ptp_date(raw_date: str) -> str:
    """
    Convert spoken dates ('today', 'tomorrow', 'Friday', etc.) 
    into standard SQL DATETIME 'YYYY-MM-DD 18:00:00'.
    """
    if not raw_date:
        return datetime.now().strftime("%Y-%m-%d 18:00:00")
    
    clean = str(raw_date).strip().lower()
    now = datetime.now()
    
    if "today" in clean:
        target = now
    elif "tomorrow" in clean:
        target = now + timedelta(days=1)
    else:
        # Check weekdays (Monday to Sunday)
        weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        found_day = None
        for idx, day in enumerate(weekdays):
            if day in clean:
                found_day = idx
                break
        if found_day is not None:
            today_idx = now.weekday()
            days_ahead = (found_day - today_idx) % 7
            if days_ahead == 0:
                days_ahead = 7
            target = now + timedelta(days=days_ahead)
        else:
            # Check YYYY-MM-DD
            m = re.search(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", clean)
            if m:
                y, mo, d = m.groups()
                try:
                    target = datetime(int(y), int(mo), int(d))
                except ValueError:
                    target = now
            else:
                target = now

    return target.strftime("%Y-%m-%d 18:00:00")


def normalize_ptp_amount(raw_amount: str, fallback_balance: str = "50000") -> str:
    """
    Convert spoken or formatted amount ('fifty thousand naira', '50,000', 'full amount')
    into clean numeric string '50000'.
    """
    if not raw_amount:
        return fallback_balance
    
    text = str(raw_amount).lower().replace(",", "")
    
    # 1. If explicit digits exist (e.g. 50000 or 50000.00)
    match = re.search(r"\b\d+(\.\d+)?\b", text)
    if match:
        return str(int(float(match.group(0))))
        
    # 2. Check for 'full' / 'balance'
    if any(k in text for k in ("full", "complete", "total", "outstanding")):
        return fallback_balance
        
    # 3. Common spoken amount phrases
    spoken_map = {
        "hundred thousand": "100000",
        "fifty thousand": "50000",
        "twenty five thousand": "25000",
        "twenty-five thousand": "25000",
        "thirty thousand": "30000",
        "forty thousand": "40000",
        "twenty thousand": "20000",
        "fifteen thousand": "15000",
        "ten thousand": "10000",
        "five thousand": "5000",
        "four thousand": "4000",
        "three thousand": "3000",
        "two thousand": "2000",
        "one thousand": "1000",
    }
    for phrase, num_str in spoken_map.items():
        if phrase in text:
            return num_str
            
    digits_only = re.sub(r"[^\d]", "", text)
    return digits_only if digits_only else fallback_balance


def normalize_callback_time(val: str | None) -> str:
    """
    Convert spoken or raw callback time ('tomorrow', 'after 2 days', 'eight o'clock', '2 PM', 'Anytime')
    into standard exact SQL DATETIME 'YYYY-MM-DD HH:MM:SS'.
    """
    if not val:
        return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d 10:00:00")

    text = str(val).strip()

    # If it is already a full ISO/SQL datetime
    if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$", text):
        return text.replace("T", " ") if len(text) >= 19 else text.replace("T", " ") + ":00"

    # If it is just a date 'YYYY-MM-DD'
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return f"{text} 10:00:00"

    now = datetime.now()
    target_date = now.date()
    target_hour = 10
    target_minute = 0
    lower = text.lower()

    # 1. Day resolution
    if "today" in lower:
        target_date = now.date()
    elif "tomorrow" in lower:
        target_date = now.date() + timedelta(days=1)
    else:
        m_days = re.search(r"(?:after|in)\s+(\w+|\d+)\s+days?", lower)
        word_to_num = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
        if m_days:
            num_str = m_days.group(1)
            days = int(num_str) if num_str.isdigit() else word_to_num.get(num_str, 1)
            target_date = now.date() + timedelta(days=days)
        else:
            weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            found_day = None
            for idx, day in enumerate(weekdays):
                if day in lower:
                    found_day = idx
                    break
            if found_day is not None:
                today_idx = now.weekday()
                days_ahead = (found_day - today_idx) % 7
                if days_ahead == 0:
                    days_ahead = 7
                target_date = now.date() + timedelta(days=days_ahead)
            else:
                target_date = now.date() + timedelta(days=1)

    # 2. Time resolution (e.g. 'eight o'clock', '2 PM', '14:00', 'morning', 'evening')
    time_text = re.sub(r"(?:after|in)?\s*(\w+|\d+)\s+days?", "", lower)
    
    # Convert spoken number words to digits:
    word_to_digit = {
        "twelve": "12", "eleven": "11", "ten": "10", "nine": "9", "eight": "8",
        "seven": "7", "six": "6", "five": "5", "four": "4", "three": "3",
        "two": "2", "one": "1", "half": "30", "quarter": "15",
        "twenty": "20", "thirty": "30", "forty": "40", "fifty": "50",
    }
    for w, d in word_to_digit.items():
        time_text = re.sub(rf"\b{w}\b", d, time_text)

    # Normalize o'clock / oclock / o clock
    time_text = re.sub(r"\bo['\s]?clock\b", "oclock", time_text)

    m_time = re.search(r"\b(\d{1,2})(?::(\d{2})|\s+(\d{2}))?\s*(am|pm|oclock)?\b", time_text)
    if m_time and (m_time.group(4) or m_time.group(2) or m_time.group(3) or "at" in lower or any(k in lower for k in ["morning", "afternoon", "evening", "night"])):
        hr = int(m_time.group(1))
        mn = int(m_time.group(2) or m_time.group(3) or 0)
        meridiem = m_time.group(4)
        if (meridiem == "pm" or any(k in lower for k in ["evening", "night", "afternoon"])) and hr < 12:
            hr += 12
        elif (meridiem == "am" or "morning" in lower) and hr == 12:
            hr = 0
        elif (not meridiem or meridiem == "oclock") and hr < 8:
            hr += 12
        if 0 <= hr <= 23 and 0 <= mn <= 59:
            target_hour = hr
            target_minute = mn
    elif m_time and m_time.group(1):
        hr = int(m_time.group(1))
        if any(k in lower for k in ["evening", "night", "afternoon"]) and hr < 12:
            hr += 12
        elif hr < 8:
            hr += 12
        if 0 <= hr <= 23:
            target_hour = hr
    elif "morning" in lower:
        target_hour = 10
    elif "afternoon" in lower:
        target_hour = 14
    elif "evening" in lower:
        target_hour = 17
    else:
        target_hour = 10

    res_dt = datetime(target_date.year, target_date.month, target_date.day, target_hour, target_minute, 0)
    if res_dt <= now and target_date == now.date():
        res_dt += timedelta(days=1)

    return res_dt.strftime("%Y-%m-%d %H:%M:%S")



def validate_payment_date(value: str) -> tuple[str, str]:
    """Validate a proposed payment date against the configured window."""
    try:
        d = datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError:
        return "rejected", "invalid format (use YYYY-MM-DD)"
    today = date.today()
    delta = (d - today).days
    if delta < 0:
        return "rejected", "date is in the past"
    if delta > MAX_PAYMENT_DATE_DAYS:
        return "rejected", f"date exceeds the {MAX_PAYMENT_DATE_DAYS}-day window"
    return "accepted", ""


@tool
def disconnect_call(reason: str = "") -> str:
    """
    End the call gracefully. Call this when the conversation is complete
    (disposition captured) or the customer asks to end the call.
    """
    return f"DISCONNECT:{reason}"


@tool
def transfer_call(destination: str = "FAIRMONEY_COLLECTIONS", reason: str = "") -> str:
    """
    Transfer the call to a human agent. Use for dispute, hardship, customer
    requesting human assistance, language requirement, or refusal cases.
    """
    return f"TRANSFER:{destination}:{reason}"


@tool
def capture_ptp(amount: str, date: str = "") -> str:
    """
    Capture the customer's Promise to Pay (PTP): the amount they commit to
    pay and optionally the date they will pay by.
    """
    return f"PTP:{amount}:{date}"


@tool
def negotiate_payment_date(proposed_date: str) -> str:
    """
    Record a payment date proposed by the customer during the Needs-Time
    negotiation. The date must be within the configured window.
    """
    status, reason = validate_payment_date(proposed_date)
    if status == "accepted":
        return f"DATE_ACCEPTED:{proposed_date}"
    return f"DATE_REJECTED:{reason}. Ask the customer for a date within {MAX_PAYMENT_DATE_DAYS} days from today."


@tool
def capture_refusal(reason: str) -> str:
    """
    Record the refusal outcome when the customer refuses to pay or commit.
    """
    return f"REFUSED:{reason}"


@tool
def share_payment_link(channel: str = "sms") -> str:
    """
    Share the payment link to the customer via the given channel (email or sms).
    """
    return f"PAYMENT_LINK:{channel}:MOCK_LINK_SENT"


@tool
def request_callback(callback_number: str, preferred_time: str = "") -> str:
    """
    Used when the person answering is NOT the RPC: record a callback request.
    """
    digits = normalize_phone_number(callback_number)
    if len(digits) == 11 and digits.startswith("0"):
        time_lower = preferred_time.strip().lower()
        if not preferred_time or time_lower in ("", "[time]", "[number]", "none", "null", "undefined"):
            preferred_time = "Anytime"
        return f"CALLBACK:{digits}:{preferred_time}"
    return f"CALLBACK_REJECTED:Invalid number '{callback_number}'. Must be an 11-digit Nigerian mobile number starting with 0."


@tool
def record_payment_mode(mode: str) -> str:
    """
    Record the customer's chosen payment mode ('fairmoney_app', 'bank_transfer', or 'payment_link').
    """
    return f"PAYMENT_MODE:{mode}"


@tool
def save_call_data(
    customer_name: str = "",
    customer_number: str = "",
    call_outcome: str = "",
    rpc_status: str = "",
    payment_intent: str = "",
    ptp_amount: str = "",
    ptp_date: str = "",
    payment_mode: str = "",
    transfer_reason: str = "",
    refusal_outcome: str = "",
    callback_required: bool = False,
    callback_details: str = "",
) -> str:
    """
    Save the complete call/disposition record to the database.
    """
    import json as _json

    record = {
        "customer_name": customer_name,
        "customer_number": customer_number,
        "call_outcome": call_outcome,
        "rpc_status": rpc_status,
        "payment_intent": payment_intent,
        "ptp_amount": ptp_amount,
        "ptp_date": ptp_date,
        "payment_mode": payment_mode,
        "transfer_reason": transfer_reason,
        "refusal_outcome": refusal_outcome,
        "callback_required": callback_required,
        "callback_details": callback_details,
    }
    return "CALL_DATA_SAVED:" + _json.dumps(record, ensure_ascii=False, default=str)
