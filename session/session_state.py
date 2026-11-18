from dataclasses import dataclass, field
from enum import Enum

from services.llmh import LLMService
from services.stt_opt import STTService


class PROMPTSELECTOR(Enum):
    INITIAL_SYSTEM_PROMPT = "initial_prompt"
    FULL_SYSTEM_PROMPT    = "full_prompt"


@dataclass
class SessionState:
    asterisk_id: str
    session_id:  str
    llm:         LLMService
    stt:         STTService
    prompt:      PROMPTSELECTOR = PROMPTSELECTOR.INITIAL_SYSTEM_PROMPT
    empty_count: int = 0
    pending_text: str | None = None
    language: str | None = None
    customer_name: str = "anonymous"
    customer_email: str = ""
    customer_number: str = ""
    customer_name_source: str = "fallback"

    # ── Fairmoney-specific call outcome state ──────────────────────────────
    rpc_verified: bool = False
    rpc_status: str | None = None
    payment_intent: str | None = None
    ptp_amount: str | None = None
    ptp_date: str | None = None
    ptp_amount2: str | None = None
    ptp_date2: str | None = None
    transfer_reason: str | None = None
    refusal_outcome: str | None = None
    callback_required: bool = False
    callback_number: str | None = None
    callback_time: str | None = None
    callback_details: str | None = None
    alternate_number: str | None = None
    payment_link_channel: str | None = None
    payment_mode: str = ""
    ptp_captured: bool = False
    disposition: str | None = None
    interaction_notes: list[str] = field(default_factory=list)
    conversation_log: list[str] = field(default_factory=list)
    saved_call_data: dict | None = None
    dpd_days: int = 0
    title: str = "Mr."
    pronoun: str = "he"
    possessive: str = "his"
