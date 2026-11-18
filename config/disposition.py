# =============================================================================
# DISPOSITION CODES — FAIRMONEY COLLECTION BOT
# =============================================================================
# TODO(config): Map these to the dialer's ACTUAL disposition code list once
# the dialer team confirms it. Keys here are internal, values are the labels
# sent to the dialer/CRM.

RPC_STATUS = {
    "verified":     "RPC_VERIFIED",
    "not_verified": "RPC_NOT_VERIFIED",
    "callback":     "RPC_CALLBACK",
}

PAYMENT_INTENT = {
    "will_pay":   "WILL_PAY",
    "needs_time": "NEEDS_TIME",
    "transfer":   "TRANSFER_TO_AGENT",
    "refuses":    "REFUSES",
}

CALL_OUTCOME = {
    "in_progress": "IN_PROGRESS",
    "completed":   "COMPLETED",
    "aborted":     "ABORTED",
    "failed":      "FAILED",
    "no_answer":   "NO_ANSWER",
    "not_rpc":     "NOT_RPC",
    "transfer":    "TRANSFER_TO_AGENT",
}


TRANSFER_REASONS = [
    "dispute",
    "hardship",
    "human_assistance",
    "language",
    "refused",
]

# ── Needs-Time negotiation rule ─────────────────────────────────────────────
# SOW 2.7: customer can negotiate a payment plan within 96 hours (4 days).
# Dates beyond this window are rejected and re-negotiated.
from config.settings import MAX_PAYMENT_DATE_DAYS, DAYS_WINDOW_MODE