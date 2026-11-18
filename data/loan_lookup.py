# =============================================================================
# LOAN DATA — FAIRMONEY (DEV MOCK)
# =============================================================================
# TODO(config): replace with the real CRM/dialer loan API. Loan amount and
# due date must come from here per customer — never hardcoded in prompts.
# Acknowledge that conversation must be available in CRM on transfer;
# export mechanism is TODO(integration) pending CRM details.
#
# title/pronoun should come from CRM/dialer (not inferred).
# title: "Mr." or "Mrs." | pronoun: "he"/"she" | possessive: "his"/"her"

MOCK_LOANS = {
    # customer_number -> loan record (dev only)
    "2347012345678": {
        "name": "Harsh",
        "title": "Mr.",
        "pronoun": "he",
        "possessive": "his",
        "amount": 50000.0,
        "due_date": "2026-09-15",
        "currency": "NGN",
        "dpd_days": 15,
    },
}


def get_loan(customer_number: str) -> dict | None:
    return MOCK_LOANS.get(customer_number or "")


from config.settings import USE_MOCK_LOAN_DATA

def get_customer_fallback() -> dict | None:
    """Dev/testing fallback: when the dialer does not pass customer data
    (e.g. simulated calls), return the first mock record so the RPC gate
    can verify against a fixed name. PRODUCTION must receive real customer
    name/number from the dialer/CRM at call time."""
    if not USE_MOCK_LOAN_DATA:
        return None
    if MOCK_LOANS:
        key = next(iter(MOCK_LOANS))
        record = dict(MOCK_LOANS[key])
        record["customer_number"] = key
        return record
    return None