import os

# =============================================================================
# TRANSFER CONFIG — FAIRMONEY
# =============================================================================
# The call is initiated through Asterisk; transfers must go back through the
# same service (fastagi_server -> HumanEscalation -> Queue(TRANSFER_QUEUE)).
# Queue name is configurable via env, never hardcoded in business logic.
from config.settings import TRANSFER_QUEUE

# TODO(config): confirm with the dialer team which human-intervention
# scenarios beyond the spec (dispute, hardship, assistance, language,
# refused) should trigger transfer.