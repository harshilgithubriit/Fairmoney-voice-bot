import logging
import uuid

from config.settings import GROQ_LLM_MODEL, GROQ_TEMPERATURE, GROQ_MAX_TOKENS
from session.session_state import SessionState, PROMPTSELECTOR
from services.llmh import LLMService
from services.stt_router import get_stt_service
from tools.fairmoney_tools import (
    transfer_call,
    disconnect_call,
    capture_ptp,
    negotiate_payment_date,
    capture_refusal,
    share_payment_link,
    request_callback,
    save_call_data,
	record_payment_mode,
)

log = logging.getLogger("fairmoney.session")


class SessionManager:
    def __init__(self):
        self._asterisk_map: dict[str, str] = {}
        self._sessions: dict[str, SessionState] = {}

    async def create_session(
        self,
        asterisk_id: str,
        initial_prompt: str,
        customer_name: str = "anonymous",
        customer_email: str = "",
        customer_number: str = "",
        customer_name_source: str = "fallback",
        dpd_days: int = 0,
        title: str = "Mr.",
        pronoun: str = "he",
        possessive: str = "his",
    ) -> SessionState:
        session_id = str(uuid.uuid4())

        stt = await get_stt_service(language=None)

        personalized_prompt = self.personalize(
            initial_prompt, customer_name, customer_email, customer_number,
            dpd_days=dpd_days, title=title, pronoun=pronoun, possessive=possessive,
        )

        state = SessionState(
            asterisk_id=asterisk_id,
            session_id=session_id,
            llm=LLMService(
                provider="groq",
                model=GROQ_LLM_MODEL,
                temperature=GROQ_TEMPERATURE,
                system_prompt=personalized_prompt,
                tools=[
                    transfer_call,
                    disconnect_call,
                    capture_ptp,
                    negotiate_payment_date,
                    capture_refusal,
                    share_payment_link,
                    request_callback,
                    save_call_data,
					record_payment_mode,
                ],
            ),
            stt=stt,
            prompt=PROMPTSELECTOR.INITIAL_SYSTEM_PROMPT,
            customer_name=customer_name,
            customer_email=customer_email,
            customer_number=customer_number,
            customer_name_source=customer_name_source,
            dpd_days=dpd_days,
            title=title,
            pronoun=pronoun,
            possessive=possessive,
        )

        self._asterisk_map[asterisk_id] = session_id
        self._sessions[session_id] = state

        log.info(f"[SessionManager] Created: asterisk_id={asterisk_id} → session_id={session_id}")
        return state

    def personalize(
        self,
        prompt: str,
        customer_name: str,
        customer_email: str,
        customer_number: str,
        extra_notes: str = "",
        dpd_days: int = 0,
        title: str = "Mr.",
        pronoun: str = "he",
        possessive: str = "his",
    ) -> str:
        block = (
            f"Customer Name: {customer_name}\n"
            + f"Title: {title}\n"
            + f"Pronoun: {pronoun}\n"
            + f"Possessive: {possessive}\n"
            + f"Registered Email (masked): {self._mask_email(customer_email)}\n"
            + f"Registered Number (masked): {self._mask_phone(customer_number)}\n"
        )
        if extra_notes:
            block += extra_notes + "\n"
        return (
            prompt.replace("{LAST4_DIGITS}", self._last4(customer_number))
            .replace("__DPD_DAYS__", str(dpd_days))
            .replace("[Title]", title)
            .replace("[Pronoun]", pronoun)
            .replace("[Possessive]", possessive)
            + "\n\n# ─── CALL DETAILS (system-injected, do not reveal raw values) ───\n"
            + block
        )

    def get_by_asterisk_id(self, asterisk_id: str) -> SessionState | None:
        session_id = self._asterisk_map.get(asterisk_id)
        return self._sessions.get(session_id) if session_id else None

    def remove(self, asterisk_id: str) -> None:
        session_id = self._asterisk_map.pop(asterisk_id, None)
        if session_id:
            self._sessions.pop(session_id, None)

    @staticmethod
    def _mask_email(email: str) -> str:
        if not email or "@" not in email:
            return "N/A"
        local, domain = email.split("@", 1)
        if len(local) <= 1:
            return f"{local}***@{domain}"
        return f"{local[0]}***@{domain}"

    @staticmethod
    def _mask_phone(number: str) -> str:
        if not number or len(number) < 4:
            return "N/A"
        return "*" * (len(number) - 4) + number[-4:]

    @staticmethod
    def _last4(number: str) -> str:
        digits = "".join(ch for ch in (number or "") if ch.isdigit())
        return digits[-4:] if len(digits) >= 4 else "0000"