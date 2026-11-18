import os
import re
import json
import time
import asyncio
import logging
from typing import Callable, Awaitable, Any

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
from langchain_core.runnables import RunnableConfig

from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_groq import ChatGroq
from langchain_ollama import ChatOllama
from langchain_core.messages import (
    HumanMessage,
    SystemMessage,
    BaseMessage,
    AIMessage,
    ToolMessage,
)
from langchain_core.tools import tool

from services.latency import LatencyTracker

logger = logging.getLogger(__name__)


# =============================================================================
# TOOL DEFINITIONS
# =============================================================================
# To add a new tool:
#   1. Define it below with @tool
#   2. Add it to DEFAULT_TOOLS list at the bottom of this section
#
# To use MCP tools instead:
#   from langchain_mcp_adapters.client import MultiServerMCPClient
#   client = MultiServerMCPClient({...})f
#   mcp_tools = await client.get_tools()
#   service = LLMService(tools=DEFAULT_TOOLS + mcp_tools)
# =============================================================================

@tool
def disconnect_call(reason: str = "call_complete") -> str:
    """
    Disconnect / hang up the current call.

    CRITICAL: You must NEVER call this tool in the same turn as asking
    the customer for confirmation. The correct sequence is:
      Turn 1 (you): Ask "Shall I disconnect?" — text only, NO tool call.
      Turn 2 (customer): They respond yes/confirm.
      Turn 3 (you): Say goodbye, THEN call this tool.

    Call this ONLY when:
    - The customer has ALREADY confirmed in their PREVIOUS message that
      they are okay to end the call.
    - The customer is completely unresponsive after repeated prompts.

    NEVER call this in the same response as the confirmation question.
    NEVER call this just because the customer said goodbye or asked to hang up.
    """
    return "acknowledged"

@tool
def save_patient_data(
    is_new_patient: bool,
    name: str,
    whatsapp_number: str,
    age: int | None = None,
    gender: str | None = None,
    problem: str | None = None,
) -> str:
    """
    Save collected patient intake data. Call this once identity/intake
    is confirmed, before emitting the phase-transition marker.
    """
    return "saved"




@tool
def ask_transfer_confirmation() -> str:
    """
    Use this tool when you want to transfer the customer to a human agent
    BUT have not asked for their confirmation yet.

    This sets a flag so the next turn allows the actual transfer.

    Call this when:
    - Customer requests to speak to a human / agent / someone else.
    - You decide the issue needs human handling.
    - Customer is repeatedly asking the same question you cannot resolve.

    After calling this tool, ask the customer:
    "Sure, shall I connect you to our support team right now?"
    Then wait for their response.

    Do NOT call transfer_call in the same turn as this tool.
    """
    return "confirmation_requested"


@tool
def transfer_call(destination: str = "support", reason: str = "") -> str:
    """
    Transfer the customer to a human agent.

    ONLY call this tool AFTER ask_transfer_confirmation has already
    been called in a previous turn AND the customer has confirmed.
    Never call this directly without confirmation.

    Args:
        destination: 'support', 'billing', 'sales', 'agent'
        reason:      Why the transfer is happening.
    """
    return "acknowledged"


DEFAULT_TOOLS = [
    # ask_transfer_confirmation,
    save_patient_data,
    transfer_call,
    disconnect_call,
]


# =============================================================================
# LEAKED TOOL-CALL TEXT GUARD
# =============================================================================
# Some Groq-hosted Llama checkpoints occasionally emit a tool call directly
# inside the text content instead of populating the structured `tool_calls`
# field. Observed shapes include:
#
#   <function=disconnect_call>{}</function>
#   { "tool": "disconnect_call", "arguments": {} }
#   {"name": "disconnect_call", "arguments": {}}
#
# Rather than chasing every exact shape with a regex (which is brittle —
# spacing/key-order/quote-style variations slip through), this guard treats
# ANY raw '{' or '<function=' encountered in streamed text as suspicious
# (legitimate spoken Hindi/English output never contains literal braces or
# that tag), holds it back, and only releases it once we can prove it is
# either:
#   (a) a complete, parseable tool-call payload  → converted to a real
#       tool call and stripped from spoken output, or
#   (b) NOT a tool-call payload after all          → released as normal text.
# =============================================================================

_TAG_OPENER = "<function="
_FUNCTION_TAG_RE = re.compile(
    r"<function\s*=\s*(?P<name>\w+)\s*>?\s*(?P<args>\{.*?\})\s*</function\s*>",
    re.DOTALL,
)
_CONTROL_PREFIX_RE = re.compile(
    r"(?:to=\w+|<\|channel\|>|<\|message\|>|commentary|<\|im_start\|>|<\|im_end\|>)",
    re.IGNORECASE,
)

# Safety cap: if we've held back this many characters waiting for a brace/tag
# to close and it still hasn't, assume it was a false positive (e.g. the
# customer's name legitimately contained a brace, or the model is just
# rambling) and flush it as normal text rather than stalling forever.
MAX_HOLD_CHARS = 1200


def _find_balanced_json_end(s: str, start: int) -> int | None:
    """
    Given s[start] == '{', scan forward respecting string literals/escapes
    and return the index just past the matching closing '}'.
    Returns None if the buffer ends before the object closes (incomplete).
    """
    if start >= len(s) or s[start] != "{":
        return None

    depth   = 0
    in_str  = False
    escape  = False
    i       = start

    while i < len(s):
        c = s[i]
        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return i + 1
        i += 1

    return None  # never closed — incomplete, need more chunks


def _extract_tool_call_from_json(obj: dict) -> tuple[str, dict] | None:
    """
    Normalises the various leaked-JSON shapes into (tool_name, args).
    Returns None if the object doesn't look like a tool call.
    """
    name = obj.get("tool") or obj.get("name")
    if not name or not isinstance(name, str):
        return None

    args = obj.get("arguments")
    if args is None:
        args = obj.get("args", {})
    if not isinstance(args, dict):
        args = {}

    return name, args


def _scan_for_leak(buf: str) -> tuple[str, dict | None, int] | None:
    """
    Looks for the earliest '<function=' tag, raw '{', or LLM control tokens in buf.

    Returns one of:
      None                         → no suspicious marker found at all
      ("incomplete", None, idx)    → marker found but not yet closed;
                                      caller should hold buf[idx:] back
      ("tool_call", call_dict, end_idx)
                                    → complete tool call parsed; caller should
                                      consume buf[:end_idx] and continue from end_idx
      ("not_a_tool_call", None, end_idx)
                                    → braces closed but it wasn't a tool call
                                      (e.g. stray JSON the model echoed back);
                                      caller should just yield buf[:end_idx] as text
    """
    tag_idx   = buf.find(_TAG_OPENER)
    brace_idx = buf.find("{")
    ctrl_match = _CONTROL_PREFIX_RE.search(buf)
    ctrl_idx = ctrl_match.start() if ctrl_match else -1

    candidates = [i for i in (tag_idx, brace_idx, ctrl_idx) if i != -1]
    if not candidates:
        return None

    idx = min(candidates)

    if idx == ctrl_idx:
        next_tag = buf.find(_TAG_OPENER, ctrl_idx)
        next_brace = buf.find("{", ctrl_idx)
        next_cand = [i for i in (next_tag, next_brace) if i != -1]
        if not next_cand:
            return "incomplete", None, idx
        actual_idx = min(next_cand)
        if actual_idx == next_tag:
            match = _FUNCTION_TAG_RE.search(buf, actual_idx)
            if match and match.start() == actual_idx:
                try:
                    args = json.loads(match.group("args"))
                except json.JSONDecodeError:
                    args = {}
                return "tool_call", {"name": match.group("name"), "args": args}, match.end()
            return "incomplete", None, idx
        else:
            end = _find_balanced_json_end(buf, actual_idx)
            if end is None:
                return "incomplete", None, idx
            raw = buf[actual_idx:end]
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return "not_a_tool_call", None, end
            extracted = _extract_tool_call_from_json(parsed) if isinstance(parsed, dict) else None
            if extracted is None:
                return "not_a_tool_call", None, end
            name, args = extracted
            return "tool_call", {"name": name, "args": args}, end

    if idx == tag_idx:
        match = _FUNCTION_TAG_RE.search(buf, idx)
        if match and match.start() == idx:
            try:
                args = json.loads(match.group("args"))
            except json.JSONDecodeError:
                args = {}
            return "tool_call", {"name": match.group("name"), "args": args}, match.end()
        # tag opener present but not yet closed (or malformed) — hold back
        return "incomplete", None, idx

    # idx == brace_idx → try to find the matching closing brace
    end = _find_balanced_json_end(buf, idx)
    if end is None:
        return "incomplete", None, idx

    raw = buf[idx:end]
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Not valid JSON at all — release it as plain text, nothing to hold.
        return "not_a_tool_call", None, end

    extracted = _extract_tool_call_from_json(parsed) if isinstance(parsed, dict) else None
    if extracted is None:
        # Valid JSON, but not shaped like a tool call (rare) — treat as text.
        return "not_a_tool_call", None, end

    name, args = extracted
    return "tool_call", {"name": name, "args": args}, end


# =============================================================================
# CONFIG
# =============================================================================

PROVIDER_DEFAULTS: dict[str, str] = {
    "openai":    "gpt-4o-mini",
    "anthropic": "claude-3-5-sonnet-20241022",
    "groq":      "llama-3.3-70b-versatile",
    "ollama":    "llama3",
}

# Cheap/fast models used for summarization per provider.
SUMMARIZER_DEFAULTS: dict[str, str] = {
    "openai":    "gpt-4o-mini",
    "anthropic": "claude-haiku-4-5-20251001",
    "groq":      "openai/gpt-oss-20b",
    "ollama":    "llama3",
}

SUPPORTED_PROVIDERS = set(PROVIDER_DEFAULTS.keys())

MAX_HISTORY  = 100   # summarise when history exceeds this many messages
RECENT_KEEP  = 50   # always keep the last N messages verbatim after summarising

# Retry config for the stream
STREAM_MAX_RETRIES      = 5
STREAM_RETRY_BASE_DELAY = 1.5   # seconds; delay = base ** attempt

# Async callable: (tool_name: str, tool_args: dict) -> None
ToolCallbackType = Callable[[str, dict], Awaitable[None]]


# =============================================================================
# LANGSMITH HELPERS
# =============================================================================

def _make_config(
    run_name: str,
    metadata: dict | None = None,
    asterisk_id: str | None = None,
) -> RunnableConfig:
    """
    Build a RunnableConfig that tags every LangChain call with:
      - run_name  : shown as the node label in LangSmith trace tree
      - tags      : searchable labels in LangSmith UI
      - metadata  : arbitrary key/value pairs shown in the trace detail panel

    This is the single place where LangSmith context is constructed so
    that summariser, main stream, and retry calls all share consistent tagging.
    """
    meta = {"project": "bajaj-priya"}
    if asterisk_id:
        meta["asterisk_id"] = asterisk_id
    if metadata:
        meta.update(metadata)

    return RunnableConfig(
        run_name=run_name,
        tags=["priya", "bajaj"],
        metadata=meta,
    )


# =============================================================================
# CALL CONTEXT
# =============================================================================

class CallContext:
    """
    Holds per-call data that should never live in conversation history.

    Usage:
        ctx = CallContext(customer_name="John", amount_due=450.00, account_ref="ACC-123")
        service = LLMService(system_prompt=PROMPT_TEMPLATE, call_context=ctx)

    In your system prompt template use {customer_name}, {amount_due} etc.
    The context is rendered once per turn into the active system prompt so
    the LLM always sees current values without them polluting history.
    """

    def __init__(self, **kwargs: Any):
        self.data: dict[str, Any] = kwargs

    def render(self, template: str) -> str:
        """Substitute all context fields into a prompt template string."""
        try:
            return template.format(**self.data)
        except KeyError as e:
            logger.warning(f"[CallContext] Missing key in prompt template: {e}")
            return template

    def update(self, **kwargs: Any) -> None:
        """Update context fields mid-call (e.g. after customer confirms name)."""
        self.data.update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)


# =============================================================================
# LLM SERVICE
# =============================================================================

class LLMService:
    """
    Streaming LLM service with:
      - Swappable provider (openai / anthropic / groq / ollama)
      - Injectable tools at construction time (supports MCP tools too)
      - Per-call CallContext rendered into system prompt each turn
      - Provider-aware summarizer (no hardcoded Groq dependency)
      - Retry middleware around the stream
      - Interrupt-safe history management (no dangling AIMessage/ToolMessage)
      - History sanitizer run before every LLM call as a safety net
      - Shape-agnostic leaked tool-call text guard for Groq/Llama models
      - LangSmith tracing via RunnableConfig (auto-instrumented, zero overhead)
    """

    def __init__(
        self,
        provider: str = "openai",
        model: str | None = None,
        temperature: float = 0.5,
        system_prompt: str = "You are a helpful assistant.",
        tools: list | None = None,
        call_context: CallContext | None = None,
        # Optional: pass asterisk_id at construction so every trace is tagged.
        # You can also pass it per-turn via ask_llm_stream(metadata=...).
        asterisk_id: str | None = None,
    ):
        if provider not in SUPPORTED_PROVIDERS:
            raise ValueError(
                f"Unsupported provider: '{provider}'. "
                f"Choose from: {sorted(SUPPORTED_PROVIDERS)}"
            )

        self.provider     = provider
        self.temperature  = temperature
        self.system_prompt = system_prompt
        self.model        = model or PROVIDER_DEFAULTS[provider]
        self.call_context = call_context
        self.asterisk_id  = asterisk_id  # stored for LangSmith tagging

        # Tool registry — injectable, not hardcoded
        self.tools    = tools if tools is not None else DEFAULT_TOOLS
        self.tool_map = {t.name: t for t in self.tools}

        self.conversation_history: list[BaseMessage] = []
        self.summary: str = ""

        self.llm        = self._build_llm()
        self.summarizer = self._build_summarizer()

    # ------------------------------------------------------------------ build

    def _build_llm(self):
        if self.provider == "openai":
            base = ChatOpenAI(
                api_key=os.getenv("OPENAI_API_KEY"),
                model=self.model,
                temperature=self.temperature,
            )
        elif self.provider == "anthropic":
            base = ChatAnthropic(
                api_key=os.getenv("ANTHROPIC_API_KEY"),
                model=self.model,
                temperature=self.temperature,
            )
        elif self.provider == "groq":
            base = ChatGroq(
                api_key=os.getenv("GROQ_API_KEY"),
                model=self.model,
                temperature=self.temperature,
                model_kwargs={"parallel_tool_calls": False},
            )
        elif self.provider == "ollama":
            return ChatOllama(model=self.model, temperature=self.temperature)

        return base.bind_tools(self.tools) if self.tools else base

    def _build_summarizer(self):
        summarizer_model = SUMMARIZER_DEFAULTS[self.provider]

        if self.provider == "openai":
            return ChatOpenAI(
                api_key=os.getenv("OPENAI_API_KEY"),
                model=summarizer_model,
                temperature=0.0,
            )
        elif self.provider == "anthropic":
            return ChatAnthropic(
                api_key=os.getenv("ANTHROPIC_API_KEY"),
                model=summarizer_model,
                temperature=0.0,
            )
        elif self.provider == "groq":
            return ChatGroq(
                api_key=os.getenv("GROQ_API_KEY"),
                model=summarizer_model,
                temperature=0.0,
            )
        elif self.provider == "ollama":
            return ChatOllama(model=summarizer_model, temperature=0.0)

    # ----------------------------------------------------------------- helpers

    def update_system_prompt(self, prompt: str) -> None:
        self.system_prompt = prompt

    def update_call_context(self, **kwargs: Any) -> None:
        """Update call context mid-call without replacing the whole object."""
        if self.call_context is None:
            self.call_context = CallContext(**kwargs)
        else:
            self.call_context.update(**kwargs)

    def clear_history(self) -> None:
        self.conversation_history = []
        self.summary = ""

    def add_tool(self, new_tool) -> None:
        """Register a new tool at runtime and rebuild the LLM binding."""
        self.tools.append(new_tool)
        self.tool_map[new_tool.name] = new_tool
        self.llm = self._build_llm()
        logger.info(f"[LLMService] Tool added: {new_tool.name}. Rebuilt LLM binding.")

    def _build_active_prompt(self, sys_prompt_override: str | None = None) -> str:
        """
        Render the active system prompt for this turn.
        Substitutes CallContext fields and appends the rolling summary.
        """
        base = sys_prompt_override or self.system_prompt
        if self.call_context:
            base = self.call_context.render(base)
        if self.summary:
            base += f"\n\nContext from earlier in this conversation:\n{self.summary}"
        return base

    # --------------------------------------------------------- history safety

    def _sanitize_history(
        self, messages: list[BaseMessage]
    ) -> list[BaseMessage]:
        """
        Remove any AIMessage with tool_calls that is NOT immediately followed
        by a ToolMessage for every tool_call_id it contains.

        This is the last-resort safety net before every LLM call.
        Groq/Llama hard-fails with "Failed to call a function" when the
        message sequence contains an unmatched tool_calls/ToolMessage pair —
        which happens when the user interrupts mid-stream after the LLM has
        already emitted tool calls.

        Also removes orphan ToolMessages whose preceding AIMessage was dropped.
        """
        sanitized: list[BaseMessage] = []

        for i, msg in enumerate(messages):
            if isinstance(msg, AIMessage) and msg.tool_calls:
                expected_ids = {tc["id"] for tc in msg.tool_calls}

                j = i + 1
                found_ids: set[str] = set()
                while j < len(messages) and isinstance(messages[j], ToolMessage):
                    found_ids.add(messages[j].tool_call_id)
                    j += 1

                if expected_ids == found_ids:
                    sanitized.append(msg)
                else:
                    logger.warning(
                        f"[LLMService] _sanitize_history: dropping dangling "
                        f"AIMessage with tool_calls={[tc['name'] for tc in msg.tool_calls]} "
                        f"(expected={expected_ids}, found={found_ids})"
                    )

            elif isinstance(msg, ToolMessage):
                prev = sanitized[-1] if sanitized else None
                if (
                    prev is not None
                    and isinstance(prev, AIMessage)
                    and any(tc["id"] == msg.tool_call_id for tc in prev.tool_calls)
                ):
                    sanitized.append(msg)
                else:
                    logger.warning(
                        f"[LLMService] _sanitize_history: dropping orphan "
                        f"ToolMessage tool_call_id={msg.tool_call_id}"
                    )
            else:
                sanitized.append(msg)

        return sanitized

    def _rollback_interrupted_turn(self, query: str) -> None:
        """
        Called when a turn is interrupted before the bot finishes speaking.

        Removes everything that was added to history during this turn:
          1. Any dangling AIMessage with tool_calls (+ its ToolMessages)
          2. The HumanMessage for `query`

        This guarantees the history is clean for the next turn regardless
        of how far the LLM got before the interrupt.
        """
        while self.conversation_history:
            last = self.conversation_history[-1]

            if isinstance(last, ToolMessage):
                self.conversation_history.pop()
                continue

            if isinstance(last, AIMessage):
                self.conversation_history.pop()
                continue

            if isinstance(last, HumanMessage) and last.content == query:
                self.conversation_history.pop()
                break

            break

        logger.debug(
            f"[LLMService] _rollback_interrupted_turn: history length "
            f"after rollback = {len(self.conversation_history)}"
        )

    # ------------------------------------------------------------ summarisation

    async def summarise_content(self, ls_config: RunnableConfig | None = None) -> None:
        """
        Summarise old conversation history and trim it.

        ls_config is the RunnableConfig from the calling turn so that the
        summarizer call appears as a child node of that turn in LangSmith,
        rather than as a disconnected top-level trace.

        Wrapped in try/except so a summarizer failure never kills an active call.
        """
        if len(self.conversation_history) < MAX_HISTORY:
            return

        to_summarize = self.conversation_history[:-RECENT_KEEP]
        recent       = self.conversation_history[-RECENT_KEEP:]

        history_text = "\n".join([
            f"{'User' if isinstance(m, HumanMessage) else ('Tool' if isinstance(m, ToolMessage) else 'Assistant')}: {m.content}"
            for m in to_summarize
        ])

        if self.summary:
            prompt = (
                f"You have an existing summary of a conversation:\n{self.summary}\n\n"
                f"Here is the next part of the conversation to add to it:\n{history_text}\n\n"
                f"Write one merged summary in 4-5 lines covering each important point. "
                f"Also include whether the email is verified or not of the customer. "
                f"Keep all names, numbers, decisions, and key facts."
            )
        else:
            prompt = (
                f"Summarize this conversation in 4-5 lines. "
                f"Keep all relevant user information (name, preferences, decisions), "
                f"names, numbers, and key facts:\n\n{history_text}"
            )

        # Build summarizer config — child of the current turn's trace in LangSmith
        summarizer_config = ls_config or _make_config(
            run_name="summarise_content",
            asterisk_id=self.asterisk_id,
        )
        # Override run_name so it's clearly labelled in the trace tree
        summarizer_config = {**summarizer_config, "run_name": "summarise_content"}

        try:
            response = await self.summarizer.ainvoke(
                [HumanMessage(content=prompt)],
                config=summarizer_config,
            )
            self.summary = response.content
            self.conversation_history = recent
            logger.info(
                f"[LLMService] Summarized. History trimmed to "
                f"{len(self.conversation_history)} messages. "
                f"Summary: {self.summary[:80]}..."
            )
        except Exception as e:
            logger.error(f"[LLMService] Summarizer failed (non-fatal): {e}")

    # ----------------------------------------------------------- tool dispatch

    async def _dispatch_tool_calls(
        self,
        ai_message: AIMessage,
        callback: ToolCallbackType | None,
    ) -> None:
        self.conversation_history.append(ai_message)

        for tool_call in ai_message.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            tool_id   = tool_call["id"]

            logger.info(f"[LLMService] Tool call → {tool_name}({tool_args})")

            tool_fn = self.tool_map.get(tool_name)
            if tool_fn is None:
                tool_result = f"error: unknown tool '{tool_name}'"
                logger.warning(
                    f"[LLMService] Unknown tool '{tool_name}' requested — "
                    f"known tools: {list(self.tool_map.keys())}"
                )
            else:
                try:
                    tool_result = tool_fn.invoke(tool_args)
                except Exception as e:
                    tool_result = f"error: {e}"
                    logger.error(f"[LLMService] Tool '{tool_name}' raised: {e}")

            self.conversation_history.append(
                ToolMessage(content=str(tool_result), tool_call_id=tool_id)
            )

            # The callback is what actually drives real side effects
            # (e.g. main.py's on_tool_call sets disconnect_event / transfer_event
            # and the call loop sends the hangup frame after the goodbye line
            # finishes playing). If this callback never fires, the call never
            # actually ends even if the tool "ran".
            if callback:
                try:
                    await callback(tool_name, tool_args)
                except Exception as e:
                    logger.error(f"[LLMService] Tool callback failed: {e}")
            else:
                logger.warning(
                    f"[LLMService] Tool '{tool_name}' dispatched but NO callback "
                    f"was provided — no real-world action (e.g. hangup) will occur."
                )

    # ----------------------------------------------------------- stream retry

    async def _stream_with_retry(self, messages: list, config: RunnableConfig):
        """
        Async generator that wraps self.llm.astream() with retry logic.
        Passes `config` to every astream call so LangSmith traces each attempt
        under the correct parent run.
        Retries transient errors with exponential backoff.
        Re-raises on the final attempt or non-retryable errors.
        """
        try:
            from openai import RateLimitError, APIConnectionError, APITimeoutError
            retryable = (RateLimitError, APIConnectionError, APITimeoutError)
        except ImportError:
            retryable = (Exception,)

        for attempt in range(STREAM_MAX_RETRIES + 1):
            try:
                async for chunk in self.llm.astream(messages, config=config):
                    yield chunk
                return

            except retryable as e:
                if attempt == STREAM_MAX_RETRIES:
                    logger.error(
                        f"[LLMService] Stream failed after {STREAM_MAX_RETRIES} retries: {e}"
                    )
                    raise

                delay = STREAM_RETRY_BASE_DELAY ** attempt
                logger.warning(
                    f"[LLMService] Stream error (attempt {attempt + 1}/{STREAM_MAX_RETRIES}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"[LLMService] Non-retryable stream error: {e}")
                raise

    # ---------------------------------------------------------------- streaming

    async def ask_llm_stream(
        self,
        query: str,
        sys_prompt: str | None = None,
        use_history: bool = True,
        tracker: LatencyTracker | None = None,
        interrupt_event: asyncio.Event | None = None,
        tool_callback: ToolCallbackType | None = None,
        run_name: str | None = None,
        metadata: dict | None = None,
    ):
        """
        Async generator that streams text tokens from the LLM.

        LangSmith tracing
        ─────────────────
        A RunnableConfig is built once per turn and passed to every downstream
        call (_stream_with_retry, summarise_content). This makes each turn
        appear as a single parent trace in LangSmith with the LLM stream,
        retries, and summarizer as child nodes.

        Interrupt safety
        ────────────────
        When `interrupt_event` fires mid-stream we set `was_interrupted=True`
        and break out early. Token counts captured so far are still committed
        to the tracker (so interrupted turns show up in LangSmith cost totals).
        _rollback_interrupted_turn then cleans up history so no dangling
        AIMessage+tool_calls remains — which is the root cause of Groq's
        "Failed to call a function" error on the next turn.

        As an additional safety net, `_sanitize_history` is called before
        every LLM request to drop any such pairs that slipped through.

        Leaked tool-call guard
        ──────────────────────
        Any raw '{' or '<function=' seen in streamed text is held back
        immediately (never yielded to TTS) and probed via `_scan_for_leak`.
        If it resolves into a parseable tool call, it's converted into a real
        manual tool call and dispatched through `_dispatch_tool_calls` exactly
        like a properly-structured tool call would be — so disconnect_call /
        transfer_call etc. actually fire and their callbacks run (which is
        what triggers the real hangup/transfer in main.py). If it resolves
        into something that isn't a tool call after all, it's released back
        into the stream as normal text.
        """
        # ── Build LangSmith config for this turn ─────────────────────────────
        ls_config = _make_config(
            run_name=run_name or "ask_llm_stream",
            metadata=metadata,
            asterisk_id=self.asterisk_id,
        )

        active_prompt = self._build_active_prompt(sys_prompt)

        if use_history:
            self.conversation_history.append(HumanMessage(content=query))
            # Pass ls_config so summarizer appears as a child trace of this turn
            await self.summarise_content(ls_config=ls_config)
            clean_history = self._sanitize_history(self.conversation_history)
            messages = [SystemMessage(content=active_prompt)] + clean_history
        else:
            messages = [
                SystemMessage(content=active_prompt),
                HumanMessage(content=query),
            ]

        full_reply        = ""
        first_token_seen  = False
        was_interrupted   = False
        pending_text      = ""
        manual_tool_calls: list[dict] = []
        collected_chunks:  list       = []

        def _emit(text: str):
            """Local helper: track first-token latency + accumulate full_reply."""
            nonlocal first_token_seen, full_reply
            if not text:
                return None
            clean = _CONTROL_PREFIX_RE.sub("", text)
            if not clean:
                return None
            if not first_token_seen:
                if tracker:
                    tracker.llm_first_token = time.perf_counter()
                first_token_seen = True
            full_reply += clean
            return clean

        if tracker:
            tracker.llm_start = time.perf_counter()

        MAX_TOOL_LOOPS = 4
        for _loop_pass in range(MAX_TOOL_LOOPS):
            full_reply        = ""
            was_interrupted   = False
            pending_text      = ""
            manual_tool_calls: list[dict] = []
            collected_chunks:  list       = []

            if use_history:
                clean_history = self._sanitize_history(self.conversation_history)
                messages = [SystemMessage(content=active_prompt)] + clean_history
            else:
                messages = [
                    SystemMessage(content=active_prompt),
                    HumanMessage(content=query),
                ]

            if interrupt_event and interrupt_event.is_set():
                was_interrupted = True
            else:
                try:
                    async for chunk in self._stream_with_retry(messages, config=ls_config):
                        if interrupt_event and interrupt_event.is_set():
                            was_interrupted = True
                            logger.debug(
                                "[LLMService] Interrupt detected mid-stream — "
                                "breaking out of LLM stream loop."
                            )
                            break

                        collected_chunks.append(chunk)

                        if not chunk.content:
                            continue

                        pending_text += chunk.content

                        # Loop in case multiple leak markers / safe segments are
                        # already fully present in this single buffer.
                        while True:
                            result = _scan_for_leak(pending_text)

                            if result is None:
                                # No suspicious marker anywhere in the buffer —
                                # entire thing is safe to yield.
                                out = _emit(pending_text)
                                if out:
                                    yield out
                                pending_text = ""
                                break

                            kind, payload, idx_or_end = result

                            if kind == "incomplete":
                                marker_idx = idx_or_end
                                # Yield everything before the marker; hold the rest.
                                before = pending_text[:marker_idx]
                                out = _emit(before)
                                if out:
                                    yield out
                                held = pending_text[marker_idx:]

                                if len(held) > MAX_HOLD_CHARS:
                                    # False positive guard — this has gone on too
                                    # long to plausibly be a tool-call payload.
                                    logger.warning(
                                        "[LLMService] Held leak-candidate buffer "
                                        f"exceeded {MAX_HOLD_CHARS} chars without "
                                        "resolving — flushing as plain text."
                                    )
                                    out = _emit(held)
                                    if out:
                                        yield out
                                    pending_text = ""
                                else:
                                    pending_text = held
                                break  # wait for next chunk either way

                            if kind == "tool_call":
                                end_idx = idx_or_end
                                start_idx = min(
                                    i for i in (
                                        pending_text.find(_TAG_OPENER),
                                        pending_text.find("{"),
                                    ) if i != -1
                                )
                                before = pending_text[:start_idx]
                                out = _emit(before)
                                if out:
                                    yield out

                                manual_tool_calls.append({
                                    "name": payload["name"],
                                    "args": payload["args"],
                                    "id":   f"manual-{len(manual_tool_calls)}",
                                })
                                logger.info(
                                    f"[LLMService] Caught leaked tool-call in stream: "
                                    f"name={payload['name']!r} args={payload['args']!r}"
                                )

                                pending_text = pending_text[end_idx:]
                                continue  # keep scanning rest of buffer

                            if kind == "not_a_tool_call":
                                end_idx = idx_or_end
                                start_idx = min(
                                    i for i in (
                                        pending_text.find(_TAG_OPENER),
                                        pending_text.find("{"),
                                    ) if i != -1
                                )
                                # Everything up to and including the parsed-but-
                                # irrelevant JSON blob is safe — it just wasn't a
                                # tool call (e.g. model echoed some JSON text).
                                out = _emit(pending_text[:end_idx])
                                if out:
                                    yield out
                                pending_text = pending_text[end_idx:]
                                continue

                except Exception as e:
                    logger.error(f"[LLMService] ask_llm_stream failed: {e}")
                    if use_history:
                        self._rollback_interrupted_turn(query)
                    return

            # ── Final flush: resolve whatever is still held in pending_text ───────
            if pending_text and not was_interrupted:
                while pending_text:
                    result = _scan_for_leak(pending_text)

                    if result is None:
                        out = _emit(pending_text)
                        if out:
                            yield out
                        pending_text = ""
                        break

                    kind, payload, idx_or_end = result

                    if kind == "incomplete":
                        # Stream ended mid-payload — it was never going to
                        # complete. Flush as plain text rather than swallowing it.
                        logger.warning(
                            "[LLMService] Stream ended with an unresolved "
                            "leak-candidate buffer — flushing as plain text."
                        )
                        out = _emit(pending_text)
                        if out:
                            yield out
                        pending_text = ""
                        break

                    start_idx = min(
                        i for i in (
                            pending_text.find(_TAG_OPENER),
                            pending_text.find("{"),
                        ) if i != -1
                    )

                    if kind == "tool_call":
                        end_idx = idx_or_end
                        out = _emit(pending_text[:start_idx])
                        if out:
                            yield out
                        manual_tool_calls.append({
                            "name": payload["name"],
                            "args": payload["args"],
                            "id":   f"manual-{len(manual_tool_calls)}",
                        })
                        logger.info(
                            f"[LLMService] Caught leaked tool-call in final flush: "
                            f"name={payload['name']!r} args={payload['args']!r}"
                        )
                        pending_text = pending_text[end_idx:]
                        continue

                    if kind == "not_a_tool_call":
                        end_idx = idx_or_end
                        out = _emit(pending_text[:end_idx])
                        if out:
                            yield out
                        pending_text = pending_text[end_idx:]
                        continue

            if tracker:
                tracker.llm_end = time.perf_counter()

            # ── Interrupted: roll back history, but token counts are already saved ──
            if was_interrupted:
                if use_history:
                    self._rollback_interrupted_turn(query)
                logger.debug(
                    "[LLMService] Interrupted turn rolled back. "
                    f"History length: {len(self.conversation_history)}"
                )
                return

            all_tool_calls = []
            if collected_chunks:
                from langchain_core.messages import AIMessageChunk

                merged: AIMessageChunk = collected_chunks[0]
                for c in collected_chunks[1:]:
                    merged = merged + c

                if tracker and hasattr(merged, "usage_metadata") and merged.usage_metadata:
                    tracker.input_tokens  = merged.usage_metadata.get("input_tokens", 0)
                    tracker.output_tokens = merged.usage_metadata.get("output_tokens", 0)

                if tracker and full_reply:
                    tracker.output_chars = len(full_reply)

                all_tool_calls = list(merged.tool_calls) + manual_tool_calls
            else:
                all_tool_calls = manual_tool_calls

            # ── Check for tool calls on a completed stream ────────────────────
            if all_tool_calls:
                ai_msg = AIMessage(
                    content=full_reply,
                    tool_calls=all_tool_calls,
                )
                await self._dispatch_tool_calls(ai_msg, tool_callback)
                # Tool calls executed and results appended to history. Continue loop so LLM streams follow-up response.
                continue

            # ── Normal text reply: commit to history and break ────────────────
            if use_history and full_reply:
                self.conversation_history.append(AIMessage(content=full_reply))
            break