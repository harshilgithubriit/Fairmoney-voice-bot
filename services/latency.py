import time
from dataclasses import dataclass, field


@dataclass
class LatencyTracker:
    # ── STT ──────────────────────────────────────────────────────────────────
    stt_start: float = 0.0          # mic stream opened (listen_turn called)
    stt_speech_start: float = 0.0   # StartOfTurn received (user actually spoke)
    stt_end: float = 0.0            # EagerEndOfTurn/EndOfTurn → transcript ready

    input_tokens: int = 0
    output_tokens: int = 0
    output_chars: int = 0      # what Cartesia actually bills on
    cartesia_cost_usd: float = 0.0

    # ── LLM ──────────────────────────────────────────────────────────────────
    llm_start: float = 0.0          # first token requested
    llm_first_token: float = 0.0    # first chunk received (TTFT)
    llm_end: float = 0.0            # last chunk received

    # ── TTS ──────────────────────────────────────────────────────────────────
    tts_start: float = 0.0          # first text chunk pushed to Cartesia
    tts_first_audio: float = 0.0    # first audio chunk received (TTFA)
    tts_end: float = 0.0            # last audio chunk played

    # ── E2E ──────────────────────────────────────────────────────────────────
    turn_start: float = field(default_factory=time.time)

    # ── helpers ───────────────────────────────────────────────────────────────
    def stt_wait_time(self) -> float:
        """Silence before user started speaking — not real STT work."""
        return round(self.stt_speech_start - self.stt_start, 3)

    def stt_processing_latency(self) -> float:
        """True STT latency: from first word spoken → transcript ready."""
        return round(self.stt_end - self.stt_speech_start, 3)

    def stt_latency(self) -> float:
        """Total STT time including silence wait (kept for e2e calculation)."""
        return round(self.stt_end - self.stt_start, 3)

    def llm_ttft(self) -> float:
        """Time-to-first-token: how long before LLM started responding."""
        return round(self.llm_first_token - self.llm_start, 3)

    def llm_stream_duration(self) -> float:
        """How long the full LLM stream took."""
        return round(self.llm_end - self.llm_start, 3)

    def tts_ttfa(self) -> float:
        """Time-to-first-audio: from first text pushed → first audio byte."""
        return round(self.tts_first_audio - self.tts_start, 3)

    def tts_total(self) -> float:
        """Full TTS playback duration."""
        return round(self.tts_end - self.tts_start, 3)

    def e2e_latency(self) -> float:
        """Total turn latency: mic start → last audio played."""
        return round(self.tts_end - self.stt_start, 3)

    def perceived_latency(self) -> float:
        """
        What the user actually feels: finished speaking → first audio heard.
        = STT processing (not including silence wait) + LLM TTFT + TTS TTFA
        """
        return round(
            self.stt_processing_latency() + self.llm_ttft() + self.tts_ttfa(), 3
        )

    def report(self) -> str:
        lines = [
            "",
            "┌─────────────────────────────────────────┐",
            "│           ⏱  LATENCY BREAKDOWN           │",
            "├─────────────────────────────────────────┤",
            f"│  STT  wait (silence before speak) : {self.stt_wait_time():>5.2f}s    │",
            f"│  STT  processing (speech → text)  : {self.stt_processing_latency():>5.2f}s    │",
            f"│  LLM  Time-to-first-token         : {self.llm_ttft():>5.2f}s    │",
            f"│  LLM  Full stream duration        : {self.llm_stream_duration():>5.2f}s    │",
            f"│  TTS  Time-to-first-audio         : {self.tts_ttfa():>5.2f}s    │",
            f"│  TTS  Full playback               : {self.tts_total():>5.2f}s    │",
            "├─────────────────────────────────────────┤",
            f"│  👂 Perceived latency              : {self.perceived_latency():>5.2f}s    │",
            f"│  🔄 End-to-end latency             : {self.e2e_latency():>5.2f}s    │",
            "└─────────────────────────────────────────┘",
            "",
        ]
        return "\n".join(lines)