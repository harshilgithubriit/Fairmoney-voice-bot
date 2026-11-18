"""Generate a clean 8kHz 16-bit mono WAV of a customer's reply using
Cartesia TTS, for load-testing the bot with high-confidence STT audio.

Usage:
  python extra/make_customer_wav.py "Yes, this is him. I can pay the full amount today." customer_pay.wav
"""
import asyncio
import os
import sys
import wave

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from cartesia import AsyncCartesia

VOICE_ID = "faf0731e-dfb9-4cfc-8119-259a79b27e12"


async def main(text: str, out_path: str) -> None:
    api_key = os.environ.get("CARTESIA_API_KEY")
    if not api_key:
        print("CARTESIA_API_KEY missing in .env")
        sys.exit(1)

    client = AsyncCartesia(api_key=api_key)
    async with client.tts.websocket_connect() as ws:
        ctx = ws.context(
            model_id="sonic-3.5",
            voice={"mode": "id", "id": VOICE_ID},
            language="en",
            output_format={
                "container": "raw",
                "encoding": "pcm_s16le",
                "sample_rate": 8000,
            },
        )
        await ctx.push(text)
        await ctx.no_more_inputs()

        chunks = []
        async for response in ctx.receive():
            if response.type == "chunk" and response.audio:
                chunks.append(response.audio)

    pcm = b"".join(chunks)
    with wave.open(out_path, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(8000)
        wf.writeframes(pcm)

    print(f"Wrote {out_path}: {len(pcm)} bytes PCM ({len(pcm) / 16000:.1f}s at 8kHz)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    asyncio.run(main(sys.argv[1], sys.argv[2]))