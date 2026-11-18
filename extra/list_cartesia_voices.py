"""List available Cartesia voices so you can pick a natural-sounding one.

Usage:
  python extra/list_cartesia_voices.py

Then swap the chosen id into services/tts.py -> _VOICE_BY_LANGUAGE["en"].
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()


async def main():
    from cartesia import AsyncCartesia

    api_key = os.environ.get("CARTESIA_API_KEY")
    if not api_key:
        print("CARTESIA_API_KEY missing in .env")
        sys.exit(1)

    client = AsyncCartesia(api_key=api_key)
    voices = await client.voices.list()

    print(f"{'ID':<40} {'NAME':<28} LANG")
    print("-" * 90)
    for v in getattr(voices, "data", voices):
        langs = ",".join(getattr(v, "language", ["?"]) or ["?"])
        print(f"{getattr(v, 'id', '?'):<40} {getattr(v, 'name', '?'):<28} {langs}")

    await client.close() if hasattr(client, "close") else None


if __name__ == "__main__":
    asyncio.run(main())