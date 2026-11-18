

import logging

from logging_config import setup_logging
setup_logging()
import asyncio


from audio_socket_server import serve as serve_audiosocket
from fastagi_server import serve as serve_fastagi

# logging.basicConfig(
#     level=logging.INFO,
#     format="[%(asctime)s] %(levelname)s %(message)s",
#     datefmt="%H:%M:%S",
# )
log = logging.getLogger("run_servers")


async def main() -> None:
    log.info("Starting AudioSocket server and FastAGI server together...")
    await asyncio.gather(
        serve_audiosocket(),
        serve_fastagi(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Servers shut down.")