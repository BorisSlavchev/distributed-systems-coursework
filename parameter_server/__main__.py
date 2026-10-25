import argparse
import asyncio
import logging
import signal

from parameter_server.config import ServerConfig
from parameter_server.server import start_server


async def serve(config: ServerConfig):
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stopped.set)
    async with await start_server(config):
        await stopped.wait()


def main():
    parser = argparse.ArgumentParser(description="Single-node parameter server")
    parser.add_argument("--config", help="JSON server configuration")
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO"
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    config = ServerConfig.from_json(args.config) if args.config else ServerConfig()
    asyncio.run(serve(config))


if __name__ == "__main__":
    main()
