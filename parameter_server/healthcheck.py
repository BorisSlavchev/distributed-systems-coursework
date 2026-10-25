import asyncio
import sys

from parameter_server import Client


async def check(address: str):
    async with await Client.connect(address, worker_id=0, connect_timeout_ms=2000):
        pass


if __name__ == "__main__":
    asyncio.run(check(sys.argv[1]))
