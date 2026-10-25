import argparse
import asyncio
from contextlib import AsyncExitStack

import numpy as np

from parameter_server import Client


async def run(address: str):
    async with AsyncExitStack() as stack:
        workers = [
            await stack.enter_async_context(await Client.connect(address, worker_id=worker_id))
            for worker_id in (1, 2)
        ]
        keys = [2, 7, 19]
        initial = await workers[0].pull(keys).wait()
        np.testing.assert_allclose(initial.data, 10.0)
        if initial.width != 2 or workers[0].server_info.update_rule != "sgd":
            raise RuntimeError("demo requires config/server.json on a fresh server")
        tasks = [
            worker.push(keys, np.full(len(keys) * initial.width, gradient))
            for worker, gradient in zip(workers, (2.0, 3.0), strict=True)
        ]
        await asyncio.gather(*(task.wait() for task in tasks))
        final = await workers[0].pull(keys).wait()
        expected = np.full(len(keys) * final.width, 10.0 - 0.1 * (2.0 + 3.0))
        np.testing.assert_allclose(final.data, expected, rtol=1e-13, atol=1e-13)
        print(
            f"PASS: two workers, keys={final.keys}, width={final.width}, "
            f"values={final.data.tolist()}"
        )


def main():
    parser = argparse.ArgumentParser(description="Deterministic two-worker correctness demo")
    parser.add_argument("--address", default="127.0.0.1:50051")
    asyncio.run(run(parser.parse_args().address))


if __name__ == "__main__":
    main()
