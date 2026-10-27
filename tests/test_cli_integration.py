import asyncio
import json
import os
import signal
import socket
import sys
from pathlib import Path

import grpc
import numpy as np

from parameter_server import Client, KeyRange

ROOT = Path(__file__).resolve().parents[1]


async def test_cli_json_configuration_in_separate_process(tmp_path):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    address = f"127.0.0.1:{port}"
    config = tmp_path / "server.json"
    config.write_text(
        json.dumps(
            {
                "address": address,
                "width": 3,
                "initial_value": 2.5,
                "update_rule": "sgd",
                "rule_parameters": {"learning_rate": 0.25},
            }
        )
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "parameter_server",
        "--config",
        str(config),
        cwd=ROOT,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with grpc.aio.insecure_channel(address) as readiness:
            await asyncio.wait_for(readiness.channel_ready(), 10)
        async with await Client.connect(address, 1) as first:
            async with await Client.connect(address, 2) as second:
                assert first.server_info.width == 3
                assert first.server_info.initial_value == 2.5
                initial = await second.pull(KeyRange(5, 7)).wait(5000)
                np.testing.assert_array_equal(initial.data, np.full(6, 2.5))
                await first.push([5, 6], [1, 2, 3, 4, 5, 6]).wait(5000)
                await second.push([6], [2, 4, 6]).wait(5000)
                result = await first.pull(KeyRange(5, 7)).wait(5000)
                assert result.keys == (5, 6)
                np.testing.assert_allclose(result.data, [2.25, 2, 1.75, 1, 0.25, -0.5])
        process.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == 0, (stdout.decode(), stderr.decode())
    finally:
        if process.returncode is None:
            process.kill()
            await asyncio.wait_for(process.communicate(), 5)
