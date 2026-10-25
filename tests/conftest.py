import asyncio
import threading
from contextlib import AsyncExitStack
from uuid import uuid4

import grpc
import pytest

from parameter_server import Client
from parameter_server.config import ServerConfig, grpc_options
from parameter_server.proto import parameter_server_pb2_grpc as rpc
from parameter_server.rules import register_update_rule
from parameter_server.server import start_server


@pytest.fixture
async def cluster():
    async with AsyncExitStack() as stack:

        async def create(**configuration):
            server = await start_server(ServerConfig(address="127.0.0.1:0", **configuration))
            await stack.enter_async_context(server)
            address = f"127.0.0.1:{server.port}"
            channel = grpc.aio.insecure_channel(address, options=grpc_options(64 * 1024 * 1024))
            await stack.enter_async_context(channel)

            async def connect(worker_id=1, **options):
                client = await Client.connect(address, worker_id, **options)
                return await stack.enter_async_context(client)

            return server, rpc.ParameterServerStub(channel), connect

        yield create


class GateRule:
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0
        self.name = f"test_gate_{uuid4().hex}"
        register_update_rule(self.name, self)

    def __call__(self, weights, updates, parameters):
        self.calls += 1
        self.entered.set()
        if not self.release.wait(10):
            raise RuntimeError("test gate was not released within ten seconds")
        return weights + updates

    async def wait_entered(self):
        assert await asyncio.to_thread(self.entered.wait, 5), "rule never started"


@pytest.fixture
def gate():
    rule = GateRule()
    try:
        yield rule
    finally:
        rule.release.set()
