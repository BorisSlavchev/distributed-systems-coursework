import asyncio
import logging

import grpc

from parameter_server.config import ServerConfig, grpc_options
from parameter_server.errors import InvalidArgumentError, UnavailableError
from parameter_server.proto import parameter_server_pb2 as pb
from parameter_server.proto import parameter_server_pb2_grpc as rpc
from parameter_server.storage import Storage
from parameter_server.tasks import TaskRegistry

logger = logging.getLogger(__name__)


class ParameterServerService(rpc.ParameterServerServicer):
    def __init__(self, registry: TaskRegistry):
        self.registry = registry

    async def _submit(self, operation, request, context):
        try:
            return self.registry.submit(operation, request)
        except InvalidArgumentError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        except UnavailableError as error:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(error))

    async def SubmitPush(self, request, context):
        return await self._submit("push", request, context)

    async def SubmitPull(self, request, context):
        return await self._submit("pull", request, context)

    async def WaitTask(self, request, context):
        try:
            return await self.registry.wait(request)
        except InvalidArgumentError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

    async def GetTaskStatus(self, request, context):
        try:
            return self.registry.status(request)
        except InvalidArgumentError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

    async def GetServerInfo(self, request, context):
        config = self.registry.storage.config
        return pb.ServerInfo(
            width=config.width,
            initial_value=config.initial_value,
            max_keys_per_request=config.max_keys_per_request,
            max_message_bytes=config.max_message_bytes,
            update_rule=config.update_rule,
        )


class RunningServer:
    def __init__(self, server: grpc.aio.Server, registry: TaskRegistry, port: int):
        self.grpc_server = server
        self.registry = registry
        self.port = port
        self._closing: asyncio.Task | None = None

    async def _close(self):
        await self.registry.close()
        await self.grpc_server.stop(1.0)
        logger.info("server stopped")

    async def close(self):
        if self._closing is None:
            self._closing = asyncio.create_task(self._close())
        await asyncio.shield(self._closing)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


async def start_server(config: ServerConfig | None = None) -> RunningServer:
    config = config or ServerConfig()
    registry = TaskRegistry(Storage(config))
    server = grpc.aio.server(options=grpc_options(config.max_message_bytes))
    rpc.add_ParameterServerServicer_to_server(ParameterServerService(registry), server)
    try:
        port = server.add_insecure_port(config.address)
        if not port:
            raise UnavailableError(f"cannot bind {config.address}")
        await server.start()
    except BaseException:
        await server.stop(0)
        raise
    logger.info("listening on %s (port %d), width=%d", config.address, port, config.width)
    return RunningServer(server, registry, port)
