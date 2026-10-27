import asyncio
from collections.abc import Sequence

import grpc
import numpy as np

from parameter_server.config import DEFAULT_MAX_MESSAGE_BYTES, grpc_options
from parameter_server.errors import (
    DependencyFailedError,
    InternalError,
    InvalidArgumentError,
    ParameterServerError,
    TaskTimeoutError,
    UnavailableError,
    application_error,
    rpc_error,
)
from parameter_server.proto import parameter_server_pb2 as pb
from parameter_server.proto import parameter_server_pb2_grpc as rpc
from parameter_server.targets import encode_target
from parameter_server.types import UINT32_MAX, UINT64_MAX, Ack, KeyRange, TaskId, Values, unsigned


def _consume_exception(future):
    if not future.cancelled():
        future.exception()


class Task:
    def __init__(self, client, task_id, request, operation, dependencies):
        self._client = client
        self.task_id = task_id
        self._registered = asyncio.get_running_loop().create_future()
        self._registered.add_done_callback(_consume_exception)
        self._completion = asyncio.create_task(self._run(request, operation, dependencies))
        self._completion.add_done_callback(_consume_exception)

    async def _run(self, request, operation, dependencies):
        try:
            if self._client._closed:
                raise UnavailableError("client is closed")
            for dependency in dependencies:
                try:
                    await asyncio.shield(dependency._registered)
                except ParameterServerError as error:
                    raise DependencyFailedError("prerequisite could not be registered") from error
            if self._client._closed:
                raise UnavailableError("client is closed")
            submit = (
                self._client._stub.SubmitPush
                if operation == "push"
                else self._client._stub.SubmitPull
            )
            await submit(request, timeout=self._client._submission_timeout)
            self._registered.set_result(None)
            if self._client._closed:
                raise UnavailableError("client closed after task registration")
            result = await self._client._stub.WaitTask(request.task_id)
            if result.WhichOneof("outcome") == "error":
                raise application_error(result.error.code, result.error.message)
            if result.WhichOneof("outcome") == "ack":
                return Ack(self.task_id)
            if result.WhichOneof("outcome") == "values":
                data = np.array(result.values.data, dtype=np.float64, copy=True)
                data.setflags(write=False)
                return Values(tuple(result.values.keys), data, result.values.width)
            raise InternalError("server returned no task outcome")
        except grpc.aio.AioRpcError as error:
            raise rpc_error(error) from error
        except asyncio.CancelledError as error:
            raise UnavailableError("client closed before local completion") from error
        finally:
            if not self._registered.done():
                self._registered.set_exception(
                    UnavailableError("task registration did not complete")
                )

    async def wait(self, timeout_ms: float | None = None) -> Values | Ack:
        if timeout_ms is not None and (
            isinstance(timeout_ms, bool)
            or not isinstance(timeout_ms, (int, float))
            or not np.isfinite(timeout_ms)
            or timeout_ms < 0
        ):
            raise InvalidArgumentError("timeout_ms must be a nonnegative finite number or None")
        if self._completion.done():
            return self._completion.result()
        try:
            if timeout_ms is None:
                return await asyncio.shield(self._completion)
            return await asyncio.wait_for(asyncio.shield(self._completion), timeout_ms / 1000)
        except TimeoutError as error:
            raise TaskTimeoutError("wait timed out; task continues") from error

    def done(self) -> bool:
        return self._completion.done()


class Client:
    def __init__(self, channel, worker_id, sequence_start, submission_timeout_ms, server_info):
        self._channel = channel
        self._stub = rpc.ParameterServerStub(channel)
        self.worker_id = worker_id
        self._next_sequence = sequence_start
        self._submission_timeout = submission_timeout_ms / 1000
        self.server_info = server_info
        self._active: set[Task] = set()
        self._closed = False
        self._closing: asyncio.Task | None = None

    @classmethod
    async def connect(
        cls,
        server_address: str,
        worker_id: int,
        *,
        sequence_start: int = 0,
        connect_timeout_ms: float = 5000,
        submission_timeout_ms: float = 5000,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ) -> Client:
        worker_id = unsigned(worker_id, UINT32_MAX, "worker_id")
        sequence_start = unsigned(sequence_start, UINT64_MAX, "sequence_start")
        if not isinstance(server_address, str) or not server_address:
            raise InvalidArgumentError("server_address must be a nonempty string")
        for timeout in (connect_timeout_ms, submission_timeout_ms):
            if (
                isinstance(timeout, bool)
                or not isinstance(timeout, (float, int))
                or not np.isfinite(timeout)
                or timeout <= 0
            ):
                raise InvalidArgumentError("connection and submission timeouts must be positive")
        unsigned(max_message_bytes, 2**31 - 1, "max_message_bytes")
        if max_message_bytes < 1024:
            raise InvalidArgumentError("max_message_bytes must be at least 1024")
        channel = grpc.aio.insecure_channel(server_address, options=grpc_options(max_message_bytes))
        try:
            info = await rpc.ParameterServerStub(channel).GetServerInfo(
                pb.Empty(), timeout=connect_timeout_ms / 1000
            )
        except grpc.aio.AioRpcError as error:
            await channel.close()
            raise rpc_error(error) from error
        except BaseException:
            await channel.close()
            raise
        return cls(channel, worker_id, sequence_start, submission_timeout_ms, info)

    @property
    def next_sequence_number(self) -> int:
        return self._next_sequence

    def _schedule(self, operation, target, values, deps, iteration):
        if self._closed:
            raise UnavailableError("client is closed")
        iteration = unsigned(iteration, UINT64_MAX, "iteration")
        sequence = unsigned(self._next_sequence, UINT64_MAX, "sequence_number")
        selected = encode_target(target)
        try:
            dependencies = tuple(deps)
        except TypeError as error:
            raise InvalidArgumentError(
                "deps must be a sequence of this client's handles"
            ) from error
        if any(not isinstance(dep, Task) or dep._client is not self for dep in dependencies):
            raise InvalidArgumentError("dependencies must be handles from this client")
        task_id = TaskId(self.worker_id, sequence)
        metadata = {
            "task_id": pb.TaskId(worker_id=self.worker_id, sequence_number=sequence),
            "target": selected,
            "dependencies": [
                pb.TaskId(
                    worker_id=dep.task_id.worker_id, sequence_number=dep.task_id.sequence_number
                )
                for dep in dependencies
            ],
            "iteration": iteration,
        }
        if operation == "push":
            try:
                copied = np.array(values, dtype=np.float64, copy=True)
            except (ValueError, TypeError, OverflowError) as error:
                raise InvalidArgumentError("values must contain float64 numbers") from error
            if copied.ndim != 1:
                raise InvalidArgumentError("values must be a flat array")
            request = pb.PushRequest(**metadata, values=copied)
        else:
            request = pb.PullRequest(**metadata)
        self._next_sequence += 1
        task = Task(self, task_id, request, operation, dependencies)
        self._active.add(task)
        task._completion.add_done_callback(lambda completed: self._active.discard(task))
        return task

    def push(
        self,
        target: Sequence[int] | KeyRange,
        values,
        deps: Sequence[Task] = (),
        iteration: int = 0,
    ) -> Task:
        return self._schedule("push", target, values, deps, iteration)

    def pull(
        self, target: Sequence[int] | KeyRange, deps: Sequence[Task] = (), iteration: int = 0
    ) -> Task:
        return self._schedule("pull", target, None, deps, iteration)

    async def _close(self):
        active = tuple(self._active)
        await self._channel.close()
        await asyncio.gather(*(task._completion for task in active), return_exceptions=True)
        for task in active:
            if not task._registered.done():
                task._registered.set_exception(
                    UnavailableError("client closed before registration")
                )

    async def close(self):
        if self._closing is None:
            self._closed = True
            self._closing = asyncio.create_task(self._close())
        await asyncio.shield(self._closing)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
