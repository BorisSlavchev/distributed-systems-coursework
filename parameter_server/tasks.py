import asyncio
import hashlib
import logging
from dataclasses import dataclass

from parameter_server.errors import (
    DependencyFailedError,
    InternalError,
    InvalidArgumentError,
    ParameterServerError,
    UnavailableError,
)
from parameter_server.proto import parameter_server_pb2 as pb
from parameter_server.storage import Storage
from parameter_server.targets import decode_target

logger = logging.getLogger(__name__)


def task_key(task_id: pb.TaskId) -> tuple[int, int]:
    return task_id.worker_id, task_id.sequence_number


@dataclass
class TaskRecord:
    task_id: pb.TaskId
    fingerprint: bytes
    completion: asyncio.Future
    state: int = pb.PENDING
    runner: asyncio.Task | None = None


class TaskRegistry:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.records: dict[tuple[int, int], TaskRecord] = {}
        self._closed = False

    def submit(self, operation: str, request: pb.PushRequest | pb.PullRequest) -> pb.Ack:
        if self._closed:
            raise UnavailableError("server is shutting down")
        if not request.HasField("task_id"):
            raise InvalidArgumentError("task_id is required")
        key = task_key(request.task_id)
        fingerprint = hashlib.sha256(
            operation.encode() + request.SerializeToString(deterministic=True)
        ).digest()
        if key in self.records:
            if self.records[key].fingerprint != fingerprint:
                raise InvalidArgumentError("task ID is already registered with different contents")
            return pb.Ack(task_id=request.task_id)
        dependencies = []
        for dependency in request.dependencies:
            dependency_key = task_key(dependency)
            if dependency.worker_id != request.task_id.worker_id:
                raise InvalidArgumentError("cross-worker dependencies are unsupported")
            if dependency_key not in self.records:
                raise InvalidArgumentError("dependencies must already be registered")
            dependencies.append(self.records[dependency_key])
        frozen_request = type(request)()
        frozen_request.CopyFrom(request)
        copied_id = pb.TaskId()
        copied_id.CopyFrom(request.task_id)
        record = TaskRecord(copied_id, fingerprint, asyncio.get_running_loop().create_future())
        self.records[key] = record
        record.runner = asyncio.create_task(
            self._execute(record, operation, frozen_request, dependencies)
        )
        logger.debug("registered %s task %s", operation, key)
        return pb.Ack(task_id=request.task_id)

    async def _execute(self, record, operation, request, dependencies):
        try:
            if dependencies:
                waiting = [asyncio.shield(dependency.completion) for dependency in dependencies]
                try:
                    for completed in asyncio.as_completed(waiting):
                        if (await completed).state != pb.SUCCEEDED:
                            raise DependencyFailedError("a prerequisite task failed")
                finally:
                    for completion in waiting:
                        if not completion.done():
                            completion.cancel()
            record.state = pb.RUNNING
            keys = decode_target(request.target, self.storage.config)
            if operation == "push":
                await asyncio.to_thread(self.storage.push, keys, request.values)
                result = pb.TaskResult(task_id=record.task_id, ack=pb.Ack(task_id=record.task_id))
            else:
                values = await asyncio.to_thread(self.storage.pull, keys)
                result = pb.TaskResult(
                    task_id=record.task_id,
                    values=pb.Values(keys=values.keys, data=values.data, width=values.width),
                )
            record.state = pb.SUCCEEDED
            result.state = pb.SUCCEEDED
        except ParameterServerError as error:
            result = self._failure(record, error)
        except Exception:
            logger.exception("task %s failed", task_key(record.task_id))
            result = self._failure(record, InternalError("internal execution failure"))
        record.completion.set_result(result)

    def _failure(self, record: TaskRecord, error: ParameterServerError) -> pb.TaskResult:
        record.state = pb.FAILED
        logger.info("task %s failed: %s", task_key(record.task_id), error)
        return pb.TaskResult(
            task_id=record.task_id,
            state=pb.FAILED,
            error=pb.TaskError(code=int(error.code), message=str(error)),
        )

    def get(self, task_id: pb.TaskId) -> TaskRecord:
        try:
            return self.records[task_key(task_id)]
        except KeyError as error:
            raise InvalidArgumentError("unknown task ID") from error

    async def wait(self, task_id: pb.TaskId) -> pb.TaskResult:
        return await asyncio.shield(self.get(task_id).completion)

    def status(self, task_id: pb.TaskId) -> pb.TaskStatus:
        record = self.get(task_id)
        status = pb.TaskStatus(task_id=record.task_id, state=record.state)
        if record.state == pb.FAILED:
            status.error.CopyFrom(record.completion.result().error)
        return status

    async def close(self):
        self._closed = True
        runners = [record.runner for record in self.records.values() if record.runner is not None]
        if runners:
            await asyncio.gather(*runners)
