import asyncio

import grpc
import numpy as np
import pytest

from parameter_server.proto import parameter_server_pb2 as pb


def task_id(sequence, worker=1):
    return pb.TaskId(worker_id=worker, sequence_number=sequence)


def sparse(*keys):
    return pb.Target(sparse=pb.KeyList(keys=keys))


async def rejected(call, code=grpc.StatusCode.INVALID_ARGUMENT):
    with pytest.raises(grpc.aio.AioRpcError) as failure:
        await call
    assert failure.value.code() == code


@pytest.mark.parametrize(
    "target",
    [
        sparse(2, 1),
        sparse(1, 1),
        pb.Target(),
        pb.Target(range=pb.KeyRange(start=3, end=2)),
        pb.Target(range=pb.KeyRange(start=0, end=1, end_at_keyspace_limit=True)),
    ],
)
async def test_malformed_target_registers_then_fails_without_touching_storage(cluster, target):
    server, stub, _ = await cluster(initial_value=3)
    server.registry.storage.push((1,), [10])
    before = server.registry.storage.pull((1, 2)).data.copy()
    request = pb.PushRequest(task_id=task_id(1), target=target, values=[100, 100])
    acknowledgement = await stub.SubmitPush(request, timeout=5)
    assert acknowledgement.task_id == request.task_id
    result = await stub.WaitTask(request.task_id, timeout=5)
    assert result.state == pb.FAILED
    assert result.WhichOneof("outcome") == "error"
    assert result.error.code == pb.INVALID_ARGUMENT
    status = await stub.GetTaskStatus(request.task_id, timeout=5)
    assert status.state == pb.FAILED
    assert status.error == result.error
    np.testing.assert_array_equal(server.registry.storage.pull((1, 2)).data, before)
    assert set(server.registry.storage._vectors) == {1}


@pytest.mark.parametrize(
    "configuration,target",
    [
        ({"max_keys_per_request": 2}, sparse(1, 2, 3)),
        ({"max_keys_per_request": 2}, pb.Target(range=pb.KeyRange(start=0, end=3))),
        ({"max_message_bytes": 1024, "width": 8}, sparse(*range(10))),
        (
            {"max_keys_per_request": 2},
            pb.Target(range=pb.KeyRange(start=0, end_at_keyspace_limit=True)),
        ),
    ],
)
async def test_request_limits_fail_before_storage_allocation(cluster, configuration, target):
    server, stub, _ = await cluster(**configuration)
    request = pb.PullRequest(task_id=task_id(1), target=target)
    await stub.SubmitPull(request, timeout=5)
    result = await stub.WaitTask(request.task_id, timeout=5)
    assert result.state == pb.FAILED
    assert result.error.code == pb.INVALID_ARGUMENT
    assert server.registry.storage._vectors == {}


async def test_missing_unknown_and_cross_worker_ids_rejected_at_submission(cluster):
    server, stub, _ = await cluster()
    await rejected(stub.SubmitPush(pb.PushRequest(target=sparse(1), values=[1]), timeout=5))
    await rejected(stub.SubmitPull(pb.PullRequest(target=sparse(1)), timeout=5))
    await rejected(stub.WaitTask(task_id(999), timeout=5))
    await rejected(stub.GetTaskStatus(task_id(999), timeout=5))
    known = pb.PushRequest(task_id=task_id(1), target=sparse(1), values=[1])
    await stub.SubmitPush(known, timeout=5)
    for dependency in (task_id(999), task_id(1, worker=2), task_id(2)):
        request = pb.PullRequest(task_id=task_id(2), target=sparse(1), dependencies=[dependency])
        await rejected(stub.SubmitPull(request, timeout=5))
    cross_worker = pb.PullRequest(
        task_id=task_id(2, worker=2), target=sparse(1), dependencies=[known.task_id]
    )
    await rejected(stub.SubmitPull(cross_worker, timeout=5))
    assert set(server.registry.records) == {(1, 1)}
    await stub.WaitTask(known.task_id, timeout=5)


async def test_server_enforces_dependencies_and_all_status_transitions(cluster, gate):
    _, stub, _ = await cluster(update_rule=gate.name, rule_parameters={})
    push = pb.PushRequest(task_id=task_id(30), target=sparse(1), values=[5])
    try:
        await stub.SubmitPush(push, timeout=5)
        await gate.wait_entered()
        assert (await stub.GetTaskStatus(push.task_id, timeout=5)).state == pb.RUNNING
        pull = pb.PullRequest(task_id=task_id(2), target=sparse(1), dependencies=[push.task_id])
        acknowledgement = await stub.SubmitPull(pull, timeout=5)
        assert acknowledgement.task_id == pull.task_id
        assert (await stub.GetTaskStatus(pull.task_id, timeout=5)).state == pb.PENDING
        assert (await stub.GetTaskStatus(push.task_id, timeout=5)).state == pb.RUNNING
    finally:
        gate.release.set()
    result = await stub.WaitTask(pull.task_id, timeout=5)
    assert result.state == pb.SUCCEEDED
    assert result.WhichOneof("outcome") == "values"
    assert list(result.values.data) == [5]
    assert (await stub.GetTaskStatus(pull.task_id, timeout=5)).state == pb.SUCCEEDED
    assert (await stub.GetTaskStatus(push.task_id, timeout=5)).state == pb.SUCCEEDED


async def test_raw_failure_propagates_through_registered_dependency_chain(cluster):
    server, stub, _ = await cluster()
    invalid = pb.PushRequest(task_id=task_id(1), target=sparse(1), values=[])
    child = pb.PushRequest(
        task_id=task_id(2), target=sparse(1), values=[100], dependencies=[invalid.task_id]
    )
    grandchild = pb.PullRequest(task_id=task_id(3), target=sparse(1), dependencies=[child.task_id])
    await stub.SubmitPush(invalid, timeout=5)
    await stub.SubmitPush(child, timeout=5)
    await stub.SubmitPull(grandchild, timeout=5)
    assert (await stub.WaitTask(invalid.task_id, timeout=5)).error.code == pb.INVALID_ARGUMENT
    for identifier in (child.task_id, grandchild.task_id):
        result = await stub.WaitTask(identifier, timeout=5)
        assert result.state == pb.FAILED
        assert result.error.code == pb.DEPENDENCY_FAILED
    assert server.registry.storage._vectors == {}


async def test_deduplication_pending_running_and_completed_and_conflicting_ids(cluster, gate):
    server, stub, _ = await cluster(update_rule=gate.name, rule_parameters={})
    blocker = pb.PushRequest(task_id=task_id(100), target=sparse(1), values=[2])
    pending = pb.PushRequest(
        task_id=task_id(5), target=sparse(1), values=[3], dependencies=[blocker.task_id]
    )
    try:
        await stub.SubmitPush(blocker, timeout=5)
        await gate.wait_entered()
        await stub.SubmitPush(pending, timeout=5)
        assert (await stub.GetTaskStatus(pending.task_id, timeout=5)).state == pb.PENDING
        for request in (blocker, pending):
            acknowledgements = await asyncio.gather(
                *(stub.SubmitPush(request, timeout=5) for _ in range(8))
            )
            assert all(ack.task_id == request.task_id for ack in acknowledgements)
            changed = pb.PushRequest()
            changed.CopyFrom(request)
            changed.values[0] = 99
            await rejected(stub.SubmitPush(changed, timeout=5))
            await rejected(
                stub.SubmitPull(
                    pb.PullRequest(task_id=request.task_id, target=request.target), timeout=5
                )
            )
        assert len(server.registry.records) == 2
    finally:
        gate.release.set()
    result = await stub.WaitTask(pending.task_id, timeout=5)
    assert result.state == pb.SUCCEEDED
    for request in (blocker, pending):
        await stub.SubmitPush(request, timeout=5)
        assert (await stub.WaitTask(request.task_id, timeout=5)).state == pb.SUCCEEDED
    assert gate.calls == 2
    read = pb.PullRequest(task_id=task_id(1), target=sparse(1))
    await stub.SubmitPull(read, timeout=5)
    original = await stub.WaitTask(read.task_id, timeout=5)
    assert list(original.values.data) == [5]
    await stub.SubmitPull(read, timeout=5)
    assert await stub.WaitTask(read.task_id, timeout=5) == original
    await rejected(
        stub.SubmitPush(
            pb.PushRequest(task_id=read.task_id, target=sparse(1), values=[1]), timeout=5
        )
    )
    changed = pb.PushRequest()
    changed.CopyFrom(pending)
    changed.iteration = 1
    await rejected(stub.SubmitPush(changed, timeout=5))
    assert gate.calls == 2
    np.testing.assert_array_equal(server.registry.storage.pull((1,)).data, [5])


async def test_failed_task_deduplicates_without_reexecution(cluster):
    server, stub, _ = await cluster()
    request = pb.PushRequest(task_id=task_id(1), target=sparse(1), values=[])
    await stub.SubmitPush(request, timeout=5)
    original = await stub.WaitTask(request.task_id, timeout=5)
    assert original.state == pb.FAILED
    await stub.SubmitPush(request, timeout=5)
    assert await stub.WaitTask(request.task_id, timeout=5) == original
    assert len(server.registry.records) == 1
    assert not server.registry.storage._vectors


async def test_wait_rpc_deadline_and_cancellation_preserve_server_task(cluster, gate, monkeypatch):
    server, stub, _ = await cluster(update_rule=gate.name, rule_parameters={})
    wait_received = asyncio.Event()
    original_wait = server.registry.wait

    async def tracked_wait(identifier):
        wait_received.set()
        return await original_wait(identifier)

    monkeypatch.setattr(server.registry, "wait", tracked_wait)
    request = pb.PushRequest(task_id=task_id(1), target=sparse(1), values=[8])
    try:
        await stub.SubmitPush(request, timeout=5)
        await gate.wait_entered()
        await rejected(
            stub.WaitTask(request.task_id, timeout=0.02), grpc.StatusCode.DEADLINE_EXCEEDED
        )
        wait_received.clear()
        waiting = stub.WaitTask(request.task_id, timeout=5)
        await asyncio.wait_for(wait_received.wait(), 5)
        assert waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert (await stub.GetTaskStatus(request.task_id, timeout=5)).state == pb.RUNNING
    finally:
        gate.release.set()
    completed = await stub.WaitTask(request.task_id, timeout=5)
    assert completed.state == pb.SUCCEEDED
    assert gate.calls == 1
    read = pb.PullRequest(task_id=task_id(2), target=sparse(1))
    await stub.SubmitPull(read, timeout=5)
    assert list((await stub.WaitTask(read.task_id, timeout=5)).values.data) == [8]


async def test_registry_copies_request_and_registers_before_execution(cluster, gate):
    server, _, _ = await cluster(update_rule=gate.name, rule_parameters={})
    request = pb.PushRequest(task_id=task_id(1), target=sparse(2), values=[3])
    identifier = pb.TaskId()
    identifier.CopyFrom(request.task_id)
    acknowledgement = server.registry.submit("push", request)
    assert acknowledgement.task_id == identifier
    assert server.registry.status(identifier).state == pb.PENDING
    request.task_id.sequence_number = 99
    request.target.sparse.keys[:] = [8]
    request.values[:] = [999]
    try:
        await gate.wait_entered()
    finally:
        gate.release.set()
    result = await asyncio.wait_for(server.registry.wait(identifier), 5)
    assert result.state == pb.SUCCEEDED
    assert result.task_id == identifier
    np.testing.assert_array_equal(server.registry.storage.pull((2, 8)).data, [3, 0])


async def test_startup_cancellation_stops_partially_started_grpc_server(monkeypatch):
    from parameter_server import server as server_module
    from parameter_server.config import ServerConfig

    stopped = asyncio.Event()

    class CancelledStartupServer:
        def add_generic_rpc_handlers(self, handlers):
            pass

        def add_registered_method_handlers(self, service_name, handlers):
            pass

        def add_insecure_port(self, address):
            return 12345

        async def start(self):
            raise asyncio.CancelledError

        async def stop(self, grace):
            stopped.set()

    monkeypatch.setattr(
        server_module.grpc.aio, "server", lambda **options: CancelledStartupServer()
    )
    with pytest.raises(asyncio.CancelledError):
        await server_module.start_server(ServerConfig(address="127.0.0.1:0"))
    assert stopped.is_set()
