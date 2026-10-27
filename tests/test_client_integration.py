import asyncio
import socket

import numpy as np
import pytest

from parameter_server import (
    Ack,
    Client,
    DependencyFailedError,
    InternalError,
    InvalidArgumentError,
    KeyRange,
    TaskId,
    TaskTimeoutError,
    UnavailableError,
    Values,
)


@pytest.mark.parametrize("width", [1, 3])
@pytest.mark.parametrize("initial", [0.0, 3.25])
async def test_public_arithmetic_sparse_range_and_snapshot(cluster, width, initial):
    server, _, connect = await cluster(width=width, initial_value=initial)
    client = await connect(7, sequence_start=20)
    assert client.server_info.width == width
    assert client.server_info.initial_value == initial
    initial_snapshot = await client.pull(KeyRange(0, 5)).wait(5000)
    assert isinstance(initial_snapshot, Values)
    assert initial_snapshot.keys == (0, 1, 2, 3, 4)
    np.testing.assert_array_equal(initial_snapshot.data, initial)
    assert not server.registry.storage._vectors
    updates = np.arange(1, 3 * width + 1, dtype=float)
    push = client.push([0, 2, 4], updates)
    assert push.task_id == TaskId(7, 21)
    acknowledgement = await push.wait(5000)
    assert isinstance(acknowledgement, Ack)
    assert acknowledgement.task_id == push.task_id
    assert push.done()
    result = await client.pull(KeyRange(0, 5), deps=[push]).wait(5000)
    expected = np.full((5, width), initial)
    expected[[0, 2, 4]] -= 0.1 * updates.reshape(3, width)
    assert result.width == width
    assert result.data.ndim == 1
    np.testing.assert_allclose(result.data.reshape(5, width), expected)
    np.testing.assert_array_equal(initial_snapshot.data, initial)
    with pytest.raises(ValueError):
        result.data[0] = 100


async def test_concurrent_workers_overlapping_keys_match_independent_totals(cluster):
    _, _, connect = await cluster(
        width=3, initial_value=0.75, rule_parameters={"learning_rate": 0.125}
    )
    clients = [await connect(worker) for worker in range(16)]
    random = np.random.default_rng(1782)
    expected = np.full((31, 3), 0.75)
    tasks = []
    for iteration in range(20):
        for worker, client in enumerate(clients):
            keys = tuple(sorted({worker, (worker + iteration) % 31, (3 * iteration) % 31}))
            updates = random.normal(size=(len(keys), 3))
            expected[list(keys)] -= 0.125 * updates
            tasks.append(client.push(keys, updates.reshape(-1)))
    await asyncio.wait_for(asyncio.gather(*(task.wait() for task in tasks)), 15)
    result = await clients[0].pull(KeyRange(0, 31)).wait(5000)
    np.testing.assert_allclose(result.data.reshape(31, 3), expected, rtol=1e-12, atol=1e-12)


@pytest.mark.parametrize("width", [1, 3])
async def test_nonempty_sparse_and_range_pushes_are_equivalent(cluster, width):
    _, _, connect = await cluster(width=width, initial_value=2.5)
    client = await connect()
    updates = np.arange(1, 3 * width + 1, dtype=float)
    sparse_first = client.push([0, 1, 2], updates)
    range_first = client.push(KeyRange(10, 13), updates)
    sparse_second = client.push([0, 1, 2], -2 * updates, deps=[sparse_first])
    range_second = client.push(KeyRange(10, 13), -2 * updates, deps=[range_first])
    sparse_result, range_result = await asyncio.gather(
        client.pull(KeyRange(0, 3), deps=[sparse_second]).wait(5000),
        client.pull([10, 11, 12], deps=[range_second]).wait(5000),
    )
    assert sparse_result.keys == (0, 1, 2)
    assert range_result.keys == (10, 11, 12)
    assert sparse_result.width == range_result.width == width
    np.testing.assert_array_equal(sparse_result.data, range_result.data)
    np.testing.assert_allclose(sparse_result.data, 2.5 + 0.1 * updates)


async def test_empty_sparse_and_range_operations(cluster):
    server, _, connect = await cluster(width=3)
    client = await connect()
    for target in ([], KeyRange(17, 17)):
        push = client.push(target, [])
        await push.wait(5000)
        result = await client.pull(target, deps=[push]).wait(5000)
        assert result.keys == ()
        assert result.width == 3
        assert result.data.shape == (0,)
    assert not server.registry.storage._vectors


async def test_uint64_max_sparse_and_exclusive_range_endpoint(cluster):
    _, _, connect = await cluster()
    client = await connect(2**32 - 1)
    maximum = 2**64 - 1
    await client.push([maximum], [10]).wait(5000)
    result = await client.pull(KeyRange(maximum, 2**64)).wait(5000)
    assert result.keys == (maximum,)
    np.testing.assert_allclose(result.data, [-1])
    await client.push(KeyRange(maximum, 2**64), [5]).wait(5000)
    np.testing.assert_allclose((await client.pull([maximum]).wait(5000)).data, [-1.5])


@pytest.mark.parametrize(
    "target",
    [
        [2, 1],
        [1, 1],
        [-1],
        [2**64],
        [True],
        [1.5],
        "123",
        None,
        KeyRange(3, 2),
        KeyRange(-1, 3),
        KeyRange(0, 2**64 + 1),
        KeyRange(True, 2),
        KeyRange(2**64, 2**64),
    ],
)
async def test_invalid_public_targets_do_not_change_storage_or_consume_ids(cluster, target):
    server, _, connect = await cluster()
    client = await connect()
    await client.push([1], [10]).wait(5000)
    sequence = client.next_sequence_number
    for operation in (lambda: client.pull(target), lambda: client.push(target, [100])):
        with pytest.raises(InvalidArgumentError):
            operation()
    assert client.next_sequence_number == sequence
    np.testing.assert_array_equal(server.registry.storage.pull((1,)).data, [-1])
    assert set(server.registry.storage._vectors) == {1}


@pytest.mark.parametrize("values", [[], [1], [1, 2, 3], [np.nan, 1], [np.inf, 1]])
async def test_invalid_push_is_registered_failed_and_preserves_storage(cluster, values):
    server, _, connect = await cluster(initial_value=4)
    client = await connect()
    await client.push([1], [10]).wait(5000)
    task = client.push([1, 2], values)
    with pytest.raises(InvalidArgumentError):
        await task.wait(5000)
    assert task.done()
    with pytest.raises(InvalidArgumentError):
        await task.wait(5000)
    result = await client.pull([1, 2]).wait(5000)
    np.testing.assert_array_equal(result.data, [3, 4])
    assert set(server.registry.storage._vectors) == {1}


@pytest.mark.parametrize("values", [[[1]], ["not-a-number"], None])
async def test_nonflat_or_nonnumeric_values_rejected_locally(cluster, values):
    _, _, connect = await cluster()
    client = await connect()
    with pytest.raises(InvalidArgumentError):
        client.push([1], values)


async def test_immediately_scheduled_dependency_chain_and_failure_propagation(cluster):
    _, _, connect = await cluster()
    client = await connect()
    first = client.push([1], [10])
    second = client.push([1], [20], deps=[first])
    read = client.pull([1], deps=[first, second])
    np.testing.assert_allclose((await read.wait(5000)).data, [-3])
    failed = client.push([1], [])
    child = client.push([1], [100], deps=[failed])
    grandchild = client.pull([1], deps=[child])
    with pytest.raises(InvalidArgumentError):
        await failed.wait(5000)
    for dependent in (child, grandchild):
        with pytest.raises(DependencyFailedError):
            await dependent.wait(5000)
        assert dependent.done()
    np.testing.assert_allclose((await client.pull([1]).wait(5000)).data, [-3])


async def test_cross_client_dependencies_rejected_even_with_same_worker(cluster):
    _, _, connect = await cluster()
    first = await connect(1)
    same_worker = await connect(1, sequence_start=100)
    other_worker = await connect(2)
    task = first.push([1], [1])
    for client in (same_worker, other_worker):
        with pytest.raises(InvalidArgumentError):
            client.pull([1], deps=[task])
        with pytest.raises(InvalidArgumentError):
            client.push([1], [2], deps=[task])
    with pytest.raises(InvalidArgumentError):
        first.pull([1], deps=[task.task_id])
    await task.wait(5000)


async def test_wait_timeout_and_waiter_cancellation_do_not_cancel_task(cluster, gate):
    _, _, connect = await cluster(update_rule=gate.name, rule_parameters={})
    client = await connect()
    task = client.push([1], [7])
    try:
        await gate.wait_entered()
        assert not task.done()
        for _ in range(2):
            with pytest.raises(TaskTimeoutError):
                await task.wait(10)
        waiter_started = asyncio.Event()

        async def wait_for_task():
            waiter_started.set()
            return await task.wait()

        waiting = asyncio.create_task(wait_for_task())
        await asyncio.wait_for(waiter_started.wait(), 5)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert not task.done()
    finally:
        gate.release.set()
    first = await task.wait(5000)
    assert await task.wait(0) == first
    assert task.done()
    np.testing.assert_array_equal((await client.pull([1]).wait(5000)).data, [7])
    assert gate.calls == 1


async def test_scheduling_copies_keys_values_and_dependency_list(cluster, gate):
    _, _, connect = await cluster(update_rule=gate.name, rule_parameters={})
    client = await connect()
    blocker = client.push([0], [1])
    try:
        await gate.wait_entered()
        keys = [2, 4]
        values = np.array([10.0, 20.0])
        dependencies = [blocker]
        task = client.push(keys, values, deps=dependencies)
        read_keys = [2, 4]
        read = client.pull(read_keys, deps=[task])
        keys[:] = [9, 10]
        read_keys[:] = [99]
        values[:] = 999
        dependencies.clear()
    finally:
        gate.release.set()
    result = await read.wait(5000)
    assert result.keys == (2, 4)
    np.testing.assert_array_equal(result.data, [10, 20])
    np.testing.assert_array_equal((await client.pull([9, 10, 99]).wait(5000)).data, 0)


@pytest.mark.parametrize("timeout_value", [-1, True, float("nan"), float("inf"), "10"])
async def test_invalid_wait_timeout(cluster, timeout_value):
    _, _, connect = await cluster()
    client = await connect()
    task = client.pull([])
    with pytest.raises(InvalidArgumentError):
        await task.wait(timeout_value)
    await task.wait(5000)


@pytest.mark.parametrize(
    "options",
    [
        {"worker_id": -1},
        {"worker_id": 2**32},
        {"worker_id": True},
        {"sequence_start": -1},
        {"sequence_start": 2**64},
        {"connect_timeout_ms": 0},
        {"connect_timeout_ms": float("nan")},
    ],
)
async def test_invalid_client_identifiers_and_options(options):
    arguments = {"worker_id": 1, **options}
    with pytest.raises(InvalidArgumentError):
        await Client.connect("127.0.0.1:1", **arguments)


async def test_sequence_limit_and_iteration_validation(cluster):
    _, _, connect = await cluster()
    client = await connect(sequence_start=2**64 - 1)
    for iteration in (-1, 2**64, True, 1.5):
        with pytest.raises(InvalidArgumentError):
            client.pull([], iteration=iteration)
    task = client.pull([])
    assert task.task_id.sequence_number == 2**64 - 1
    await task.wait(5000)
    with pytest.raises(InvalidArgumentError):
        client.pull([])


async def test_large_payload_exceeds_default_grpc_four_megabytes(cluster):
    _, _, connect = await cluster(width=8)
    client = await connect()
    keys = np.arange(100_000, dtype=np.uint64)
    updates = np.arange(800_000, dtype=np.float64) / 1000
    assert updates.nbytes > 4 * 1024 * 1024
    task = client.push(keys, updates)
    result = await client.pull(KeyRange(0, len(keys)), deps=[task]).wait(15000)
    assert result.width == 8
    assert result.keys == tuple(range(100_000))
    np.testing.assert_allclose(result.data, -0.1 * updates)


async def test_unavailable_endpoint_and_idempotent_clean_shutdown(cluster):
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        address = f"127.0.0.1:{reserved.getsockname()[1]}"
        with pytest.raises(UnavailableError):
            await Client.connect(address, 1, connect_timeout_ms=100)
    server, _, connect = await cluster()
    client = await connect()
    await client.pull([]).wait(5000)
    await asyncio.wait_for(server.close(), 5)
    await asyncio.wait_for(server.close(), 5)
    with pytest.raises(UnavailableError):
        await client.pull([]).wait(5000)
    await asyncio.wait_for(client.close(), 5)
    await asyncio.wait_for(client.close(), 5)
    with pytest.raises(UnavailableError):
        client.pull([])


async def test_rule_exception_maps_to_internal_and_dependency_failure(cluster):
    from uuid import uuid4

    from parameter_server.rules import register_update_rule

    def broken(weights, updates, parameters):
        weights[:] = 999
        raise RuntimeError("deliberate failure")

    name = f"broken_{uuid4().hex}"
    register_update_rule(name, broken)
    server, _, connect = await cluster(update_rule=name, rule_parameters={}, initial_value=6)
    client = await connect()
    failed = client.push([1, 2], [3, 4])
    dependent = client.pull([1, 2], deps=[failed])
    with pytest.raises(InternalError):
        await failed.wait(5000)
    with pytest.raises(DependencyFailedError):
        await dependent.wait(5000)
    np.testing.assert_array_equal((await client.pull([1, 2]).wait(5000)).data, [6, 6])
    assert not server.registry.storage._vectors


async def test_add_rule_without_parameters_uses_default_scale(cluster):
    _, _, connect = await cluster(update_rule="add", width=3, initial_value=2)
    client = await connect()
    first = client.push([1], [1, 2, 3])
    second = client.push([1], [4, 5, 6], deps=[first])
    result = await client.pull([1], deps=[second]).wait(5000)
    np.testing.assert_array_equal(result.data, [7, 9, 11])


async def test_closing_client_with_inflight_task_does_not_cancel_server_work(
    cluster, gate, monkeypatch
):
    _, stub, connect = await cluster(update_rule=gate.name, rule_parameters={})
    client = await connect()
    acknowledged = asyncio.Event()
    channel_closed = asyncio.Event()
    original_submit = client._stub.SubmitPush
    original_close = client._channel.close

    async def submit_before_close(request, **options):
        acknowledgement = await original_submit(request, **options)
        acknowledged.set()
        await asyncio.wait_for(channel_closed.wait(), 5)
        return acknowledgement

    async def close_channel():
        await original_close()
        channel_closed.set()

    monkeypatch.setattr(client._stub, "SubmitPush", submit_before_close)
    monkeypatch.setattr(client._channel, "close", close_channel)
    task = client.push([1], [6])
    try:
        await gate.wait_entered()
        await asyncio.wait_for(acknowledged.wait(), 5)
        await asyncio.wait_for(client.close(), 5)
        assert task.done()
        with pytest.raises(UnavailableError):
            await task.wait(5000)
        with pytest.raises(UnavailableError):
            client.push([1], [100])
    finally:
        gate.release.set()
    from parameter_server.proto import parameter_server_pb2 as pb

    identifier = pb.TaskId(
        worker_id=task.task_id.worker_id, sequence_number=task.task_id.sequence_number
    )
    assert (await stub.WaitTask(identifier, timeout=5)).state == pb.SUCCEEDED
    observer = await connect(2)
    np.testing.assert_array_equal((await observer.pull([1]).wait(5000)).data, [6])


async def test_closing_client_with_dependency_registration_pending_is_bounded(cluster, gate):
    _, _, connect = await cluster(update_rule=gate.name, rule_parameters={})
    client = await connect()
    first = client.push([1], [1])
    dependent = client.pull([1], deps=[first])
    try:
        await asyncio.wait_for(client.close(), 5)
        for task in (first, dependent):
            with pytest.raises((UnavailableError, DependencyFailedError)):
                await task.wait(5000)
            assert task.done()
    finally:
        gate.release.set()
