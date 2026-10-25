# Single-node parameter server

Part A of the Distributed Systems coursework: multiple asyncio workers communicate with one real Python server process over gRPC. Numeric updates use NumPy `float64`; the default rule is `weights -= learning_rate * gradients`. Every accepted push applies individually, exactly once per task ID during this server process lifetime. There is no worker barrier or iteration-based consistency policy.

## Local setup

Use **Python 3.14.4** for development and Docker. Dependencies, including transitive development dependencies, are pinned in `requirements*.txt`; `.python-version` records the interpreter version. On Debian/Ubuntu install the matching `python3.14-venv` system package if `venv` reports that `ensurepip` is unavailable.

```sh
python3.14 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pip install --no-build-isolation --no-deps -e .
python -m parameter_server --config config/server.json
```

The last command starts the actual server. In another terminal, activate the same environment and run:

```sh
python -m examples.demo
```

The deterministic demo uses two worker clients, checks initial values of 10, concurrently pushes gradients of 2 and 3, and verifies all final vector components equal 9.5. Use the supplied configuration and a **fresh server** for each demo run. Stop with Ctrl-C or SIGTERM. Alternatively the installed `parameter-server` command starts the same entry point. Without `--config`, defaults are localhost port 50051, width 1, zero initialization, SGD with learning rate 0.1.

## Client usage

```python
import asyncio
from parameter_server import Client, KeyRange


async def train():
    async with await Client.connect("127.0.0.1:50051", worker_id=10) as client:
        pull = client.pull(KeyRange(2, 5), iteration=0)
        weights = await pull.wait()
        push = client.push(weights.keys, [1.0] * len(weights.data), deps=[pull])
        print((await client.pull(weights.keys, deps=[push]).wait()).data)


asyncio.run(train())
```

The runnable equivalent is `python -m examples.quickstart`. Future harnesses should use only `Client`, `Task`, `KeyRange`, `Values`, and the public errors, not storage or scheduler internals.

`push(target, values, deps=(), iteration=0)` and `pull(target, deps=(), iteration=0)` are ordinary methods. They copy the target and update array and return a `Task` immediately, scheduling submission on the current event loop without awaiting network acknowledgement. Input encoding/copying is synchronous and proportional to request size. Malformed local arguments may raise `InvalidArgumentError` immediately; server validation errors arrive through `wait`.

- `await task.wait(timeout_ms=None)` suspends until a terminal result. Pull returns `Values(keys: tuple[int, ...], data: flat float64 ndarray, width: int)`; push returns `Ack(task_id: TaskId)`. Data are key-major. Returned arrays are local copies, initially marked read-only, never server storage references.
- `task.done()` reads only local state and performs no network I/O. It can lag server completion until the result is received.
- Repeated waits return the same result object or terminal exception. A wait timeout raises `TaskTimeoutError` without cancelling submission or execution; waiting again is legal. Cancelling the waiting coroutine likewise leaves the operation running.
- `await client.close()` is idempotent. It rejects new work, closes the channel and finishes local background tasks; unfinished handles fail with `UnavailableError`. It does not wait for accepted server work to finish or roll it back. A task may have executed even if the client did not receive its result. Wait for desired results before closing. Completed handles remain usable.
- The client creates sequence numbers starting at `sequence_start=0`, exposed through `next_sequence_number`. Worker IDs must identify a single logical client for the entire server lifetime. A reconnect must use a new worker ID or a persisted sequence start strictly above **all previously issued IDs**, including requests whose outcome was uncertain. There is no worker-ID lease service or automatic restart-safe allocator.
- `connect_timeout_ms` and `submission_timeout_ms` default to 5000. These RPC deadlines are distinct from a handle's local wait timeout. Transport/submission errors are terminal for that handle; no automatic retries hide ambiguous outcomes. Advanced users can retry the identical wire request with its original ID, or inspect it through `WaitTask`/`GetTaskStatus`.

## Targets and validation

Keys span the entire unsigned 64-bit space, `0` through `2**64 - 1`. Worker IDs are unsigned 32-bit; sequence numbers and `iteration` are unsigned 64-bit. Zero IDs are valid. Booleans are not accepted as integer IDs in the Python API. Iteration is metadata only.

`[2, 7, 19]` selects explicit sparse keys; lists must be strictly ascending and unique. `KeyRange(2, 5)` selects 2, 3, 4. Range upper bounds are **exclusive**. Python range ends may equal `2**64`, so `KeyRange(2**64 - 1, 2**64)` selects the largest key; a sparse list `[2**64 - 1]` also works. Starts must be actual uint64 keys. The protocol represents the otherwise unencodable upper bound with `end_at_keyspace_limit=true, end=0`; any other end with that flag is invalid.

Empty sparse lists and ranges with `start == end` are valid, including pushes with no values. Pulls always return ascending keys. Updates must be flat and contain exactly `number_of_keys * width` finite float64 numbers. Unsorted/duplicate keys, missing targets, reversed ranges, invalid IDs, wrong shapes, and NaN/infinity are rejected. Unstored keys read as the configured initial scalar broadcast to the vector, without allocation. The logical space exceeds one million keys without preallocating the space.

The default per-request limit is 1,000,000 keys. Both client and server configure **64 MiB** send and receive message limits. The server additionally rejects targets whose conservative snapshot budget, `keys * (10 + 8 * width) + 512`, exceeds that limit, before expanding ranges or allocating snapshots. Large widths therefore reduce the allowed key count. Oversized wire messages are rejected by gRPC itself. Match `Client.connect(max_message_bytes=...)` to any nondefault server setting. This is an in-memory unary-RPC baseline, not a streaming bulk-transfer implementation.

## Atomicity and task execution

The service acknowledges **registration**, not successful execution. The server task registry runs on one asyncio event loop and follows `PENDING → RUNNING → SUCCEEDED/FAILED`. Dependencies must already be registered; unknown, self/forward, or cross-worker dependencies are rejected with `INVALID_ARGUMENT`. The public API further restricts dependencies to earlier handles from the exact same client instance, including when multiple handles are issued immediately.

The client waits only for prerequisite **registration** before submitting a dependent, not prerequisite completion. The server waits for dependency success and emits `DEPENDENCY_FAILED` without executing the dependent if a prerequisite fails. Waiting uses independent futures, so it does not block unrelated tasks or hold the storage lock. A local timeout never marks the prerequisite failed. Because dependencies only point to registered tasks, cycles cannot be introduced.

Storage has one `threading.Lock`. Workers execute synchronous storage operations through `asyncio.to_thread`, keeping the event loop available. A push validates updates, then acquires this lock, copies the affected weights, computes the rule on private arrays, validates the complete result, stages every changed vector, and commits under the same lock. A pull takes its complete snapshot under this lock. Concurrent overlapping pushes cannot overwrite one another, and pulls cannot observe a partial multi-key update. No network or dependency waits occur under the lock. Execution order among independent ready tasks is unspecified; there is no global consistency mode.

On orderly server shutdown, registration stops, accepted tasks drain, then gRPC stops. Rule functions must terminate; an unbounded custom function can delay shutdown indefinitely. Container shutdown permits 30 seconds before forced termination. Abrupt termination loses all state.

## Update rules and configuration

`config/server.json` configures `address`, `width >= 1`, finite `initial_value`, `update_rule`, numeric `rule_parameters`, `max_keys_per_request`, and `max_message_bytes`. Unknown configuration fields are rejected. Defaults and validation live in `parameter_server/config.py`.

Two rules are registered at startup:

- `sgd`: `weights - learning_rate * updates`, default learning rate 0.1, nonnegative.
- `add`: `weights + scale * updates`, default scale 1.0. Select `"update_rule": "add"` and optionally `"rule_parameters": {"scale": 1.0}`. Omitted rule parameters default to an empty object, allowing each rule to supply its own defaults.

Register a custom function in a trusted Python startup module before `start_server`:

```python
from parameter_server.rules import register_update_rule


def scaled_add(weights, updates, parameters):
    return weights + parameters["factor"] * updates


register_update_rule("scaled_add", scaled_add)
```

Signature: `(weights: float64 ndarray[n, width], updates: float64 ndarray[n, width], parameters: Mapping[str, float]) -> ndarray[n, width]`. Parameters are JSON finite numbers. The function must return finite values with the exact input shape, terminate, and avoid external side effects. It may mutate its private argument arrays; exceptions, overflow, invalid shapes, and nonfinite outputs leave all stored weights unchanged and fail the task with `INTERNAL`. Parameters are copied per call. The selected function is captured when storage starts; changing rules during a workload is unsupported. Never send Python functions or executable code through gRPC.

## Idempotence and errors

Each `(worker_id, sequence_number)` has its own record, regardless of arrival order. SHA-256 of the operation and deterministically serialized Protobuf request identifies its contents. Identical retries reuse the same pending/running/completed task and immutable server result; conflicting reuse, including a push versus a pull, fails with `INVALID_ARGUMENT`. All request fields, including iteration and dependency ordering, participate in identity. Deduplication and results last only for this process lifetime; nothing is persisted.

| Python exception | Application code | gRPC mapping |
| --- | --- | --- |
| `InvalidArgumentError` | `INVALID_ARGUMENT` | Submission conflicts/unsupported dependencies use `INVALID_ARGUMENT`; execution validation uses a failed `TaskResult`; wire message overflow `RESOURCE_EXHAUSTED` maps to this error |
| `UnavailableError` | `UNAVAILABLE` | `UNAVAILABLE`, transport cancellation, or local client closure |
| `TaskTimeoutError` | `TIMEOUT` | Local wait expiry or RPC `DEADLINE_EXCEEDED` |
| `DependencyFailedError` | `DEPENDENCY_FAILED` | Failed `TaskResult`, normally delivered with gRPC `OK` |
| `InternalError` | `INTERNAL` | Rule/execution failure in `TaskResult`, or unexpected gRPC failure |

All inherit `ParameterServerError` and expose `.code`. `WaitTask` on an unknown ID returns gRPC `INVALID_ARGUMENT`. `GetTaskStatus` reports states and terminal errors. An invalid execution request with a valid registered ID remains a failed task, so dependents can observe failure. Protobuf integer fields reject out-of-range values before transmission, and the Python API reports invalid integers as `InvalidArgumentError`. Waiting-RPC cancellation/deadline never cancels the server runner.

## Protocol, tests, and lint

Generated Python files are included. Regenerate reproducibly with the pinned toolchain:

```sh
python scripts/generate_proto.py
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m pip check
```

The generation script invokes `python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parameter_server/proto/parameter_server.proto` from the repository root, then removes generated comment tokens to preserve this repository's no-comments requirement. Generated files are excluded from Ruff. Tests cover storage arithmetic/rollback, real localhost gRPC, subprocess server operation, dependencies/cancellation, deduplication, concurrency against independently calculated totals, and requests larger than gRPC's usual 4 MiB default.

## Docker

No host Python setup is needed for this path:

```sh
docker compose up --build -d --wait server
docker compose --profile demo run --rm demo
docker compose --profile test run --build --rm test
docker compose down
```

The server runs as an unprivileged user and exposes localhost port 50051. Compose health checks use a real gRPC connection. The demo connects to the one server container rather than starting per-worker servers. Restart the server to reset weights/task history. `docker compose --profile test run --rm test python -m ruff check .` also runs lint inside the test image. The JSON configuration is mounted read-only. Both Docker stages use the same pinned Python 3.14.4 base.

## Module boundaries and limitations

- `types.py`, `errors.py`: public data and errors; `proto/`: typed messages, service, generated code.
- `storage.py`, `rules.py`: in-memory vectors and transactional update rules.
- `tasks.py`: task registry, dependencies, idempotence; `server.py`: gRPC transport/lifecycle.
- `client.py`: public handles, submission ordering, error mapping; `config.py`, `__main__.py`: configuration/startup.
- `examples/`, `tests/`: small public-API demonstrations and verification, not a full experiment harness.

Task records, request digests, terminal outcomes, and pull snapshots are retained for the entire run. Memory grows with touched keys, number of tasks, and accumulated pull-result sizes; there is no eviction, admission queue bound, or persistence. Slow rules serialize storage; arbitrary custom code is trusted. Traffic is plaintext and unauthenticated, intended for localhost/coursework Docker networks only. No restart-safe exactly-once guarantee is claimed.

Not implemented: multiple parameter servers, manager, partitioning, vector clocks, consistency modes, replication, elasticity, cluster-management daemon, or full experiment harness.
