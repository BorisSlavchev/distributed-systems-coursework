from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import numpy as np
import pytest

from parameter_server import InternalError, InvalidArgumentError
from parameter_server.config import ServerConfig
from parameter_server.rules import register_update_rule
from parameter_server.storage import Storage


@pytest.mark.parametrize("width", [1, 3])
@pytest.mark.parametrize("initial", [0.0, 2.5])
def test_sgd_arithmetic_flat_shape_and_independent_snapshots(width, initial):
    storage = Storage(ServerConfig(width=width, initial_value=initial))
    keys = (2, 8, 99)
    updates = np.arange(1, len(keys) * width + 1, dtype=float)
    original = storage.pull(keys)
    assert original.keys == keys
    assert original.width == width
    assert original.data.shape == (len(keys) * width,)
    np.testing.assert_array_equal(original.data, initial)
    assert not storage._vectors
    storage.push(keys, updates)
    storage.push(keys, updates * 2)
    expected = initial - 0.3 * updates
    np.testing.assert_allclose(storage.pull(keys).data, expected)
    np.testing.assert_array_equal(original.data, initial)
    snapshot = storage.pull(keys)
    snapshot.data[:] = -999
    np.testing.assert_allclose(storage.pull(keys).data, expected)


@pytest.mark.parametrize("width", [1, 3])
def test_empty_operations_and_unseen_pulls_do_not_allocate(width):
    storage = Storage(ServerConfig(width=width, initial_value=7))
    storage.push((), [])
    empty = storage.pull(())
    assert empty.keys == ()
    assert empty.width == width
    assert empty.data.shape == (0,)
    np.testing.assert_array_equal(storage.pull((0, 10, 2**64 - 1)).data, 7)
    assert storage._vectors == {}


@pytest.mark.parametrize(
    "values", [[], [1], [1, 2, 3], [[1, 2]], [np.nan, 1], [np.inf, 1], [-np.inf, 1], ["bad", 1]]
)
def test_invalid_values_leave_existing_and_unseen_keys_unchanged(values):
    storage = Storage(ServerConfig(initial_value=5))
    storage.push((1,), [10])
    before = storage.pull((1, 2)).data.copy()
    with pytest.raises(InvalidArgumentError):
        storage.push((1, 2), values)
    np.testing.assert_array_equal(storage.pull((1, 2)).data, before)
    assert set(storage._vectors) == {1}


@pytest.mark.parametrize("failure", ["raise", "shape", "nan", "infinity"])
def test_custom_rule_failure_is_atomic_even_if_inputs_are_mutated(failure):
    def rule(weights, updates, parameters):
        if parameters["mode"] == 0:
            return weights + updates
        weights[:] = -123
        updates[:] = 999
        parameters["mode"] = 0
        if failure == "raise":
            raise RuntimeError("deliberate failure after mutation")
        if failure == "shape":
            return np.zeros((1, 1))
        return np.full_like(weights, np.nan if failure == "nan" else np.inf)

    name = f"rollback_{uuid4().hex}"
    register_update_rule(name, rule)
    storage = Storage(
        ServerConfig(width=3, initial_value=2, update_rule=name, rule_parameters={"mode": 0})
    )
    storage.push((1,), [1, 2, 3])
    storage._parameters["mode"] = 1
    updates = np.arange(6, dtype=float)
    original_updates = updates.copy()
    before = storage.pull((1, 9)).data.copy()
    for _ in range(2):
        with pytest.raises(InternalError):
            storage.push((1, 9), updates)
        np.testing.assert_array_equal(storage.pull((1, 9)).data, before)
        np.testing.assert_array_equal(updates, original_updates)
        assert set(storage._vectors) == {1}
        assert storage._parameters == {"mode": 1}


def test_concurrent_storage_pushes_match_independent_totals():
    storage = Storage(ServerConfig(width=3, update_rule="add", rule_parameters={"scale": 0.25}))
    requests = []
    expected = np.zeros((17, 3))
    for worker in range(12):
        for iteration in range(15):
            keys = tuple(sorted({worker % 17, (worker + iteration) % 17, (iteration * 3) % 17}))
            values = np.full((len(keys), 3), (worker + 1) / (iteration + 1))
            expected[list(keys)] += 0.25 * values
            requests.append((keys, values.reshape(-1)))
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(storage.push, keys, values) for keys, values in requests]
        for future in futures:
            future.result(timeout=10)
    np.testing.assert_allclose(
        storage.pull(tuple(range(17))).data.reshape(17, 3), expected, rtol=1e-12, atol=1e-12
    )


@pytest.mark.parametrize(
    "configuration",
    [
        {"width": 0},
        {"width": -1},
        {"width": True},
        {"width": 1.5},
        {"width": 2**32},
        {"initial_value": np.nan},
        {"initial_value": np.inf},
        {"initial_value": True},
        {"address": ""},
        {"max_keys_per_request": 0},
        {"max_keys_per_request": -1},
        {"max_message_bytes": 1023},
        {"max_message_bytes": 2**31},
        {"rule_parameters": []},
        {"rule_parameters": {"learning_rate": np.inf}},
        {"rule_parameters": {"learning_rate": True}},
        {"update_rule": ""},
    ],
)
def test_invalid_configuration_rejected(configuration):
    with pytest.raises(InvalidArgumentError):
        ServerConfig(**configuration)


@pytest.mark.parametrize(
    "configuration",
    [
        {"update_rule": "unknown_rule"},
        {"rule_parameters": {"learning_rate": -1}},
        {"rule_parameters": {"not_a_parameter": 1}},
        {"update_rule": "add", "rule_parameters": {"learning_rate": 1}},
    ],
)
def test_invalid_rule_configuration_rejected(configuration):
    with pytest.raises(InvalidArgumentError):
        Storage(ServerConfig(**configuration))


def test_json_configuration_loading_and_validation(tmp_path):
    path = tmp_path / "server.json"
    path.write_text('{"width":3,"initial_value":4,"rule_parameters":{"learning_rate":0.5}}')
    config = ServerConfig.from_json(path)
    assert config.width == 3
    assert config.initial_value == 4
    assert config.rule_parameters == {"learning_rate": 0.5}
    for contents in ("[]", '{"unknown":1}'):
        path.write_text(contents)
        with pytest.raises(InvalidArgumentError):
            ServerConfig.from_json(path)
