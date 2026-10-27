import copy
import threading

import numpy as np

from parameter_server.config import ServerConfig
from parameter_server.errors import InternalError, InvalidArgumentError
from parameter_server.rules import get_update_rule
from parameter_server.types import Values


class Storage:
    def __init__(self, config: ServerConfig):
        self.config = config
        self._rule = get_update_rule(config.update_rule)
        self._parameters = copy.deepcopy(config.rule_parameters)
        self._vectors: dict[int, np.ndarray] = {}
        self._lock = threading.Lock()
        if config.update_rule in {"sgd", "add"}:
            self._rule(np.zeros((0, config.width)), np.zeros((0, config.width)), self._parameters)

    def _snapshot(self, keys: tuple[int, ...]) -> np.ndarray:
        snapshot = np.full(
            (len(keys), self.config.width), self.config.initial_value, dtype=np.float64
        )
        for index, key in enumerate(keys):
            if key in self._vectors:
                snapshot[index] = self._vectors[key]
        return snapshot

    def pull(self, keys: tuple[int, ...]) -> Values:
        with self._lock:
            snapshot = self._snapshot(keys)
        return Values(keys, snapshot.reshape(-1), self.config.width)

    def push(self, keys: tuple[int, ...], values) -> None:
        try:
            updates = np.array(values, dtype=np.float64, copy=True)
        except (ValueError, TypeError, OverflowError) as error:
            raise InvalidArgumentError("updates must be float64 numbers") from error
        if updates.ndim != 1 or updates.size != len(keys) * self.config.width:
            raise InvalidArgumentError("values must be flat with len(keys) * width entries")
        if not np.isfinite(updates).all():
            raise InvalidArgumentError("updates must be finite")
        updates = updates.reshape(len(keys), self.config.width)
        with self._lock:
            weights = self._snapshot(keys)
            try:
                with np.errstate(all="raise"):
                    result = np.array(
                        self._rule(weights, updates, copy.deepcopy(self._parameters)),
                        dtype=np.float64,
                        copy=True,
                    )
                if result.shape != (len(keys), self.config.width) or not np.isfinite(result).all():
                    raise ValueError("rule returned incorrect shape or nonfinite weights")
            except Exception as error:
                raise InternalError("update rule failed; no weights changed") from error
            staged = {key: result[index].copy() for index, key in enumerate(keys)}
            self._vectors.update(staged)
