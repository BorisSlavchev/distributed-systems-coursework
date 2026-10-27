from collections.abc import Callable, Mapping

import numpy as np
from numpy.typing import NDArray

from parameter_server.errors import InvalidArgumentError

UpdateRule = Callable[
    [NDArray[np.float64], NDArray[np.float64], Mapping[str, float]], NDArray[np.float64]
]
_RULES: dict[str, UpdateRule] = {}


def register_update_rule(name: str, function: UpdateRule) -> None:
    if not isinstance(name, str) or not name or not callable(function):
        raise InvalidArgumentError("a rule needs a nonempty name and callable")
    if name in _RULES:
        raise InvalidArgumentError(f"update rule already registered: {name}")
    _RULES[name] = function


def get_update_rule(name: str) -> UpdateRule:
    try:
        return _RULES[name]
    except KeyError as error:
        raise InvalidArgumentError(f"unknown update rule: {name}") from error


def sgd(weights, updates, parameters):
    if set(parameters) - {"learning_rate"}:
        raise InvalidArgumentError("sgd accepts only learning_rate")
    learning_rate = parameters.get("learning_rate", 0.1)
    if learning_rate < 0:
        raise InvalidArgumentError("learning_rate must be nonnegative")
    return weights - learning_rate * updates


def add(weights, updates, parameters):
    if set(parameters) - {"scale"}:
        raise InvalidArgumentError("add accepts only scale")
    return weights + parameters.get("scale", 1.0) * updates


register_update_rule("sgd", sgd)
register_update_rule("add", add)
