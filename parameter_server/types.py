from dataclasses import dataclass
from numbers import Integral

import numpy as np
from numpy.typing import NDArray

from parameter_server.errors import InvalidArgumentError

UINT32_MAX = 2**32 - 1
UINT64_MAX = 2**64 - 1
KEYSPACE_END = 2**64


def unsigned(value: int, maximum: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or not 0 <= value <= maximum:
        raise InvalidArgumentError(f"{name} must be an integer in [0, {maximum}]")
    return int(value)


@dataclass(frozen=True)
class KeyRange:
    start: int
    end: int


@dataclass(frozen=True)
class TaskId:
    worker_id: int
    sequence_number: int


@dataclass(frozen=True)
class Ack:
    task_id: TaskId


@dataclass(frozen=True)
class Values:
    keys: tuple[int, ...]
    data: NDArray[np.float64]
    width: int
