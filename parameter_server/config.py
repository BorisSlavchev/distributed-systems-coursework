import json
import math
from dataclasses import dataclass, field
from pathlib import Path

from parameter_server.errors import InvalidArgumentError
from parameter_server.types import UINT32_MAX, UINT64_MAX, unsigned

DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class ServerConfig:
    address: str = "127.0.0.1:50051"
    width: int = 1
    initial_value: float = 0.0
    update_rule: str = "sgd"
    rule_parameters: dict[str, float] = field(default_factory=dict)
    max_keys_per_request: int = 1_000_000
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES

    def __post_init__(self):
        if not isinstance(self.address, str) or not self.address:
            raise InvalidArgumentError("address must be a nonempty string")
        if unsigned(self.width, UINT32_MAX, "width") < 1:
            raise InvalidArgumentError("width must be at least 1")
        if (
            isinstance(self.initial_value, bool)
            or not isinstance(self.initial_value, (float, int))
            or not math.isfinite(self.initial_value)
        ):
            raise InvalidArgumentError("initial_value must be finite")
        unsigned(self.max_keys_per_request, UINT64_MAX, "max_keys_per_request")
        unsigned(self.max_message_bytes, 2**31 - 1, "max_message_bytes")
        if self.max_keys_per_request < 1 or self.max_message_bytes < 1024:
            raise InvalidArgumentError(
                "request limits must be positive, message limit at least 1024"
            )
        if not isinstance(self.update_rule, str) or not self.update_rule:
            raise InvalidArgumentError("update_rule must be a nonempty string")
        if not isinstance(self.rule_parameters, dict):
            raise InvalidArgumentError("rule_parameters must be an object")
        for name, value in self.rule_parameters.items():
            if (
                not isinstance(name, str)
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise InvalidArgumentError("rule parameters must be named finite numbers")

    @classmethod
    def from_json(cls, path: str | Path) -> ServerConfig:
        with Path(path).open() as config_file:
            data = json.load(config_file)
        if not isinstance(data, dict):
            raise InvalidArgumentError("configuration must be a JSON object")
        try:
            return cls(**data)
        except TypeError as error:
            raise InvalidArgumentError(str(error)) from error


def grpc_options(max_message_bytes: int) -> list[tuple[str, int]]:
    return [
        ("grpc.max_send_message_length", max_message_bytes),
        ("grpc.max_receive_message_length", max_message_bytes),
    ]
