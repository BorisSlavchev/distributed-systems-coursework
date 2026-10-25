from collections.abc import Sequence

from parameter_server.config import ServerConfig
from parameter_server.errors import InvalidArgumentError
from parameter_server.proto import parameter_server_pb2 as pb
from parameter_server.types import KEYSPACE_END, UINT64_MAX, KeyRange, unsigned


def encode_target(target: Sequence[int] | KeyRange) -> pb.Target:
    if isinstance(target, KeyRange):
        start = unsigned(target.start, UINT64_MAX, "range start")
        end = unsigned(target.end, KEYSPACE_END, "range end")
        if start > end:
            raise InvalidArgumentError("range start must not exceed end")
        return pb.Target(
            range=pb.KeyRange(
                start=start,
                end=0 if end == KEYSPACE_END else end,
                end_at_keyspace_limit=end == KEYSPACE_END,
            )
        )
    if isinstance(target, (str, bytes)):
        raise InvalidArgumentError("target must be a key sequence or KeyRange")
    try:
        keys = tuple(unsigned(key, UINT64_MAX, "key") for key in target)
    except TypeError as error:
        raise InvalidArgumentError("target must be a key sequence or KeyRange") from error
    if any(left >= right for left, right in zip(keys, keys[1:], strict=False)):
        raise InvalidArgumentError("sparse keys must be sorted and unique")
    return pb.Target(sparse=pb.KeyList(keys=keys))


def decode_target(target: pb.Target, config: ServerConfig) -> tuple[int, ...]:
    selection = target.WhichOneof("selection")
    if selection == "sparse":
        keys = target.sparse.keys
        count = len(keys)
        if any(left >= right for left, right in zip(keys, keys[1:], strict=False)):
            raise InvalidArgumentError("sparse keys must be sorted and unique")
    elif selection == "range":
        selected = target.range
        if selected.end_at_keyspace_limit and selected.end != 0:
            raise InvalidArgumentError("keyspace-limit ranges require end=0 on the wire")
        end = KEYSPACE_END if selected.end_at_keyspace_limit else selected.end
        if selected.start > end:
            raise InvalidArgumentError("range start must not exceed end")
        count = end - selected.start
        keys = range(selected.start, end)
    else:
        raise InvalidArgumentError("target selection is required")
    if count > config.max_keys_per_request:
        raise InvalidArgumentError("target exceeds max_keys_per_request")
    if count * (10 + 8 * config.width) + 512 > config.max_message_bytes:
        raise InvalidArgumentError("target snapshot exceeds max_message_bytes budget")
    return tuple(keys)
