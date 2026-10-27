from enum import IntEnum

import grpc


class ErrorCode(IntEnum):
    INVALID_ARGUMENT = 1
    UNAVAILABLE = 2
    TIMEOUT = 3
    DEPENDENCY_FAILED = 4
    INTERNAL = 5


class ParameterServerError(Exception):
    code: ErrorCode


class InvalidArgumentError(ParameterServerError):
    code = ErrorCode.INVALID_ARGUMENT


class UnavailableError(ParameterServerError):
    code = ErrorCode.UNAVAILABLE


class TaskTimeoutError(ParameterServerError):
    code = ErrorCode.TIMEOUT


class DependencyFailedError(ParameterServerError):
    code = ErrorCode.DEPENDENCY_FAILED


class InternalError(ParameterServerError):
    code = ErrorCode.INTERNAL


ERROR_TYPES = {
    error.code: error
    for error in (
        InvalidArgumentError,
        UnavailableError,
        TaskTimeoutError,
        DependencyFailedError,
        InternalError,
    )
}


def application_error(code: int, message: str) -> ParameterServerError:
    return ERROR_TYPES.get(code, InternalError)(message)


def rpc_error(error: grpc.aio.AioRpcError) -> ParameterServerError:
    error_type = {
        grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentError,
        grpc.StatusCode.NOT_FOUND: InvalidArgumentError,
        grpc.StatusCode.ALREADY_EXISTS: InvalidArgumentError,
        grpc.StatusCode.RESOURCE_EXHAUSTED: InvalidArgumentError,
        grpc.StatusCode.UNAVAILABLE: UnavailableError,
        grpc.StatusCode.CANCELLED: UnavailableError,
        grpc.StatusCode.DEADLINE_EXCEEDED: TaskTimeoutError,
    }.get(error.code(), InternalError)
    return error_type(error.details() or error.code().name)
