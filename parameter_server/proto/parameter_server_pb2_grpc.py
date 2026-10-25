
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

from parameter_server.proto import parameter_server_pb2 as parameter__server_dot_proto_dot_parameter__server__pb2

GRPC_GENERATED_VERSION = '1.84.1'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + ' but the generated code in parameter_server/proto/parameter_server_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class ParameterServerStub:
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SubmitPush = channel.unary_unary(
                '/parameter_server.v1.ParameterServer/SubmitPush',
                request_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.PushRequest.SerializeToString,
                response_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.Ack.FromString,
                _registered_method=True)
        self.SubmitPull = channel.unary_unary(
                '/parameter_server.v1.ParameterServer/SubmitPull',
                request_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.PullRequest.SerializeToString,
                response_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.Ack.FromString,
                _registered_method=True)
        self.WaitTask = channel.unary_unary(
                '/parameter_server.v1.ParameterServer/WaitTask',
                request_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskId.SerializeToString,
                response_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskResult.FromString,
                _registered_method=True)
        self.GetTaskStatus = channel.unary_unary(
                '/parameter_server.v1.ParameterServer/GetTaskStatus',
                request_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskId.SerializeToString,
                response_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskStatus.FromString,
                _registered_method=True)
        self.GetServerInfo = channel.unary_unary(
                '/parameter_server.v1.ParameterServer/GetServerInfo',
                request_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.Empty.SerializeToString,
                response_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.ServerInfo.FromString,
                _registered_method=True)


class ParameterServerServicer:
    """Missing associated documentation comment in .proto file."""

    def SubmitPush(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SubmitPull(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def WaitTask(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetTaskStatus(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetServerInfo(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ParameterServerServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SubmitPush': grpc.unary_unary_rpc_method_handler(
                    servicer.SubmitPush,
                    request_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.PushRequest.FromString,
                    response_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.Ack.SerializeToString,
            ),
            'SubmitPull': grpc.unary_unary_rpc_method_handler(
                    servicer.SubmitPull,
                    request_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.PullRequest.FromString,
                    response_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.Ack.SerializeToString,
            ),
            'WaitTask': grpc.unary_unary_rpc_method_handler(
                    servicer.WaitTask,
                    request_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskId.FromString,
                    response_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskResult.SerializeToString,
            ),
            'GetTaskStatus': grpc.unary_unary_rpc_method_handler(
                    servicer.GetTaskStatus,
                    request_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskId.FromString,
                    response_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.TaskStatus.SerializeToString,
            ),
            'GetServerInfo': grpc.unary_unary_rpc_method_handler(
                    servicer.GetServerInfo,
                    request_deserializer=parameter__server_dot_proto_dot_parameter__server__pb2.Empty.FromString,
                    response_serializer=parameter__server_dot_proto_dot_parameter__server__pb2.ServerInfo.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'parameter_server.v1.ParameterServer', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('parameter_server.v1.ParameterServer', rpc_method_handlers)



class ParameterServer:
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SubmitPush(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/parameter_server.v1.ParameterServer/SubmitPush',
            parameter__server_dot_proto_dot_parameter__server__pb2.PushRequest.SerializeToString,
            parameter__server_dot_proto_dot_parameter__server__pb2.Ack.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def SubmitPull(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/parameter_server.v1.ParameterServer/SubmitPull',
            parameter__server_dot_proto_dot_parameter__server__pb2.PullRequest.SerializeToString,
            parameter__server_dot_proto_dot_parameter__server__pb2.Ack.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def WaitTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/parameter_server.v1.ParameterServer/WaitTask',
            parameter__server_dot_proto_dot_parameter__server__pb2.TaskId.SerializeToString,
            parameter__server_dot_proto_dot_parameter__server__pb2.TaskResult.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetTaskStatus(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/parameter_server.v1.ParameterServer/GetTaskStatus',
            parameter__server_dot_proto_dot_parameter__server__pb2.TaskId.SerializeToString,
            parameter__server_dot_proto_dot_parameter__server__pb2.TaskStatus.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetServerInfo(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/parameter_server.v1.ParameterServer/GetServerInfo',
            parameter__server_dot_proto_dot_parameter__server__pb2.Empty.SerializeToString,
            parameter__server_dot_proto_dot_parameter__server__pb2.ServerInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
