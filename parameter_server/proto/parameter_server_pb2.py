




"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    7,
    35,
    1,
    '',
    'parameter_server/proto/parameter_server.proto'
)


_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n-parameter_server/proto/parameter_server.proto\x12\x13parameter_server.v1\"4\n\x06TaskId\x12\x11\n\tworker_id\x18\x01 \x01(\r\x12\x17\n\x0fsequence_number\x18\x02 \x01(\x04\"\x17\n\x07KeyList\x12\x0c\n\x04keys\x18\x01 \x03(\x04\"E\n\x08KeyRange\x12\r\n\x05start\x18\x01 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x04\x12\x1d\n\x15\x65nd_at_keyspace_limit\x18\x03 \x01(\x08\"u\n\x06Target\x12.\n\x06sparse\x18\x01 \x01(\x0b\x32\x1c.parameter_server.v1.KeyListH\x00\x12.\n\x05range\x18\x02 \x01(\x0b\x32\x1d.parameter_server.v1.KeyRangeH\x00\x42\x0b\n\tselection\"\xbe\x01\n\x0bPushRequest\x12,\n\x07task_id\x18\x01 \x01(\x0b\x32\x1b.parameter_server.v1.TaskId\x12+\n\x06target\x18\x02 \x01(\x0b\x32\x1b.parameter_server.v1.Target\x12\x0e\n\x06values\x18\x03 \x03(\x01\x12\x31\n\x0c\x64\x65pendencies\x18\x04 \x03(\x0b\x32\x1b.parameter_server.v1.TaskId\x12\x11\n\titeration\x18\x05 \x01(\x04\"\xae\x01\n\x0bPullRequest\x12,\n\x07task_id\x18\x01 \x01(\x0b\x32\x1b.parameter_server.v1.TaskId\x12+\n\x06target\x18\x02 \x01(\x0b\x32\x1b.parameter_server.v1.Target\x12\x31\n\x0c\x64\x65pendencies\x18\x03 \x03(\x0b\x32\x1b.parameter_server.v1.TaskId\x12\x11\n\titeration\x18\x04 \x01(\x04\"3\n\x03\x41\x63k\x12,\n\x07task_id\x18\x01 \x01(\x0b\x32\x1b.parameter_server.v1.TaskId\"3\n\x06Values\x12\x0c\n\x04keys\x18\x01 \x03(\x04\x12\x0c\n\x04\x64\x61ta\x18\x02 \x03(\x01\x12\r\n\x05width\x18\x03 \x01(\r\"J\n\tTaskError\x12,\n\x04\x63ode\x18\x01 \x01(\x0e\x32\x1e.parameter_server.v1.ErrorCode\x12\x0f\n\x07message\x18\x02 \x01(\t\"\xfd\x01\n\nTaskResult\x12,\n\x07task_id\x18\x01 \x01(\x0b\x32\x1b.parameter_server.v1.TaskId\x12-\n\x05state\x18\x02 \x01(\x0e\x32\x1e.parameter_server.v1.TaskState\x12\'\n\x03\x61\x63k\x18\x03 \x01(\x0b\x32\x18.parameter_server.v1.AckH\x00\x12-\n\x06values\x18\x04 \x01(\x0b\x32\x1b.parameter_server.v1.ValuesH\x00\x12/\n\x05\x65rror\x18\x05 \x01(\x0b\x32\x1e.parameter_server.v1.TaskErrorH\x00\x42\t\n\x07outcome\"\x98\x01\n\nTaskStatus\x12,\n\x07task_id\x18\x01 \x01(\x0b\x32\x1b.parameter_server.v1.TaskId\x12-\n\x05state\x18\x02 \x01(\x0e\x32\x1e.parameter_server.v1.TaskState\x12-\n\x05\x65rror\x18\x03 \x01(\x0b\x32\x1e.parameter_server.v1.TaskError\"\x07\n\x05\x45mpty\"\x80\x01\n\nServerInfo\x12\r\n\x05width\x18\x01 \x01(\r\x12\x15\n\rinitial_value\x18\x02 \x01(\x01\x12\x1c\n\x14max_keys_per_request\x18\x03 \x01(\x04\x12\x19\n\x11max_message_bytes\x18\x04 \x01(\r\x12\x13\n\x0bupdate_rule\x18\x05 \x01(\t*\\\n\tTaskState\x12\x1a\n\x16TASK_STATE_UNSPECIFIED\x10\x00\x12\x0b\n\x07PENDING\x10\x01\x12\x0b\n\x07RUNNING\x10\x02\x12\r\n\tSUCCEEDED\x10\x03\x12\n\n\x06\x46\x41ILED\x10\x04*\x80\x01\n\tErrorCode\x12\x1a\n\x16\x45RROR_CODE_UNSPECIFIED\x10\x00\x12\x14\n\x10INVALID_ARGUMENT\x10\x01\x12\x0f\n\x0bUNAVAILABLE\x10\x02\x12\x0b\n\x07TIMEOUT\x10\x03\x12\x15\n\x11\x44\x45PENDENCY_FAILED\x10\x04\x12\x0c\n\x08INTERNAL\x10\x05\x32\x8c\x03\n\x0fParameterServer\x12H\n\nSubmitPush\x12 .parameter_server.v1.PushRequest\x1a\x18.parameter_server.v1.Ack\x12H\n\nSubmitPull\x12 .parameter_server.v1.PullRequest\x1a\x18.parameter_server.v1.Ack\x12H\n\x08WaitTask\x12\x1b.parameter_server.v1.TaskId\x1a\x1f.parameter_server.v1.TaskResult\x12M\n\rGetTaskStatus\x12\x1b.parameter_server.v1.TaskId\x1a\x1f.parameter_server.v1.TaskStatus\x12L\n\rGetServerInfo\x12\x1a.parameter_server.v1.Empty\x1a\x1f.parameter_server.v1.ServerInfob\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'parameter_server.proto.parameter_server_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_TASKSTATE']._serialized_start=1442
  _globals['_TASKSTATE']._serialized_end=1534
  _globals['_ERRORCODE']._serialized_start=1537
  _globals['_ERRORCODE']._serialized_end=1665
  _globals['_TASKID']._serialized_start=70
  _globals['_TASKID']._serialized_end=122
  _globals['_KEYLIST']._serialized_start=124
  _globals['_KEYLIST']._serialized_end=147
  _globals['_KEYRANGE']._serialized_start=149
  _globals['_KEYRANGE']._serialized_end=218
  _globals['_TARGET']._serialized_start=220
  _globals['_TARGET']._serialized_end=337
  _globals['_PUSHREQUEST']._serialized_start=340
  _globals['_PUSHREQUEST']._serialized_end=530
  _globals['_PULLREQUEST']._serialized_start=533
  _globals['_PULLREQUEST']._serialized_end=707
  _globals['_ACK']._serialized_start=709
  _globals['_ACK']._serialized_end=760
  _globals['_VALUES']._serialized_start=762
  _globals['_VALUES']._serialized_end=813
  _globals['_TASKERROR']._serialized_start=815
  _globals['_TASKERROR']._serialized_end=889
  _globals['_TASKRESULT']._serialized_start=892
  _globals['_TASKRESULT']._serialized_end=1145
  _globals['_TASKSTATUS']._serialized_start=1148
  _globals['_TASKSTATUS']._serialized_end=1300
  _globals['_EMPTY']._serialized_start=1302
  _globals['_EMPTY']._serialized_end=1309
  _globals['_SERVERINFO']._serialized_start=1312
  _globals['_SERVERINFO']._serialized_end=1440
  _globals['_PARAMETERSERVER']._serialized_start=1668
  _globals['_PARAMETERSERVER']._serialized_end=2064
