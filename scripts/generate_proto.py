import io
import subprocess
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{ROOT}",
            f"--python_out={ROOT}",
            f"--grpc_python_out={ROOT}",
            str(ROOT / "parameter_server/proto/parameter_server.proto"),
        ],
        check=True,
    )
    for filename in ("parameter_server_pb2.py", "parameter_server_pb2_grpc.py"):
        path = ROOT / "parameter_server/proto" / filename
        tokens = tokenize.generate_tokens(io.StringIO(path.read_text()).readline)
        source = tokenize.untokenize(token for token in tokens if token.type != tokenize.COMMENT)
        path.write_text("\n".join(line.rstrip() for line in source.splitlines()).rstrip() + "\n")


if __name__ == "__main__":
    main()
