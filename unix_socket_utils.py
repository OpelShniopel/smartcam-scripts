import os
import socket


def create_unix_stream_server(path: str, label: str, *, backlog: int = 2, mode: int = 0o660) -> socket.socket:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    os.chmod(path, mode)
    srv.listen(backlog)
    print(f"{label} -> {path}")
    return srv
