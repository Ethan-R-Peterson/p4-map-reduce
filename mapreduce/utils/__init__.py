"""Utils package.

This package is for code shared by the Manager and the Worker.
"""
import json
import logging
import socket

from mapreduce.utils.ordered_dict import ThreadSafeOrderedDict

LOGGER = logging.getLogger(__name__)


def run_tcp_server(host, port, shutdown_checker, message_handler):
    """Run a TCP server until shutdown_checker returns True."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        sock.settimeout(1)

        LOGGER.info("Listening on TCP port %s", port)

        while not shutdown_checker():
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue

            with conn:
                data = b""
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk

            try:
                msg = json.loads(data.decode())
            except json.JSONDecodeError:
                continue

            message_handler(msg)
