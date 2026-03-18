"""MapReduce framework Worker node."""
import os
import logging
import json
import threading
import socket
import tempfile
import subprocess
import hashlib
import click
import heapq
import shutil
import contextlib

LOGGER = logging.getLogger(__name__)


class Worker:
    def __init__(self, host, port, manager_host, manager_port):
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown = False

        tcp_thread = threading.Thread(target=self.tcp_server)
        tcp_thread.start()

        self.register()
        tcp_thread.join()

    def tcp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            LOGGER.info("Listening on TCP port %s", self.port)

            while not self.shutdown:
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

                self.handle_message(msg)

    def register(self):
        self.send_tcp(self.manager_host, self.manager_port, {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        })

    def handle_message(self, msg):
        t = msg.get("message_type")

        if t == "register_ack":
            LOGGER.info("Connected to Manager")

        elif t == "shutdown":
            LOGGER.info("Shutting down")
            self.shutdown = True

        elif t == "new_map_task":
            self.run_map_task(msg)

        elif t == "new_reduce_task":
            self.run_reduce_task(msg)

    def run_map_task(self, task):
        task_id = task["task_id"]
        num_partitions = task["num_partitions"]

        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as tmpdir:

            partition_files = {}
            for p in range(num_partitions):
                path = os.path.join(
                    tmpdir,
                    f"maptask{task_id:05d}-part{p:05d}"
                )
                partition_files[p] = open(path, "w")

            for input_path in task["input_paths"]:
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [task["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as proc:

                        for line in proc.stdout:
                            key, _, _ = line.partition("\t")

                            h = hashlib.md5(key.encode("utf-8")).hexdigest()
                            partition = int(h, 16) % num_partitions

                            partition_files[partition].write(line)

            for f in partition_files.values():
                f.close()

            for p in range(num_partitions):
                path = os.path.join(
                    tmpdir,
                    f"maptask{task_id:05d}-part{p:05d}"
                )
                subprocess.run(["sort", "-o", path, path], check=True)

            for p in range(num_partitions):
                src = os.path.join(
                    tmpdir,
                    f"maptask{task_id:05d}-part{p:05d}"
                )
                dst = os.path.join(
                    task["output_directory"],
                    os.path.basename(src)
                )
                os.replace(src, dst)

        self.send_tcp(self.manager_host, self.manager_port, {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        })

    def run_reduce_task(self, task):
        task_id = task["task_id"]
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as tmpdir:
            output_path = os.path.join(tmpdir, f"part-{task_id:05d}")
            infiles = []
            for path in task["input_paths"]:
                infiles.append(open(path, "r"))
            merged_input = heapq.merge(*infiles)
            with open(output_path, "w") as outfile:
                with subprocess.Popen(
                    [task["executable"]],
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                    text=True,
                ) as proc:
                    for line in merged_input:
                        proc.stdin.write(line)
                    proc.stdin.close()
                    proc.wait()
            for f in infiles:
                f.close()
            final_output_path = os.path.join(
                task["output_directory"],
                f"part-{task_id:05d}"
            )
            shutil.move(output_path, final_output_path)
        self.send_tcp(self.manager_host, self.manager_port, {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        })

    def finish_task(self, msg):
        self.send_tcp(self.manager_host, self.manager_port, {
            "message_type": "finished",
            "worker_host": self.host,
            "worker_port": self.port,
            "task_id": msg["task_id"],
        })

    def send_tcp(self, host, port, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(json.dumps(message).encode())


@click.command()
@click.option("--host", default="localhost")
@click.option("--port", default="6001")
@click.option("--manager-host", default="localhost")
@click.option("--manager-port", default="6000")
@click.option("--logfile", default=None)
@click.option("--loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    handler = logging.FileHandler(logfile) if logfile else logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())

    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()