"""MapReduce framework Worker node."""
import os
import logging
import json
import threading
import socket
import tempfile
import subprocess
import hashlib
import heapq
import shutil
import time
import contextlib
import click

from mapreduce.utils import run_tcp_server
LOGGER = logging.getLogger(__name__)


class Worker:
    """Worker node that executes map and reduce tasks."""

    def __init__(self, host, port, manager_host, manager_port):
        """Initialize Worker and start TCP server + registration."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown = False
        self.heartbeat_thread = None

        tcp_thread = threading.Thread(
            target=run_tcp_server,
            kwargs={
                "host": self.host,
                "port": self.port,
                "shutdown_checker": lambda: self.shutdown,
                "message_handler": self.handle_message,
            },
        )
        tcp_thread.start()

        self.register()
        tcp_thread.join()
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()

    def register(self):
        """Register this Worker with the Manager."""
        self.send_tcp(self.manager_host, self.manager_port, {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        })

    def heartbeat(self):
        """Send periodic heartbeat messages to Manager via UDP."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            while not self.shutdown:
                message = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
                sock.sendall(json.dumps(message).encode())
                time.sleep(2)

    def handle_message(self, msg):
        """Handle incoming messages from Manager."""
        t = msg.get("message_type")

        if t == "register_ack":
            LOGGER.info("Connected to Manager")
            if self.heartbeat_thread is None:
                self.heartbeat_thread = threading.Thread(target=self.heartbeat)
                self.heartbeat_thread.start()

        elif t == "shutdown":
            LOGGER.info("Shutting down")
            self.shutdown = True

        elif t == "new_map_task":
            self.run_map_task(msg)

        elif t == "new_reduce_task":
            self.run_reduce_task(msg)

    def _move_partitions(
        self,
        tmpdir,
        task_id,
        num_partitions,
        output_directory,
    ):
        """Move sorted partition files to shared output directory."""
        for partition_num in range(num_partitions):
            src = os.path.join(
                tmpdir,
                f"maptask{task_id:05d}-part{partition_num:05d}",
            )
            dst = os.path.join(output_directory, os.path.basename(src))
            os.replace(src, dst)

    def run_map_task(self, task):
        """Execute a map task and write partitioned outputs."""
        task_id = task["task_id"]
        num_partitions = task["num_partitions"]

        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as tmpdir:
            with contextlib.ExitStack() as stack:
                partition_files = {}
                for partition_num in range(num_partitions):
                    path = os.path.join(
                        tmpdir,
                        f"maptask{task_id:05d}-part{partition_num:05d}",
                    )
                    partition_files[partition_num] = stack.enter_context(
                        open(path, "w", encoding="utf-8")
                    )

                for input_path in task["input_paths"]:
                    with open(input_path, encoding="utf-8") as infile:
                        with subprocess.Popen(
                            [task["executable"]],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as proc:
                            for line in proc.stdout:
                                key, _, _ = line.partition("\t")
                                key_hash = hashlib.md5(
                                    key.encode("utf-8")
                                ).hexdigest()
                                partition_num = (
                                    int(key_hash, 16)
                                    % num_partitions
                                )
                                partition_files[partition_num].write(line)

            for partition_num in range(num_partitions):
                path = os.path.join(
                    tmpdir,
                    f"maptask{task_id:05d}-part{partition_num:05d}",
                )
                subprocess.run(["sort", "-o", path, path], check=True)

            self._move_partitions(
                tmpdir,
                task_id,
                num_partitions,
                task["output_directory"],
            )

        self.send_tcp(
            self.manager_host,
            self.manager_port,
            {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port,
            },
        )

    def run_reduce_task(self, task):
        """Execute a reduce task by merging inputs and producing output."""
        task_id = task["task_id"]
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as tmpdir:
            output_path = os.path.join(tmpdir, f"part-{task_id:05d}")

            with contextlib.ExitStack() as stack:
                infiles = [
                    stack.enter_context(open(path, "r", encoding="utf-8"))
                    for path in task["input_paths"]
                ]
                merged_input = heapq.merge(*infiles)

                with open(output_path, "w", encoding="utf-8") as outfile:
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

            final_output_path = os.path.join(
                task["output_directory"],
                f"part-{task_id:05d}",
            )
            shutil.move(output_path, final_output_path)

        self.send_tcp(
            self.manager_host,
            self.manager_port,
            {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port,
            },
        )

    def finish_task(self, msg):
        """Notify Manager that a task has completed."""
        self.send_tcp(self.manager_host, self.manager_port, {
            "message_type": "finished",
            "worker_host": self.host,
            "worker_port": self.port,
            "task_id": msg["task_id"],
        })

    def send_tcp(self, host, port, message):
        """Send a JSON message to the given host/port via TCP."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(json.dumps(message).encode())


@click.command()
@click.option("--host", default="localhost")
@click.option("--port", default="6001", type=int)
@click.option("--manager-host", default="localhost")
@click.option("--manager-port", default="6000", type=int)
@click.option("--logfile", default=None)
@click.option("--loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Start Worker process with logging configuration."""
    handler = (
        logging.FileHandler(logfile)
        if logfile
        else logging.StreamHandler()
    )
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())

    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
