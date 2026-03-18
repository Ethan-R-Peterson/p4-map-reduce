"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import threading
import socket
import shutil
from collections import OrderedDict
import click
import time

LOGGER = logging.getLogger(__name__)


class Manager:
    def __init__(self, host, port):
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = port
        self.shutdown = False

        self.workers = OrderedDict()

        self.job_id = 0
        self.job_queue = []
        self.current_job = None

        self.stage = None
        self.tasks_total = 0
        self.tasks_finished = 0

        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            self.tmpdir = tmpdir

            tcp_thread = threading.Thread(target=self.tcp_server)
            udp_thread = threading.Thread(target=self.udp_server)
            fault_thread = threading.Thread(target=self.fault_monitor)
            tcp_thread.start()
            udp_thread.start()
            fault_thread.start()
            tcp_thread.join()
            udp_thread.join()
            fault_thread.join()

        LOGGER.info("Cleaned up tmpdir %s", tmpdir)


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

    def handle_heartbeat(self, msg):
        worker_key = (msg["worker_host"], msg["worker_port"])
        if worker_key not in self.workers:
            return
        if self.workers[worker_key]["state"] != "dead":
            self.workers[worker_key]["missed_pings"] = 0

    def udp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)

            while not self.shutdown:
                try:
                    dta = sock.recv(4096)
                except socket.timeout:
                    continue

                try:
                    msg = json.loads(dta.decode())
                except json.JSONDecodeError:
                    continue

                if msg.get("message_type") == "heartbeat":
                    self.handle_heartbeat(msg)


    def mark_worker_dead(self, worker):
        if worker["state"] == "dead":
            return

        worker["state"] = "dead"

        if worker["task"] is not None:
            self.pending_tasks.append(worker["task"])
            worker["task"] = None

        self.assign_tasks()


    def fault_monitor(self):
        while not self.shutdown:
            time.sleep(2)
            for worker in self.workers.values():
                if worker["state"] == "dead":
                    continue

                worker["missed_pings"] += 1

                if worker["missed_pings"] > 5:
                    self.mark_worker_dead(worker)


    def handle_message(self, msg):
        t = msg.get("message_type")

        if t == "shutdown":
            self.handle_shutdown()
        elif t == "register":
            self.handle_register(msg)
        elif t == "new_manager_job":
            self.handle_new_job(msg)
        elif t == "finished":
            self.handle_task_finished(msg)


    def handle_shutdown(self):
        LOGGER.info("Received shutdown")

        for w in self.workers.values():
            if w["state"] != "dead":
                self.send_tcp(w["host"], w["port"], {
                    "message_type": "shutdown"
                })

        self.shutdown = True


    def handle_register(self, msg):
        host = msg["worker_host"]
        port = msg["worker_port"]

        LOGGER.info("Registered Worker (%s, %s)", host, port)

        self.workers[(host, port)] = {
            "host": host,
            "port": port,
            "state": "ready",
            "task": None,
            "missed_pings": 0,
        }

        self.send_tcp(host, port, {"message_type": "register_ack"})
        self.try_start_job()


    def handle_new_job(self, msg):
        msg["job_id"] = self.job_id
        self.job_id += 1

        self.job_queue.append(msg)
        LOGGER.info("Added job %s to queue", msg["job_id"])
        self.try_start_job()

    def try_start_job(self):
        if self.current_job or not self.job_queue:
            return

        self.current_job = self.job_queue.pop(0)
        self.run_job(self.current_job)

    def run_job(self, job):
        input_dir = job["input_directory"]
        output_dir = job["output_directory"]

        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)

        self.job_dir = os.path.join(self.tmpdir, f"job-{job['job_id']:05d}")
        os.makedirs(self.job_dir)

        LOGGER.info("Running job %s", job["job_id"])

        # -------- MAP STAGE --------
        files = sorted(os.listdir(input_dir))
        num_mappers = job["num_mappers"]

        partitions = [[] for _ in range(num_mappers)]
        for i, f in enumerate(files):
            partitions[i % num_mappers].append(os.path.join(input_dir, f))

        self.pending_tasks = []
        for task_id, paths in enumerate(partitions):
            self.pending_tasks.append({
                "message_type": "new_map_task",
                "task_id": task_id,
                "input_paths": paths,
                "executable": job["mapper_executable"],
                "output_directory": self.job_dir,
                "num_partitions": job["num_reducers"],
            })

        self.stage = "map"
        self.tasks_total = len(self.pending_tasks)
        self.tasks_finished = 0

        self.assign_tasks()


    def assign_tasks(self):
        for worker in self.workers.values():
            if worker["state"] == "ready" and self.pending_tasks:
                task = self.pending_tasks.pop(0)
                worker["state"] = "busy"
                worker["task"] = task

                success = self.send_tcp(worker["host"], worker["port"], task)
                if not success:
                    if worker["task"] is not None:
                        self.pending_tasks.append(worker["task"])
                        worker["task"] = None
    
    def start_reduce(self):
        job = self.current_job
        num_reducers = job["num_reducers"]
        self.pending_tasks = []
        for task in range(num_reducers):
            suffix = f"part{task:05d}"
            input_paths = []
            for filename in sorted(os.listdir(self.job_dir)):
                if filename.endswith(suffix):
                    input_paths.append(os.path.join(self.job_dir, filename))
            self.pending_tasks.append({
                "message_type": "new_reduce_task",
                "task_id": task,
                "executable": job["reducer_executable"],
                "input_paths": input_paths,
                "output_directory": job["output_directory"],
            })
        self.stage = "reduce"
        self.tasks_total = len(self.pending_tasks)
        self.tasks_finished = 0
        self.assign_tasks()

    def handle_task_finished(self, msg):
        worker_key = (msg["worker_host"], msg["worker_port"])
        if worker_key in self.workers and self.workers[worker_key]["state"] != "dead":
            self.workers[worker_key]["state"] = "ready"
            self.workers[worker_key]["task"] = None

        self.tasks_finished += 1

        if self.tasks_finished == self.tasks_total:
            if self.stage == "map":
                self.start_reduce()
                return
            if self.stage == "reduce":
                self.handle_shutdown()
                return

        self.assign_tasks()


    def send_tcp(self, host, port, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                sock.sendall(json.dumps(message).encode())
            return True
        except ConnectionRefusedError:
            worker_key = (host, port)
            if worker_key in self.workers:
                self.mark_worker_dead(self.workers[worker_key])
            return False


@click.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6000)
@click.option("--logfile", default=None)
@click.option("--loglevel", default="info")
@click.option("--shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    tempfile.tempdir = shared_dir

    handler = logging.FileHandler(logfile) if logfile else logging.StreamHandler()
    formatter = logging.Formatter(f"Manager:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())

    Manager(host, port)


if __name__ == "__main__":
    main()