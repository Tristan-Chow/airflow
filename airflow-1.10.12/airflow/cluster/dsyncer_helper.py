# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys
import os
import signal
import multiprocessing
import airflow
import logging
import time
import traceback
import six
from airflow.utils.db import provide_session
from six.moves import reload_module
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.configuration import conf
from multiprocessing.queues import Empty
from setproctitle import setproctitle
from airflow.utils.mixins import MultiprocessingStartMethodMixin
from airflow.cluster.message import Message, TYPE_DSYNCER
from airflow.cluster.dag_file import DagFile, get_df_upsert_function
from airflow.utils.dag_processing import check_file
from airflow.utils import timezone


depth = conf.getint("scheduler", "dag_dir_list_depth")
safe_mode_conf = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE', fallback=True)
update_dag_file_in_dsyncer = conf.getboolean('dsyncer', 'update_dag_file_in_dsyncer', fallback=False)

DF = DagFile
df_upsert = get_df_upsert_function()


def get(queue):
    try:
        return queue.get_nowait()
    except Empty:
        return None


def format_path(file_path):
    if "//" in file_path:
        file_path = file_path.replace("//", "/")
        file_path = format_path(file_path)
    if not file_path.endswith("/"):
        file_path += "/"
    return file_path.strip()


def get_multiprocessing_context():
    if six.PY2:
        return multiprocessing
    else:
        start_method = MultiprocessingStartMethodMixin()._get_multiprocessing_start_method()
        return multiprocessing.get_context(start_method)


@provide_session
def update_dag_file_in_db(dags_folder, change_files, log=logging.getLogger(__name__), session=None):
    for file in change_files:
        if depth != 0 and len([p for p in os.path.relpath(file, dags_folder).split(os.path.sep) if p != "."]) > depth:
            continue
        if not check_file(file, [], safe_mode_conf):
            log.info("Delete %s.", file)
            DF.delete(file, session)
            continue
        log.info("Upsert %s.", file)
        df_upsert(file, timezone.utcnow(), session)


def update_dag_file_do_nothing(dags_folder, change_files, log=logging.getLogger(__name__)):
    pass


update_dag_file = update_dag_file_do_nothing
if update_dag_file_in_dsyncer:
    update_dag_file = update_dag_file_in_db


class HandlePathProcess(LoggingMixin, MultiprocessingStartMethodMixin):

    def __init__(self, dags_folder, message_queue, target_method, id, *args, **kwargs):
        super(HandlePathProcess, self).__init__(*args, **kwargs)
        self.dags_folder = dags_folder
        self.message_queue = message_queue
        self._process = None
        self._heartbeat = None
        self.target_method = target_method
        self.timeout = 10
        self.id = id

    @staticmethod
    def notify(dags_folder, message_queue, heartbeat, id):
        setproctitle("airflow dsyncer NotifyProcess-{}".format(id))
        change_dirs = set()
        change_files = set()
        log = logging.getLogger("airflow.dsyncer.notify")
        log.info("NotifyProcess-{} start.".format(id))

        def signal_handler(signum, frame):
            airflow.settings.dispose_orm()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        reload_module(airflow.settings)
        airflow.settings.initialize()

        def parse_message(dirs, files):
            message = get(message_queue)
            if message is None:
                return False
            dir, name = message
            dirs.add(dir)
            path = os.path.join(dir, name)
            file_name, ext = os.path.splitext(os.path.split(path)[-1])
            if ext == ".py":
                files.add(path)
            return True

        def add_message(path):
            Message.add(TYPE_DSYNCER, format_path(path))
            log.info("Notify path: %s", path)

        try:
            add_message(dags_folder)
            while True:
                heartbeat.value = int(time.time())
                if not parse_message(change_dirs, change_files):
                    time.sleep(0.1)
                    continue

                start = time.time()
                while time.time() - start < 1:
                    if not parse_message(change_dirs, change_files):
                        time.sleep(0.1)
                        continue
                add_message(os.path.commonprefix(list(change_dirs)))
                update_dag_file(dags_folder, change_files, log)
                change_dirs.clear()
                change_files.clear()

        except Exception:
            traceback.print_exc()
        finally:
            airflow.settings.dispose_orm()

        log.info("NotifyProcess-{} stop.".format(id))
        sys.exit(0)

    def start(self):
        context = get_multiprocessing_context()
        self._heartbeat = context.Value('l', int(time.time()))
        self._process = context.Process(
            target=self.target_method,
            args=(
                self.dags_folder,
                self.message_queue,
                self._heartbeat,
                self.id
            ),
            name="Process"
        )
        self._process.start()

    def alive(self):
        if not self._process.is_alive():
            self._process.join()
            return False
        if int(time.time()) - self._heartbeat.value > self.timeout:
            self.log.warning("{}-{} timeout. Last heartbeat is {}.".format(self.target_method.__name__,
                                                                           self.id, self._heartbeat.value))
            self.terminate()
            return False
        return True

    def check(self):
        if not self.alive():
            self.log.warning("{}-{} stop.".format(self.target_method.__name__, self.id))
            return False
        return True

    def process_is_alive(self):
        return self._process.is_alive()

    def _kill_process(self):
        if self._process.is_alive():
            self.log.info("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    def terminate(self, sigkill=True):
        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        if six.PY2:
            self._process.join(5)
        else:
            from contextlib import suppress
            with suppress(TimeoutError):
                self._process._popen.wait(5)  # pylint: disable=protected-access
        if sigkill:
            self._kill_process()


class ProcessManager(LoggingMixin, MultiprocessingStartMethodMixin):
    def __init__(self, dags_folder, target_method, *args, **kwargs):
        self.path_queue = get_multiprocessing_context().Queue()
        self.dags_folder = dags_folder
        self.init_count = 1
        self.processes = []
        self.target_method = target_method
        self.count = 0

    def run_one(self):
        self.count += 1
        p = HandlePathProcess(self.dags_folder, self.path_queue, self.target_method, self.count)
        p.start()
        self.processes.append(p)

    def start(self):
        for i in range(0, self.init_count):
            self.run_one()

    def heartbeat(self):
        self.processes = [p for p in self.processes if p.check()]
        if len(self.processes) < self.init_count:
            for i in range(0, self.init_count - len(self.processes)):
                self.run_one()

    def put(self, file_path):
        self.path_queue.put(file_path)

    def stop(self):
        for p in self.processes:
            p.terminate()
        self.path_queue.close()
