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
import socket
import signal
import time
import os
import shutil
import pexpect
import traceback
from airflow.cluster.nodeinstance import NodeInstance, NODE_INSTANCE_DEAD, NODE_INSTANCE_RUNNING, INSTANCE_DSYNCER
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow import settings
from airflow.configuration import conf
from airflow.cluster.message import Message, TYPE_DSYNCER
from airflow.utils.db import provide_session
from airflow.utils import timezone
from pyinotify import WatchManager, Notifier, ProcessEvent, IN_DELETE, IN_CREATE, IN_MODIFY, IN_MOVED_FROM, IN_MOVED_TO
from airflow.cluster.dsyncer_helper import format_path
from airflow.cluster.dsyncer_helper import ProcessManager, HandlePathProcess, get_multiprocessing_context
from airflow.utils.helpers import kill_all_children


class Dsyncer(LoggingMixin):
    MASTER_ID = 2
    TRY_NUMBER = 0

    def __init__(self,
                 master_hostname_or_ip,
                 master_port,
                 dags_folder=settings.DAGS_FOLDER,
                 *args, **kwargs):
        super(Dsyncer, self).__init__(*args, **kwargs)
        self.dags_folder = format_path(dags_folder)
        self.master_hostname_or_ip = master_hostname_or_ip
        self.master_port = master_port

        self.dsyncer_master_username = conf.get("dsyncer", "dsyncer_master_username")
        self.dsyncer_master_password = conf.get("dsyncer", "dsyncer_master_password")

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def start(self):
        pass

    def terminate(self):
        pass

    def _exit_gracefully(self, signum, frame):
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.terminate()
        sys.exit(0)


class Master(Dsyncer):
    def __init__(self, *args, **kwargs):
        super(Master, self).__init__(*args, **kwargs)
        self.node = NodeInstance.create_instance(Dsyncer.MASTER_ID, INSTANCE_DSYNCER)
        self.last_update_heartbeat = timezone.datetime(2000, 1, 1)
        self.notify_process = None
        self.notifier = None

    @provide_session
    def update_heartbeat(self, session=None):
        if (timezone.utcnow() - self.last_update_heartbeat).total_seconds() < 10:
            return
        self.node.latest_heartbeat = timezone.utcnow()
        session.merge(self.node)
        session.commit()
        self.last_update_heartbeat = timezone.utcnow()

    @provide_session
    def logout(self, session=None):
        if self.node is None:
            return
        self.node.state = NODE_INSTANCE_DEAD
        session.merge(self.node)
        session.commit()
        self.log.info("Log out success.")
        self.node = None

    def terminate(self):
        try:
            self.logout()
        except Exception:
            self.log.error("Log out failed.")
        if self.notifier is not None:
            self.notifier.stop()
            self.notifier = None
        if self.notify_process is not None:
            self.notify_process.stop()
            self.notify_process = None
        kill_all_children(self.log)
        self.log.info("Stop.")

    def start(self):
        log = self.log

        self.notify_process = ProcessManager(self.dags_folder, HandlePathProcess.notify)
        notify_process = self.notify_process

        class EventHandler(ProcessEvent):

            def do_process(self, event):
                name, ext = os.path.splitext(event.name)
                if event.name == "__pycache__" \
                        or ext in [".swp", ".pyc", ".swx"] \
                        or event.path.endswith("__pycache__"):
                    return
                notify_process.put((event.path + "/", event.name))
                log.info("Put %s to notify queue.", (event.path, event.name))

            def process_IN_CREATE(self, event):
                self.do_process(event)

            def process_IN_DELETE(self, event):
                self.do_process(event)

            def process_IN_MODIFY(self, event):
                self.do_process(event)

            def process_IN_MOVED_FROM(self, event):
                self.do_process(event)

            def process_IN_MOVED_TO(self, event):
                self.do_process(event)

        try:
            wm = WatchManager()
            mask = IN_DELETE | IN_CREATE | IN_MODIFY | IN_MOVED_FROM | IN_MOVED_TO
            self.notifier = Notifier(wm, EventHandler())
            wm.add_watch(self.dags_folder, mask, auto_add=True, rec=True)
            self.log.info("Create notifier for %s.", self.dags_folder)

            self.notify_process.start()
            while True:
                self.update_heartbeat()
                self.notify_process.heartbeat()
                self.notifier.process_events()
                if self.notifier.check_events(2000):
                    self.notifier.read_events()
                Dsyncer.TRY_NUMBER = 0
        finally:
            self.terminate()


class Slave(Dsyncer):
    def __init__(self, *args, **kwargs):
        super(Slave, self).__init__(*args, **kwargs)
        self.last_check_master_alive = timezone.datetime(2000, 1, 1)
        self.last_sync_all = timezone.datetime(2000, 1, 1)
        self.watermark = None
        self.last_clean_pycache_dir = timezone.datetime(2000, 1, 1)
        self._process = None
        self.filter_message_type = [TYPE_DSYNCER]

    @staticmethod
    def clean_pycache_dir(directory):
        deleted = []
        for root, dirs, files in os.walk(directory, followlinks=True):
            if root.endswith("__pycache__"):
                continue
            if "__pycache__" in dirs:
                pys = [file for file in files if file.endswith(".py")]
                if len(pys) == 0:
                    path = os.path.join(root, "__pycache__")
                    deleted.append(os.path.join(root, "__pycache__"))
                    shutil.rmtree(path)
                    print("Clean: " + path)

    def run_sub_process_to_clean_pycache_dir(self):
        if (timezone.utcnow() - self.last_clean_pycache_dir).total_seconds() < 60 * 60:
            return
        self.last_clean_pycache_dir = timezone.utcnow()
        if self._process is not None and self._process.is_alive():
            return
        context = get_multiprocessing_context()
        self._process = context.Process(target=self.__class__.clean_pycache_dir,
                                        args=(self.dags_folder, ), name="CleanPycache")
        self._process.start()

    def rsync_dir(self, rsync_dir):
        self.log.info("Start sync: %s", rsync_dir)
        start = time.time()
        cmd = 'rsync -e "ssh -p {rPort}" -vpgtr --delete --exclude=__pycache__ {rUser}@{rHost}:{dstDir} {srcDir}'\
            .format(rPort=self.master_port,
                    rUser=self.dsyncer_master_username,
                    rHost=self.master_hostname_or_ip,
                    srcDir=rsync_dir,
                    dstDir=rsync_dir)
        self.log.info("Exec cmd: %s", cmd)
        ssh = None
        try:
            ssh = pexpect.spawn(cmd, timeout=3600)

            def expect():
                return ssh.expect(
                    ['continue connecting (yes/no)?',
                     'password:',
                     'No such file or directory',
                     'total size is '],
                    timeout=600)
            i = expect()

            if i == 0:
                ssh.sendline('yes')
                i = expect()

            if i == 1:
                ssh.sendline(self.dsyncer_master_password)
                i = expect()

            if i == 2:
                self.log.error('No such file or directory: %s', rsync_dir)
                if len(rsync_dir) > len(self.dags_folder):
                    self.rsync_dir(self.dags_folder)

            if i == 3:
                self.log.info("Sync dir %s take %s s.", rsync_dir, str(time.time() - start))

        except Exception as e:
            traceback.print_exc()
            raise e
        finally:
            if ssh:
                ssh.close()

    def rsync_all(self):
        self.watermark = Message.look_for_max_id()
        self.rsync_dir(self.dags_folder)
        self.last_sync_all = timezone.utcnow()

    def terminate(self):
        if self._process is not None and self._process.is_alive:
            self._process.terminate()
        kill_all_children(self.log)
        self.log.info("Stop.")

    def check_master_alive(self):
        master = NodeInstance.select(id=Dsyncer.MASTER_ID)
        return NodeInstance.check_node_alive(master)

    def start(self):
        self.log.info("Start dsyncer as slave.....")
        try:
            while True:
                self.run_sub_process_to_clean_pycache_dir()
                time.sleep(2)
                now = timezone.utcnow()
                if (now - self.last_check_master_alive).total_seconds() > 10:
                    if not self.check_master_alive():
                        continue
                    self.last_check_master_alive = timezone.utcnow()
                if (now - self.last_sync_all).total_seconds() > 1800:
                    self.rsync_all()
                self.watermark, messages = Message.find_messages(self.watermark,
                                                                 type=self.filter_message_type)
                filelocs = [message.content for message in messages]
                if len(filelocs) == 0:
                    Dsyncer.TRY_NUMBER = 0
                    continue
                self.log.info("Receive these dirs to sync: %s", filelocs)
                path = format_path(os.path.commonprefix(filelocs))
                if not path.startswith(self.dags_folder):
                    Dsyncer.TRY_NUMBER = 0
                    continue
                if path == self.dags_folder:
                    self.rsync_all()
                else:
                    self.rsync_dir(path)
                Dsyncer.TRY_NUMBER = 0
        finally:
            self.terminate()


def get_dsyncer_instance():
    address = conf.get("dsyncer", "dsyncer_master").split(":", 1)
    master_hostname_or_ip = address[0]
    master_port = 22
    if len(address) == 2:
        master_port = int(address[1])
    self_hostname = socket.gethostname()
    self_ip = socket.gethostbyname(self_hostname)
    if master_hostname_or_ip in [self_hostname, self_ip]:
        return Master(master_hostname_or_ip, master_port)
    else:
        return Slave(master_hostname_or_ip, master_port)
