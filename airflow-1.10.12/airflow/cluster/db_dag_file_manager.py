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
import time
import sys
import os
import signal
from airflow.cluster import BaseDagFileManager
from airflow.utils.db import provide_session
from sqlalchemy import Boolean, Column, Index, Integer, String, func, or_, desc
from airflow.utils import timezone

from airflow.exceptions import AirflowException
from tabulate import tabulate
from airflow.cluster.db_helper import LoadLocalFileProcess
from airflow.cluster.nodeinstance import NodeInstance, NODE_INSTANCE_DEAD, NODE_INSTANCE_READY, NODE_INSTANCE_RUNNING,\
                                         INSTANCE_MASTER, INSTANCE_SCHEDULER
from airflow.cluster.dag_file import DagFile, DAG_FILE_READY, DAG_FILE_ASSIGNED, DAG_FILE_RELEASE, DAG_FILE_SCHEDULED, \
                                     get_df_upsert_function
from airflow.utils.helpers import kill_all_children

DF = DagFile
NI = NodeInstance


class DbDagFileManager(BaseDagFileManager):
    MASTER_ID = 1

    def __init__(self, *args, **kwargs):
        super(DbDagFileManager, self).__init__(*args, **kwargs)
        self.node = None
        self.count_metrics_cache = {}
        self.last_check_schedulers = timezone.datetime(2000, 1, 1)
        self.last_refresh_cache = timezone.datetime(2000, 1, 1)
        self.last_update_heartbeat = timezone.datetime(2000, 1, 1)
        self.t = LoadLocalFileProcess()
        self.scheduler_id_map = {}

    @provide_session
    def registry_master(self, session=None):
        node = NI.select(id=DbDagFileManager.MASTER_ID, session=session)

        if NI.check_node_alive(node):
            session.commit()
            return False

        node = NI.select(id=DbDagFileManager.MASTER_ID, lock_for_update=True, session=session)
        if NI.check_node_alive(node):
            session.commit()
            return False

        self.node = NI.create_instance(id=DbDagFileManager.MASTER_ID, instance=INSTANCE_MASTER)
        session.merge(self.node)
        session.commit()
        return True

    @provide_session
    def update_master_heartbeat(self, session=None):
        node = NI.select(self.node.id, lock_for_update=True)
        if node.hostname != self.node.hostname:
            session.commit()
            raise AirflowException("Update Heartbeat Failed.")
        self.node.latest_heartbeat = timezone.utcnow()
        session.merge(self.node)
        session.commit()
        self.check_schedulers(refresh_cache=(timezone.utcnow()
                                             - self.last_refresh_cache).total_seconds() > 60)

    def load_files(self):
        now = timezone.utcnow()
        elapsed_time_since_refresh = (now - self.last_dag_dir_refresh_time).total_seconds()
        if elapsed_time_since_refresh > self.dag_dir_list_interval:
            self.last_dag_dir_refresh_time = now
            if not self.t.is_alive():
                self.log.info("Start a process to load all files.")
                self.t.start()

    @provide_session
    def handle_file_paths(self, context, session=None):
        upsert = get_df_upsert_function()
        start_time = timezone.utcnow()
        latest_refresh = context['timestamp']
        for fileloc in context['file_paths']:
            upsert(fileloc, latest_refresh, session)
        session.query(DF).filter(DF.latest_refresh.__lt__(latest_refresh)) \
            .update({DF.is_exist: False}, synchronize_session=False)
        session.commit()
        self.log.info("Cost %.2f to sync dag files.", (timezone.utcnow() - start_time).total_seconds())

    @provide_session
    def get_schedulers_files_count(self, alive_schedulers, session=None):
        schedulers_files_count = (
            session
                .query(DF.node_id, func.count('*'))
                .filter(DF.state.in_([DAG_FILE_ASSIGNED, DAG_FILE_SCHEDULED]))
                .filter(DF.node_id != None)
                .group_by(DF.node_id)
        ).all()
        for result in schedulers_files_count:
            node_id, count = result
            if node_id is not None:
                if node_id in alive_schedulers:
                    self.count_metrics_cache[node_id] = count
                else:
                    self.count_metrics_cache.pop(node_id, 0)

    @provide_session
    def reset_dag_file_state(self, alive_schedulers, session=None):
        session.query(DF).filter(~DF.node_id.in_(alive_schedulers)) \
            .update({DF.state: DAG_FILE_READY}, synchronize_session=False)
        session.commit()
        self.count_metrics_cache = {node_id: count for node_id, count in self.count_metrics_cache.items()
                                    if node_id in alive_schedulers}

    @provide_session
    def check_schedulers(self, refresh_cache=False, session=None):
        if refresh_cache:
            self.count_metrics_cache = {}
        schedulers = session.query(NI).filter(NI.instance == INSTANCE_SCHEDULER) \
            .filter(NI.state == NODE_INSTANCE_RUNNING)
        self.scheduler_id_map = {}
        alive_schedulers = []
        for scheduler in schedulers:
            if not NI.check_node_alive(scheduler):
                node = NI.select(scheduler.id, True, session)
                if not NI.check_node_alive(scheduler):
                    self.log.info("%s[id:%s] dead!", scheduler.hostname, scheduler.id)
                    node.state = NODE_INSTANCE_DEAD
                    self.count_metrics_cache.pop(node.id, 0)
                    session.merge(node)
                    session.commit()
                    continue
            alive_schedulers.append(scheduler.id)
            self.scheduler_id_map[scheduler.id] = scheduler.hostname
            if scheduler.id not in self.count_metrics_cache:
                self.count_metrics_cache[scheduler.id] = 0

        self.reset_dag_file_state(alive_schedulers, session)
        if refresh_cache:
            self.get_schedulers_files_count(alive_schedulers)
            self.last_refresh_cache = timezone.utcnow()

    @provide_session
    def welcome_new_scheduler(self, session=None):
        schedulers = session.query(NI).filter(NI.instance == INSTANCE_SCHEDULER) \
            .filter(NI.state == NODE_INSTANCE_READY)
        for scheduler in schedulers:
            self.log.info("%s[id:%s] join, welcome!", scheduler.hostname, scheduler.id)
            self.scheduler_id_map[scheduler.id] = scheduler.hostname
            scheduler.state = NODE_INSTANCE_RUNNING
            session.merge(scheduler)
            session.commit()
            self.count_metrics_cache[scheduler.id] = 0

    @provide_session
    def assigned_paths(self, session=None):
        if len(self.count_metrics_cache) == 0:
            return
        state_count = (
            session
                .query(DF.state, func.count('*'))
                .filter(DF.state.in_([DAG_FILE_READY, DAG_FILE_RELEASE]), DF.is_exist)
                .group_by(DF.state)
                .all())
        all_count = 0
        count_map = {}
        for state, count in state_count:
            count_map[state] = count
            all_count += count

        for scheduler, count in self.count_metrics_cache.items():
            all_count += count
        average = int(all_count // len(self.count_metrics_cache)) + 1

        nodes_count = (
            session
                .query(DF.node_id, func.count('*'))
                .filter(DF.state == DAG_FILE_ASSIGNED)
                .group_by(DF.node_id)
                .all())
        nodes_map = {}
        for node, count in nodes_count:
            nodes_map[node] = count
        if count_map.get(DAG_FILE_READY, 0) != 0:
            for scheduler, count in sorted(self.count_metrics_cache.items(), key=lambda x: x[1], reverse=False):
                if count >= average:
                    break
                if nodes_map.get(scheduler, 0) > 512:
                    continue
                limit = min(512, average - count)
                filelocs = [fileloc for fileloc, in
                            session.query(DF.fileloc).filter(DF.state == DAG_FILE_READY, DF.is_exist).limit(
                                limit).all()]
                count = 0
                for fileloc in filelocs:
                    df = session.query(DF).filter(DF.fileloc == fileloc).with_for_update().populate_existing().first()
                    if df.state == DAG_FILE_READY and df.is_exist:
                        df.node_id = scheduler
                        df.state = DAG_FILE_ASSIGNED
                        self.count_metrics_cache[scheduler] = self.count_metrics_cache[scheduler] + 1
                        session.merge(df)
                        self.log.info("Assigned %s to scheduler: %s",
                                      df.fileloc, self.scheduler_id_map.get(df.node_id, None))
                        count += 1
                    session.commit()
                if len(filelocs) < limit:
                    break
        self.balance(average, session)

    @provide_session
    def balance(self, average, session=None):
        max_count = int(average * 1.05)
        for scheduler, count in self.count_metrics_cache.items():
            if count <= max_count:
                continue
            over_count = count - average
            self.log.info("%s over average: %s", self.scheduler_id_map.get(scheduler, None), over_count)
            release_count = self.release_by_state(scheduler, DAG_FILE_ASSIGNED, DAG_FILE_READY, over_count, session)
            over_count = over_count - release_count
            if over_count > 0:
                self.release_by_state(scheduler, DAG_FILE_SCHEDULED, DAG_FILE_RELEASE, over_count, session)

    def release_by_state(self, node_id, filter_by_state, set_state, limit, session):
        release_count = 0
        filelocs = [fileloc for fileloc, in
                    session.query(DF.fileloc).filter(DF.node_id == node_id, DF.state == filter_by_state)
                    .order_by(desc(DF.fileloc)).limit(limit).all()]
        for fileloc in filelocs:
            df = session.query(DF).filter(DF.fileloc == fileloc).with_for_update().populate_existing().first()
            if df.state == filter_by_state:
                df.state = set_state
            session.merge(df)
            release_count += 1
            self.count_metrics_cache[node_id] = self.count_metrics_cache[node_id] - 1
            session.commit()
            self.log.info("Release %s from %s", fileloc, self.scheduler_id_map.get(df.node_id, None))
        return release_count

    @provide_session
    def delete(self, session=None):
        session.query(DF).filter(DF.state == DAG_FILE_READY, ~DF.is_exist).delete(synchronize_session='fetch')
        dfs = session.query(DF).filter(DF.state.in_([DAG_FILE_ASSIGNED, DAG_FILE_SCHEDULED]), ~DF.is_exist).all()
        for df in dfs:
            f = session.query(DF).filter(DF.fileloc == df.fileloc).with_for_update().populate_existing().first()
            if not f.is_exist and f.state in [DAG_FILE_ASSIGNED, DAG_FILE_SCHEDULED]:
                if f.node_id in self.count_metrics_cache:
                    self.count_metrics_cache[f.node_id] = self.count_metrics_cache[f.node_id] - 1
                if f.state == DAG_FILE_ASSIGNED:
                    session.delete(f)
                    self.log.info("Delete %s.", df.fileloc)
                else:
                    f.state = DAG_FILE_RELEASE
            session.commit()

    def start_server(self):
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        try:
            self.log.info("Standby...")
            while not self.registry_master():
                BaseDagFileManager.TRY_NUMBER = 0
                time.sleep(5)
            self.log.info("Registry successfully, become a active master...")
            while True:
                now = timezone.utcnow()
                if (now - self.last_update_heartbeat).total_seconds() > 10:
                    self.update_master_heartbeat()
                    self.last_update_heartbeat = now
                loop_start_time = time.time()
                self.load_files()
                self.welcome_new_scheduler()
                self.delete()
                self.assigned_paths()
                loop_duration = time.time() - loop_start_time
                if loop_duration > 0.5:
                    self.log.info("Ran cost %.2f seconds", loop_duration)
                if loop_duration < 1:
                    time.sleep(1 - loop_duration)
                BaseDagFileManager.TRY_NUMBER = 0
        finally:
            self.terminate()

    @provide_session
    def logout(self, session=None):
        if self.node is None:
            return
        node = NI.select(id=self.node.id, lock_for_update=True, session=session)
        if node.hostname == self.node.hostname:
            node.state = NODE_INSTANCE_DEAD
            session.merge(node)
            session.commit()
        self.node = None

    @provide_session
    def update_heartbeat(self, session=None):
        now = timezone.utcnow()
        if (now - self.last_update_heartbeat).total_seconds() < 10:
            return
        node = NI.select(id=self.node.id, lock_for_update=True, session=session)
        if node.state == NODE_INSTANCE_DEAD or node.hostname != self.node.hostname:
            session.commit()
            raise AirflowException("Update heartbeat failed.")

        self.node.state = node.state
        self.node.latest_heartbeat = timezone.utcnow()
        session.merge(self.node)
        session.commit()
        self.log.info("Update heartbeat success.")
        self.last_update_heartbeat = timezone.utcnow()

    @provide_session
    def registry_scheduler(self, session=None):
        self.node = NI.create_instance(instance=INSTANCE_SCHEDULER, state=NODE_INSTANCE_READY)
        session.add(self.node)
        session.commit()
        self.log.info("Registry successfully, id: %s.", self.node.id)

    @provide_session
    def release_files(self, session=None):
        dfs = session.query(DF.fileloc).filter(DF.node_id == self.node.id, DF.state == DAG_FILE_RELEASE).limit(
            512).all()
        result = []
        set_dfs = (
            session
            .query(DF)
            .filter(DF.fileloc.in_([fileloc for fileloc, in dfs]),
                    DF.node_id == self.node.id,
                    DF.state == DAG_FILE_RELEASE)
            .with_for_update()
            .populate_existing()
            .all())
        for df in set_dfs:
            df.state = DAG_FILE_READY
            session.merge(df)
            result.append(df.fileloc)
        session.commit()
        return result

    @provide_session
    def get_all_files(self, session=None):
        filelocs = session.query(DF.fileloc)\
            .filter(DF.node_id == self.node.id, DF.state == DAG_FILE_SCHEDULED)\
            .all()
        return [fileloc for fileloc, in filelocs]

    @provide_session
    def add_files(self, session=None):
        dfs = session.query(DF.fileloc).filter(DF.node_id == self.node.id, DF.state == DAG_FILE_ASSIGNED).limit(
            512).all()
        result = []
        set_dfs = (
            session
            .query(DF)
            .filter(DF.fileloc.in_([fileloc for fileloc, in dfs]),
                    DF.node_id == self.node.id,
                    DF.state == DAG_FILE_ASSIGNED)
            .with_for_update()
            .populate_existing()
            .all())
        for df in set_dfs:
            df.state = DAG_FILE_SCHEDULED
            session.merge(df)
            result.append(df.fileloc)
        session.commit()

        return result

    def terminate(self):
        if self.t is not None:
            self.t.terminate()
        try:
            self.logout()
        except Exception:
            pass
        kill_all_children(self.log)

    def _exit_gracefully(self, signum, frame):
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        sys.exit(os.EX_OK)

    @provide_session
    def upsert_path(self, fileloc):
        upsert = get_df_upsert_function()
        upsert(fileloc, timezone.utcnow(), None)

    def parse_datetime(self, value):
        if value is None:
            return "None"
        i = (timezone.utcnow() - value).total_seconds()
        return value.strftime('%Y-%m-%d %H:%M:%S') if i > 30 else "{} seconds ago".format(int(i))

    @provide_session
    def reset_state_for_orphaned_tasks(self, file_paths, session=None):
        from airflow.models import DagBag
        dag_ids = []
        for file_path in file_paths:
            dag_ids += DagBag(dag_folder=file_path).dag_ids
        if dag_ids:
            if self.base_job is None:
                from airflow.jobs.base_job import BaseJob
                self.base_job = BaseJob()
            self.base_job.reset_state_for_orphaned_tasks(dag_ids=dag_ids)

    @provide_session
    def print_path_info(self, fileloc, session=None):
        df = session.query(DF).filter(DF.fileloc == fileloc).first()
        if df is None:
            print("Not found this dag file.")
            return
        print("Path schedule information:")
        print(tabulate([[df.fileloc, str(df.node_id), df.state, str(df.is_exist), self.parse_datetime(df.latest_refresh)]],
                       headers=['fileloc', 'scheduler', 'state', 'is_exist', 'latest_refresh'], tablefmt="fancy_grid"))
        if df.node_id is not None:
            print("Scheduler information:")
            node = session.query(NI).filter(NI.id == df.node_id).first()
            print(tabulate([[node.id, node.hostname, node.state, self.parse_datetime(node.start_date),
                             self.parse_datetime(node.latest_heartbeat),
                             ]],
                           headers=['id', 'hostname', 'state', 'start_date', 'latest_heartbeat'], tablefmt="fancy_grid"))

    @provide_session
    def print_cluster_info(self, session):
        masters = session.query(NI).filter(NI.instance == INSTANCE_MASTER).all()
        master_info = []
        for master in masters:
            master_info.append([str(master.id), master.hostname, master.state,
                                self.parse_datetime(master.start_date),
                                self.parse_datetime(master.latest_heartbeat),
                                ])
        print("Master information:")
        print(tabulate(master_info,
                       headers=['id', 'hostname', 'state', 'start_date', 'latest_heartbeat'],
                       tablefmt="fancy_grid"))

        host_state_query = session.query(DF.node_id, DF.state, func.count('*')).group_by(DF.node_id, DF.state).all()
        host_map = {}
        state_map = dict({DAG_FILE_READY: 0, DAG_FILE_ASSIGNED: 0, DAG_FILE_RELEASE: 0, DAG_FILE_SCHEDULED: 0})
        for node_id, state, count in host_state_query:
            i = host_map.get(node_id, {})
            i[state] = count
            host_map[node_id] = i
            state_map[state] = state_map.get(state, 0) + count
        nodes = (session.query(NI)
                 .filter(or_(NI.id.in_(host_map.keys()),
                             NI.state.in_([NODE_INSTANCE_READY, NODE_INSTANCE_RUNNING])))
                 .filter(NI.instance == INSTANCE_SCHEDULER).all())

        def get_total(d):
            return d.get(DAG_FILE_READY, 0) + d.get(DAG_FILE_ASSIGNED, 0) \
                   + d.get(DAG_FILE_SCHEDULED, 0) + d.get(DAG_FILE_RELEASE, 0)
        print("Dag File Information:")
        print(tabulate([[str(state_map.get(DAG_FILE_READY)), str(state_map.get(DAG_FILE_ASSIGNED)),
                        str(state_map.get(DAG_FILE_SCHEDULED)), str(state_map.get(DAG_FILE_RELEASE)),
                        str(get_total(state_map))]],
                       headers=[DAG_FILE_READY, DAG_FILE_ASSIGNED, DAG_FILE_SCHEDULED, DAG_FILE_RELEASE, "total"],
                       tablefmt="fancy_grid"))
        print("Schedulers information:")
        schedulers_info = []
        for node in nodes:
            scheduler = [str(node.id), node.hostname, node.state, self.parse_datetime(node.start_date),
                         self.parse_datetime(node.latest_heartbeat),
                         str(host_map.get(node.id, {}).get(DAG_FILE_READY, 0)),
                         str(host_map.get(node.id, {}).get(DAG_FILE_ASSIGNED, 0)),
                         str(host_map.get(node.id, {}).get(DAG_FILE_SCHEDULED, 0)),
                         str(host_map.get(node.id, {}).get(DAG_FILE_RELEASE, 0)),
                         str(get_total(host_map.get(node.id, {})))]
            schedulers_info.append(scheduler)
        print(tabulate(schedulers_info,
                       headers=['id', 'hostname', 'state', 'start_date', 'latest_heartbeat',
                                DAG_FILE_READY, DAG_FILE_ASSIGNED, DAG_FILE_SCHEDULED, DAG_FILE_RELEASE, "total"],
                       tablefmt="fancy_grid"))
