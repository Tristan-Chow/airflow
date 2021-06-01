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

from airflow.configuration import conf
from airflow import settings
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.exceptions import AirflowException


def get_all_files(directory, include_examples, depth):
    from airflow.utils.dag_processing import list_py_file_paths
    return list_py_file_paths(directory, include_examples=include_examples, depth=depth)


class BaseDagFileManager(LoggingMixin):
    TRY_NUMBER = 0

    def __init__(self,
                 directory=settings.DAGS_FOLDER,
                 include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
                 depth=conf.getint("scheduler", "dag_dir_list_depth"),
                 dag_dir_list_interval=conf.getint('scheduler', 'dag_dir_list_interval'),
                 log=None,
                 *args, **kwargs):
        super(BaseDagFileManager, self).__init__(*args, **kwargs)
        self.directory = directory
        self.include_examples = include_examples
        self.depth = depth
        if log:
            self._log = log
        # How often to scan the DAGs directory for new files. Default to 5 minutes.
        self.dag_dir_list_interval = dag_dir_list_interval
        self.log.info(
            "Checking for new files in %s every %s seconds", self.directory , self.dag_dir_list_interval
        )
        self.last_dag_dir_refresh_time = timezone.datetime(2000, 1, 1)
        self.base_job = None

    def handle_file_paths(self, context):
        return

    def get_all_files(self):
        return []

    def release_files(self):
        return []

    def add_files(self):
        return []

    def update_heartbeat(self):
        pass

    def registry_scheduler(self):
        pass

    def terminate(self):
        pass

    def start_server(self):
        self.log.warning(self.__class__.__name__ + " not support server!")

    def list_local_path(self):

        start_time = timezone.utcnow()
        self.log.info("Start searching local paths .... ")
        file_paths = get_all_files(self.directory, self.include_examples, self.depth)
        loop_duration = (timezone.utcnow() - start_time).total_seconds()
        self.log.info("There is %s files in local, searching token %.2f seconds",
                      len(file_paths), loop_duration)

        self.handle_file_paths({'file_paths': file_paths, 'timestamp': start_time})

        try:
            self.log.info("Removing old import errors.")
            self.clear_nonexistent_import_errors(file_paths)
        except Exception:
            self.log.exception("Error removing old import errors")

        if settings.STORE_SERIALIZED_DAGS:
            from airflow.models.serialized_dag import SerializedDagModel
            from airflow.models.dag import DagModel
            SerializedDagModel.remove_deleted_dags(file_paths)
            DagModel.deactivate_deleted_dags(file_paths)

        if settings.STORE_DAG_CODE:
            from airflow.models.dagcode import DagCode
            DagCode.remove_deleted_code(file_paths)
        return file_paths

    @provide_session
    def clear_nonexistent_import_errors(self, file_paths, session=None):
        """
        Clears import errors for files that no longer exist.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        """
        from airflow.models import ImportError
        errors = session.query(ImportError).all()
        ids = [error.id for error in errors if error.filename not in file_paths]
        session.query(ImportError).filter(ImportError.id.in_(ids)).delete(synchronize_session='fetch')
        session.commit()

    def refresh_all_files(self):
        now = timezone.utcnow()
        elapsed_time_since_refresh = (now - self.last_dag_dir_refresh_time).total_seconds()
        if elapsed_time_since_refresh < self.dag_dir_list_interval:
            return False, []
        file_paths = self.get_all_files()
        self.last_dag_dir_refresh_time = timezone.utcnow()
        return True, file_paths

    def logout(self):
        pass

    @provide_session
    def reset_state_for_orphaned_tasks(self, file_paths, session=None):
        pass


def get_dag_file_manager(log=None):
    if settings.SCHEDULER_CLUSTER:
        from airflow.cluster.db_dag_file_manager import DbDagFileManager
        return DbDagFileManager(log=log)
    else:
        from airflow.cluster.standalone_dag_file_manager import StandAloneDagFileManager
        return StandAloneDagFileManager(log=log)
