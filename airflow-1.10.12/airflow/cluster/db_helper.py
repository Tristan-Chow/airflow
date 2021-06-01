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
import signal
import os
import six
import multiprocessing
from six.moves import reload_module
import airflow
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.mixins import MultiprocessingStartMethodMixin


class LoadLocalFileProcess(LoggingMixin, MultiprocessingStartMethodMixin):

    def __init__(self):
        self.t = None

    def is_alive(self):
        if self.t is None or not self.t.is_alive():
            return False
        return True

    def terminate(self):
        if not self.is_alive():
            return
        self.t.terminate()
        self.t = None

    def start(self):
        if six.PY2:
            context = multiprocessing
        else:
            mp_start_method = self._get_multiprocessing_start_method()
            context = multiprocessing.get_context(mp_start_method)

        self.t = context.Process(target=type(self)._run)
        self.t.start()

    @staticmethod
    def _run():
        def exit_gracefully(signum, frame):
            sys.exit(os.EX_OK)

        signal.signal(signal.SIGINT, exit_gracefully)
        signal.signal(signal.SIGTERM, exit_gracefully)
        reload_module(airflow.settings)
        airflow.settings.initialize()
        from airflow.cluster.db_dag_file_manager import DbDagFileManager
        dfm = DbDagFileManager()
        dfm.list_local_path()
        airflow.settings.dispose_orm()
        sys.exit(os.EX_OK)
