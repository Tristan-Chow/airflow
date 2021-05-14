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


from future.moves.urllib.parse import urljoin
import requests
from airflow.utils import timezone
from airflow.configuration import conf
from airflow.exceptions import AirflowException


class Client:

    def __init__(self):
        self.job_rest_server_url = conf.get("scheduler", "job_rest_server_url")

    def _request(self, url, method='GET', json=None):
        params = {
            'url': url
        }
        if json is not None:
            params['json'] = json

        resp = getattr(requests, method.lower())(**params)  # pylint: disable=not-callable
        if not resp.ok:
            # It is justified here because there might be many resp types.
            # noinspection PyBroadException
            try:
                data = resp.json()
            except Exception:  # pylint: disable=broad-except
                data = {}
            raise IOError(data.get('error', 'Server error'))

        return resp.json()

    def get_task_info(self, ti):
        endpoint = '/api/experimental/new_dags/{}/dag_runs/{}/tasks/{}'.format(ti.dag_id,
                                                                           ti.execution_date.isoformat(),
                                                                           ti.task_id)

        url = urljoin(self.job_rest_server_url, endpoint)
        rs = self._request(url, method='GET')

        if rs['code'] == 0:
            ti_info = rs['message']
            ti.start_date = timezone.parse(ti_info.get('start_date')) if ti_info.get('start_date', None) else None
            ti.end_date = timezone.parse(ti_info.get('end_date')) if ti_info.get('end_date', None) else None
            ti.queued_dttm = timezone.parse(ti_info.get('queued_dttm')) if ti_info.get('queued_dttm', None) else None
            ti.state = ti_info.get('state', None)
            ti.duration = ti_info.get('duration', None)
            ti.try_number = ti_info.get('try_number', None)
            ti.max_tries = ti_info.get('max_tries', None)
            ti.hostname = ti_info.get('hostname', None)
            ti.unixname = ti_info.get('unixname', None)
            ti.job_id = ti_info.get('job_id', None)
            ti.pool = ti_info.get('pool', None)
            ti.pool_slots = ti_info.get('pool_slots', None) or 1
            ti.queue = ti_info.get('queue', None)
            ti.priority_weight = ti_info.get('priority_weight', None)
            ti.operator = ti_info.get('operator', None)
            ti.pid = ti_info.get('pid', None)
        elif rs['code'] == 1:
            ti.state = None

    def update_heartbeat(self, job):
        endpoint = '/api/experimental/update_heartbeat/{}'.format(job.id)
        url = urljoin(self.job_rest_server_url, endpoint)
        rs = self._request(url, method='GET')
        if rs['code'] == 0:
            job.state = rs['state']
            job.latest_heartbeat = timezone.parse(rs.get('latest_heartbeat')) if rs.get('latest_heartbeat', None) else None
        else:
            raise AirflowException('Update job heartbeat failed. return {}'.format(rs))


job_client = Client()
