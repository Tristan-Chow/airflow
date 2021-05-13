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
from airflow.utils.state import State
import json
import logging
import importlib
import traceback
from airflow.configuration import conf
from airflow.utils import timezone

DR_SUCCESS = 'dr_success'
DR_FAILED = 'dr_failed'
DR_SLAS = 'dr_slas_miss'
TI_SUCCESS = 'ti_success'
TI_FAILED = 'ti_failed'
TI_RETRY = 'ti_retry'
TI_SLAS = 'ti_slas_miss'
TI_EXECUTION_SLA = 'ti_execution_sla'

ti_state_map = {State.SUCCESS: TI_SUCCESS,
                State.FAILED: TI_FAILED,
                State.UP_FOR_RETRY: TI_RETRY}

log = logging.getLogger(__name__)


def get_event_action_callable(callable_config):
    path, attr = conf.get('core', callable_config).rsplit('.', 1)
    module = importlib.import_module(path)
    backend = getattr(module, attr)
    return backend


def datetime_f(p):
    dttm = p.strftime('%Y-%m-%d %H:%M:%S') if p else ''
    return dttm


def default_dag_run_callable(event, dag, dag_run, extend_msg=None):
    msg = {'notifyId': dag.notify_id,
           'dagId': dag.dag_id,
           'state': dag_run.state,
           'executionDate': datetime_f(dag_run.execution_date),
           'event': event,
           'timestamp': datetime_f(timezone.utcnow())}

    if dag_run.start_date is not None:
        msg['startDate'] = datetime_f(dag_run.start_date)

    if dag_run.state != State.RUNNING and dag_run.end_date is not None:
        msg['endDate'] = datetime_f(dag_run.end_date)

    if extend_msg is not None:
        for k, v in extend_msg.items():
            msg[k] = v

    log.info(json.dumps(msg))


def default_task_instance_callable(event, task, task_instance, context=None, extend_msg=None):
    msg = {'notifyId': task.notify_id,
           'dagId': task.dag_id,
           'taskId': task.task_id,
           'state': task_instance.state,
           'executionDate': datetime_f(task_instance.execution_date),
           'maxTries': task_instance.max_tries + 1,
           'event': event,
           'timestamp': datetime_f(timezone.utcnow())}

    if task_instance.state == State.RUNNING:
        msg['tryNumber'] = task_instance.try_number
    else:
        msg['tryNumber'] = task_instance.try_number - 1

    if task_instance.start_date is not None:
        msg['startDate'] = datetime_f(task_instance.start_date)

    if task_instance.state != State.RUNNING and task_instance.end_date is not None:
        msg['endDate'] = datetime_f(task_instance.end_date)

    if context and 'exception' in context:
        msg['exception'] = str(context['exception']).replace('\n', '<br>').replace("\"", "`")

    if extend_msg is not None:
        for k, v in extend_msg.items():
            msg[k] = v

    log.info(json.dumps(msg))


def do_dr_action(event, dag, dag_run, extend_msg=None, *args, **kwargs):
    if event not in dag.notify_events or dag.notify_id is None or dag.notify_id.strip() == '':
        return
    try:
        _callable = get_event_action_callable("event_action_dag_run_callable")
        _callable(event, dag, dag_run, extend_msg)
    except Exception:
        log.error("Call method %s failed. %s", conf.get('core', "event_action_dag_run_callable"),
                  traceback.format_exc())


def do_ti_action(event, task, task_instance, context=None, extend_msg=None, *args, **kwargs):
    if event not in task.notify_events or task.notify_id is None or task.notify_id.strip() == '':
        return

    try:
        _callable = get_event_action_callable("event_action_task_instance_callable")
        _callable(event, task, task_instance, context, extend_msg)
    except Exception:
        log.error("Call method %s failed. %s", conf.get('core', "event_action_task_instance_callable"),
                  traceback.format_exc())

