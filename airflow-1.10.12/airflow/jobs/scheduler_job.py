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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import timedelta
from time import sleep
import traceback

from past.builtins import basestring
import six
from setproctitle import setproctitle
from sqlalchemy import and_, func, not_, or_
from sqlalchemy.orm.session import make_transient

from airflow.configuration import conf
from airflow import executors, models, settings
from airflow.exceptions import AirflowException, TaskNotFound
from airflow.jobs.base_job import BaseJob
from airflow.models import DagRun, SlaMiss, errors
from airflow.settings import Stats
from airflow.ti_deps.dep_context import DepContext, SCHEDULEABLE_STATES, SCHEDULED_DEPS
from airflow.operators.dummy_operator import DummyOperator
from airflow.ti_deps.deps.pool_slots_available_dep import STATES_TO_COUNT_AS_RUNNING
from airflow.utils import asciiart, helpers, timezone
from airflow.utils.dag_processing import (AbstractDagFileProcessor,
                                          DagFileProcessorAgent,
                                          SimpleDag,
                                          SimpleDagBag,
                                          SimpleTaskInstance,
                                          list_py_file_paths)
from airflow.utils.db import provide_session
from airflow.utils.email import get_email_address_list, send_email
from airflow.utils.mixins import MultiprocessingStartMethodMixin
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter, set_context
from airflow.utils.state import State
from airflow import event_action
from airflow.models.simpledag import SimpleDagModel
from airflow.cluster.nodeinstance import NodeInstance, INSTANCE_SCHEDULER_LEADER, NODE_INSTANCE_DEAD
from airflow.utils.trigger_rule import TriggerRule


class DagFileProcessor(AbstractDagFileProcessor, LoggingMixin, MultiprocessingStartMethodMixin):
    """Helps call SchedulerJob.process_file() in a separate process.

    :param file_path: a Python file containing Airflow DAG definitions
    :type file_path: unicode
    :param pickle_dags: whether to serialize the DAG objects to the DB
    :type pickle_dags: bool
    :param dag_ids: If specified, only look at these DAG ID's
    :type dag_ids: list[unicode]
    :param zombies: zombie task instances to kill
    :type zombies: list[airflow.utils.dag_processing.SimpleTaskInstance]
    """

    # Counter that increments every time an instance of this class is created
    class_creation_counter = 0

    def __init__(self, file_path, pickle_dags, dag_ids, zombies):
        self._file_path = file_path

        # The process that was launched to process the given .
        self._process = None
        self._dag_ids = dag_ids
        self._pickle_dags = pickle_dags
        self._zombies = zombies
        # The result of Scheduler.process_file(file_path).
        self._result = None
        # Whether the process is done running.
        self._done = False
        # When the process started.
        self._start_time = None
        # This ID is use to uniquely name the process / thread that's launched
        # by this processor instance
        self._instance_id = DagFileProcessor.class_creation_counter
        DagFileProcessor.class_creation_counter += 1

    @property
    def file_path(self):
        return self._file_path

    @staticmethod
    def _run_file_processor(result_channel,
                            file_path,
                            pickle_dags,
                            dag_ids,
                            thread_name,
                            zombies):
        """
        Process the given file.

        :param result_channel: the connection to use for passing back the result
        :type result_channel: multiprocessing.Connection
        :param file_path: the file to process
        :type file_path: unicode
        :param pickle_dags: whether to pickle the DAGs found in the file and
            save them to the DB
        :type pickle_dags: bool
        :param dag_ids: if specified, only examine DAG ID's that are
            in this list
        :type dag_ids: list[unicode]
        :param thread_name: the name to use for the process that is launched
        :type thread_name: unicode
        :param zombies: zombie task instances to kill
        :type zombies: list[airflow.utils.dag_processing.SimpleTaskInstance]
        :return: the process that was launched
        :rtype: multiprocessing.Process
        """
        # This helper runs in the newly created process
        log = logging.getLogger("airflow.processor")

        stdout = StreamLogWriter(log, logging.INFO)
        stderr = StreamLogWriter(log, logging.WARN)

        set_context(log, file_path)
        setproctitle("airflow scheduler - DagFileProcessor {}".format(file_path))

        try:
            # redirect stdout/stderr to log
            sys.stdout = stdout
            sys.stderr = stderr

            # Re-configure the ORM engine as there are issues with multiple processes
            settings.configure_orm()

            # Change the thread name to differentiate log lines. This is
            # really a separate process, but changing the name of the
            # process doesn't work, so changing the thread name instead.
            threading.current_thread().name = thread_name
            start_time = time.time()

            log.info("Started process (PID=%s) to work on %s",
                     os.getpid(), file_path)
            scheduler_job = SchedulerJob(dag_ids=dag_ids, log=log)
            result = scheduler_job.process_file(file_path,
                                                zombies,
                                                pickle_dags)
            result_channel.send(result)
            end_time = time.time()
            log.info(
                "Processing %s took %.3f seconds", file_path, end_time - start_time
            )
        except Exception:
            # Log exceptions through the logging framework.
            log.exception("Got an exception! Propagating...")
            raise
        finally:
            result_channel.close()
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            # We re-initialized the ORM within this Process above so we need to
            # tear it down manually here
            settings.dispose_orm()

    def start(self):
        """
        Launch the process and start processing the DAG.
        """
        if six.PY2:
            context = multiprocessing
        else:
            start_method = self._get_multiprocessing_start_method()
            context = multiprocessing.get_context(start_method)

        self._parent_channel, _child_channel = context.Pipe()
        self._process = context.Process(
            target=type(self)._run_file_processor,
            args=(
                _child_channel,
                self.file_path,
                self._pickle_dags,
                self._dag_ids,
                "DagFileProcessor{}".format(self._instance_id),
                self._zombies
            ),
            name="DagFileProcessor{}-Process".format(self._instance_id)
        )
        self._start_time = timezone.utcnow()
        self._process.start()

    def kill(self):
        """
        Kill the process launched to process the file, and ensure consistent state.
        """
        if self._process is None:
            raise AirflowException("Tried to kill before starting!")
        # The queue will likely get corrupted, so remove the reference
        self._result_queue = None
        self._kill_process()

    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        :type sigkill: bool
        """
        if self._process is None:
            raise AirflowException("Tried to call terminate before starting!")

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
        self._parent_channel.close()

    def _kill_process(self):
        if self._process.is_alive():
            self.log.warning("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    @property
    def pid(self):
        """
        :return: the PID of the process launched to process the given file
        :rtype: int
        """
        if self._process is None:
            raise AirflowException("Tried to get PID before starting!")
        return self._process.pid

    @property
    def exit_code(self):
        """
        After the process is finished, this can be called to get the return code

        :return: the exit code of the process
        :rtype: int
        """
        if not self._done:
            raise AirflowException("Tried to call retcode before process was finished!")
        return self._process.exitcode

    @property
    def done(self):
        """
        Check if the process launched to process this file is done.

        :return: whether the process is finished running
        :rtype: bool
        """
        if self._process is None:
            raise AirflowException("Tried to see if it's done before starting!")

        if self._done:
            return True

        if self._parent_channel.poll():
            try:
                self._result = self._parent_channel.recv()
                self._done = True
                self.log.debug("Waiting for %s", self._process)
                self._process.join()
                self._parent_channel.close()
                return True
            except EOFError:
                pass

        if not self._process.is_alive():
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            self._parent_channel.close()
            return True

        return False

    @property
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: airflow.utils.dag_processing.SimpleDag
        """
        if not self.done:
            raise AirflowException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self):
        """
        :return: when this started to process the file
        :rtype: datetime
        """
        if self._start_time is None:
            raise AirflowException("Tried to get start time before it started!")
        return self._start_time


SCHEDULER_POOL = "scheduler_pool"
SUB_DAG_POOL = "sub_dag_pool"


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs for a specific time interval and schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and sees if the dependencies for the next schedules are met.
    If so, it creates appropriate TaskInstances and sends run commands to the
    executor. It does this for each task in each DAG and repeats.
   :param dag_id: if specified, only schedule tasks with this DAG ID
    :type dag_id: unicode
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :type dag_ids: list[unicode]
    :param subdir: directory containing Python files with Airflow DAG
        definitions, or a specific path to a file
    :type subdir: unicode
    :param num_runs: The number of times to try to schedule each DAG file.
        -1 for unlimited times.
    :type num_runs: int
    :param processor_poll_interval: The number of seconds to wait between
        polls of running processors
    :type processor_poll_interval: int
    :param run_duration: how long to run (in seconds) before exiting
    :type run_duration: int
    :param do_pickle: once a DAG object is obtained by executing the Python
        file, whether to serialize the DAG object to the DB
    :type do_pickle: bool
    """
    MASTER_ID = 3
    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }
    heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')

    def __init__(
            self,
            dag_id=None,
            dag_ids=None,
            subdir=settings.DAGS_FOLDER,
            num_runs=conf.getint('scheduler', 'num_runs', fallback=-1),
            processor_poll_interval=conf.getfloat(
                'scheduler', 'processor_poll_interval', fallback=1),
            run_duration=None,
            do_pickle=False,
            log=None,
            *args, **kwargs):
        """
        :param dag_id: if specified, only schedule tasks with this DAG ID
        :type dag_id: unicode
        :param dag_ids: if specified, only schedule tasks with these DAG IDs
        :type dag_ids: list[unicode]
        :param subdir: directory containing Python files with Airflow DAG
            definitions, or a specific path to a file
        :type subdir: unicode
        :param num_runs: The number of times to try to schedule each DAG file.
            -1 for unlimited times.
        :type num_runs: int
        :param processor_poll_interval: The number of seconds to wait between
            polls of running processors
        :type processor_poll_interval: int
        :param do_pickle: once a DAG object is obtained by executing the Python
            file, whether to serialize the DAG object to the DB
        :type do_pickle: bool
        """
        # for BaseJob compatibility
        self.dag_id = dag_id
        self.dag_ids = [dag_id] if dag_id else []
        if dag_ids:
            self.dag_ids.extend(dag_ids)

        self.subdir = subdir

        self.num_runs = num_runs
        self.run_duration = run_duration
        self._processor_poll_interval = processor_poll_interval

        self.do_pickle = do_pickle
        super(SchedulerJob, self).__init__(*args, **kwargs)

        self.max_threads = conf.getint('scheduler', 'max_threads')

        if log:
            self._log = log

        self.using_sqlite = False
        self.using_mysql = False
        if conf.get('core', 'sql_alchemy_conn').lower().startswith('sqlite'):
            self.using_sqlite = True
        if conf.get('core', 'sql_alchemy_conn').lower().startswith('mysql'):
            self.using_mysql = True

        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        self.processor_agent = None

        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        if run_duration is None:
            self.run_duration = conf.getint('scheduler',
                                            'run_duration')

        self.processor_agent = None
        self.is_leader = False
        self.node = None
        self.ignore_pool = [SCHEDULER_POOL, SUB_DAG_POOL]
        self.parallelism = self.executor.parallelism
        self.last_check_abnormal_state = timezone.datetime(2000, 1, 1)

        self.parent_dag_contain_sub_dags_map = {}  # {dag_id: set([sub_dag_ids])}

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    def is_alive(self, grace_multiplier=None):
        """
        Is this SchedulerJob alive?

        We define alive as in a state of running and a heartbeat within the
        threshold defined in the ``scheduler_health_check_threshold`` config
        setting.

        ``grace_multiplier`` is accepted for compatibility with the parent class.

        :rtype: boolean
        """
        if grace_multiplier is not None:
            # Accept the same behaviour as superclass
            return super(SchedulerJob, self).is_alive(grace_multiplier=grace_multiplier)
        scheduler_health_check_threshold = conf.getint('scheduler', 'scheduler_health_check_threshold')
        return (
            self.state == State.RUNNING and
            (timezone.utcnow() - self.latest_heartbeat).total_seconds() < scheduler_health_check_threshold
        )

    @provide_session
    def manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        We are assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        if not any([isinstance(ti.sla, timedelta) for ti in dag.tasks]):
            self.log.info("Skipping SLA check for %s because no tasks in DAG have SLAs", dag)
            return

        # This is a temporary fix for 1.10.4 release.
        # Background: AIRFLOW-4297
        # TODO: refactor manage_slas() to handle related issues.
        if dag.normalized_schedule_interval is None:
            self.log.info("SLA check for DAGs with schedule_interval 'None'/'@once' are "
                          "skipped in 1.10.4, due to related refactoring going on.")
            return

        TI = models.TaskInstance
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .with_hint(TI, 'USE INDEX (PRIMARY)', dialect_name='mysql')
            .filter(TI.dag_id == dag.dag_id)
            .filter(or_(
                TI.state == State.SUCCESS,
                TI.state == State.SKIPPED))
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        ).all()

        ts = timezone.utcnow()
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if isinstance(task.sla, timedelta):
                dttm = dag.following_schedule(dttm)
                while dttm < timezone.utcnow():
                    if dttm + task.sla < timezone.utcnow():
                        session.merge(SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.notification_sent == False, SlaMiss.dag_id == dag.dag_id)  # noqa pylint: disable=singleton-comparison
            .all()
        )

        if slas:
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(
                    TI.state != State.SUCCESS,
                    TI.execution_date.in_(sla_dates),
                    TI.dag_id == dag.dag_id
                ).all()
            )
            blocking_tis = []
            for ti in qry:
                if ti.task_id in dag.task_ids:
                    ti.task = dag.get_task(ti.task_id)
                    blocking_tis.append(ti)
                else:
                    session.delete(ti)
                    session.commit()

            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            # Track whether email or any alert notification sent
            # We consider email or the alert callback as notifications
            email_sent = False
            notification_sent = False
            if dag.sla_miss_callback:
                # Execute the alert callback
                self.log.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                try:
                    dag.sla_miss_callback(dag, task_list, blocking_task_list, slas,
                                          blocking_tis)
                    notification_sent = True
                except Exception:
                    self.log.exception("Could not call sla_miss_callback for DAG %s",
                                       dag.dag_id)
            email_content = """\
            Here's a list of tasks that missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(task_list=task_list, blocking_task_list=blocking_task_list,
                       bug=asciiart.bug)

            tasks_missed_sla = []
            for sla in slas:
                try:
                    task = dag.get_task(sla.task_id)
                except TaskNotFound:
                    # task already deleted from DAG, skip it
                    self.log.warning(
                        "Task %s doesn't exist in DAG anymore, skipping SLA miss notification.",
                        sla.task_id)
                    continue
                tasks_missed_sla.append(task)

            emails = set()
            for task in tasks_missed_sla:
                if task.email:
                    if isinstance(task.email, basestring):
                        emails |= set(get_email_address_list(task.email))
                    elif isinstance(task.email, (list, tuple)):
                        emails |= set(task.email)
            if emails:
                try:
                    send_email(
                        emails,
                        "[airflow] SLA miss on DAG=" + dag.dag_id,
                        email_content)
                    email_sent = True
                    notification_sent = True
                except Exception:
                    self.log.exception("Could not send SLA Miss email notification for"
                                       " DAG %s", dag.dag_id)
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)

                    try:
                        task = dag.get_task(sla.task_id)
                        ti = [t for t in blocking_tis if t.task_id == sla.task_id][0]
                        event_action.do_ti_action(event_action.TI_SLAS, task, ti)
                    except Exception as e:
                        self.log.error('Executing event action error for %s.%s.%s sla miss event',
                                       dag.dag_id, sla.task_id, sla.execution_date)
                        self.log.exception(e)

            session.commit()

    @provide_session
    def manage_dag_notify_slas(self, dag, session=None):
        if dag.notify_sla is None:
            self.log.info("Skip dag notify slas miss.")
            return

        latest_dr = dag.get_last_dagrun(session=session)

        if latest_dr is None or latest_dr.state == State.SUCCESS:
            return

        mt = latest_dr.execution_date + dag.notify_sla
        now = timezone.utcnow()
        if now < mt:
            return

        DS = models.DagSlaMiss
        dsla = session.query(DS).filter(DS.dag_id == dag.dag_id,
                                        DS.execution_date == latest_dr.execution_date,
                                        ~DS.notification_sent).first()

        if dsla is not None:
            return

        event_action.do_dr_action(event_action.DR_SLAS, dag, latest_dr, extend_msg={
            'msg': '{}.{} sla miss. sla: {}'.format(dag.dag_id, latest_dr.execution_date, str(dag.notify_sla))
        })

        session.merge(DS(dag_id=dag.dag_id,
                         execution_date=latest_dr.execution_date,
                         notification_sent=True,
                         timestamp=timezone.utcnow()))

        session.commit()

    @staticmethod
    def update_import_errors(session, dagbag):
        """
        For the DAGs in the given DagBag, record any associated import errors and clears
        errors for files that no longer have them. These are usually displayed through the
        Airflow UI so that users know that there are issues parsing DAGs.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        :param dagbag: DagBag containing DAGs with import errors
        :type dagbag: airflow.models.DagBag
        """
        # Clear the errors of the processed files
        for dagbag_file in dagbag.file_last_changed:
            session.query(errors.ImportError).filter(
                errors.ImportError.filename == dagbag_file
            ).delete()

        # Add the errors of the processed files
        for filename, stacktrace in six.iteritems(dagbag.import_errors):
            session.add(errors.ImportError(
                filename=filename,
                timestamp=timezone.utcnow(),
                stacktrace=stacktrace))
        session.commit()

    @provide_session
    def create_dag_run(self, dag, session=None):
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval.
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        if dag.schedule_interval and conf.getboolean('scheduler', 'USE_JOB_SCHEDULE'):
            active_runs = DagRun.find(
                dag_id=dag.dag_id,
                state=State.RUNNING,
                external_trigger=False,
                session=session
            )
            # return if already reached maximum active runs and no timeout setting
            if len(active_runs) >= dag.max_active_runs and not dag.dagrun_timeout:
                return
            timedout_runs = 0
            for dr in active_runs:
                if (
                        dr.start_date and dag.dagrun_timeout and
                        dr.start_date < timezone.utcnow() - dag.dagrun_timeout):
                    dr.state = State.FAILED
                    dr.end_date = timezone.utcnow()
                    dag.handle_callback(dr, success=False, reason='dagrun_timeout',
                                        session=session)
                    timedout_runs += 1
            session.commit()
            if len(active_runs) - timedout_runs >= dag.max_active_runs:
                return

            # this query should be replaced by find dagrun
            qry = (
                session.query(func.max(DagRun.execution_date))
                .filter_by(dag_id=dag.dag_id)
                .filter(or_(
                    DagRun.external_trigger == False,  # noqa: E712 pylint: disable=singleton-comparison
                    # add % as a wildcard for the like query
                    DagRun.run_id.like(DagRun.ID_PREFIX + '%')
                ))
            )
            last_scheduled_run = qry.scalar()

            # don't schedule @once again
            if dag.schedule_interval == '@once' and last_scheduled_run:
                return None

            # don't do scheduler catchup for dag's that don't have dag.catchup = True
            if not (dag.catchup or dag.schedule_interval == '@once'):
                # The logic is that we move start_date up until
                # one period before, so that timezone.utcnow() is AFTER
                # the period end, and the job can be created...
                now = timezone.utcnow()
                next_start = dag.following_schedule(now)
                last_start = dag.previous_schedule(now)
                if next_start <= now or isinstance(dag.schedule_interval, timedelta):
                    new_start = last_start
                else:
                    new_start = dag.previous_schedule(last_start)

                if dag.start_date:
                    if new_start >= dag.start_date:
                        dag.start_date = new_start
                else:
                    dag.start_date = new_start

            next_run_date = None
            if not last_scheduled_run:
                # First run
                task_start_dates = [t.start_date for t in dag.tasks]
                if task_start_dates:
                    next_run_date = dag.normalize_schedule(min(task_start_dates))
                    self.log.debug(
                        "Next run date based on tasks %s",
                        next_run_date
                    )
            else:
                next_run_date = dag.following_schedule(last_scheduled_run)

            # make sure backfills are also considered
            last_run = dag.get_last_dagrun(session=session)
            if last_run and next_run_date:
                while next_run_date <= last_run.execution_date:
                    next_run_date = dag.following_schedule(next_run_date)

            # don't ever schedule prior to the dag's start_date
            if dag.start_date:
                next_run_date = (dag.start_date if not next_run_date
                                 else max(next_run_date, dag.start_date))
                if next_run_date == dag.start_date:
                    next_run_date = dag.normalize_schedule(dag.start_date)

                self.log.debug(
                    "Dag start date: %s. Next run date: %s",
                    dag.start_date, next_run_date
                )

            # don't ever schedule in the future or if next_run_date is None
            if not next_run_date or next_run_date > timezone.utcnow():
                return

            # this structure is necessary to avoid a TypeError from concatenating
            # NoneType
            if dag.schedule_interval == '@once':
                period_end = next_run_date
            elif next_run_date:
                period_end = next_run_date

            # Don't schedule a dag beyond its end_date (as specified by the dag param)
            if next_run_date and dag.end_date and next_run_date > dag.end_date:
                return

            # Don't schedule a dag beyond its end_date (as specified by the task params)
            # Get the min task end date, which may come from the dag.default_args
            min_task_end_date = []
            task_end_dates = [t.end_date for t in dag.tasks if t.end_date]
            if task_end_dates:
                min_task_end_date = min(task_end_dates)
            if next_run_date and min_task_end_date and next_run_date > min_task_end_date:
                return

            if next_run_date and period_end and period_end <= timezone.utcnow():
                next_run = dag.create_dagrun(
                    run_id=DagRun.ID_PREFIX + next_run_date.isoformat(),
                    execution_date=next_run_date,
                    start_date=timezone.utcnow(),
                    state=State.RUNNING,
                    external_trigger=False
                )
                return next_run

    @provide_session
    def _process_task_instances(self, dagbag, dag, task_instances_list, session=None):
        """
        This method schedules the tasks for a single DAG by looking at the
        active DAG runs and adding task instances that should run to the
        queue.
        """

        # update the state of the previously active dag runs
        dag_runs = DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)
        active_dag_runs = []
        for run in dag_runs:
            self.log.info("Examining DAG run %s", run)
            # don't consider runs that are executed in the future unless
            # specified by config and schedule_interval is None
            if run.execution_date > timezone.utcnow() and not dag.allow_future_exec_dates:
                self.log.error(
                    "Execution date is in future: %s",
                    run.execution_date
                )
                continue

            if len(active_dag_runs) >= dag.max_active_runs:
                self.log.info("Number of active dag runs reached max_active_run.")
                break

            # skip backfill dagruns for now as long as they are not really scheduled
            if not settings.SCHEDULE_BACKFILL_IN_SCHEDULER and run.is_backfill:
                continue

            # todo: run.dag is transient but needs to be set
            run.dag = dag
            # todo: preferably the integrity check happens at dag collection time
            run.verify_integrity(session=session)
            run.update_state(session=session)
            if run.state == State.RUNNING:
                make_transient(run)
                active_dag_runs.append(run)
                self._process_active_run(dag,
                                         run,
                                         task_instances_list,
                                         session)
                if settings.SCHEDULE_BACKFILL_IN_SCHEDULER:
                    self._process_sub_dag_task_instances(dagbag,
                                                         run,
                                                         task_instances_list,
                                                         session)

    def _process_active_run(self,
                            dag,
                            run,
                            task_instances_list,
                            session=None):
        self.log.debug("Examining active DAG run: %s", run)
        tis = run.get_task_instances(state=SCHEDULEABLE_STATES)

        task_to_downstream_ids = dict()
        task_to_upstream_ids = dict()
        for ti in tis:
            ti.task = dag.get_task(ti.task_id)
            upstream_list_set = set()
            downstream_list_set = set()
            for upstream_task_id in ti.task.upstream_task_ids:
                if ti.task_id != upstream_task_id:
                    upstream_list_set.add(upstream_task_id)
            task_to_upstream_ids[ti.task_id] = (ti, upstream_list_set)
            for downstream_task_id in ti.task.downstream_task_ids:
                if ti.task_id != downstream_task_id and \
                        dag.get_task(downstream_task_id).trigger_rule in {TriggerRule.ALL_SUCCESS,
                                                                          TriggerRule.ALL_FAILED,
                                                                          TriggerRule.ALL_DONE,
                                                                          TriggerRule.NONE_FAILED,
                                                                          TriggerRule.NONE_FAILED_OR_SKIPPED,
                                                                          TriggerRule.NONE_SKIPPED}:
                    downstream_list_set.add(downstream_task_id)
            task_to_downstream_ids[ti.task_id] = downstream_list_set
        all_scheduleable_task_ids = set(task_to_upstream_ids.keys())
        # sort by dependencies
        tis = []
        while True:
            task_ids = set(task_to_upstream_ids.keys())
            if len(task_ids) == 0:
                break
            this_loop_add_task_ids = []
            for this_task_id, ti_to_upstream_list in task_to_upstream_ids.items():
                ti, upstream_list_set = ti_to_upstream_list
                if len(task_ids & upstream_list_set) == 0:
                    tis.append(ti)
                    this_loop_add_task_ids.append(this_task_id)
            for this_task_id in this_loop_add_task_ids:
                del task_to_upstream_ids[this_task_id]

        ignore_task_ids = set()

        def add_task_ids_to_ignore(task_id):
            if task_id not in task_to_downstream_ids:
                return
            downstream_ids = task_to_downstream_ids.pop(task_id)
            for downstream_id in downstream_ids:
                ignore_task_ids.add(downstream_id)
                add_task_ids_to_ignore(downstream_id)

        # this loop is quite slow as it uses are_dependencies_met for
        # every task (in ti.is_runnable). This is also called in
        # update_state above which has already checked these tasks
        for ti in tis:
            if ti.task_id in ignore_task_ids:
                continue
            if ti.task.trigger_rule in {TriggerRule.ONE_FAILED, TriggerRule.ONE_SUCCESS} and \
                    len(ti.task.upstream_task_ids) > 0 and \
                    len(ti.task.upstream_task_ids) == len(set(ti.task.upstream_task_ids) & all_scheduleable_task_ids):
                add_task_ids_to_ignore(ti.task_id)
                continue
            if ti.are_dependencies_met(
                    dep_context=DepContext(flag_upstream_failed=True),
                    session=session):
                self.log.debug('Queuing task: %s', ti)
                task_instances_list.append(ti.key)

            if ti.state in {State.NONE,
                            State.SCHEDULED,
                            State.QUEUED,
                            State.RUNNING,
                            State.UP_FOR_RETRY,
                            State.UP_FOR_RESCHEDULE}:
                add_task_ids_to_ignore(ti.task_id)
            else:
                all_scheduleable_task_ids.remove(ti.task_id)

    @provide_session
    def _change_state_for_tis_without_dagrun(self,
                                             simple_dag_bag,
                                             old_states,
                                             new_state,
                                             check_all=False,
                                             session=None):
        """
        For all DAG IDs in the SimpleDagBag, look for task instances in the
        old_states and set them to new_state if the corresponding DagRun
        does not exist or exists but is not in the running state. This
        normally should not happen, but it can if the state of DagRuns are
        changed manually.

        :param old_states: examine TaskInstances in this state
        :type old_states: list[airflow.utils.state.State]
        :param new_state: set TaskInstances to this state
        :type new_state: airflow.utils.state.State
        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag and with states in the old_states will be examined
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        """
        tis_changed = 0
        query = session \
            .query(models.TaskInstance) \
            .outerjoin(models.DagRun, and_(
               models.TaskInstance.dag_id == models.DagRun.dag_id,
               models.TaskInstance.execution_date == models.DagRun.execution_date)) \
            .filter(models.TaskInstance.state.in_(old_states)) \
            .filter(or_(
                models.DagRun.state != State.RUNNING,
                models.DagRun.state.is_(None)))
        if not check_all:
            query.filter(models.TaskInstance.dag_id.in_(simple_dag_bag.dag_ids))

        # We need to do this for mysql as well because it can cause deadlocks
        # as discussed in https://issues.apache.org/jira/browse/AIRFLOW-2516
        if self.using_sqlite:
            tis_to_change = query \
                .with_for_update() \
                .all()
            for ti in tis_to_change:
                ti.set_state(new_state, session=session)
                tis_changed += 1
        else:
            tis = query.all()
            tis_changed = len(tis)
            if tis_changed > 0:
                for ti in tis:
                    session.query(models.TaskInstance) \
                        .filter(and_(
                            models.TaskInstance.dag_id == ti.dag_id,
                            models.TaskInstance.task_id == ti.task_id,
                            models.TaskInstance.execution_date == ti.execution_date)) \
                        .update({models.TaskInstance.state: new_state},
                                synchronize_session=False)
                    session.commit()

        if tis_changed > 0:
            self.log.warning(
                "Set %s task instances to state=%s as their associated DagRun was not in RUNNING state",
                tis_changed, new_state
            )
            Stats.gauge('scheduler.tasks.without_dagrun', tis_changed)

    @provide_session
    def __get_concurrency_maps(self, states, session=None):
        """
        Get the concurrency maps.

        :param states: List of states to query for
        :type states: list[airflow.utils.state.State]
        :return: A map from (dag_id, task_id) to # of task instances and
         a map from (dag_id, task_id) to # of task instances in the given state list
        :rtype: dict[tuple[str, str], int]

        """
        TI = models.TaskInstance
        ti_concurrency_query = (
            session
            .query(TI.task_id, TI.dag_id, func.count('*'))
            .filter(TI.state.in_(states))
            .group_by(TI.task_id, TI.dag_id)
        ).all()
        dag_map = defaultdict(int)
        task_map = defaultdict(int)
        for result in ti_concurrency_query:
            task_id, dag_id, count = result
            dag_map[dag_id] += count
            task_map[(dag_id, task_id)] = count
        return dag_map, task_map

    @provide_session
    def _find_executable_task_instances_by_orm(self, states, session=None):
        from airflow.jobs.backfill_job import BackfillJob  # Avoid circular import
        simple_dag_bag = SimpleDagBag([])
        executable_tis = []

        TI = models.TaskInstance
        DR = models.DagRun
        DM = models.DagModel

        ti_query = (
            session
                .query(TI.pool, func.count('*'))
                .outerjoin(
                DR,
                and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date))
                .filter(not_(TI.pool.in_(self.ignore_pool)))
                .filter(DR.state == State.RUNNING)
        )

        if not settings.SCHEDULE_BACKFILL_IN_SCHEDULER:
            ti_query = ti_query.filter(or_(DR.run_id == None,  # noqa: E711 pylint: disable=singleton-comparison
                                       not_(DR.run_id.like(BackfillJob.ID_PREFIX + '%'))))

        ti_query = (ti_query.outerjoin(DM, DM.dag_id == TI.dag_id)
                            .filter(or_(DM.dag_id == None,  # noqa: E711 pylint: disable=singleton-comparison
                                    not_(DM.is_paused)))
                            .group_by(TI.pool))

        # Additional filters on task instance state
        if None in states:
            ti_query = ti_query.filter(
                or_(TI.state == None, TI.state.in_(states))  # noqa: E711 pylint: disable=singleton-comparison
            )
        else:
            ti_query = ti_query.filter(TI.state.in_(states))

        task_instances_to_examine = {pool: count for pool, count in ti_query.all()}

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return simple_dag_bag, executable_tis

        remain_count = self.max_tis_per_query
        if self.parallelism:
            running_count = session.query(func.count('*')) \
                .filter(TI.state.in_(STATES_TO_COUNT_AS_RUNNING)) \
                .filter(not_(TI.pool.in_(self.ignore_pool))) \
                .first()[0]
            remain_count = min(self.parallelism - running_count, remain_count)
            if remain_count <= 0:
                self.log.info(
                    "Not scheduling since there are %s in running, and remain slots are %s",
                    running_count, remain_count
                )
                return simple_dag_bag, executable_tis

        pools = {p.pool: p.slots for p in session.query(models.Pool)
            .filter(models.Pool.pool.in_(task_instances_to_examine.keys()))
            .all()}

        pool_slots = {}
        for pool, count in task_instances_to_examine.items():
            if pool not in pools:
                self.log.warning(
                    "Tasks using non-existent pool '%s' will not be scheduled",
                    pool
                )
                continue
            pool_slots[pool] = pools[pool]

        if len(pool_slots) == 0:
            return simple_dag_bag, executable_tis

        pool_occupied_slots = {pool: occupied_slots
                               for pool, occupied_slots in session.query(TI.pool, func.sum(TI.pool_slots))
                               .filter(TI.pool.in_(pool_slots.keys()))
                               .filter(TI.state.in_(STATES_TO_COUNT_AS_RUNNING))
                               .group_by(TI.pool)
                               .all()}

        pool_open_slots = {pool: slots - pool_occupied_slots.get(pool, 0)
                           for pool, slots in pool_slots.items()}

        have_remain_slots_pool = {}
        for pool, open_slots in pool_open_slots.items():
            if open_slots <= 0:
                self.log.info(
                    "Not scheduling since there are %s open slots in pool %s",
                    open_slots, pool
                )
                continue
            have_remain_slots_pool[pool] = open_slots

        if len(have_remain_slots_pool) == 0:
            return simple_dag_bag, executable_tis

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        dag_concurrency_map, task_concurrency_map = self.__get_concurrency_maps(
            states=STATES_TO_COUNT_AS_RUNNING, session=session)

        slots_used_rate = [(pool, int(pool_occupied_slots.get(pool, 0)) * 1.0 / pool_slots[pool])
                      for pool, _ in have_remain_slots_pool.items()]

        slots_used_rate = sorted(slots_used_rate, key=lambda rate: rate[1])

        pools_usable_count = defaultdict(int)
        fix_pools = []

        def assigned_slots(count):
            for i in range(0, count):
                if len(slots_used_rate) == 0:
                    break
                pool, _ = slots_used_rate[0]
                pools_usable_count[pool] += 1
                task_instances_to_examine[pool] -= 1
                have_remain_slots_pool[pool] -= - 1
                rate = 1.0 * (pools_usable_count[pool] + int(pool_occupied_slots.get(pool, 0))) / pool_slots[pool]
                if task_instances_to_examine[pool] == 0 or have_remain_slots_pool[pool] == 0:
                    fix_pools.append((pool, rate))
                    slots_used_rate.pop(0)
                    continue
                slots_used_rate[0] = (pool, rate)
                for i in range(0, len(slots_used_rate) - 1):
                    if slots_used_rate[i][1] > slots_used_rate[i+1][1]:
                        slots_used_rate[i], slots_used_rate[i+1] = slots_used_rate[i+1], slots_used_rate[i]
                    else:
                        break
        assigned_slots(remain_count)
        simple_dag_cache = {}

        while True:
            if len(fix_pools) == 0:
                if len(slots_used_rate) == 0:
                    break
                fix_pools.append(slots_used_rate.pop(0))
            pool, _ = fix_pools.pop(0)
            usable_count = pools_usable_count[pool]
            watermark = None
            open_slots = pool_open_slots[pool]
            skip = False
            while usable_count > 0:
                limit = max(16, usable_count)
                tis = self.find_scheduled_task_instances(pool, states, limit, session, watermark=watermark)
                if len(tis) == 0:
                    break
                dag_ids = set([ti.dag_id for ti in tis if ti.dag_id not in simple_dag_cache])
                if len(dag_ids) != 0:
                    SDM = SimpleDagModel
                    simple_dag_models = session.query(SimpleDagModel) \
                        .filter(SDM.dag_id.in_(dag_ids)).all()
                    for simple_dag_model in simple_dag_models:
                        simple_dag = SimpleDag(simple_dag_model=simple_dag_model)
                        simple_dag_cache[simple_dag.dag_id] = simple_dag
                for ti in tis:
                    simple_dag = simple_dag_cache.get(ti.dag_id, None)
                    if simple_dag is None or not os.path.exists(simple_dag.full_filepath):
                        ti.state = None
                        session.merge(ti)
                        continue
                    if ti.pool_slots > open_slots:
                        self.log.info("Not executing %s since it requires %s slots "
                                      "but there are %s open slots in the pool %s.",
                                      ti, ti.pool_slots, open_slots, pool)
                        continue
                    current_dag_concurrency = dag_concurrency_map[ti.dag_id]
                    dag_concurrency_limit = simple_dag_cache[ti.dag_id].concurrency
                    self.log.info(
                        "DAG %s has %s/%s running and queued tasks",
                        ti.dag_id, current_dag_concurrency, dag_concurrency_limit
                    )
                    if current_dag_concurrency >= dag_concurrency_limit:
                        self.log.info(
                            "Not executing %s since the number of tasks running or queued "
                            "from DAG %s is >= to the DAG's task concurrency limit of %s",
                            ti, ti.dag_id, dag_concurrency_limit
                        )
                        continue

                    task_concurrency_limit = simple_dag.get_task_special_arg(
                        ti.task_id,
                        'task_concurrency')
                    if task_concurrency_limit is not None:
                        current_task_concurrency = task_concurrency_map[
                            (ti.dag_id, ti.task_id)
                        ]

                        if current_task_concurrency >= task_concurrency_limit:
                            self.log.info("Not executing %s since the task concurrency for"
                                          " this task has been reached.", ti)
                            continue

                    if self.executor.has_task(ti):
                        self.log.debug(
                            "Not handling task %s as the executor reports it is running",
                            ti.key
                        )
                        continue
                    usable_count -= 1
                    executable_tis.append(ti)
                    open_slots -= ti.pool_slots
                    dag_concurrency_map[ti.dag_id] += 1
                    task_concurrency_map[(ti.dag_id, ti.task_id)] += 1
                    if open_slots <= 0:
                        self.log.info(
                            "Not scheduling since there are %s open slots in pool %s",
                            open_slots, pool
                        )
                        skip = True
                        break
                    if usable_count <= 0:
                        skip = True
                        break
                session.commit()
                if skip or len(tis) < limit:
                    break
                watermark = tis[-1].priority_weight, tis[-1].execution_date, tis[-1].task_id, tis[-1].dag_id
            assigned_slots(usable_count)
        task_instance_str = "\n\t".join(
            [repr(x) for x in executable_tis])
        self.log.info(
            "Setting the following tasks to queued state:\n\t%s", task_instance_str)
        # so these dont expire on commit
        for ti in executable_tis:
            copy_dag_id = ti.dag_id
            copy_execution_date = ti.execution_date
            copy_task_id = ti.task_id
            make_transient(ti)
            ti.dag_id = copy_dag_id
            ti.execution_date = copy_execution_date
            ti.task_id = copy_task_id
        return SimpleDagBag(simple_dag_cache.values()), executable_tis

    def find_scheduled_task_instances(self, pool, states, limit, session, watermark=None):
        from airflow.jobs.backfill_job import BackfillJob  # Avoid circular import
        TI = models.TaskInstance
        DR = models.DagRun
        DM = models.DagModel

        ti_qry = (
            session
                .query(TI)
                .filter(TI.pool == pool)
                .outerjoin(
                    DR,
                    and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date)
                )
                .filter(DR.state == State.RUNNING)
        )

        if not settings.SCHEDULE_BACKFILL_IN_SCHEDULER:
            ti_qry = ti_qry.filter(or_(DR.run_id == None,  # noqa: E711 pylint: disable=singleton-comparison
                                       not_(DR.run_id.like(BackfillJob.ID_PREFIX + '%'))))

        ti_qry = (ti_qry.outerjoin(DM, DM.dag_id == TI.dag_id)
                        .filter(or_(DM.dag_id == None,  # noqa: E711 pylint: disable=singleton-comparison
                                not_(DM.is_paused))))

        # Additional filters on task instance state
        if None in states:
            ti_qry = ti_qry.filter(
                or_(TI.state == None, TI.state.in_(states))  # noqa: E711 pylint: disable=singleton-comparison
            )
        else:
            ti_qry = ti_qry.filter(TI.state.in_(states))

        if watermark is not None:
            priority_weight, execution_date, task_id, dag_id = watermark
            ti_qry = ti_qry.filter(or_(TI.priority_weight < priority_weight,
                                       and_(TI.priority_weight == priority_weight,
                                            or_(TI.execution_date > execution_date,
                                                and_(TI.execution_date == execution_date,
                                                     or_(TI.task_id > task_id,
                                                         and_(TI.task_id == task_id,
                                                              TI.dag_id > dag_id)))))))
        ti_qry = ti_qry.order_by(TI.priority_weight.desc(),
                                 TI.execution_date.asc(),
                                 TI.task_id.asc(),
                                 TI.dag_id.asc())
        ti_qry = ti_qry.limit(limit)
        return ti_qry.all()

    @provide_session
    def _find_executable_task_instances(self, simple_dag_bag, states, session=None):
        """
        Finds TIs that are ready for execution with respect to pool limits,
        dag concurrency, executor state, and priority.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        :param executor: the executor that runs task instances
        :type executor: BaseExecutor
        :param states: Execute TaskInstances in these states
        :type states: tuple[airflow.utils.state.State]
        :return: list[airflow.models.TaskInstance]
        """
        from airflow.jobs.backfill_job import BackfillJob  # Avoid circular import
        executable_tis = []

        # Get all task instances associated with scheduled
        # DagRuns which are not backfilled, in the given states,
        # and the dag is not paused
        TI = models.TaskInstance
        DR = models.DagRun
        DM = models.DagModel
        ti_query = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(simple_dag_bag.dag_ids))
            .outerjoin(
                DR,
                and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date)
            )
        )

        if not settings.SCHEDULE_BACKFILL_IN_SCHEDULER:
            ti_query = ti_query.filter(or_(DR.run_id == None,  # noqa: E711 pylint: disable=singleton-comparison
                                       not_(DR.run_id.like(BackfillJob.ID_PREFIX + '%'))))

        ti_query = (ti_query.outerjoin(DM, DM.dag_id == TI.dag_id)
                            .filter(or_(DM.dag_id == None,  # noqa: E711 pylint: disable=singleton-comparison
                                    not_(DM.is_paused))))
        # Additional filters on task instance state
        if None in states:
            ti_query = ti_query.filter(
                or_(TI.state == None, TI.state.in_(states))  # noqa: E711 pylint: disable=singleton-comparison
            )
        else:
            ti_query = ti_query.filter(TI.state.in_(states))

        task_instances_to_examine = ti_query.all()

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return executable_tis

        # Put one task instance on each line
        task_instance_str = "\n\t".join(
            [repr(x) for x in task_instances_to_examine])
        self.log.info(
            "%s tasks up for execution:\n\t%s", len(task_instances_to_examine),
            task_instance_str
        )

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        dag_concurrency_map, task_concurrency_map = self.__get_concurrency_maps(
            states=STATES_TO_COUNT_AS_RUNNING, session=session)

        num_tasks_in_executor = 0
        num_starving_tasks_total = 0

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        for pool, task_instances in pool_to_task_instances.items():
            pool_name = pool
            if pool not in pools:
                self.log.warning(
                    "Tasks using non-existent pool '%s' will not be scheduled",
                    pool
                )
                continue
            else:
                open_slots = pools[pool].open_slots(session=session)

            num_ready = len(task_instances)
            self.log.info(
                "Figuring out tasks to run in Pool(name=%s) with %s open slots "
                "and %s task instances ready to be queued",
                pool, open_slots, num_ready
            )

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date))

            num_starving_tasks = 0
            for current_index, task_instance in enumerate(priority_sorted_task_instances):
                if open_slots <= 0:
                    self.log.info(
                        "Not scheduling since there are %s open slots in pool %s",
                        open_slots, pool
                    )
                    # Can't schedule any more since there are no more open slots.
                    num_unhandled = len(priority_sorted_task_instances) - current_index
                    num_starving_tasks += num_unhandled
                    num_starving_tasks_total += num_unhandled
                    break

                # Check to make sure that the task concurrency of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id
                simple_dag = simple_dag_bag.get_dag(dag_id)

                current_dag_concurrency = dag_concurrency_map[dag_id]
                dag_concurrency_limit = simple_dag_bag.get_dag(dag_id).concurrency
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id, current_dag_concurrency, dag_concurrency_limit
                )
                if current_dag_concurrency >= dag_concurrency_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's task concurrency limit of %s",
                        task_instance, dag_id, dag_concurrency_limit
                    )
                    continue

                task_concurrency_limit = simple_dag.get_task_special_arg(
                    task_instance.task_id,
                    'task_concurrency')
                if task_concurrency_limit is not None:
                    current_task_concurrency = task_concurrency_map[
                        (task_instance.dag_id, task_instance.task_id)
                    ]

                    if current_task_concurrency >= task_concurrency_limit:
                        self.log.info("Not executing %s since the task concurrency for"
                                      " this task has been reached.", task_instance)
                        continue

                if self.executor.has_task(task_instance):
                    self.log.debug(
                        "Not handling task %s as the executor reports it is running",
                        task_instance.key
                    )
                    num_tasks_in_executor += 1
                    continue

                if task_instance.pool_slots > open_slots:
                    self.log.info("Not executing %s since it requires %s slots "
                                  "but there are %s open slots in the pool %s.",
                                  task_instance, task_instance.pool_slots, open_slots, pool)
                    num_starving_tasks += 1
                    num_starving_tasks_total += 1
                    # Though we can execute tasks with lower priority if there's enough room
                    continue

                executable_tis.append(task_instance)
                open_slots -= task_instance.pool_slots
                dag_concurrency_map[dag_id] += 1
                task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1

            Stats.gauge('pool.starving_tasks.{pool_name}'.format(pool_name=pool_name),
                        num_starving_tasks)
            Stats.gauge('pool.open_slots.{pool_name}'.format(pool_name=pool_name),
                        pools[pool_name].open_slots())
            Stats.gauge('pool.used_slots.{pool_name}'.format(pool_name=pool_name),
                        pools[pool_name].occupied_slots())

        Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))
        Stats.gauge('scheduler.tasks.running', num_tasks_in_executor)
        Stats.gauge('scheduler.tasks.starving', num_starving_tasks_total)
        Stats.gauge('scheduler.tasks.executable', len(executable_tis))

        task_instance_str = "\n\t".join(
            [repr(x) for x in executable_tis])
        self.log.info(
            "Setting the following tasks to queued state:\n\t%s", task_instance_str)
        # so these dont expire on commit
        for ti in executable_tis:
            copy_dag_id = ti.dag_id
            copy_execution_date = ti.execution_date
            copy_task_id = ti.task_id
            make_transient(ti)
            ti.dag_id = copy_dag_id
            ti.execution_date = copy_execution_date
            ti.task_id = copy_task_id
        return executable_tis

    @provide_session
    def _change_state_for_executable_task_instances(self, task_instances,
                                                    acceptable_states, session=None):
        """
        Changes the state of task instances in the list with one of the given states
        to QUEUED atomically, and returns the TIs changed in SimpleTaskInstance format.

        :param task_instances: TaskInstances to change the state of
        :type task_instances: list[airflow.models.TaskInstance]
        :param acceptable_states: Filters the TaskInstances updated to be in these states
        :type acceptable_states: Iterable[State]
        :rtype: list[airflow.utils.dag_processing.SimpleTaskInstance]
        """
        if len(task_instances) == 0:
            session.commit()
            return []

        TI = models.TaskInstance
        filter_for_ti_state_change = (
            [and_(
                TI.dag_id == ti.dag_id,
                TI.task_id == ti.task_id,
                TI.execution_date == ti.execution_date)
                for ti in task_instances])
        ti_query = (
            session
            .query(TI)
            .filter(or_(*filter_for_ti_state_change)))

        if None in acceptable_states:
            ti_query = ti_query.filter(
                or_(TI.state == None, TI.state.in_(acceptable_states))  # noqa pylint: disable=singleton-comparison
            )
        else:
            ti_query = ti_query.filter(TI.state.in_(acceptable_states))

        tis_to_set_to_queued = (
            ti_query
            .with_for_update()
            .all())

        if len(tis_to_set_to_queued) == 0:
            self.log.info("No tasks were able to have their state changed to queued.")
            session.commit()
            return []

        # set TIs to queued state
        for task_instance in tis_to_set_to_queued:
            task_instance.state = State.QUEUED
            task_instance.queued_dttm = timezone.utcnow()
            session.merge(task_instance)

        # Generate a list of SimpleTaskInstance for the use of queuing
        # them in the executor.
        simple_task_instances = [SimpleTaskInstance(ti) for ti in
                                 tis_to_set_to_queued]

        task_instance_str = "\n\t".join(
            [repr(x) for x in tis_to_set_to_queued])

        session.commit()
        self.log.info("Setting the following %s tasks to queued state:\n\t%s",
                      len(tis_to_set_to_queued), task_instance_str)
        return simple_task_instances

    def _enqueue_task_instances_with_queued_state(self, simple_dag_bag,
                                                  simple_task_instances):
        """
        Takes task_instances, which should have been set to queued, and enqueues them
        with the executor.

        :param simple_task_instances: TaskInstances to enqueue
        :type simple_task_instances: list[SimpleTaskInstance]
        :param simple_dag_bag: Should contains all of the task_instances' dags
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        """
        TI = models.TaskInstance
        # actually enqueue them
        for simple_task_instance in simple_task_instances:
            simple_dag = simple_dag_bag.get_dag(simple_task_instance.dag_id)
            command = TI.generate_command(
                simple_task_instance.dag_id,
                simple_task_instance.task_id,
                simple_task_instance.execution_date,
                local=True,
                mark_success=False,
                ignore_all_deps=False,
                ignore_depends_on_past=False,
                ignore_task_deps=False,
                ignore_ti_state=False,
                pool=simple_task_instance.pool,
                file_path=simple_dag.full_filepath,
                pickle_id=simple_dag.pickle_id)

            priority = simple_task_instance.priority_weight
            queue = simple_task_instance.queue
            self.log.info(
                "Sending %s to executor with priority %s and queue %s",
                simple_task_instance.key, priority, queue
            )

            self.executor.queue_command(
                simple_task_instance,
                command,
                priority=priority,
                queue=queue)

    @provide_session
    def _execute_task_instances(self,
                                simple_dag_bag,
                                states,
                                by_orm=False,
                                session=None):
        """
        Attempts to execute TaskInstances that should be executed by the scheduler.

        There are three steps:
        1. Pick TIs by priority with the constraint that they are in the expected states
        and that we do exceed max_active_runs or pool limits.
        2. Change the state for the TIs above atomically.
        3. Enqueue the TIs in the executor.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        :param states: Execute TaskInstances in these states
        :type states: tuple[airflow.utils.state.State]
        :return: Number of task instance with state changed.
        """
        if by_orm:
            simple_dag_bag, executable_tis = self._find_executable_task_instances_by_orm(states)
        else:
            executable_tis = self._find_executable_task_instances(simple_dag_bag, states,
                                                                  session=session)

        def query(result, items):
            simple_tis_with_state_changed = \
                self._change_state_for_executable_task_instances(items,
                                                                 states,
                                                                 session=session)
            self._enqueue_task_instances_with_queued_state(
                simple_dag_bag,
                simple_tis_with_state_changed)
            session.commit()
            return result + len(simple_tis_with_state_changed)

        return helpers.reduce_in_chunks(query, executable_tis, 0, self.max_tis_per_query)

    @provide_session
    def _change_state_for_tasks_failed_to_execute(self, session):
        """
        If there are tasks left over in the executor,
        we set them back to SCHEDULED to avoid creating hanging tasks.

        :param session: session for ORM operations
        """
        if self.executor.queued_tasks:
            TI = models.TaskInstance
            filter_for_ti_state_change = (
                [and_(
                    TI.dag_id == dag_id,
                    TI.task_id == task_id,
                    TI.execution_date == execution_date,
                    # The TI.try_number will return raw try_number+1 since the
                    # ti is not running. And we need to -1 to match the DB record.
                    TI._try_number == try_number - 1,
                    TI.state == State.QUEUED)
                    for dag_id, task_id, execution_date, try_number
                    in self.executor.queued_tasks.keys()])
            ti_query = (session.query(TI)
                        .filter(or_(*filter_for_ti_state_change)))
            tis_to_set_to_scheduled = (ti_query
                                       .with_for_update()
                                       .all())
            if len(tis_to_set_to_scheduled) == 0:
                session.commit()
                return

            # set TIs to queued state
            for task_instance in tis_to_set_to_scheduled:
                task_instance.state = State.SCHEDULED
                task_instance.queued_dttm = None
                self.executor.queued_tasks.pop(task_instance.key)

            task_instance_str = "\n\t".join(
                [repr(x) for x in tis_to_set_to_scheduled])

            session.commit()
            self.log.info("Set the following tasks to scheduled state:\n\t%s", task_instance_str)

    def _process_dags(self, dagbag, dags, tis_out):
        """
        Iterates over the dags and processes them. Processing includes:

        1. Create appropriate DagRun(s) in the DB.
        2. Create appropriate TaskInstance(s) in the DB.
        3. Send emails for tasks that have missed SLAs.

        :param dagbag: a collection of DAGs to process
        :type dagbag: airflow.models.DagBag
        :param dags: the DAGs from the DagBag to process
        :type dags: list[airflow.models.DAG]
        :param tis_out: A list to add generated TaskInstance objects
        :type tis_out: list[TaskInstance]
        :rtype: None
        """
        for dag in dags:
            dag = dagbag.get_dag(dag.dag_id)
            if not dag:
                self.log.error("DAG ID %s was not found in the DagBag", dag.dag_id)
                continue

            self.log.info("Processing %s", dag.dag_id)

            if settings.SCHEDULE_BACKFILL_IN_SCHEDULER:
                self.delete_not_exists_sub_dag_records(dagbag, dag)

            dag_run = self.create_dag_run(dag)
            if dag_run:
                expected_start_date = dag.following_schedule(dag_run.execution_date)
                if expected_start_date:
                    schedule_delay = dag_run.start_date - expected_start_date
                    Stats.timing(
                        'dagrun.schedule_delay.{dag_id}'.format(dag_id=dag.dag_id),
                        schedule_delay)
                self.log.info("Created %s", dag_run)
            self._process_task_instances(dagbag, dag, tis_out)
            if conf.getboolean('core', 'CHECK_SLAS', fallback=True):
                self.manage_slas(dag)

    @provide_session
    def get_simple_dag_by_orm(self, dag_id, session=None):
        simple_dag_model = session.query(SimpleDagModel) \
            .filter(SimpleDagModel.dag_id == dag_id).first()
        if simple_dag_model is None:
            return None
        return SimpleDag(simple_dag_model=simple_dag_model)

    @provide_session
    def _process_executor_events(self, simple_dag_bag, session=None, get_simple_dag=None):
        """
        Respond to executor events.
        """
        # TODO: this shares quite a lot of code with _manage_executor_state

        TI = models.TaskInstance
        for key, state in list(self.executor.get_event_buffer(
            simple_dag_bag.dag_ids if get_simple_dag is None else None)
                                   .items()):
            dag_id, task_id, execution_date, try_number = key
            self.log.info(
                "Executor reports execution of %s.%s execution_date=%s "
                "exited with status %s for try_number %s",
                dag_id, task_id, execution_date, state, try_number
            )
            if state == State.FAILED or state == State.SUCCESS:
                qry = session.query(TI).filter(TI.dag_id == dag_id,
                                               TI.task_id == task_id,
                                               TI.execution_date == execution_date)
                ti = qry.first()
                if not ti:
                    self.log.warning("TaskInstance %s went missing from the database", ti)
                    continue

                # TODO: should we fail RUNNING as well, as we do in Backfills?
                if ti.try_number == try_number and ti.state == State.QUEUED:
                    msg = ("Executor reports task instance {} finished ({}) "
                           "although the task says its {}. Was the task "
                           "killed externally?".format(ti, state, ti.state))
                    Stats.incr('scheduler.tasks.killed_externally')
                    self.log.error(msg)
                    try:
                        if get_simple_dag is not None:
                            simple_dag = get_simple_dag(dag_id, session=session)
                        else:
                            simple_dag = simple_dag_bag.get_dag(dag_id)
                        dagbag = models.DagBag(simple_dag.full_filepath)
                        dag = dagbag.get_dag(dag_id)
                        ti.task = dag.get_task(task_id)
                        ti.handle_failure(msg)
                    except Exception:
                        self.log.error("Cannot load the dag bag to handle failure for %s"
                                       ". Setting task to FAILED without callbacks or "
                                       "retries. Do you have enough resources?", ti)
                        ti.state = State.FAILED
                        session.merge(ti)
                        session.commit()

    def _execute(self):
        self.log.info("Starting the scheduler")

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = False
        if self.do_pickle and self.executor.__class__ not in \
                (executors.LocalExecutor, executors.SequentialExecutor):
            pickle_dags = True

        self.log.info("Running execute loop for %s seconds", self.run_duration)
        self.log.info("Processing each file at most %s times", self.num_runs)

        known_file_paths = []
        if not settings.SCHEDULER_CLUSTER:
            # Build up a list of Python files that could contain DAGs
            self.log.info("Searching for files in %s", self.subdir)
            known_file_paths = list_py_file_paths(self.subdir)
            self.log.info("There are %s files in %s", len(known_file_paths), self.subdir)

        # When using sqlite, we do not use async_mode
        # so the scheduler job and DAG parser don't access the DB at the same time.
        async_mode = not self.using_sqlite

        processor_timeout_seconds = conf.getint('core', 'dag_file_processor_timeout')
        processor_timeout = timedelta(seconds=processor_timeout_seconds)
        self.processor_agent = DagFileProcessorAgent(self.subdir,
                                                     known_file_paths,
                                                     self.num_runs,
                                                     type(self)._create_dag_file_processor,
                                                     processor_timeout,
                                                     self.dag_ids,
                                                     pickle_dags,
                                                     async_mode)

        try:
            self._execute_helper()
        except Exception:
            self.log.exception("Exception when executing execute_helper")
        finally:
            self.processor_agent.end()
            self.log_out()
            self.log.info("Exited execute loop")

    @staticmethod
    def _create_dag_file_processor(file_path, zombies, dag_ids, pickle_dags):
        """
        Creates DagFileProcessorProcess instance.
        """
        return DagFileProcessor(file_path,
                                pickle_dags,
                                dag_ids,
                                zombies)

    def _get_simple_dags(self):
        return self.processor_agent.harvest_simple_dags()

    @provide_session
    def registry_or_update_heartbeat(self, session=None):
        if self.is_leader:
            node = NodeInstance.select(self.node.id, lock_for_update=True)
            if node.hostname != self.node.hostname:
                session.commit()
                self.is_leader = False
                self.node = None
                self.executor.end()
                return
            self.node.latest_heartbeat = timezone.utcnow()
            session.merge(self.node)
            session.commit()
        else:
            node = NodeInstance.select(id=SchedulerJob.MASTER_ID, session=session)

            if NodeInstance.check_node_alive(node, timeout=60):
                session.commit()
                return

            node = NodeInstance.select(id=SchedulerJob.MASTER_ID, lock_for_update=True, session=session)
            if NodeInstance.check_node_alive(node, timeout=60):
                session.commit()
                return

            self.node = NodeInstance.create_instance(id=SchedulerJob.MASTER_ID, instance=INSTANCE_SCHEDULER_LEADER)
            session.merge(self.node)
            session.commit()
            self.is_leader = True
            self.reset_state_for_orphaned_tasks(session=session, reset_backfill=settings.SCHEDULE_BACKFILL_IN_SCHEDULER)
            self.executor = self.executor.__class__()
            self.executor.start()
            self.executor.parallelism = 0
            return

    @provide_session
    def log_out(self, session=None):
        if self.is_leader:
            node = NodeInstance.select(self.node.id, lock_for_update=True)
            if node.hostname == self.node.hostname:
                node.state = NODE_INSTANCE_DEAD
                session.merge(node)
                session.commit()
            self.log.info("Logout success")

    def _execute_helper(self):
        """
        The actual scheduler loop. The main steps in the loop are:
            #. Harvest DAG parsing results through DagFileProcessorAgent
            #. Find and queue executable tasks
                #. Change task instance state in DB
                #. Queue tasks in executor
            #. Heartbeat executor
                #. Execute queued tasks in executor asynchronously
                #. Sync on the states of running tasks

        Following is a graphic representation of these steps.

        .. image:: ../docs/img/scheduler_loop.jpg

        :rtype: None
        """
        self.executor.start()

        if not settings.SCHEDULER_CLUSTER:
            self.log.info("Resetting orphaned tasks for active dag runs")
            self.reset_state_for_orphaned_tasks(reset_backfill=settings.SCHEDULE_BACKFILL_IN_SCHEDULER)

        # Start after resetting orphaned tasks to avoid stressing out DB.
        self.processor_agent.start()

        execute_start_time = timezone.utcnow()

        # Last time that self.heartbeat() was called.
        last_self_heartbeat_time = timezone.utcnow()

        # For the execute duration, parse and schedule DAGs
        while (timezone.utcnow() - execute_start_time).total_seconds() < \
                self.run_duration or self.run_duration < 0:
            self.log.debug("Starting Loop...")
            loop_start_time = time.time()

            if self.using_sqlite:
                self.processor_agent.heartbeat()
                # For the sqlite case w/ 1 thread, wait until the processor
                # is finished to avoid concurrent access to the DB.
                self.log.debug(
                    "Waiting for processors to finish since we're using sqlite")
                self.processor_agent.wait_until_finished()

            self.log.debug("Harvesting DAG parsing results")
            simple_dags = self._get_simple_dags()
            self.log.debug("Harvested {} SimpleDAGs".format(len(simple_dags)))

            # Send tasks for execution if available
            simple_dag_bag = SimpleDagBag(simple_dags)

            if settings.GLOBAL_SCHEDULE_MODE:
                self.registry_or_update_heartbeat()
                if self.is_leader:
                    if not self._validate_and_run_task_instances_by_orm():
                        continue
            elif not self._validate_and_run_task_instances(simple_dag_bag=simple_dag_bag):
                continue

            # Heartbeat the scheduler periodically
            time_since_last_heartbeat = (timezone.utcnow() -
                                         last_self_heartbeat_time).total_seconds()
            if time_since_last_heartbeat > self.heartrate:
                self.log.debug("Heartbeating the scheduler")
                self.heartbeat()
                last_self_heartbeat_time = timezone.utcnow()

            is_unit_test = conf.getboolean('core', 'unit_test_mode')
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            self.log.info(
                "Ran scheduling loop in %.2f seconds",
                loop_duration)

            if not is_unit_test:
                self.log.debug("Sleeping for %.2f seconds", self._processor_poll_interval)
                time.sleep(self._processor_poll_interval)

            if self.processor_agent.done:
                self.log.info("Exiting scheduler loop as all files"
                              " have been processed {} times".format(self.num_runs))
                break

            if loop_duration < 1 and not is_unit_test:
                sleep_length = 1 - loop_duration
                self.log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                    .format(sleep_length))
                sleep(sleep_length)

        # Stop any processors
        self.processor_agent.terminate()

        # Verify that all files were processed, and if so, deactivate DAGs that
        # haven't been touched by the scheduler as they likely have been
        # deleted.
        if self.processor_agent.all_files_processed:
            self.log.info(
                "Deactivating DAGs that haven't been touched since %s",
                execute_start_time.isoformat()
            )
            models.DAG.deactivate_stale_dags(execute_start_time)

        self.executor.end()

        settings.Session.remove()

    def _validate_and_run_task_instances(self, simple_dag_bag):
        if len(simple_dag_bag.simple_dags) > 0:
            try:
                self._process_and_execute_tasks(simple_dag_bag)
            except Exception as e:
                self.log.error("Error queuing tasks")
                self.log.exception(e)
                return False

        # Call heartbeats
        self.log.debug("Heartbeating the executor")
        self.executor.heartbeat()

        self._change_state_for_tasks_failed_to_execute()

        # Process events from the executor
        self._process_executor_events(simple_dag_bag)
        return True

    @provide_session
    def change_abnormal_queued_state_taskinstance(self, session=None):
        TI = models.TaskInstance
        keys = session.query(TI.dag_id, TI.task_id, TI.execution_date).filter(TI.state == State.QUEUED).all()
        if len(keys) == 0:
            return
        running_keys = set()
        for key, _ in self.executor.running.items():
            dag_id = key[0]
            task_id = key[1]
            execution_date = key[2]
            running_keys.add((dag_id, task_id, execution_date))
        for key in keys:
            if key not in running_keys:
                dag_id, task_id, execution_date = key
                ti = session.query(TI).filter(
                    TI.dag_id == dag_id,
                    TI.task_id == task_id,
                    TI.execution_date == execution_date) \
                    .with_for_update().populate_existing().first()
                if ti.state == State.QUEUED:
                    ti.state = None
                    session.merge(ti)
                session.commit()

    def _validate_and_run_task_instances_by_orm(self):
        simple_dag_bag = SimpleDagBag([])
        try:
            if (timezone.utcnow() - self.last_check_abnormal_state).total_seconds() > 60:
                self.change_abnormal_queued_state_taskinstance()
                self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                          [State.UP_FOR_RETRY],
                                                          State.FAILED,
                                                          check_all=True)
                self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                          [State.QUEUED,
                                                           State.SCHEDULED,
                                                           State.UP_FOR_RESCHEDULE],
                                                          State.NONE,
                                                          check_all=True)
                self.last_check_abnormal_state = timezone.utcnow()

            self._execute_task_instances(simple_dag_bag,
                                         (State.SCHEDULED,),
                                         by_orm=True)
        except Exception as e:
            self.log.error("Error queuing tasks")
            self.log.exception(e)
            return False

        # Call heartbeats
        self.log.debug("Heartbeating the executor")
        self.executor.heartbeat()

        self._change_state_for_tasks_failed_to_execute()

        # Process events from the executor
        self._process_executor_events(simple_dag_bag, get_simple_dag=self.get_simple_dag_by_orm)
        return True

    def _process_and_execute_tasks(self, simple_dag_bag):
        # Handle cases where a DAG run state is set (perhaps manually) to
        # a non-running state. Handle task instances that belong to
        # DAG runs in those states
        # If a task instance is up for retry but the corresponding DAG run
        # isn't running, mark the task instance as FAILED so we don't try
        # to re-run it.
        self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                  [State.UP_FOR_RETRY],
                                                  State.FAILED)
        # If a task instance is scheduled or queued or up for reschedule,
        # but the corresponding DAG run isn't running, set the state to
        # NONE so we don't try to re-run it.
        self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                  [State.QUEUED,
                                                   State.SCHEDULED,
                                                   State.UP_FOR_RESCHEDULE],
                                                  State.NONE)
        self._execute_task_instances(simple_dag_bag,
                                     (State.SCHEDULED,))

    @provide_session
    def _run_internal(self, task_instance, run, check_dependencies_met=True, session=None):
        from airflow.utils.log.logging_mixin import redirect_stdout, redirect_stderr

        task_instance.refresh_from_db(session=session)
        ti_log_handlers = task_instance.log.handlers
        root_logger = logging.getLogger()
        root_logger_handlers = root_logger.handlers

        for handler in root_logger_handlers:
            root_logger.removeHandler(handler)

        for handler in ti_log_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(task_instance.log.level)
        try:
            task_instance._set_context(task_instance)
            if check_dependencies_met:
                dep_context = DepContext(deps=SCHEDULED_DEPS, ignore_task_deps=True)
                if not task_instance.are_dependencies_met(
                        dep_context=dep_context,
                        session=session,
                        verbose=True):
                    return
            self.log.info("Start to run %s directly.", task_instance)
            task_instance.state = State.RUNNING
            error = None
            with redirect_stdout(task_instance.log, logging.INFO), \
                    redirect_stderr(task_instance.log, logging.WARN):
                try:
                    run(task_instance)
                    self.log.info("Run %s successful in scheduler.", task_instance)
                except Exception as e1:
                    error = traceback.format_exc()
                    task_instance.log.exception(e1)
                task_instance.log.info("End.\n")
            if error is not None:
                self.log.warning("Run %s error inner:\n%s", task_instance, error)
        except Exception as e2:
            self.log.exception("Run %s error outer:\n%s", task_instance, e2)
        finally:
            for handler in ti_log_handlers:
                try:
                    handler.flush()
                    handler.close()
                    handler.handler = None
                except AttributeError:
                    pass
                root_logger.removeHandler(handler)
            for handler in root_logger_handlers:
                root_logger.addHandler(handler)

    @provide_session
    def process_file(self, file_path, zombies, pickle_dags=False, session=None):
        """
        Process a Python file containing Airflow DAGs.

        This includes:

        1. Execute the file and look for DAG objects in the namespace.
        2. Pickle the DAG and save it to the DB (if necessary).
        3. For each DAG, see what tasks should run and create appropriate task
        instances in the DB.
        4. Record any errors importing the file into ORM
        5. Kill (in ORM) any task instances belonging to the DAGs that haven't
        issued a heartbeat in a while.

        Returns a list of SimpleDag objects that represent the DAGs found in
        the file

        :param file_path: the path to the Python file that should be executed
        :type file_path: unicode
        :param zombies: zombie task instances to kill.
        :type zombies: list[airflow.utils.dag_processing.SimpleTaskInstance]
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :type pickle_dags: bool
        :return: a list of SimpleDags made from the Dags found in the file
        :rtype: list[airflow.utils.dag_processing.SimpleDagBag]
        """
        self.log.info("Processing file %s for tasks to queue", file_path)
        # As DAGs are parsed from this file, they will be converted into SimpleDags
        simple_dags = []
        found_dag_count = 0
        try:
            dagbag = models.DagBag(file_path, include_examples=False)
        except Exception:
            self.log.exception("Failed at reloading the DAG file %s", file_path)
            Stats.incr('dag_file_refresh_error', 1, 1)
            return simple_dags, found_dag_count, 0

        if len(dagbag.dags) > 0:
            self.log.info("DAG(s) %s retrieved from %s", dagbag.dags.keys(), file_path)
        else:
            self.log.warning("No viable dags retrieved from %s", file_path)
            self.update_import_errors(session, dagbag)
            return simple_dags, found_dag_count, len(dagbag.import_errors)

        # Save individual DAGs in the ORM and update DagModel.last_scheduled_time
        for dag in dagbag.dags.values():
            dag.sync_to_db()

        paused_dag_ids = models.DagModel.get_paused_dag_ids(dag_ids=dagbag.dag_ids)

        # Pickle the DAGs (if necessary) and put them into a SimpleDag
        for dag_id in dagbag.dags:
            # Only return DAGs that are not paused
            if dag_id not in paused_dag_ids:
                dag = dagbag.get_dag(dag_id)
                pickle_id = None
                if pickle_dags:
                    pickle_id = dag.pickle(session).id
                simple_dag = SimpleDag(dag, pickle_id=pickle_id)
                found_dag_count += 1
                if settings.GLOBAL_SCHEDULE_MODE:
                    SimpleDagModel.sync_to_db(simple_dag)
                else:
                    simple_dags.append(simple_dag)

        if paused_dag_ids is not None and len(paused_dag_ids) > 0:
            SimpleDagModel.delete(paused_dag_ids)

        if len(self.dag_ids) > 0:
            if settings.SCHEDULE_BACKFILL_IN_SCHEDULER:
                dags = [dag for dag in dagbag.dags.values()
                        if not dag.parent_dag and
                        dag.dag_id in self.dag_ids and
                        dag.dag_id not in paused_dag_ids]
            else:
                dags = [dag for dag in dagbag.dags.values()
                        if dag.dag_id in self.dag_ids and
                        dag.dag_id not in paused_dag_ids]
        else:
            dags = [dag for dag in dagbag.dags.values()
                    if not dag.parent_dag and
                    dag.dag_id not in paused_dag_ids]

        # Not using multiprocessing.Queue() since it's no longer a separate
        # process and due to some unusual behavior. (empty() incorrectly
        # returns true as described in https://bugs.python.org/issue23582 )
        ti_keys_to_schedule = []

        self._process_dags(dagbag, dags, ti_keys_to_schedule)

        run_tis = []

        sub_dag_operators = []

        for ti_key in ti_keys_to_schedule:
            dag = dagbag.dags[ti_key[0]]
            task = dag.get_task(ti_key[1])
            ti = models.TaskInstance(task, ti_key[2])

            if ti.task.__class__.__name__ in settings.OPERATOR_RUN_IN_SCHEDULER_SET and ti.task.run_in_scheduler:
                run_tis.append(ti)
                continue

            if settings.SCHEDULE_BACKFILL_IN_SCHEDULER and ti.task.__class__.__name__ == 'SubDagOperator':
                sub_dag_operators.append(ti)
                continue

            ti.refresh_from_db(session=session, lock_for_update=True)
            # We check only deps needed to set TI to SCHEDULED state here.
            # Deps needed to set TI to QUEUED state will be batch checked later
            # by the scheduler for better performance.
            dep_context = DepContext(deps=SCHEDULED_DEPS, ignore_task_deps=True)

            # Only schedule tasks that have their dependencies met, e.g. to avoid
            # a task that recently got its state changed to RUNNING from somewhere
            # other than the scheduler from getting its state overwritten.
            if ti.are_dependencies_met(
                    dep_context=dep_context,
                    session=session,
                    verbose=True):
                ti.state = State.SCHEDULED
                # If the task is dummy, then mark it as done automatically
                if isinstance(ti.task, DummyOperator) \
                        and not ti.task.on_success_callback:
                    ti.state = State.SUCCESS
                    ti.start_date = ti.end_date = timezone.utcnow()
                    ti.duration = 0

                    try:
                        event = event_action.TI_SUCCESS
                        event_action.do_ti_action(event, ti.task, self)
                    except Exception as e:
                        self.log.error('Executing event action error for %s.%s.%s',
                                       ti.dag_id, ti.task_id, ti.execution_date)
                        self.log.exception(e)

            # Also save this task instance to the DB.
            self.log.info("Creating / updating %s in ORM", ti)
            session.merge(ti)
        # commit batch
        session.commit()

        for ti in run_tis:
            self._run_internal(ti,
                               lambda task_instance: task_instance.run_in_scheduler(pool=SCHEDULER_POOL,
                                                                                    session=session),
                               check_dependencies_met=True,
                               session=session)

        for ti in sub_dag_operators:
            dep_context = DepContext(deps=SCHEDULED_DEPS, ignore_task_deps=True)
            self._run_internal(ti,
                               lambda task_instance: task_instance.run_sub_dag_task(pool=SUB_DAG_POOL,
                                                                                    dep_context=dep_context,
                                                                                    session=session),
                               check_dependencies_met=True,
                               session=session)

        # Record import errors into the ORM
        try:
            self.update_import_errors(session, dagbag)
        except Exception:
            self.log.exception("Error logging import errors!")
        try:
            dagbag.kill_zombies(zombies)
        except Exception:
            self.log.exception("Error killing zombies!")

        for dag in dagbag.dags.values():
            try:
                self.manage_dag_notify_slas(dag)
            except Exception:
                self.log.exception("Error manage dag notify_slas for %s!", dag.dag_id)

        return simple_dags, found_dag_count, len(dagbag.import_errors)

    @provide_session
    def heartbeat_callback(self, session=None):
        Stats.incr('scheduler_heartbeat', 1, 1)

    @provide_session
    def _process_sub_dag_task_instances(self,
                                            dagbag,
                                            run,
                                            task_instances_list,
                                            session=None):

        sub_dags = self.parent_dag_contain_sub_dags_map.get(run.dag_id)
        if not sub_dags:
            return

        sub_drs = session.query(models.DagRun) \
            .filter(models.DagRun.dag_id.in_(sub_dags)) \
            .filter(models.DagRun.state == State.RUNNING) \
            .filter(models.DagRun.execution_date == run.execution_date)

        for sub_dag_run in sub_drs:
            sub_dag = dagbag.get_dag(sub_dag_run.dag_id)
            sub_dag_run.dag = sub_dag
            sub_dag_run.verify_integrity(session=session)
            make_transient(sub_dag_run)
            self._process_active_run(sub_dag,
                                     sub_dag_run,
                                     task_instances_list,
                                     session)
            sub_dag_run.update_state(session=session)
            if sub_dag_run.state != State.RUNNING:
                parent_dag_id = sub_dag_run.dag.parent_dag.dag_id
                task_id_in_parent_dag = sub_dag_run.dag_id.split('.')[-1]
                task = dagbag.get_dag(parent_dag_id).get_task(task_id_in_parent_dag)
                ti = models.TaskInstance(task, sub_dag_run.execution_date)
                self._run_internal(ti,
                                   lambda task_instance: task_instance.handle_sub_dag_operator_task_instance_state(
                                       sub_dag_run,
                                       session=session),
                                   check_dependencies_met=False,
                                   session=session)

    @provide_session
    def delete_not_exists_sub_dag_records(self, dagbag, dag, session=None):
        sub_dag_ids = session.query(models.DagModel.dag_id) \
            .filter(models.DagModel.dag_id.like(dag.dag_id + ".%")) \
            .all()
        if len(sub_dag_ids) == 0:
            return

        sub_dag_ids = set([x[0] for x in sub_dag_ids])
        dirty_ids = set()
        for sub_dag_id in sub_dag_ids:
            sub_dag = dagbag.get_dag(sub_dag_id)
            if not sub_dag:
                self.log.info("SubDag %s not found in dagbag.", sub_dag_id)
                sub_dag_run_count = session.query(models.DagRun) \
                    .filter(models.DagRun.dag_id == sub_dag_id) \
                    .delete()
                self.log.info("Delete %s dagRuns for %s.", sub_dag_run_count, sub_dag_id)
                sub_dag_ti_count = session.query(models.TaskInstance) \
                    .filter(models.TaskInstance.dag_id == sub_dag_id) \
                    .delete()
                self.log.info("Delete %s taskInstances for %s.", sub_dag_ti_count, sub_dag_id)
                session.query(models.DagModel) \
                    .filter(models.DagModel.dag_id == sub_dag_id) \
                    .delete()
                self.log.info("Delete dag records for %s.", sub_dag_id)
                session.commit()
                dirty_ids.add(sub_dag_id)

        sub_dags = sub_dag_ids - dirty_ids

        self.parent_dag_contain_sub_dags_map[dag.dag_id] = sub_dags

        sub_tis = session \
            .query(models.TaskInstance) \
            .filter(or_(models.TaskInstance.dag_id.in_(sub_dags),
                        models.TaskInstance.dag_id == dag.dag_id)) \
            .filter(models.TaskInstance.state == State.RUNNING) \
            .filter(models.TaskInstance.operator == "SubDagOperator") \
            .all()

        self.log.info("Running SubDagOperator taskInstance number is %s in %s", len(sub_tis), dag)
        running_sub_tis = {ti.dag_id + "." + ti.task_id + ti.execution_date.isoformat(): ti for ti in sub_tis}

        sub_drs = session \
            .query(models.DagRun) \
            .filter(models.DagRun.dag_id.in_(sub_dags)) \
            .filter(models.DagRun.state == State.RUNNING) \
            .all()
        running_sub_drs = {dr.dag_id + dr.execution_date.isoformat(): dr for dr in sub_drs}
        self.log.info("Running SubDag dagRun number is %s in %s", len(sub_drs), dag)
        if len(running_sub_tis) > 0:
            for key, ti in running_sub_tis.items():
                if key not in running_sub_drs:
                    self.log.warning("TaskInstance %s state is error, the dagRun is not running which refer to.", ti)
                    ti.state = State.NONE
                    session.merge(ti)
                    session.commit()
        if len(running_sub_drs) > 0:
            for key, dr in running_sub_drs.items():
                if key not in running_sub_tis:
                    self.log.warning("SubDagRun %s state is error, the taskInstance is not running which refer to.", dr)
                    dr.state = State.NONE
                    session.merge(dr)
                    session.commit()
