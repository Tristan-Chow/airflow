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

"""current schema

Revision ID: e3a246e0dc1
Revises:
Create Date: 2015-08-18 16:35:00.883495

"""
import dill
from alembic import op
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = 'e3a246e0dc1'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()

    if 'connection' not in tables:
        op.create_table(
            'connection',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('conn_id', sa.String(length=250), nullable=True),
            sa.Column('conn_type', sa.String(length=500), nullable=True),
            sa.Column('host', sa.String(length=500), nullable=True),
            sa.Column('schema', sa.String(length=500), nullable=True),
            sa.Column('login', sa.String(length=500), nullable=True),
            sa.Column('password', sa.String(length=500), nullable=True),
            sa.Column('port', sa.Integer(), nullable=True),
            sa.Column('extra', sa.String(length=5000), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    if 'dag' not in tables:
        op.create_table(
            'dag',
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('is_paused', sa.Boolean(), nullable=True),
            sa.Column('is_subdag', sa.Boolean(), nullable=True),
            sa.Column('is_active', sa.Boolean(), nullable=True),
            sa.Column('last_scheduler_run', sa.DateTime(), nullable=True),
            sa.Column('last_pickled', sa.DateTime(), nullable=True),
            sa.Column('last_expired', sa.DateTime(), nullable=True),
            sa.Column('scheduler_lock', sa.Boolean(), nullable=True),
            sa.Column('pickle_id', sa.Integer(), nullable=True),
            sa.Column('fileloc', sa.String(length=2000), nullable=True),
            sa.Column('owners', sa.String(length=2000), nullable=True),
            sa.PrimaryKeyConstraint('dag_id')
        )
    if 'dag_pickle' not in tables:
        op.create_table(
            'dag_pickle',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('pickle', sa.PickleType(), nullable=True),
            sa.Column('created_dttm', sa.DateTime(), nullable=True),
            sa.Column('pickle_hash', sa.BigInteger(), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    if 'import_error' not in tables:
        op.create_table(
            'import_error',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('timestamp', sa.DateTime(), nullable=True),
            sa.Column('filename', sa.String(length=1024), nullable=True),
            sa.Column('stacktrace', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    if 'job' not in tables:
        op.create_table(
            'job',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=True),
            sa.Column('state', sa.String(length=20), nullable=True),
            sa.Column('job_type', sa.String(length=30), nullable=True),
            sa.Column('start_date', sa.DateTime(), nullable=True),
            sa.Column('end_date', sa.DateTime(), nullable=True),
            sa.Column('latest_heartbeat', sa.DateTime(), nullable=True),
            sa.Column('executor_class', sa.String(length=500), nullable=True),
            sa.Column('hostname', sa.String(length=500), nullable=True),
            sa.Column('unixname', sa.String(length=1000), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index(
            'job_type_heart',
            'job',
            ['job_type', 'latest_heartbeat'],
            unique=False
        )
    if 'known_event_type' not in tables:
        op.create_table(
            'known_event_type',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('know_event_type', sa.String(length=200), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    if 'log' not in tables:
        op.create_table(
            'log',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('dttm', sa.DateTime(), nullable=True),
            sa.Column('dag_id', sa.String(length=250), nullable=True),
            sa.Column('task_id', sa.String(length=250), nullable=True),
            sa.Column('event', sa.String(length=30), nullable=True),
            sa.Column('execution_date', sa.DateTime(), nullable=True),
            sa.Column('owner', sa.String(length=500), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    if 'sla_miss' not in tables:
        op.create_table(
            'sla_miss',
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('execution_date', sa.DateTime(), nullable=False),
            sa.Column('email_sent', sa.Boolean(), nullable=True),
            sa.Column('timestamp', sa.DateTime(), nullable=True),
            sa.Column('description', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('task_id', 'dag_id', 'execution_date')
        )
    if 'slot_pool' not in tables:
        op.create_table(
            'slot_pool',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('pool', sa.String(length=50), nullable=True),
            sa.Column('slots', sa.Integer(), nullable=True),
            sa.Column('description', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('pool')
        )
    if 'task_instance' not in tables:
        op.create_table(
            'task_instance',
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('execution_date', sa.DateTime(), nullable=False),
            sa.Column('start_date', sa.DateTime(), nullable=True),
            sa.Column('end_date', sa.DateTime(), nullable=True),
            sa.Column('duration', sa.Integer(), nullable=True),
            sa.Column('state', sa.String(length=20), nullable=True),
            sa.Column('try_number', sa.Integer(), nullable=True),
            sa.Column('hostname', sa.String(length=1000), nullable=True),
            sa.Column('unixname', sa.String(length=1000), nullable=True),
            sa.Column('job_id', sa.Integer(), nullable=True),
            sa.Column('pool', sa.String(length=50), nullable=True),
            sa.Column('queue', sa.String(length=50), nullable=True),
            sa.Column('priority_weight', sa.Integer(), nullable=True),
            sa.PrimaryKeyConstraint('task_id', 'dag_id', 'execution_date')
        )
        op.create_index(
            'ti_dag_state',
            'task_instance',
            ['dag_id', 'state'],
            unique=False
        )
        op.create_index(
            'ti_pool',
            'task_instance',
            ['pool', 'state', 'priority_weight'],
            unique=False
        )
        op.create_index(
            'ti_state_lkp',
            'task_instance',
            ['dag_id', 'task_id', 'execution_date', 'state'],
            unique=False
        )

    if 'user' not in tables:
        op.create_table(
            'user',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('username', sa.String(length=250), nullable=True),
            sa.Column('email', sa.String(length=500), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('username')
        )
    if 'variable' not in tables:
        op.create_table(
            'variable',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('key', sa.String(length=250), nullable=True),
            sa.Column('val', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('key')
        )
    if 'chart' not in tables:
        op.create_table(
            'chart',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('label', sa.String(length=200), nullable=True),
            sa.Column('conn_id', sa.String(length=250), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=True),
            sa.Column('chart_type', sa.String(length=100), nullable=True),
            sa.Column('sql_layout', sa.String(length=50), nullable=True),
            sa.Column('sql', sa.Text(), nullable=True),
            sa.Column('y_log_scale', sa.Boolean(), nullable=True),
            sa.Column('show_datatable', sa.Boolean(), nullable=True),
            sa.Column('show_sql', sa.Boolean(), nullable=True),
            sa.Column('height', sa.Integer(), nullable=True),
            sa.Column('default_params', sa.String(length=5000), nullable=True),
            sa.Column('x_is_date', sa.Boolean(), nullable=True),
            sa.Column('iteration_no', sa.Integer(), nullable=True),
            sa.Column('last_modified', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['user_id'], ['user.id'], ),
            sa.PrimaryKeyConstraint('id')
        )
    if 'known_event' not in tables:
        op.create_table(
            'known_event',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('label', sa.String(length=200), nullable=True),
            sa.Column('start_date', sa.DateTime(), nullable=True),
            sa.Column('end_date', sa.DateTime(), nullable=True),
            sa.Column('user_id', sa.Integer(), nullable=True),
            sa.Column('known_event_type_id', sa.Integer(), nullable=True),
            sa.Column('description', sa.Text(), nullable=True),
            sa.ForeignKeyConstraint(['known_event_type_id'],
                                    ['known_event_type.id'], ),
            sa.ForeignKeyConstraint(['user_id'], ['user.id'], ),
            sa.PrimaryKeyConstraint('id')
        )
    if 'xcom' not in tables:
        op.create_table(
            'xcom',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('key', sa.String(length=512), nullable=True),
            sa.Column('value', sa.PickleType(), nullable=True),
            sa.Column(
                'timestamp',
                sa.DateTime(),
                default=func.now(),
                nullable=False),
            sa.Column('execution_date', sa.DateTime(), nullable=False),
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.PrimaryKeyConstraint('id')
        )
    if 'dag_sla_miss' not in tables:
        op.create_table(
            'dag_sla_miss',
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('execution_date', sa.DateTime(), nullable=False),
            sa.Column('notification_sent', sa.Boolean(), nullable=True),
            sa.Column('timestamp', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('dag_id', 'execution_date')
        )
    if 'schedule_plan_tag' not in tables:
        op.create_table(
            'schedule_plan_tag',
            sa.Column('name', sa.String(length=250), nullable=False),
            sa.Column('description', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('name')
        )

    if 'schedule_plan' not in tables:
        op.create_table(
            'schedule_plan',
            sa.Column('tag', sa.String(length=250), nullable=False),
            sa.Column('schedule_date', sa.Date(), nullable=False),
            sa.PrimaryKeyConstraint('tag', 'schedule_date')
        )

    if 'dag_file' not in tables:
        op.create_table(
            'dag_file',
            sa.Column('fileloc', sa.String(length=700), nullable=False),
            sa.Column('node_id', sa.Integer(), nullable=True),
            sa.Column('state', sa.String(length=20), nullable=True),
            sa.Column('is_exist', sa.Boolean(), nullable=True),
            sa.Column('latest_refresh', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('fileloc')
        )
        op.create_index('df_node_id', 'dag_file', ['node_id'], unique=False)
        op.create_index('df_state', 'dag_file', ['state'], unique=False)
        op.create_index('df_is_exist', 'dag_file', ['is_exist'], unique=False)
        op.create_index('df_latest_refresh', 'dag_file', ['latest_refresh'], unique=False)

    if 'node_instance' not in tables:
        op.create_table(
            'node_instance',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('instance', sa.String(length=100), nullable=False),
            sa.Column('hostname', sa.String(length=500), nullable=True),
            sa.Column('state', sa.String(length=20), nullable=True),
            sa.Column('start_date', sa.DateTime(), nullable=True),
            sa.Column('latest_heartbeat', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index('ni_instance', 'node_instance', ['instance'], unique=False)
        op.create_index('ni_state', 'node_instance', ['state'], unique=False)

    if 'message' not in tables:
        op.create_table(
            'message',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('type', sa.String(length=100), nullable=False),
            sa.Column('content', sa.String(length=2000), nullable=True),
            sa.Column('create_date', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index('m_type_id', 'message', ['type', 'id'], unique=False)

    if 'simple_dag' not in tables:
        op.create_table(
            'simple_dag',
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('task_ids', sa.PickleType(pickler=dill)),
            sa.Column('full_filepath', sa.String(length=2000), nullable=True),
            sa.Column('concurrency', sa.Integer(), nullable=True),
            sa.Column('pickle_id', sa.Integer(), nullable=True),
            sa.Column('task_special_args', sa.PickleType(pickler=dill)),
            sa.PrimaryKeyConstraint('dag_id')
        )


def downgrade():
    op.drop_table('known_event')
    op.drop_table('chart')
    op.drop_table('variable')
    op.drop_table('user')
    op.drop_index('ti_state_lkp', table_name='task_instance')
    op.drop_index('ti_pool', table_name='task_instance')
    op.drop_index('ti_dag_state', table_name='task_instance')
    op.drop_table('task_instance')
    op.drop_table('slot_pool')
    op.drop_table('sla_miss')
    op.drop_table('log')
    op.drop_table('known_event_type')
    op.drop_index('job_type_heart', table_name='job')
    op.drop_table('job')
    op.drop_table('import_error')
    op.drop_table('dag_pickle')
    op.drop_table('dag')
    op.drop_table('connection')
    op.drop_table('xcom')
    op.drop_table('dag_sla_miss')
    op.drop_table('schedule_plan')
    op.drop_table('schedule_plan_tag')
    op.drop_table('dag_file')
    op.drop_table('node_instance')
    op.drop_table('message')
    op.drop_table('simple_dag')
