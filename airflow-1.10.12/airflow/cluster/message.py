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
import json
from sqlalchemy import Column, Index, Integer, String, func, asc
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.models import Base
import sqlalchemy as sqla

TYPE_DSYNCER = "dsyncer"
TYPE_TASK_RUNNING = "task_running"


class Message(Base):
    __tablename__ = "message"

    id = Column(Integer, primary_key=True)
    type = Column(String(100))
    content = Column(String(2000))
    create_date = Column(UtcDateTime())

    __table_args__ = (
        Index('m_type_id', type, id),
    )

    @staticmethod
    @provide_session
    def add(type, contents, session=None):
        if isinstance(contents, list) or isinstance(contents, set):
            for content in contents:
                session.add(Message(type=type, content=content, create_date=timezone.utcnow()))
        else:
            session.add(Message(type=type, content=contents, create_date=timezone.utcnow()))
        session.commit()

    @staticmethod
    @provide_session
    def find_messages(watermark, type=None, session=None):
        max_id = Message.look_for_max_id(session)
        if max_id is None or max_id == watermark:
            return max_id, []
        qry = session.query(Message)
        if type is not None:
            qry = qry.filter(Message.type.in_(type))
        if watermark is not None:
            qry = qry.filter(Message.id.__gt__(watermark))
        qry = qry.filter(Message.id.__le__(max_id))
        qry = qry.order_by(asc(Message.id))
        return max_id, qry.all()

    @staticmethod
    @provide_session
    def look_for_max_id(session=None):
        return session.query(sqla.func.max(Message.id)).first()[0]


def make_task_running_content(dag_id, task_id, execution_date, try_number):
    return json.dumps({'dag_id': dag_id,
                       'task_id': task_id,
                       'execution_date': execution_date.isoformat(),
                       'try_number': try_number})


def parse_task_running_content(content):
    ti = json.loads(content)
    return ti['dag_id'], ti['task_id'], timezone.parse(ti['execution_date']), ti['try_number']
