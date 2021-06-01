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
from sqlalchemy import Boolean, Column, Index, Integer, String, func, or_, desc
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.db import provide_session
from airflow.models import Base
from airflow import settings
from airflow.exceptions import AirflowException

DAG_FILE_READY = 'ready'
DAG_FILE_ASSIGNED = 'assigned'
DAG_FILE_SCHEDULED = 'scheduled'
DAG_FILE_RELEASE = 'release'


class DagFile(Base):

    __tablename__ = "dag_file"

    fileloc = Column(String(700), primary_key=True)
    node_id = Column(Integer)
    state = Column(String(20))
    is_exist = Column(Boolean, default=True)
    latest_refresh = Column(UtcDateTime())

    __table_args__ = (
        Index('df_node_id', node_id),
        Index('df_state', state),
        Index('df_is_exist', is_exist),
        Index('df_latest_refresh', latest_refresh),
    )

    @staticmethod
    @provide_session
    def upsert_mysql(insert, fileloc, latest_refresh, session=None):
        insert_stmt = insert(DagFile).values(fileloc=fileloc,
                                             state=DAG_FILE_READY,
                                             is_exist=True,
                                             latest_refresh=latest_refresh)
        up_stmt = insert_stmt.on_duplicate_key_update(fileloc=fileloc, is_exist=True, latest_refresh=latest_refresh)
        session.execute(up_stmt)
        session.commit()

    @staticmethod
    @provide_session
    def upsert_postgresql(insert, fileloc, latest_refresh, session=None):
        insert_stmt = insert(DagFile).values(fileloc=fileloc,
                                             state=DAG_FILE_READY,
                                             is_exist=True,
                                             latest_refresh=latest_refresh)
        up_stmt = insert_stmt.on_conflict_do_update(index_elements=['fileloc'],
                                                    set_=dict(is_exist=True, latest_refresh=latest_refresh))
        session.excute(up_stmt)
        session.commit()

    @staticmethod
    @provide_session
    def delete(fileloc, session=None):
        session.query(DagFile).filter(DagFile.fileloc == fileloc)\
            .update({DagFile.is_exist: False}, synchronize_session=False)


def get_df_upsert_function():
    if settings.engine.dialect.name == "mysql":
        from sqlalchemy.dialects.mysql import insert
        return lambda fileloc, latest_refresh, session: DagFile.upsert_mysql(insert, fileloc, latest_refresh, session)
    elif settings.engine.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
        return lambda fileloc, latest_refresh, session: DagFile.upsert_postgresql(insert, fileloc, latest_refresh, session)
    else:
        raise AirflowException("Engine dialect not support: " + settings.engine.dialect.name)
