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
from airflow.models.base import Base, ID_LEN
import dill
from sqlalchemy import Column, PickleType, Integer, String
from airflow.utils.db import provide_session


class SimpleDagModel(Base):
    __tablename__ = "simple_dag"

    dag_id = Column(String(ID_LEN), primary_key=True)
    task_ids = Column(PickleType(pickler=dill))
    full_filepath = Column(String(2000))
    concurrency = Column(Integer)
    pickle_id = Column(Integer)
    task_special_args = Column(PickleType(pickler=dill))

    @staticmethod
    @provide_session
    def sync_to_db(simple_dag, session=None):
        sdm = SimpleDagModel()
        sdm.dag_id = simple_dag.dag_id
        sdm.task_ids = simple_dag.task_ids
        sdm.full_filepath = simple_dag.full_filepath
        sdm.concurrency = simple_dag.concurrency
        sdm.pickle_id = simple_dag.pickle_id
        sdm.task_special_args = simple_dag.task_special_args
        session.merge(sdm)
        session.commit()

    @staticmethod
    @provide_session
    def delete(dag_ids, session=None):
        session.query(SimpleDagModel).filter(SimpleDagModel.dag_id.in_(dag_ids)).delete(synchronize_session='fetch')
