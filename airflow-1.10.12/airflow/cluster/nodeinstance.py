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
from airflow.utils.net import get_hostname
from sqlalchemy import Boolean, Column, Index, Integer, String, func, or_, desc
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.models import Base

NODE_INSTANCE_DEAD = 'dead'
NODE_INSTANCE_READY = 'ready'
NODE_INSTANCE_RUNNING = 'running'

INSTANCE_MASTER = 'master'
INSTANCE_SCHEDULER = 'scheduler'


class NodeInstance(Base):

    __tablename__ = "node_instance"

    id = Column(Integer, primary_key=True)
    instance = Column(String(100))
    hostname = Column(String(500))
    state = Column(String(20))
    start_date = Column(UtcDateTime())
    latest_heartbeat = Column(UtcDateTime())

    __table_args__ = (
        Index('ni_instance', instance),
        Index('ni_state', state),
    )

    @staticmethod
    def create_instance(id=None, instance=None, state=NODE_INSTANCE_RUNNING):
        node = NodeInstance()
        if id is not None:
            node.id = id
        node.instance = instance
        node.hostname = get_hostname()
        node.state = state
        node.start_date = timezone.utcnow()
        node.latest_heartbeat = timezone.utcnow()
        return node

    @staticmethod
    def check_node_alive(node, timeout=30):
        if node.hostname is None \
            or node.state is None \
            or node.state == NODE_INSTANCE_DEAD \
            or (0 < timeout < (timezone.utcnow() - node.latest_heartbeat).total_seconds()):
            return False
        return True

    @staticmethod
    @provide_session
    def select(id=None, lock_for_update=False, session=None):
        qry = session.query(NodeInstance).filter(NodeInstance.id == id)
        if lock_for_update:
            node = qry.with_for_update().populate_existing().first()
        else:
            node = qry.first()
        return node
