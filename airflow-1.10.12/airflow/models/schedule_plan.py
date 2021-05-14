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

from sqlalchemy import Column, String, Date

from airflow.models.base import Base, ID_LEN
from datetime import timedelta, datetime
from airflow.utils.db import provide_session
from airflow.utils import timezone


class SchedulePlan(Base):
    __tablename__ = "schedule_plan"
    tag = Column(String(ID_LEN), primary_key=True)
    schedule_date = Column(Date, primary_key=True)


class SchedulePlanCache(object):
    def __init__(self):
        self.cache = {}
        self.last_refresh = {}
        self.refresh_cache_interval = 3600

    @provide_session
    def exists(self, name, d,  use_cache=False, session=None):
        if not use_cache:
            sp = session.query(SchedulePlan)\
                .filter(SchedulePlan.tag == name,
                        SchedulePlan.schedule_date == d)\
                .first()
            return sp is not None
        return False

    @provide_session
    def get_last(self, name, d, use_cache=False,  session=None):
        if not use_cache:
            sp = session.query(SchedulePlan)\
                .filter(SchedulePlan.tag == name)\
                .filter(SchedulePlan.schedule_date.__lt__(d))\
                .order_by(SchedulePlan.schedule_date.desc())\
                .first()
            if sp is not None:
                return sp.schedule_date
        return None

    @provide_session
    def get_next(self, name, d, use_cache=False, session=None):
        if not use_cache:
            sp = session.query(SchedulePlan) \
                .filter(SchedulePlan.tag == name) \
                .filter(SchedulePlan.schedule_date.__gt__(d)) \
                .order_by(SchedulePlan.schedule_date) \
                .first()
            if sp is not None:
                return sp.schedule_date
        return None

    @provide_session
    def get_self_or_last(self, name, d, use_cache=False, session=None):
        if not use_cache:
            sp = session.query(SchedulePlan) \
                .filter(SchedulePlan.tag == name) \
                .filter(SchedulePlan.schedule_date.__le__(d)) \
                .order_by(SchedulePlan.schedule_date.desc()) \
                .first()
            if sp is not None:
                return sp.schedule_date
        return None

    @provide_session
    def get_self_or_next(self, name, d, use_cache=False, session=None):
        if not use_cache:
            sp = session.query(SchedulePlan) \
                .filter(SchedulePlan.tag == name) \
                .filter(SchedulePlan.schedule_date.__ge__(d)) \
                .order_by(SchedulePlan.schedule_date) \
                .first()
            if sp is not None:
                return sp.schedule_date
        return None


schedule_plan_cache = SchedulePlanCache()


def resolve_time(time):
    if not timezone.is_naive(time):
        time = timezone.make_naive(time)
    time = timezone.make_aware(time)
    return timezone.convert_to_utc(time)


earliest_time = resolve_time(timezone.parse('1970-01-02 00:00:00'))
latest_time = resolve_time(timezone.parse('2038-01-18 00:00:00'))


@provide_session
def check_following_schedule(name, dttm, offset=0, naive=False, session=None):
    d = dttm.date() - timedelta(days=offset)
    self_or_next = schedule_plan_cache.get_self_or_next(name, d, session=session)
    if self_or_next is None:
        return True, latest_time
    if d == self_or_next:
        return True, dttm
    r = self_or_next + timedelta(days=offset - 1)
    if naive:
        return False, datetime(year=r.year, month=r.month, day=r.day, hour=23, minute=59, second=59)
    else:
        return False, resolve_time(timezone.parse(str(r) + " 23:59:59"))


@provide_session
def check_previous_schedule(name, dttm, offset=0, naive=False, session=None):
    d = dttm.date() - timedelta(days=offset)
    self_or_last = schedule_plan_cache.get_self_or_last(name, d, session=session)
    if self_or_last is None:
        return True, earliest_time
    if d == self_or_last:
        return True, dttm
    r = self_or_last + timedelta(days=offset + 1)
    if naive:
        return False, datetime(year=r.year, month=r.month, day=r.day, hour=0, minute=0, second=0)
    else:
        return False, resolve_time(timezone.parse(str(r) + " 00:00:00"))
