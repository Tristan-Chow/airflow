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

from __future__ import absolute_import
from datetime import datetime, timedelta
import dateutil # noqa
from random import random # noqa
import time # noqa
from . import hive # noqa
import uuid # noqa
import calendar


def ds_get_date(ds, _type, interval, first_or_last, _format='%Y%m%d'):
    if _type == "daily":
        ds = datetime.strptime(ds, '%Y-%m-%d')
        if interval > 0:
            ds = ds - timedelta(interval)
        return ds.strftime(_format)
    if _type == "weekly":
        ds = datetime.strptime(ds, '%Y-%m-%d')
        if interval > 0:
            ds = ds - timedelta(weeks=interval)
        one_day = timedelta(days=1)
        if first_or_last == "first":
            while ds.weekday() != 0:
                ds -= one_day
            return ds.strftime(_format)
        if first_or_last == "last":
            while ds.weekday() != 6:
                ds += one_day
            return ds.strftime(_format)
    if _type == "monthly":
        year_month_dag = ds.split("-")
        year = int(year_month_dag[0])
        month = int(year_month_dag[1])
        while interval > 0:
            month -= 1
            if month == 0:
                month = 12
                year -= 1
            interval -= 1
        if first_or_last == "first":
            return datetime(year=year, month=month, day=1).strftime(_format)
        if first_or_last == "last":
            _, month_days = calendar.monthrange(year, month)
            return datetime(year=year, month=month, day=month_days).strftime(_format)


def ds_add(ds, days):
    """
    Add or subtract days from a YYYY-MM-DD

    :param ds: anchor date in ``YYYY-MM-DD`` format to add to
    :type ds: str
    :param days: number of days to add to the ds, you can use negative values
    :type days: int

    >>> ds_add('2015-01-01', 5)
    '2015-01-06'
    >>> ds_add('2015-01-06', -5)
    '2015-01-01'
    """

    ds = datetime.strptime(ds, '%Y-%m-%d')
    if days:
        ds = ds + timedelta(days)
    return ds.isoformat()[:10]


def ds_format(ds, input_format, output_format):
    """
    Takes an input string and outputs another string
    as specified in the output format

    :param ds: input string which contains a date
    :type ds: str
    :param input_format: input string format. E.g. %Y-%m-%d
    :type input_format: str
    :param output_format: output string format  E.g. %Y-%m-%d
    :type output_format: str

    >>> ds_format('2015-01-01', "%Y-%m-%d", "%m-%d-%y")
    '01-01-15'
    >>> ds_format('1/5/2015', "%m/%d/%Y",  "%Y-%m-%d")
    '2015-01-05'
    """
    return datetime.strptime(ds, input_format).strftime(output_format)


def datetime_diff_for_humans(dt, since=None):
    """
    Return a human-readable/approximate difference between two datetimes, or
    one and now.

    :param dt: The datetime to display the diff for
    :type dt: datetime
    :param since: When to display the date from. If ``None`` then the diff is
        between ``dt`` and now.
    :type since: None or datetime
    :rtype: str
    """
    import pendulum

    return pendulum.instance(dt).diff_for_humans(since)


def _integrate_plugins():
    """Integrate plugins to the context"""
    import sys
    from airflow.plugins_manager import macros_modules
    for macros_module in macros_modules:
        sys.modules[macros_module.__name__] = macros_module
        globals()[macros_module._name] = macros_module

        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        import os as _os
        if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _macro in macros_module._objects:
                macro_name = _macro.__name__
                globals()[macro_name] = _macro
                _deprecated(
                    macro_name,
                    "Importing plugin macro '{i}' directly from "
                    "'airflow.macros' has been deprecated. Please "
                    "import from 'airflow.macros.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=macro_name))
