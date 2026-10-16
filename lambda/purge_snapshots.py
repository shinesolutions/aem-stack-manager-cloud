# -*- coding: utf8 -*-

"""
Lambda function to manage AEM Stack resources.
"""

import os
import logging
import datetime
import re
import time
import boto3


__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', str(logging.INFO))))


def purge_old_snapshots(params):
    """
    Delete completed snapshots that are older than the requested age.

    Snapshots are matched on the optional 'StackPrefix' tag, the 'SnapshotType'
    tag and an 'AemId' tag of author or publish.

    Args:
        params: dict with 'SnapshotType', 'Age' (e.g. 2d, 12h, 1w) and
            optionally 'StackPrefix'.

    Raises:
        RuntimeError: if the unit of 'Age' is not supported.
    """

    filters = [
        {
            'Name': 'status',
            'Values': ['completed']
        }
    ]

    if 'StackPrefix' in params:
        filters.append({
            'Name': 'tag:StackPrefix',
            'Values': [params['StackPrefix']]
        })
    else:
        filters.append({
            'Name': 'tag-key',
            'Values': ['StackPrefix']
        })

    filters.append(
        {
            'Name': 'tag:SnapshotType',
            'Values': [params['SnapshotType']]
        }
    )

    filters.append(
        {
            'Name': 'tag:AemId',
            'Values': ['author', 'publish']
        }
    )

    # snapshots is a list
    snapshots = boto3.resource('ec2').snapshots.filter(Filters=filters)

    quantity = params['Age'][:-1]
    unit = params['Age'][-1]

    if unit == 'd':
        delta = datetime.timedelta(days=int(quantity))
    elif unit == 'h':
        delta = datetime.timedelta(hours=int(quantity))
    elif unit == 'w':
        delta = datetime.timedelta(weeks=int(quantity))
    else:
        raise RuntimeError('Unsupported time unit')

    now = datetime.datetime.utcnow()
    old_snapshots = [snapshot for snapshot in snapshots if (now - snapshot.start_time.replace(tzinfo=None)) > delta]

    logger.info('Deleting %d %s snapshots older than %s', len(old_snapshots), params['SnapshotType'], delta)
    for snapshot in old_snapshots:
        print(f'Deleting snapshot {snapshot.snapshot_id}')
        snapshot.delete()
        time.sleep(0.5)


def handler(event, context):  # pylint: disable=unused-argument
    """
    Lambda entry point that purges old snapshots.

    Args:
        event: dict with 'SnapshotType' (live, offline or orchestration),
            'Age' in the form <number>[h|d|w] and optionally 'StackPrefix'.
        context: Lambda context object, not used.

    Raises:
        RuntimeError: if 'SnapshotType' or 'Age' is missing or invalid.
    """

    if 'SnapshotType' not in event or event['SnapshotType'] not in ['live', 'offline', 'orchestration']:
        logger.error('SnapshotType [live|offline] must be specified')
        raise RuntimeError('\'SnapshotType\' is expected in inputs and have value of \'live\', \'offline\', or \'orchestration\'')

    if 'Age' not in event or not re.match(r'^\d+[hdw]$', event['Age'], re.I):
        logger.error('Age must be specified in xx[h|d|m] format')
        raise RuntimeError('\'Age\' is expected in inputs and in correct format')

    purge_old_snapshots(event)
