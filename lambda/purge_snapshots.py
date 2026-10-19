# -*- coding: utf8 -*-

"""
Lambda function that purges old AEM EBS snapshots.

It is normally triggered on a schedule by an AWS CloudWatch Events rule. Each
invocation deletes the completed snapshots of one snapshot type (``live``,
``offline`` or ``orchestration``) that are older than a given age.

Example event::

    {
        "SnapshotType": "offline",
        "Age": "2w",
        "StackPrefix": "my-stack"
    }
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


#: Module logger. Its level comes from the ``LOG_LEVEL`` environment variable
#: and defaults to ``logging.INFO``.
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', str(logging.INFO))))


def purge_old_snapshots(params):
    """
    Delete completed EBS snapshots that are older than the requested age.

    Only snapshots with all of these properties are deleted:

    * status is ``completed``
    * a ``StackPrefix`` tag equal to ``params['StackPrefix']``, or any
      ``StackPrefix`` tag at all when no stack prefix is given
    * a ``SnapshotType`` tag equal to ``params['SnapshotType']``
    * an ``AemId`` tag of ``author`` or ``publish``

    Deletions are spaced 0.5 seconds apart to stay under the EC2 API rate
    limits.

    :param params: Purge criteria with these keys:

        * ``SnapshotType`` (*str*) -- value of the ``SnapshotType`` tag to match.
        * ``Age`` (*str*) -- minimum age of a snapshot to delete, written as a
          number followed by a unit: ``h`` (hours), ``d`` (days) or ``w``
          (weeks), e.g. ``12h``, ``2d`` or ``1w``.
        * ``StackPrefix`` (*str*, optional) -- only purge snapshots of this
          stack.
    :type params: dict
    :raises RuntimeError: If the unit of ``Age`` is not ``h``, ``d`` or ``w``.
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

    # lazily evaluated collection of matching ec2.Snapshot resources
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
    Lambda entry point: validate the event and purge old snapshots.

    :param event: Purge criteria. ``SnapshotType`` must be ``live``,
        ``offline`` or ``orchestration``. ``Age`` must match
        ``<number>[h|d|w]`` (case-insensitive). ``StackPrefix`` is optional.
        See :func:`purge_old_snapshots` for details.
    :type event: dict
    :param context: Lambda context object. Not used.
    :type context: object
    :raises RuntimeError: If ``SnapshotType`` or ``Age`` is missing or
        invalid.
    """

    if 'SnapshotType' not in event or event['SnapshotType'] not in ['live', 'offline', 'orchestration']:
        logger.error('SnapshotType [live|offline] must be specified')
        raise RuntimeError('\'SnapshotType\' is expected in inputs and have value of \'live\', \'offline\', or \'orchestration\'')

    if 'Age' not in event or not re.match(r'^\d+[hdw]$', event['Age'], re.I):
        logger.error('Age must be specified in xx[h|d|m] format')
        raise RuntimeError('\'Age\' is expected in inputs and in correct format')

    purge_old_snapshots(event)
