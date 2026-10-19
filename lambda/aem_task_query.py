# -*- coding: utf8 -*-

"""
Lambda function that reports the status of an AEM Stack Manager task.

Callers such as a CI job pass the ``externalId`` they supplied when they
triggered the task. The function looks up the latest state recorded for that
ID in the Stack Manager DynamoDB table.

Example event::

    {"externalId": "jenkins-build-1234"}

Example response::

    {"externalId": "jenkins-build-1234", "status": "Success"}
"""


import os
import logging
import json
import datetime
import boto3


__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


#: Module logger. Its level comes from the ``LOG_LEVEL`` environment variable
#: and defaults to ``logging.INFO``.
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', str(logging.INFO))))

#: S3 client, used to download ``config.json``.
s3 = boto3.client('s3')
#: DynamoDB client, used to query task state.
dynamodb = boto3.client('dynamodb')


class MyEncoder(json.JSONEncoder):
    """JSON encoder that serialises :class:`datetime.datetime` objects.

    Datetimes become ISO 8601 strings in the form ``YYYY-MM-DDTHH:MM:SS.000Z``.
    Any other type is passed to :class:`json.JSONEncoder`.
    """

    def default(self, o):
        """Return a JSON-serialisable form of ``o``.

        :param o: Object that the standard encoder cannot serialise.
        :type o: object
        :returns: ``o`` formatted as a timestamp string if it is a datetime.
        :rtype: str
        :raises TypeError: If ``o`` is not a datetime and cannot be serialised.
        """
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        return json.JSONEncoder.default(self, o)

def query_state_by_external_id(table_name, external_id):
    """Return the latest recorded state of the task with the given external ID.

    The ``ExternalQuery`` global secondary index is queried in descending sort
    order, and only the first item is read.

    :param table_name: Name of the Stack Manager DynamoDB table.
    :type table_name: str
    :param external_id: ID the caller supplied when triggering the task.
    :type external_id: str
    :returns: A dict that always holds ``externalId``. It also holds
        ``status`` (the task state, e.g. ``Success``) if a matching task was
        found.
    :rtype: dict
    """

    response = dynamodb.query(
        TableName=table_name,
        IndexName='ExternalQuery',
        ProjectionExpression='#s',
        KeyConditionExpression = 'externalId = :kval',
        ExpressionAttributeNames = {
            '#s': 'state'
        },
        ExpressionAttributeValues = {
            ':kval': {
                'S': external_id
            }
        },
        ScanIndexForward = False,
        ReturnConsumedCapacity='NONE',
        Limit=1
    )

    task_state = {'externalId': external_id}
    if len(response['Items']) != 0:
        task_state['status'] = response['Items'][0]['state']['S']

    return task_state


def handler(event, context):  # pylint: disable=unused-argument
    """Lambda entry point: return the state of the task named in the event.

    ``config.json`` is downloaded from ``s3://$S3_BUCKET/$S3_PREFIX/`` when both
    environment variables are set. Otherwise the copy bundled with the
    function is used. The DynamoDB table name is read from
    ``ec2_run_command.dynamodb-table`` in that file.

    :param event: Invocation payload. Should hold ``externalId``.
    :type event: dict
    :param context: Lambda context object. Not used.
    :type context: object
    :returns: The result of :func:`query_state_by_external_id`, or an empty
        dict if the event has no ``externalId``.
    :rtype: dict
    """

    # reading in config info from either s3 or within bundle
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')

    if bucket is not None and prefix is not None:
        config_file = '/tmp/config.json'
        s3.download_file(bucket, f'{prefix}/config.json', config_file)
    else:
        logger.info('Unable to locate config.json in S3, searching within bundle')
        config_file = 'config.json'

    with open(config_file, 'r', encoding='utf-8') as f:
        content = ''.join(f.readlines()).replace('\n', '')
        logger.debug('config file: %s', content)
        config = json.loads(content)
        run_command = config['ec2_run_command']
        dynamodb_table = run_command['dynamodb-table']

    if 'externalId' in event:
        external_id = event['externalId']
        result = query_state_by_external_id(dynamodb_table, external_id)
    else:
        logger.info('Unknown message found and ignored')
        result = {}
    return result
