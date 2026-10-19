# -*- coding: utf8 -*-

"""
Lambda function to manage AEM Stack resources.
"""


import os
import logging
import json
import datetime
import boto3


__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', str(logging.INFO))))

# AWS resources
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')


class MyEncoder(json.JSONEncoder):
    """JSON encoder that serialises datetime objects as ISO-8601 UTC strings."""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        return json.JSONEncoder.default(self, o)

def query_state_by_external_id(table_name, external_id):
    """Query the latest task state for an external ID from the DynamoDB table.

    Returns a dict with 'externalId' and, if a task was found, 'status'.
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
    """Lambda entry point: load config from S3 or the bundle, then return the
    task state for event['externalId'].
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
