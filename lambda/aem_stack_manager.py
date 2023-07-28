# -*- coding: utf8 -*-

"""
Lambda function to manage AEM Stack resources.
"""


import os
import boto3
import logging
import json
import datetime
import re


__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', logging.INFO)))

# AWS resources
aws_region = os.getenv("AWS_REGION")
ssm = boto3.client('ssm', region_name=aws_region)
ec2 = boto3.client('ec2', region_name=aws_region)
s3 = boto3.client('s3', region_name=aws_region)
dynamodb = boto3.client('dynamodb', region_name=aws_region)


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        return json.JSONEncoder.default(self, obj)


def instance_ids_by_tags(filters):
    response = ec2.describe_instances(
        Filters=filters
    )
    response2 = json.loads(json.dumps(response, cls=MyEncoder))

    instance_ids = []
    for reservation in response2['Reservations']:
        instance_ids += [instance['InstanceId'] for instance in reservation['Instances']]
    return instance_ids


def send_ssm_cmd(cmd_details):
    logger.info(' calling ssm commands')
    return json.loads(json.dumps(ssm.send_command(**cmd_details), cls=MyEncoder))


def execute_task(message, ssm_common_params):
    component = message['details']['component']
    target_filter = [
        {
            'Name': 'tag:StackPrefix',
            'Values': [message['stack_prefix']]
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }, {
            'Name': 'tag:Component',
            'Values': component
        }
    ]
    # boto3 ssm client does not accept multiple filter for Targets
    if 'parameters' in message['details']:
        parameters = {
         'Parameters': message['details']['parameters']
         }
        details = {
            'InstanceIds': instance_ids_by_tags(target_filter),
            'Comment': message['details']['comment'],
        }

        details.update(parameters)
    else:
        details = {
            'InstanceIds': instance_ids_by_tags(target_filter),
            'Comment': message['details']['comment'],
        }

    params = ssm_common_params.copy()
    params.update(details)

    return send_ssm_cmd(params)

def put_state_in_dynamodb(table_name, command_id, environment, task, state, timestamp, message_id, **kwargs):

    """
    schema:
    key: command_id, ec2 run command id, or externalId if provided and no cmd
         has ran yet
    attr:
      environment: S usually stack_prefix
      state: S STOP_AUTHOR_STANDBY, STOP_AUTHOR_PRIMARY, .... Succeeded, Failed
      timestamp: S, example: 2017-05-16T01:57:05.9Z
      ttl: one day
    Optional attr:
      instance_info: M, exmaple: author-primary: i-13ad9rxxxx
      last_command:  S, Last EC2 Run Command Id that trigggers this command,
                     used more for debugging
      externalId: S, provided by external parties, like Jenkins/Bamboo job id
    """

    # item ttl is set to 1 day
    ttl = (datetime.datetime.now() -
           datetime.datetime.fromtimestamp(0)).total_seconds()
    ttl += datetime.timedelta(days=1).total_seconds()

    item = {
        'command_id': {
            'S': command_id
        },
        'environment': {
            'S': environment
        },
        'task': {
            'S': task
        },
        'state': {
            'S': state
        },
        'timestamp': {
            'S': timestamp
        },
        'ttl': {
            'N': str(ttl)
        },
        'message_id': {
            'S': message_id
        }
    }

    if 'InstanceInfo' in kwargs and kwargs['InstanceInfo'] is not None:
        item['instance_info'] = {'M': kwargs['InstanceInfo']}

    if 'LastCommand' in kwargs and kwargs['LastCommand'] is not None:
        item['last_command'] = {'S': kwargs['LastCommand']}

    if 'ExternalId' in kwargs and kwargs['ExternalId'] is not None:
        item['externalId'] = {'S': kwargs['ExternalId']}

    dynamodb.put_item(
        TableName=table_name,
        Item=item
    )


# dynamodb is used to host state information
def get_state_from_dynamodb(table_name, command_id):

    item = dynamodb.get_item(
        TableName=table_name,
        Key={
            'command_id': {
                'S': command_id
            }
        },
        ConsistentRead=True,
        ReturnConsumedCapacity='NONE',
        ProjectionExpression='environment, task, #command_state, instance_info, externalId',
        ExpressionAttributeNames={
            '#command_state': 'state'
        }
    )

    return item


def update_state_in_dynamodb(table_name, command_id, new_state, timestamp):

    item_update = {
        'TableName': table_name,
        'Key': {
            'command_id': {
                'S': command_id
            }
        },
        'UpdateExpression': 'SET #S = :sval, #T = :tval',
        'ExpressionAttributeNames': {
            '#S': 'state',
            '#T': 'timestamp'
        },
        'ExpressionAttributeValues': {
            ':sval': {
                'S': new_state
            },
            ':tval': {
                'S': timestamp
            }
        }
    }

    dynamodb.update_item(**item_update)

def sns_message_processor(event, context):

    # reading in config info from either s3 or within bundle
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')

    if bucket is not None and prefix is not None:
        config_file = '/tmp/config.json'
        s3.download_file(bucket, '{}/config.json'.format(prefix), config_file)
    else:
        logger.info('Unable to locate config.json in S3, searching within bundle')
        config_file = 'config.json'

    with open(config_file, 'r') as f:
        content = ''.join(f.readlines()).replace('\n', '')
        logger.debug('config file: ' + content)
        config = json.loads(content)
        task_document_mapping = config['document_mapping']
        run_command = config['ec2_run_command']

        dynamodb_table = run_command['dynamodb-table']

    responses=[]
    for record in event['Records']:
        message_text = record['Sns']['Message']
        message_id = record['Sns']['MessageId']

        logger.info("Message ID: " + message_id)

        # we could receive message from Stack Manager Topic, which trigger actions
        # and Status Topic, which tells us how the command ends
        if 'status' not in message_text:
            # Escape packageFilter if packageFilter exist
            if message_text.find('packageFilter') is not -1:
                # Escape packageFilter
                message_text = re.sub('\\\\', '\\\\\\\\\\\\\\\\', message_text)
                message_text = re.sub('\[\"\[', '["\\\"[', message_text)
                message_text = re.sub('\]\"\]', ']\\\""]', message_text)
                # RegEx to find the packageFilter for export-backup Job and replace all single quotes with escaped double quotes
                package_filter_escape_double_quotes = re.sub(r'\'', r'\\\\\\\\\\"', re.search(r'(?<=\"\\\"\[{).*(?=}\]\\\"\")', message_text).group(0))
                message_text = re.sub(r'(?<=\"\\\"\[{).*(?=}\]\\\"\")', package_filter_escape_double_quotes, message_text)

        message_text = message_text.replace('\'', '"')
        message = json.loads(message_text)

        if 'task' in message and message['task'] is not None:
            stack_prefix = message['stack_prefix']

            external_id = None
            if 'externalId' in message:
                external_id = message['externalId']

            logger.info('Received request for task {}'.format(message['task']))
            ssm_common_params = {
                'TimeoutSeconds': 120,
                'DocumentName': task_document_mapping[message['task']],
                'OutputS3BucketName': run_command['cmd-output-bucket'],
                'OutputS3KeyPrefix': run_command['cmd-output-prefix'],
                'ServiceRoleArn': run_command['ssm-service-role-arn'],
                'NotificationConfig': {
                    'NotificationArn': run_command['status-topic-arn'],
                    'NotificationEvents': [
                        'Success',
                        'Failed'
                    ],
                    'NotificationType': 'Command'
                }
            }

            respone = execute_task(message, ssm_common_params)
            put_state_in_dynamodb(
                dynamodb_table,
                respone['Command']['CommandId'],
                stack_prefix,
                message['task'],
                respone['Command']['Status'],
                respone['Command']['RequestedDateTime'],
                message_id,
                ExternalId=external_id
            )

            responses.append(respone)

        elif 'commandId' in message:
            cmd_id = message['commandId']
            update_state_in_dynamodb(
                dynamodb_table,
                cmd_id,
                message['status'],
                message['eventTime']
            )

            response = {
                'status': message['status']
            }
            responses.append(response)

        else:
            logger.error('Unknown message found  and ignored')

    return responses
