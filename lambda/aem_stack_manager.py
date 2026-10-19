# -*- coding: utf8 -*-

"""
Lambda function that runs AEM Stack Manager tasks with EC2 Run Command.

The function is subscribed to two SNS topics:

* **Stack Manager topic** -- task requests published by AEM Stack Manager
  Messenger, e.g. ``deploy-artifacts``. Each task is mapped to an SSM
  document, which is sent to the running instances of the requested
  components. The new command is recorded in DynamoDB.
* **Status topic** -- EC2 Run Command completion notifications. The state of
  the matching command in DynamoDB is updated.

The task-to-document mapping and the Run Command settings are read from
``config.json``.
"""


import os
import logging
import json
import datetime
import re
import boto3


__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


#: Module logger. Its level comes from the ``LOG_LEVEL`` environment variable
#: and defaults to ``logging.INFO``.
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', str(logging.INFO))))

#: AWS region of the clients below, taken from ``AWS_REGION``.
aws_region = os.getenv("AWS_REGION")
#: SSM client, used to send EC2 Run Commands.
ssm = boto3.client('ssm', region_name=aws_region)
#: EC2 client, used to find target instances by tag.
ec2 = boto3.client('ec2', region_name=aws_region)
#: S3 client, used to download ``config.json``.
s3 = boto3.client('s3', region_name=aws_region)
#: DynamoDB client, used to record task state.
dynamodb = boto3.client('dynamodb', region_name=aws_region)


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


def instance_ids_by_tags(filters):
    """Return the IDs of the EC2 instances that match the given filters.

    :param filters: Filters in the format of
        :meth:`EC2.Client.describe_instances`, e.g.
        ``[{'Name': 'tag:Component', 'Values': ['publish']}]``.
    :type filters: list[dict]
    :returns: IDs of the matching instances, across all reservations.
    :rtype: list[str]
    """
    response = ec2.describe_instances(
        Filters=filters
    )
    response2 = json.loads(json.dumps(response, cls=MyEncoder))

    instance_ids = []
    for reservation in response2['Reservations']:
        instance_ids += [instance['InstanceId'] for instance in reservation['Instances']]
    return instance_ids


def send_ssm_cmd(cmd_details):
    """Send an EC2 Run Command.

    :param cmd_details: Keyword arguments for :meth:`SSM.Client.send_command`.
    :type cmd_details: dict
    :returns: The ``send_command`` response, with datetimes converted to
        strings by :class:`MyEncoder`.
    :rtype: dict
    """
    logger.info(' calling ssm commands')
    return json.loads(json.dumps(ssm.send_command(**cmd_details), cls=MyEncoder))


def execute_task(message, ssm_common_params):
    """Run a task's SSM document on the matching instances of a stack.

    The targets are the running instances whose ``StackPrefix`` tag equals
    ``message['stack_prefix']`` and whose ``Component`` tag is one of
    ``message['details']['component']``.

    :param message: Task request from the Stack Manager topic. Must hold
        ``stack_prefix`` and ``details``. ``details`` holds ``component``
        (list of component names), ``comment`` and, optionally,
        ``parameters`` (the SSM document parameters).
    :type message: dict
    :param ssm_common_params: Task-independent arguments for
        :meth:`SSM.Client.send_command`, such as ``DocumentName``, output
        bucket and notification settings. Not modified.
    :type ssm_common_params: dict
    :returns: The response of :func:`send_ssm_cmd`.
    :rtype: dict
    """
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
    # SSM Targets cannot combine several tag filters, so the instances are
    # resolved up front and passed as InstanceIds instead
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


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def put_state_in_dynamodb(table_name, command_id, environment, task, state, timestamp, message_id, **kwargs):

    """Write the state of a command to DynamoDB, replacing any existing item.

    Item schema (DynamoDB attribute types in brackets):

    ===================  ====  ===============================================
    Attribute            Type  Description
    ===================  ====  ===============================================
    ``command_id``       S     Partition key. The EC2 Run Command ID, or the
                               external ID if no command has run yet.
    ``environment``      S     Usually the stack prefix.
    ``task``             S     Task name, e.g. ``deploy-artifacts``.
    ``state``            S     Command status, e.g. ``Pending``, ``Success``
                               or ``Failed``.
    ``timestamp``        S     Time of the state, e.g.
                               ``2017-05-16T01:57:05.9Z``.
    ``ttl``              N     Expiry time in epoch seconds, one day from now.
    ``message_id``       S     ID of the SNS message that caused the write.
    ``instance_info``    M     Optional. Instance ID by component, e.g.
                               ``{'author-primary': {'S': 'i-13ad9rxxxx'}}``.
    ``last_command``     S     Optional. ID of the previous Run Command in
                               the chain. Mostly for debugging.
    ``externalId``       S     Optional. ID from an external caller, e.g. a
                               Jenkins or Bamboo job ID.
    ===================  ====  ===============================================

    :param table_name: Name of the DynamoDB table.
    :type table_name: str
    :param command_id: Value of the ``command_id`` key.
    :type command_id: str
    :param environment: Value of ``environment``.
    :type environment: str
    :param task: Value of ``task``.
    :type task: str
    :param state: Value of ``state``.
    :type state: str
    :param timestamp: Value of ``timestamp``.
    :type timestamp: str
    :param message_id: Value of ``message_id``.
    :type message_id: str
    :param kwargs: Optional attributes. ``InstanceInfo`` (*dict*),
        ``LastCommand`` (*str*) and ``ExternalId`` (*str*) are written when
        given and not ``None``. Other keys are ignored.
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


def get_state_from_dynamodb(table_name, command_id):
    """Read the state of a command from DynamoDB with a consistent read.

    Only ``environment``, ``task``, ``state``, ``instance_info`` and
    ``externalId`` are returned.

    :param table_name: Name of the DynamoDB table.
    :type table_name: str
    :param command_id: The ``command_id`` key to read.
    :type command_id: str
    :returns: The raw :meth:`DynamoDB.Client.get_item` response. The item is
        under ``Item`` and that key is missing if no item was found.
    :rtype: dict
    """

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
    """Set the ``state`` and ``timestamp`` of a command in DynamoDB.

    :param table_name: Name of the DynamoDB table.
    :type table_name: str
    :param command_id: The ``command_id`` key to update.
    :type command_id: str
    :param new_state: New value of ``state``, e.g. ``Success``.
    :type new_state: str
    :param timestamp: New value of ``timestamp``.
    :type timestamp: str
    """

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


# pylint: disable-next=unused-argument
def sns_message_processor(event, context):
    """Lambda entry point: process the SNS records in the event.

    ``config.json`` is downloaded from ``s3://$S3_BUCKET/$S3_PREFIX/`` when both
    environment variables are set. Otherwise the copy bundled with the
    function is used.

    The message of each record is handled as follows:

    * If it has a ``task``, the task's SSM document (from
      ``document_mapping``) is run by :func:`execute_task`. The new command is
      recorded by :func:`put_state_in_dynamodb`.
    * If it has a ``commandId``, it is a Run Command status notification. The
      command's state is updated by :func:`update_state_in_dynamodb`.
    * Anything else is logged and ignored.

    Messages may use single quotes instead of double quotes. A
    ``packageFilter`` value in a task message is escaped before the message is
    parsed as JSON.

    :param event: SNS event with one or more ``Records``.
    :type event: dict
    :param context: Lambda context object. Not used.
    :type context: object
    :returns: One result per handled record: the ``send_command`` response for
        a task, or ``{'status': <status>}`` for a status notification.
    :rtype: list[dict]
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
        task_document_mapping = config['document_mapping']
        run_command = config['ec2_run_command']

        dynamodb_table = run_command['dynamodb-table']

    responses=[]
    for record in event['Records']:
        message_text = record['Sns']['Message']
        message_id = record['Sns']['MessageId']

        logger.info('Message ID: %s', message_id)

        # messages come from either the Stack Manager topic (task requests) or
        # the Status topic (Run Command results); only task requests can carry
        # a packageFilter
        if 'status' not in message_text:
            if message_text.find('packageFilter') != -1:
                # the packageFilter value is a JSON string embedded in the
                # message; escape it so it survives the json.loads() below
                message_text = re.sub('\\\\', '\\\\\\\\\\\\\\\\', message_text)
                message_text = re.sub(r'\["\[','["\\\"[', message_text)
                message_text = re.sub(r'\]"\]',']\\\""]', message_text)
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

            logger.info('Received request for task %s', message['task'])
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
