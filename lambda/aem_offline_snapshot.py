# -*- coding: utf8 -*-

"""
Lambda function to manage AEM stack offline backups.
It uses a SNS topic to help orchestrate sequence of steps
"""

import os
import boto3
import botocore
import logging
import json
import datetime


__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv('LOG_LEVEL', logging.INFO)))


# AWS resources
ssm = boto3.client('ssm')
ec2 = boto3.client('ec2')
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
sns = boto3.client('sns')


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
    ssm_document = cmd_details['DocumentName']
    print('Start calling SSM Document {} ...'.format(ssm_document))
    response = json.loads(
        json.dumps(
            ssm.send_command(
                **cmd_details
                ),
            cls=MyEncoder
            )
        )
    print('Finished calling SSM Document {}.'.format(ssm_document))
    return response


def get_author_primary_ids(stack_prefix):
    filters = [
        {
            'Name': 'tag:StackPrefix',
            'Values': [stack_prefix]
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }, {
            'Name': 'tag:Component',
            'Values': ['author-primary']
        }
    ]

    return instance_ids_by_tags(filters)


def get_author_standby_ids(stack_prefix):
    filters = [
        {
            'Name': 'tag:StackPrefix',
            'Values': [stack_prefix]
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }, {
            'Name': 'tag:Component',
            'Values': ['author-standby']
        }
    ]

    return instance_ids_by_tags(filters)


def get_promoted_author_standby_ids(stack_prefix):
    filters = [
        {
            'Name': 'tag:StackPrefix',
            'Values': [stack_prefix]
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }, {
            'Name': 'tag:Name',
            'Values': ['AEM Author - Primary - Promoted from Standby']
        }
    ]

    return instance_ids_by_tags(filters)


def get_publish_ids(stack_prefix):
    filters = [
        {
            'Name': 'tag:StackPrefix',
            'Values': [stack_prefix]
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }, {
            'Name': 'tag:Component',
            'Values': ['publish']
        }
    ]

    return instance_ids_by_tags(filters)


def manage_autoscaling_standby(stack_prefix, action, **kwargs):
    """
    put instances in an autoscaling group into or bring them out of
    standby mode one of byComponent or byInstanceIds must be give and not both.
    If byInstanceIds are given, it is assumed that all the instances
    are in the same group
    """
    if 'byComponent' in kwargs:
            filters = [{
                    'Name': 'tag:StackPrefix',
                    'Values': [stack_prefix]
                }, {
                    'Name': 'instance-state-name',
                    'Values': ['running']
                }, {
                    'Name': 'tag:Component',
                    'Values': [kwargs['byComponent']]
                }
            ]
    elif 'byInstanceIds' in kwargs:
        filters = [{
            'Name': 'instance-state-name',
            'Values': ['running']
            }, {
                'Name': 'instance-id',
                'Values': kwargs['byInstanceIds'][0:1]
            }
        ]
    else:
        raise Exception('neither byComponent or byInstanceIds found in arguments')

    # find the autoscaling group those instances are in
    response = ec2.describe_instances(
        Filters=filters
    )
    response2 = json.loads(json.dumps(response, cls=MyEncoder))

    instance_ids = []
    if 'byComponent' in kwargs:
        for reservation in response2['Reservations']:
            instance_ids += [instance['InstanceId'] for instance in reservation['Instances']]
    else:
        instance_ids = kwargs['byInstanceIds']

    instance_tags = response2['Reservations'][0]['Instances'][0]['Tags']
    asg_name = [tag['Value'] for tag in instance_tags if tag['Key'] == 'aws:autoscaling:groupName'][0]

    # try to get the min size of the atutocaling group
    autoscaling = boto3.client('autoscaling')
    asg_dcrb = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    asg_min_size = asg_dcrb['AutoScalingGroups'][0]['MinSize']
    asg_max_size = asg_dcrb['AutoScalingGroups'][0]['MaxSize']

    min_size = max(asg_min_size - len(instance_ids), 0)

    # manage the instances standby mode
    if action == 'enter':
        print('[{}] Start updating ASG {} to suspend processes ...'.format(stack_prefix, asg_name))
        autoscaling.suspend_processes(
            AutoScalingGroupName=asg_name,
            ScalingProcesses=[
                'AlarmNotification',
                'AZrebalance'
            ]
        )
        print('[{}] Finished updating ASG {} to suspend processes ...'.format(stack_prefix, asg_name))

        print('[{}] Start updating ASG {} to {} instances ...'.format(stack_prefix, asg_name, min_size))
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            MinSize=min_size
        )
        print('[{}] Finished updating ASG {} to {} instances.'.format(stack_prefix, asg_name, min_size))

        print('[{}] Start entering instance {} into standby in ASG {} ...'.format(stack_prefix, instance_ids, asg_name))
        autoscaling.enter_standby(
            InstanceIds=instance_ids,
            AutoScalingGroupName=asg_name,
            ShouldDecrementDesiredCapacity=True
        )
        print('[{}] Finished entering instance {} into standby in ASG {}.'.format(stack_prefix, instance_ids, asg_name))
    elif action == 'exit':
        print('[{}] Start exiting instance {} from standby in ASG {} ...'.format(stack_prefix, instance_ids, asg_name))
        autoscaling.exit_standby(
            InstanceIds=instance_ids,
            AutoScalingGroupName=asg_name
        )
        print('[{}] Finished exiting instance {} from standby in ASG {}.'.format(stack_prefix, instance_ids, asg_name))

        print('[{}] Start updating ASG {} to {} instances ...'.format(stack_prefix, asg_name, asg_max_size))
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            MinSize=min(asg_min_size + len(instance_ids), asg_max_size)
        )
        print('[{}] Finished updating ASG {} to {} instances.'.format(stack_prefix, asg_name, asg_max_size))

        print('[{}] Start updating ASG {} to resume suspended processes again ...'.format(stack_prefix, asg_name))
        autoscaling.resume_processes(
            AutoScalingGroupName=asg_name,
            ScalingProcesses=[
                'AlarmNotification',
                'AZrebalance'
            ]
        )
        print('[{}] Finished updating ASG {} to resume suspended processes again ...'.format(stack_prefix, asg_name))

def retrieve_tag_value(instance_id, tag_key):
    response = boto3.client('ec2').describe_tags(
        Filters=[{
            'Name': 'resource-id',
            'Values': [instance_id]
        }, {
            'Name': 'key',
            'Values': [tag_key]
        }]
    )

    tags = {tag['Key']: tag['Value'] for tag in response['Tags']}

    tag_value = None
    if len(tags) != 0:
        tag_value = tags[tag_key]

    return tag_value


def manage_lock_for_environment(table_name, lock, action):
    """
    use lock as command_id to prevent concurrent backup processes
    """

    succeeded = False

    if action == 'trylock':
        try:
            # put a timestamp shows when the lock is set
            timestamp = datetime.datetime.utcnow()

            # item ttl is set to 1 day
            ttl = (timestamp - datetime.datetime.utcfromtimestamp(0)).total_seconds()
            ttl += datetime.timedelta(days=1).total_seconds()

            item = {
                'command_id': {
                    'S': lock
                },
                'timestamp': {
                    'S': timestamp.isoformat()[:-3] + 'Z'
                },
                'ttl': {
                    'N': str(ttl)
                }
            }
            print('Try to lock DynamoDB Table {} ...'.format(table_name))
            dynamodb.put_item(
                TableName=table_name,
                Item=item,
                ConditionExpression='attribute_not_exists(command_id)',
                ReturnValues='NONE'
            )
            print(
                'DynamoDB Table {} locked successfully.'.format(
                    table_name
                )
            )
            succeeded = True
        except botocore.exceptions.ClientError:
            succeeded = False

    elif action == 'unlock':
        print('Unlock DynamoDB Table {}'.format(table_name))
        dynamodb.delete_item(
            TableName=table_name,
            Key={
                'command_id': {
                    'S': lock
                }
            }
        )
        succeeded = True

    return succeeded


def stack_health_check(stack_prefix, min_publish_instances):
    """
    Simple AEM stack health check based on the number of author-primary,
    author-standby and publisher instances.
    """

    # Get instances by components
    author_primary_instances = get_author_primary_ids(stack_prefix)
    author_standby_instances = get_author_standby_ids(stack_prefix)
    publish_instances = get_publish_ids(stack_prefix)
    promoted_author_standby_instances = get_promoted_author_standby_ids(stack_prefix)

    # Count instances
    author_primary_count = len(author_primary_instances)
    author_standby_count = len(author_standby_instances)
    publish_count = len(publish_instances)
    promoted_author_standby_instances_count = len(promoted_author_standby_instances)

    # make sure min_publish_instances is integer
    if type(min_publish_instances) is str:
        min_publish_instances = int(min_publish_instances)

    print('[{}] Start checking Stack health ...'.format(stack_prefix))

    if (author_primary_count == 1 and
        author_standby_count == 1 and
        publish_count >= min_publish_instances):
        paired_publish_dispatcher_id = retrieve_tag_value(publish_instances[0], 'PairInstanceId')
        print('[{}] Finished checking Stack health successfully.'.format(stack_prefix))
        return {
          'author-primary': author_primary_instances[0],
          'author-standby': author_standby_instances[0],
          'publish': publish_instances[0],
          'publish-dispatcher': paired_publish_dispatcher_id
        }
    elif(promoted_author_standby_instances_count == 1 and
        author_standby_count == 0 and
        publish_count >= min_publish_instances):
        paired_publish_dispatcher_id = retrieve_tag_value(publish_instances[0], 'PairInstanceId')
        print('[{}] WARN: Found promoted Author Standby going to run offline-snapshot only on promoted Author instance.'.format(stack_prefix))
        print('[{}] Finished checking Stack health successfully.'.format(stack_prefix))
        return {
          'author-primary': promoted_author_standby_instances[0],
          'author-standby': 'Promoted',
          'publish': publish_instances[0],
          'publish-dispatcher': paired_publish_dispatcher_id
        }

    if author_primary_count != 1:
        logger.error('Found {} author-primary instances. Unhealthy stack.'.format(author_primary_count))

    if author_standby_count != 1:
        logger.error('Found {} author-standby instances. Unhealthy stack.'.format(author_standby_count))

    if promoted_author_standby_instances_count != 0:
        logger.error('Found {} promoted author-standby instances. Unhealthy stack.'.format(author_primary_count))

    if publish_count < min_publish_instances:
        logger.error('Found {} publish instances. Unhealthy stack.'.format(publish_count))

    return None


def put_state_in_dynamodb(table_name, command_id, environment, task, state, timestamp, message_id, **kwargs):

    """
    schema:
    key: command_id, ec2 run command id, or externalId if provided and no cmd
         has ran yet
    attr:
      environment: S usually stack_prefix
      task: S the task this command is for, offline-snapshot-full-set, offline-compaction-snapshot-full-set, etc
      state: S STOP_AUTHOR_STANDBY, STOP_AUTHOR_PRIMARY, .... Success, Failed
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

    # the following three attributes are exclusively for compacting remaining publish instances
    if 'PublishIds' in kwargs and kwargs['PublishIds'] is not None:
        item['publish_ids'] = {'SS': kwargs['PublishIds']}

    if 'DispatcherIds' in kwargs and kwargs['DispatcherIds'] is not None:
        item['dispatcher_ids'] = {'SS': kwargs['DispatcherIds']}

    if 'SubState' in kwargs and kwargs['SubState'] is not None:
        item['sub_state'] = {'S': kwargs['SubState']}

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
        ReturnConsumedCapacity='NONE'
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


# currently just forward the notification message from EC2 Run command.
def publish_status_message(topic, message):

    payload = {
        'default': message
    }

    sns.publish(
        TopicArn=topic,
        MessageStructure='json',
        Message=json.dumps(payload)
    )


def get_remaining_publish_dispatcher_pairs(stack_prefix, completed_publish_id):

    filters = [
        {
            'Name': 'tag:StackPrefix',
            'Values': [stack_prefix]
        }, {
            'Name': 'instance-state-name',
            'Values': ['running']
        }, {
            'Name': 'tag:Component',
            'Values': ['publish']
        }
    ]

    publish_ids = instance_ids_by_tags(filters)
    publish_ids.remove(completed_publish_id)
    publish_dispatcher_ids = []
    for publish_id in publish_ids:
        publish_dispatcher_id = retrieve_tag_value(publish_id, 'PairInstanceId')
        publish_dispatcher_ids.append(publish_dispatcher_id)

    return publish_ids, publish_dispatcher_ids


def compact_remaining_publish_instances(context):
    """
    compact the remaining publish instances not covered in the main flow
    """

    # compaction_context = {
    #     'StackPrefix': stack_prefix,
    #     'Task': task,
    #     'State': state,
    #     'ExternalId': external_id,
    #     'LastCmdId': cmd_id,
    #     'LastCmdResponse': response,
    #     'DynamoDbTable': dynamodb_table,
    #     'TaskDocumentMapping': task_document_mapping,
    #     'SSMCommonParams': ssm_common_params,
    #     'Message': message,
    #     'MessageID': message_id,
    #     'PublishIds': item['Item']['publish_ids']['SS'],
    #     'DispatcherIds': item['Item']['dispatcher_ids']['SS'],
    #     'SubState': item['Item']['sub_state']['S'],
    #     'StatusTopic': status_topic_arn
    # }

    cmd_id = context['LastCmdId']
    sub_state = context['SubState']
    stack_prefix = context['StackPrefix']
    put_state = context['State']

    if sub_state == 'PUBLISH_READY':
        manage_autoscaling_standby(stack_prefix, 'enter', byInstanceIds=context['DispatcherIds'])
        ssm_params = context['SSMCommonParams'].copy()
        ssm_params.update(
            {
                'InstanceIds': context['PublishIds'],
                'DocumentName': context['TaskDocumentMapping']['manage-service'],
                'Comment': 'Stop AEM service on remaining publish instances',
                'Parameters': {
                    'aemid': ['publish'],
                    'action': ['stop'],
                    'executionTimeout': ['3600']
                }
            }
        )

        # Defining DynamoDB Sub State
        put_sub_state = 'STOP_PUBLISH'

        # Logging pre-infos
        log_command_info(
            send_command=put_state + ' / ' + put_sub_state,
            stack_prefix=stack_prefix,
            instance_id=ssm_params['InstanceIds'],
            aem_component='publish',
        )
        # Sending out ssm command
        response = send_ssm_cmd(ssm_params)
        command_id = response['Command']['CommandId']

        # Logging post-infos
        log_command_info(
            send_command=put_state + ' / ' + put_sub_state,
            stack_prefix=stack_prefix,
            instance_id=ssm_params['InstanceIds'],
            aem_component='publish',
            command_id=command_id
        )

        put_state_in_dynamodb(
            context['DynamoDbTable'],
            command_id,
            stack_prefix,
            context['Task'],
            put_state,
            context['Message']['eventTime'],
            context['MessageID'],
            ExternalId=context['ExternalId'],
            LastCommand=cmd_id,
            InstanceInfo=context['InstanceInfo'],
            PublishIds=context['PublishIds'],
            DispatcherIds=context['DispatcherIds'],
            SubState=put_sub_state
        )

    elif sub_state == 'STOP_PUBLISH':
        ssm_params = context['SSMCommonParams'].copy()
        ssm_params.update(
            {
                'InstanceIds': context['PublishIds'],
                'DocumentName': context['TaskDocumentMapping']['offline-compaction-snapshot-full-set'],
                'Comment': 'Run offline compaction on all remaining publish instances',
                'Parameters': {
                    'executionTimeout': ['14400']
                }
            }
        )

        # Defining DynamoDB Sub State
        put_sub_state = 'COMPACT_PUBLISH'

        # Logging pre-infos
        log_command_info(
            send_command=put_state + ' / ' + put_sub_state,
            stack_prefix=stack_prefix,
            instance_id=ssm_params['InstanceIds'],
            aem_component='publish',
        )
        # Sending out ssm command
        response = send_ssm_cmd(ssm_params)
        command_id = response['Command']['CommandId']

        # Logging post-infos
        log_command_info(
            send_command=put_state + ' / ' + put_sub_state,
            stack_prefix=stack_prefix,
            instance_id=ssm_params['InstanceIds'],
            aem_component='publish',
            command_id=command_id
        )

        put_state_in_dynamodb(
            context['DynamoDbTable'],
            command_id,
            stack_prefix,
            context['Task'],
            put_state,
            context['Message']['eventTime'],
            context['MessageID'],
            ExternalId=context['ExternalId'],
            LastCommand=cmd_id,
            InstanceInfo=context['InstanceInfo'],
            PublishIds=context['PublishIds'],
            DispatcherIds=context['DispatcherIds'],
            SubState=put_sub_state
        )
    elif sub_state == 'COMPACT_PUBLISH':
        ssm_params = context['SSMCommonParams'].copy()
        ssm_params.update(
            {
                'InstanceIds': context['PublishIds'],
                'DocumentName': context['TaskDocumentMapping']['manage-service'],
                'Comment': 'Start AEM Service on all the publish instances',
                'Parameters': {
                    'aemid': ['publish'],
                    'action': ['start'],
                    'executionTimeout': ['3600']
                }
            }
        )

        # Defining DynamoDB Sub State
        put_sub_state = 'START_PUBLISH'

        # Logging pre-infos
        log_command_info(
            send_command=put_state + ' / ' + put_sub_state,
            stack_prefix=stack_prefix,
            instance_id=ssm_params['InstanceIds'],
            aem_component='publish',
        )
        # Sending out ssm command
        response = send_ssm_cmd(ssm_params)
        command_id = response['Command']['CommandId']

        # Logging post-infos
        log_command_info(
            send_command=put_state + ' / ' + put_sub_state,
            stack_prefix=stack_prefix,
            instance_id=ssm_params['InstanceIds'],
            aem_component='publish',
            command_id=command_id
        )

        put_state_in_dynamodb(
            context['DynamoDbTable'],
            command_id,
            stack_prefix,
            context['Task'],
            put_state,
            context['Message']['eventTime'],
            context['MessageID'],
            ExternalId=context['ExternalId'],
            LastCommand=cmd_id,
            InstanceInfo=context['InstanceInfo'],
            PublishIds=context['PublishIds'],
            DispatcherIds=context['DispatcherIds'],
            SubState=put_sub_state
        )
    elif sub_state == 'START_PUBLISH':
        manage_autoscaling_standby(stack_prefix, 'exit', byInstanceIds=context['DispatcherIds'])
        update_state_in_dynamodb(
            context['DynamoDbTable'],
            cmd_id,
            'Success',
            context['Message']['eventTime'],
        )

        manage_lock_for_environment(
            context['DynamoDbTable'],
            stack_prefix + '_backup_lock',
            'unlock'
        )

        print('[{}] Offline compaction backup successfully'.format(stack_prefix))

        response = {
            'status': 'Success'
        }

    return response


def log_command_info(send_command=None, stack_prefix=None, instance_id=None, aem_component=None, command_id=None):
    if command_id is not None:
        print('[{}/{}] Command ID: {} ...'.format(stack_prefix, aem_component, command_id))
        print('[{}/{}] Finished sending command {}'.format(stack_prefix, aem_component, send_command))
    elif stack_prefix is not None and send_command is not None:
        print('[{}/{}] Start sending command {} for instance ids:'.format(stack_prefix, aem_component, send_command))
        print('[{}/{}] {} ...'.format(stack_prefix, aem_component, instance_id))
    else:
        print('[{}/{}] {} '.format(stack_prefix, aem_component, send_command))


def sns_message_processor(event, context):
    """
    offline snapshot is a complicated process, requiring a few things happen in the
    right order. This function will kick start the process. The bulk of operations
    will happen in another lambada function.
    """

    # reading in config info from either s3 or within bundle
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')
    if bucket is not None and prefix is not None:
        config_file = '/tmp/config.json'
        s3.download_file(bucket, '{}/config.json'.format(prefix), config_file)
    else:
        print('Unable to locate config.json in S3, searching within bundle')
        config_file = 'config.json'

    with open(config_file, 'r') as f:
        content = ''.join(f.readlines()).replace('\n', '')
        logger.debug('config file: ' + content)
        config = json.loads(content)

        run_command = config['ec2_run_command']
        task_document_mapping = config['document_mapping']
        offline_snapshot_config = config['offline_snapshot']

        dynamodb_table = run_command['dynamodb-table']
        status_topic_arn = run_command['status-topic-arn']

    ssm_common_params = {
        'TimeoutSeconds': 120,
        'OutputS3BucketName': run_command['cmd-output-bucket'],
        'OutputS3KeyPrefix': run_command['cmd-output-prefix'],
        'ServiceRoleArn': run_command['ssm-service-role-arn'],
        'NotificationConfig': {
            'NotificationArn': offline_snapshot_config['sns-topic-arn'],
            'NotificationEvents': [
                'Success',
                'Failed'
            ],
            'NotificationType': 'Command'
         }
     }

    responses = []
    for record in event['Records']:
        message_text = record['Sns']['Message']
        message_id = record['Sns']['MessageId']
        logger.debug(message_text)
        message = json.loads(message_text.replace('\'', '"'))

        # message that start-offline snapshot has a task key
        response = None
        if 'task' in message and (message['task'] == 'offline-snapshot-full-set' or
                                  message['task'] == 'offline-compaction-snapshot-full-set'):

            stack_prefix = message['stack_prefix']
            task = message['task']

            external_id = None
            if 'externalId' in message:
                external_id = message['externalId']

            # enclosed in try is sanity check: stack health and no concurrent runs
            try:
                min_publish_instance = int(offline_snapshot_config['min-publish-instances'])
                instances = stack_health_check(
                    stack_prefix,
                    min_publish_instance
                )
                if instances is None:
                    raise RuntimeError('Unhealthy Stack')

                # try to acquire stack lock
                locked = manage_lock_for_environment(dynamodb_table, stack_prefix + '_backup_lock', 'trylock')
                if not locked:
                    logger.warn("Cannot have two offline snapshots/compactions run in parallel")
                    raise RuntimeError('Another offline snapshot backup/compaction backup is running')

            except RuntimeError:

                # if externalId is present, it means other parties are interested in the status.
                # Need to put a record in daynamodb with this id duplicated to command_id
                if external_id is not None:
                    put_state_in_dynamodb(
                        dynamodb_table, external_id, stack_prefix, task, 'Failed',
                        datetime.datetime.utcnow().isoformat()[:-3] + 'Z',
                        message_id,
                        ExternalId=external_id
                    )

                # rethrow to fail the execution
                raise

            # put both author-dispather in standby mode after health check
            manage_autoscaling_standby(stack_prefix, 'enter', byComponent='author-dispatcher')
            # put both publish-dispatch in standby mode
            manage_autoscaling_standby(stack_prefix, 'enter', byInstanceIds=[instances['publish-dispatcher']])

            ssm_params = ssm_common_params.copy()
            ssm_params.update(
                {
                    'InstanceIds': [instances['author-standby']],
                    'DocumentName': task_document_mapping['manage-service'],
                    'Comment': 'Kick start offline backup with stopping AEM service on Author standby instance',
                    'Parameters': {
                        'aemid': ['author'],
                        'action': ['stop'],
                        'executionTimeout': ['3600']
                    }
                }
            )

            # Check if Author Standby is promoted to primary
            if instances['author-standby'] is not 'Promoted':
                # Defining DynamoDB State
                put_state = 'STOP_AUTHOR_STANDBY'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-standby',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-standby',
                    command_id=command_id
                )

                instance_info = {key: {'S': value} for (key, value) in instances.items()}
                supplement = {
                    'InstanceInfo': instance_info,
                    'ExternalId': external_id
                }

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    datetime.datetime.utcnow().isoformat()[:-3] + 'Z',
                    message_id,
                    **supplement
                )

                responses.append(response)
            # If Author Standby is promoted to Author Primary skip stop for Author Standby
            else:
                author_primary_id = instances['author-primary']
                instance_info = {key: {'S': value} for (key, value) in instances.items()}
                supplement = {
                    'InstanceInfo': instance_info,
                    'ExternalId': external_id
                }

                ssm_params = ssm_common_params.copy()
                ssm_params.update(
                    {
                        'InstanceIds': [author_primary_id],
                        'DocumentName': task_document_mapping['manage-service'],
                        'Comment': 'Stop AEM service on Author primary instances',
                        'Parameters': {
                            'aemid': ['author'],
                            'action': ['stop'],
                            'executionTimeout': ['3600']
                        }
                    }
                )

                # Defining DynamoDB State
                put_state = 'STOP_AUTHOR_PRIMARY'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-primary',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-primary',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    datetime.datetime.utcnow().isoformat()[:-3] + 'Z',
                    message_id,
                    **supplement
                )
                responses.append(response)
        else:
            cmd_id = message['commandId']

            # get back the state of this task
            item = get_state_from_dynamodb(
                dynamodb_table,
                cmd_id
            )

            stack_prefix = item['Item']['environment']['S']
            task = item['Item']['task']['S']
            state = item['Item']['state']['S']
            instance_info = item['Item']['instance_info']['M']
            external_id = None
            if 'externalId' in item['Item']:
                external_id = item['Item']['externalId']['S']

            author_primary_id = instance_info['author-primary']['S']
            author_standby_id = instance_info['author-standby']['S']
            publish_id = instance_info['publish']['S']
            publish_dispatcher_id = instance_info['publish-dispatcher']['S']

            if message['status'] == 'Failed':
                logger.error('[{}] ERROR: While running offline snapshot process ...'.format(stack_prefix))
                logger.error('[{}] ERROR: Command ID: {}.'.format(stack_prefix, cmd_id))
                logger.error('[{}] ERROR: SSM Document: {}.'.format(stack_prefix, message['documentName']))
                logger.error('[{}] ERROR: Instance ID: {}.'.format(stack_prefix, message['instanceIds']))
                logger.error('[{}] ERROR: Offline Snapshotting failed.'.format(stack_prefix))

                update_state_in_dynamodb(dynamodb_table, cmd_id, 'Failed', message['eventTime'])
                # move author-dispatcher instances out of standby
                manage_autoscaling_standby(stack_prefix, 'exit', byComponent='author-dispatcher')
                # move publish-dispatcher instnace out of standby
                manage_autoscaling_standby(stack_prefix, 'exit', byInstanceIds=[publish_dispatcher_id])

                manage_lock_for_environment(dynamodb_table, stack_prefix + '_backup_lock', 'unlock')

                raise RuntimeError('Command {} failed.'.format(cmd_id))

            if state == 'STOP_AUTHOR_STANDBY':
                ssm_params = ssm_common_params.copy()
                ssm_params.update(
                    {
                        'InstanceIds': [author_primary_id],
                        'DocumentName': task_document_mapping['manage-service'],
                        'Comment': 'Stop AEM service on Author primary instances',
                        'Parameters': {
                            'aemid': ['author'],
                            'action': ['stop'],
                            'executionTimeout': ['3600']
                        }
                    }
                )

                # Defining DynamoDB State
                put_state = 'STOP_AUTHOR_PRIMARY'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-primary',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-primary',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    message['eventTime'],
                    message_id,
                    ExternalId=external_id,
                    InstanceInfo=instance_info,
                    LastCommand=cmd_id
                )
                responses.append(response)

            elif state == 'STOP_AUTHOR_PRIMARY':
                ssm_params = ssm_common_params.copy()
                ssm_params.update(
                    {
                        'InstanceIds': [publish_id],
                        'DocumentName': task_document_mapping['manage-service'],
                        'Comment': 'Stop AEM service on Publish instances',
                        'Parameters': {
                            'aemid': ['publish'],
                            'action': ['stop'],
                            'executionTimeout': ['3600']
                        }
                    }
                )

                # Defining DynamoDB State
                put_state = 'STOP_PUBLISH'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='publish',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='publish',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    message['eventTime'],
                    message_id,
                    ExternalId=external_id,
                    InstanceInfo=instance_info,
                    LastCommand=cmd_id
                )
                responses.append(response)

            elif state == 'STOP_PUBLISH':
                ssm_params = ssm_common_params.copy()
                if task == 'offline-snapshot-full-set':
                    if author_standby_id == 'Promoted':
                        ssm_params.update(
                            {
                                'InstanceIds': [author_primary_id, publish_id],
                                'DocumentName': task_document_mapping['offline-snapshot-full-set'],
                                'Comment': 'Run offline snapshot on Author and a select publish instances',
                                'Parameters': {
                                    'executionTimeout': ['14400']
                                }
                            }
                        )
                    else:
                        ssm_params.update(
                            {
                                'InstanceIds': [author_primary_id, author_standby_id, publish_id],
                                'DocumentName': task_document_mapping['offline-snapshot-full-set'],
                                'Comment': 'Run offline snapshot on Author and a select publish instances',
                                'Parameters': {
                                    'executionTimeout': ['14400']
                                }
                            }
                        )

                    # Defining DynamoDB State
                    put_state = 'OFFLINE_BACKUP'

                    # Logging pre-infos
                    log_command_info(
                        send_command=put_state,
                        stack_prefix=stack_prefix,
                        instance_id=ssm_params['InstanceIds'],
                        aem_component='author-standby/author-primary/publish',
                    )
                    # Sending out ssm command
                    response = send_ssm_cmd(ssm_params)
                    command_id = response['Command']['CommandId']

                    # Logging post-infos
                    log_command_info(
                        send_command=put_state,
                        stack_prefix=stack_prefix,
                        instance_id=ssm_params['InstanceIds'],
                        aem_component='author-standby/author-primary/publish',
                        command_id=command_id
                    )

                    put_state_in_dynamodb(
                        dynamodb_table,
                        command_id,
                        stack_prefix,
                        task,
                        put_state,
                        message['eventTime'],
                        message_id,
                        ExternalId=external_id,
                        InstanceInfo=instance_info,
                        LastCommand=cmd_id
                    )

                    responses.append(response)
                elif task == 'offline-compaction-snapshot-full-set':
                    if author_standby_id == 'Promoted':
                        ssm_params.update(
                            {
                                'InstanceIds': [author_primary_id, publish_id],
                                'DocumentName': task_document_mapping['offline-compaction-snapshot-full-set'],
                                'Comment': 'Run offline compaction on Author and a selected Publish instances',
                                'Parameters': {
                                    'executionTimeout': ['14400']
                                }
                            }
                        )
                    else:
                        ssm_params.update(
                            {
                                'InstanceIds': [author_primary_id, author_standby_id, publish_id],
                                'DocumentName': task_document_mapping['offline-compaction-snapshot-full-set'],
                                'Comment': 'Run offline compaction on Author and a selected Publish instances',
                                'Parameters': {
                                    'executionTimeout': ['14400']
                                }
                            }
                        )
                    # Defining DynamoDB State
                    put_state = 'OFFLINE_COMPACTION'

                    # Logging pre-infos
                    log_command_info(
                        send_command=put_state,
                        stack_prefix=stack_prefix,
                        instance_id=ssm_params['InstanceIds'],
                        aem_component='author-standby/author-primary/publish',
                    )
                    # Sending out ssm command
                    response = send_ssm_cmd(ssm_params)
                    command_id = response['Command']['CommandId']

                    # Logging post-infos
                    log_command_info(
                        send_command=put_state,
                        stack_prefix=stack_prefix,
                        instance_id=ssm_params['InstanceIds'],
                        aem_component='author-standby/author-primary/publish',
                        command_id=command_id
                    )

                    put_state_in_dynamodb(
                        dynamodb_table,
                        command_id,
                        stack_prefix,
                        task,
                        put_state,
                        message['eventTime'],
                        message_id,
                        ExternalId=external_id,
                        InstanceInfo=instance_info,
                        LastCommand=cmd_id
                    )

                    responses.append(response)

            elif state == 'OFFLINE_COMPACTION':
                ssm_params = ssm_common_params.copy()
                if author_standby_id == 'Promoted':
                    ssm_params.update(
                        {
                            'InstanceIds': [author_primary_id, publish_id],
                            'DocumentName': task_document_mapping['offline-snapshot-full-set'],
                            'Comment': 'Run offline EBS snapshot on Author and a selected Publish instances',
                            'Parameters': {
                                'executionTimeout': ['14400']
                            }
                        }
                    )
                else:
                    ssm_params.update(
                        {
                            'InstanceIds': [author_primary_id, author_standby_id, publish_id],
                            'DocumentName': task_document_mapping['offline-snapshot-full-set'],
                            'Comment': 'Run offline EBS snapshot on Author and a selected Publish instances',
                            'Parameters': {
                                'executionTimeout': ['14400']
                            }
                        }
                    )


                # Defining DynamoDB State
                put_state = 'OFFLINE_BACKUP'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-standby/author-primary/publish',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-standby/author-primary/publish',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    message['eventTime'],
                    message_id,
                    ExternalId=external_id,
                    InstanceInfo=instance_info,
                    LastCommand=cmd_id
                )
                responses.append(response)

            elif state == 'OFFLINE_BACKUP':
                ssm_params = ssm_common_params.copy()
                ssm_params.update(
                    {
                        'InstanceIds': [author_primary_id],
                        'DocumentName': task_document_mapping['manage-service'],
                        'Comment': 'Start AEM service on Author primary instance',
                        'Parameters': {
                            'aemid': ['author'],
                            'action': ['start'],
                            'executionTimeout': ['3600']
                        }
                    }
                )

                # Defining DynamoDB State
                put_state = 'START_AUTHOR_PRIMARY'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-primary',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-primary',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    message['eventTime'],
                    message_id,
                    ExternalId=external_id,
                    InstanceInfo=instance_info,
                    LastCommand=cmd_id
                )

                responses.append(response)

            elif state == 'START_AUTHOR_PRIMARY' and author_standby_id != 'Promoted':
                ssm_params = ssm_common_params.copy()
                ssm_params.update(
                    {
                        'InstanceIds': [author_standby_id],
                        'DocumentName': task_document_mapping['manage-service'],
                        'Comment': 'Start AEM service on Author standby instances',
                        'Parameters': {
                            'aemid': ['author'],
                            'action': ['start'],
                            'executionTimeout': ['3600']
                        }
                    }
                )

                # Defining DynamoDB State
                put_state = 'START_AUTHOR_STANDBY'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-standby',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='author-standby',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    message['eventTime'],
                    message_id,
                    ExternalId=external_id,
                    InstanceInfo=instance_info,
                    LastCommand=cmd_id
                )

                responses.append(response)

            elif state == 'START_AUTHOR_STANDBY' or state == 'START_AUTHOR_PRIMARY' and author_standby_id == 'Promoted':
                ssm_params = ssm_common_params.copy()
                ssm_params.update(
                    {
                        'InstanceIds': [publish_id],
                        'DocumentName': task_document_mapping['manage-service'],
                        'Comment': 'Stop AEM service on Publish instances',
                        'Parameters': {
                            'aemid': ['publish'],
                            'action': ['start'],
                            'executionTimeout': ['3600']
                        }
                    }
                )

                # Defining DynamoDB State
                put_state = 'START_PUBLISH'

                # Logging pre-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='publish',
                )
                # Sending out ssm command
                response = send_ssm_cmd(ssm_params)
                command_id = response['Command']['CommandId']

                # Logging post-infos
                log_command_info(
                    send_command=put_state,
                    stack_prefix=stack_prefix,
                    instance_id=ssm_params['InstanceIds'],
                    aem_component='publish',
                    command_id=command_id
                )

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    put_state,
                    message['eventTime'],
                    message_id,
                    ExternalId=external_id,
                    InstanceInfo=instance_info,
                    LastCommand=cmd_id
                )
                responses.append(response)

            elif state == 'START_PUBLISH':

                # this is the success notification message
                if task == 'offline-snapshot-full-set':
                    update_state_in_dynamodb(dynamodb_table, cmd_id, 'Success', message['eventTime'])
                    print('[{}] Offline backup finished successfully'.format(stack_prefix))

                    # move author-dispatcher instances out of standby
                    manage_autoscaling_standby(stack_prefix, 'exit', byComponent='author-dispatcher')
                    # move publish-dispatcher instance out of standby
                    manage_autoscaling_standby(stack_prefix, 'exit', byInstanceIds=[publish_dispatcher_id])

                    manage_lock_for_environment(dynamodb_table, stack_prefix + '_backup_lock', 'unlock')

                    response = {
                        'status': 'Success'
                    }
                    responses.append(response)

                elif task == 'offline-compaction-snapshot-full-set':
                    # move author-dispatcher instances out of standby
                    manage_autoscaling_standby(stack_prefix, 'exit', byComponent='author-dispatcher')
                    # move publish-dispatcher instance out of standby
                    manage_autoscaling_standby(stack_prefix, 'exit', byInstanceIds=[publish_dispatcher_id])

                    remaining_pub_disp_pairs = get_remaining_publish_dispatcher_pairs(stack_prefix, publish_id)

                    # need to continue with compact other publish instances
                    # start with checking the selected publish instance is ready after compaction
                    ssm_params = ssm_common_params.copy()
                    ssm_params.update(
                        {
                            'InstanceIds': [publish_id],
                            'DocumentName': task_document_mapping['wait-until-ready'],
                            'Comment': 'Wait Until AEM Service is properly up on the selected publish instance',
                            'Parameters': {
                                'executionTimeout': ['3600']
                            }
                        }
                    )

                    # Defining DynamoDB State
                    put_state = 'COMPACT_REMAINING_PUBLISHERS'
                    put_sub_state = 'PUBLISH_READY'

                    # Logging pre-infos
                    log_command_info(
                        send_command=put_state + ' / ' + put_sub_state,
                        stack_prefix=stack_prefix,
                        instance_id=ssm_params['InstanceIds'],
                        aem_component='publish',
                    )
                    # Sending out ssm command
                    response = send_ssm_cmd(ssm_params)
                    command_id = response['Command']['CommandId']

                    # Logging post-infos
                    log_command_info(
                        send_command=put_state + ' / ' + put_sub_state,
                        stack_prefix=stack_prefix,
                        instance_id=ssm_params['InstanceIds'],
                        aem_component='publish',
                        command_id=command_id
                    )

                    put_state_in_dynamodb(
                        dynamodb_table,
                        command_id,
                        stack_prefix,
                        task,
                        put_state,
                        message['eventTime'],
                        message_id,
                        ExternalId=external_id,
                        InstanceInfo=instance_info,
                        LastCommand=cmd_id,
                        PublishIds=remaining_pub_disp_pairs[0],
                        DispatcherIds=remaining_pub_disp_pairs[1],
                        SubState=put_sub_state
                    )

                    responses.append(response)

            elif state == 'COMPACT_REMAINING_PUBLISHERS':

                compaction_context = {
                    'StackPrefix': stack_prefix,
                    'Task': task,
                    'State': state,
                    'ExternalId': external_id,
                    'LastCmdId': cmd_id,
                    'LastCmdResponse': response,
                    'DynamoDbTable': dynamodb_table,
                    'TaskDocumentMapping': task_document_mapping,
                    'SSMCommonParams': ssm_common_params,
                    'Message': message,
                    'MessageID': message_id,
                    'PublishIds': item['Item']['publish_ids']['SS'],
                    'DispatcherIds': item['Item']['dispatcher_ids']['SS'],
                    'SubState': item['Item']['sub_state']['S'],
                    'StatusTopic': status_topic_arn,
                    'InstanceInfo': instance_info
                }
                logger.debug('Dumping context for remaining publish compaction: {}'.format(compaction_context))
                response = compact_remaining_publish_instances(compaction_context)

                responses.append(response)

            else:
                raise RuntimeError('Unexpected state {} for {}'.format(state, cmd_id))

    return responses
