# -*- coding: utf8 -*-

"""
Lambda function to manage AEM stack offline backups.
It uses a SNS topic to help orchestrate sequence of steps
"""

import os
import logging
import json
import datetime
import boto3
import botocore


__author__ = "Andy Wang (andy.wang@shinesolutions.com)"
__copyright__ = "Shine Solutions"
__license__ = "Apache License, Version 2.0"


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv("LOG_LEVEL", logging.INFO)))


# AWS resources
ssm = boto3.client("ssm")
ec2 = boto3.client("ec2")
s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")
sns = boto3.client("sns")


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        return json.JSONEncoder.default(self, obj)


def instance_ids_by_tags(filters):
    response = ec2.describe_instances(Filters=filters)
    response2 = json.loads(json.dumps(response, cls=MyEncoder))

    instance_ids = []
    for reservation in response2["Reservations"]:
        instance_ids += [
            instance["InstanceId"] for instance in reservation["Instances"]
        ]
    return instance_ids


def send_ssm_cmd(cmd_details):
    ssm_document = cmd_details["DocumentName"]
    logger.info("Start calling SSM Document %s ...", ssm_document)
    response = json.loads(json.dumps(ssm.send_command(**cmd_details), cls=MyEncoder))
    logger.info("Finished calling SSM Document %s .", ssm_document)
    return response


def get_author_primary_ids(stack_prefix):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["author-primary"]},
    ]

    return instance_ids_by_tags(filters)


def get_author_standby_ids(stack_prefix):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["author-standby"]},
    ]

    return instance_ids_by_tags(filters)


def get_promoted_author_standby_ids(stack_prefix):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {
            "Name": "tag:Name",
            "Values": ["AEM Author - Primary - Promoted from Standby"],
        },
    ]

    return instance_ids_by_tags(filters)


def get_publish_ids(stack_prefix):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["publish"]},
    ]

    return instance_ids_by_tags(filters)


def get_preview_publish_ids(stack_prefix):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["preview-publish"]},
    ]

    return instance_ids_by_tags(filters)


def manage_autoscaling_standby(stack_prefix, action, **kwargs):
    """
    put instances in an autoscaling group into or bring them out of
    standby mode one of byComponent or byInstanceIds must be give and not both.
    If byInstanceIds are given, it is assumed that all the instances
    are in the same group
    """
    if "byComponent" in kwargs:
        filters = [
            {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
            {"Name": "instance-state-name", "Values": ["running"]},
            {"Name": "tag:Component", "Values": [kwargs["byComponent"]]},
        ]
    elif "byInstanceIds" in kwargs:
        filters = [
            {"Name": "instance-state-name", "Values": ["running"]},
            {"Name": "instance-id", "Values": kwargs["byInstanceIds"][0:1]},
        ]
    else:
        raise Exception("neither byComponent or byInstanceIds found in arguments")

    # find the autoscaling group those instances are in
    response = ec2.describe_instances(Filters=filters)
    response2 = json.loads(json.dumps(response, cls=MyEncoder))

    instance_ids = []
    if "byComponent" in kwargs:
        for reservation in response2["Reservations"]:
            instance_ids += [
                instance["InstanceId"] for instance in reservation["Instances"]
            ]
    else:
        instance_ids = kwargs["byInstanceIds"]

    instance_tags = response2["Reservations"][0]["Instances"][0]["Tags"]
    asg_name = [
        tag["Value"]
        for tag in instance_tags
        if tag["Key"] == "aws:autoscaling:groupName"
    ][0]

    # try to get the min size of the atutocaling group
    autoscaling = boto3.client("autoscaling")
    asg_dcrb = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    asg_min_size = asg_dcrb["AutoScalingGroups"][0]["MinSize"]
    asg_max_size = asg_dcrb["AutoScalingGroups"][0]["MaxSize"]

    min_size = max(asg_min_size - len(instance_ids), 0)

    # manage the instances standby mode
    if action == "enter":
        logger.info(
            "[%s] Start updating ASG %s to suspend processes ...",
            stack_prefix,
            asg_name,
        )
        autoscaling.suspend_processes(
            AutoScalingGroupName=asg_name,
            ScalingProcesses=["AlarmNotification", "AZRebalance"],
        )
        logger.info(
            "[%s] Finished updating ASG %s to suspend processes ...",
            stack_prefix,
            asg_name,
        )

        logger.info(
            "[%s] Start updating ASG %s to %s instances ...",
            stack_prefix,
            asg_name,
            min_size,
        )
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg_name, MinSize=min_size
        )
        logger.info(
            "[%s] Finished updating ASG %s to %s instances.",
            stack_prefix,
            asg_name,
            min_size,
        )

        logger.info(
            "[%s] Start entering instance %s into standby in ASG %s ...",
            stack_prefix,
            instance_ids,
            asg_name,
        )
        autoscaling.enter_standby(
            InstanceIds=instance_ids,
            AutoScalingGroupName=asg_name,
            ShouldDecrementDesiredCapacity=True,
        )
        logger.info(
            "[%s] Finished entering instance %s into standby in ASG %s.",
            stack_prefix,
            instance_ids,
            asg_name,
        )
    elif action == "exit":
        logger.info(
            "[%s] Start exiting instance %s from standby in ASG %s ...",
            stack_prefix,
            instance_ids,
            asg_name,
        )
        autoscaling.exit_standby(
            InstanceIds=instance_ids, AutoScalingGroupName=asg_name
        )
        logger.info(
            "[%s] Finished exiting instance %s from standby in ASG %s.",
            stack_prefix,
            instance_ids,
            asg_name,
        )

        logger.info(
            "[%s] Start updating ASG %s to %s instances ...",
            stack_prefix,
            asg_name,
            asg_max_size,
        )
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            MinSize=min(asg_min_size + len(instance_ids), asg_max_size),
        )
        logger.info(
            "[%s] Finished updating ASG %s to %s instances.",
            stack_prefix,
            asg_name,
            asg_max_size,
        )

        logger.info(
            "[%s] Start updating ASG %s to resume suspended processes again ...",
            stack_prefix,
            asg_name,
        )
        autoscaling.resume_processes(
            AutoScalingGroupName=asg_name,
            ScalingProcesses=["AlarmNotification", "AZRebalance"],
        )
        logger.info(
            "[%s] Finished updating ASG %s to resume suspended processes again ...",
            stack_prefix,
            asg_name,
        )


def retrieve_tag_value(instance_id, tag_key):
    response = boto3.client("ec2").describe_tags(
        Filters=[
            {"Name": "resource-id", "Values": [instance_id]},
            {"Name": "key", "Values": [tag_key]},
        ]
    )
    tags = {tag["Key"]: tag["Value"] for tag in response["Tags"]}

    tag_value = None
    if len(tags) != 0:
        tag_value = tags[tag_key]

    return tag_value


def manage_lock_for_environment(table_name, lock, action):
    """
    use lock as command_id to prevent concurrent backup processes
    """

    succeeded = False

    if action == "trylock":
        try:
            # put a timestamp shows when the lock is set
            timestamp = datetime.datetime.utcnow()

            # item ttl is set to 1 day
            ttl = (timestamp - datetime.datetime.utcfromtimestamp(0)).total_seconds()
            ttl += datetime.timedelta(days=1).total_seconds()

            item = {
                "command_id": {"S": lock},
                "timestamp": {"S": timestamp.isoformat()[:-3] + "Z"},
                "ttl": {"N": str(ttl)},
            }
            logger.info("Try to lock DynamoDB Table %s} ...", table_name)
            dynamodb.put_item(
                TableName=table_name,
                Item=item,
                ConditionExpression="attribute_not_exists(command_id)",
                ReturnValues="NONE",
            )
            logger.info("DynamoDB Table %s locked successfully.", table_name)
            succeeded = True
        except botocore.exceptions.ClientError:
            succeeded = False

    elif action == "unlock":
        logger.info("Unlock DynamoDB Table %s", table_name)
        dynamodb.delete_item(TableName=table_name, Key={"command_id": {"S": lock}})
        succeeded = True

    return succeeded


def stack_health_check(
    stack_prefix, min_publish_instances, min_preview_publish_instance
):
    """
    Simple AEM stack health check based on the number of author-primary,
    author-standby and publisher instances.
    """

    # Get instances by components
    author_primary_instances = get_author_primary_ids(stack_prefix)
    author_standby_instances = get_author_standby_ids(stack_prefix)
    publish_instances = get_publish_ids(stack_prefix)
    preview_publish_instances = get_preview_publish_ids(stack_prefix)
    promoted_author_standby_instances = get_promoted_author_standby_ids(stack_prefix)

    # Count instances
    author_primary_count = len(author_primary_instances)
    author_standby_count = len(author_standby_instances)
    publish_count = len(publish_instances)
    preview_publish_count = len(preview_publish_instances)
    promoted_author_standby_instances_count = len(promoted_author_standby_instances)

    # make sure min_publish_instances is integer
    if isinstance(min_publish_instances, str):
        min_publish_instances = int(min_publish_instances)

    # make sure min_preview_publish_instance is integer
    if isinstance(min_preview_publish_instance, str):
        min_preview_publish_instance = int(min_preview_publish_instance)

    logger.info("[%s] Start checking Stack health ...", stack_prefix)

    if publish_count >= min_publish_instances:
        publish_instance_id = publish_instances[0]
        paired_publish_dispatcher_id = retrieve_tag_value(
            publish_instance_id, "PairInstanceId"
        )
        if paired_publish_dispatcher_id is None:
            logger.error(
                "No Publish-dispatcher pair found for Publish Instance %s not found.\nUnhealthy stack.",
                publish_instance_id,
            )
            return None
    else:
        logger.error(
            "Found %s publish instances. Unhealthy stack.", publish_count
        )
        return None

    if promoted_author_standby_instances_count == 1:
        author_primary_instance_id = promoted_author_standby_instances[0]
        author_standby_instance_id = "Promoted"
        logger.warn(
            "[%s] Found promoted Author Standby going to run offline-snapshot only on promoted Author instance.",
            stack_prefix,
        )
    elif author_primary_count == 1:
        author_primary_instance_id = author_primary_instances[0]
    else:
        logger.error(
            "Found %s author-primary instances. Unhealthy stack.", author_primary_count
        )

        return None

    if author_standby_count > 0:
        author_standby_instance_id = author_standby_instances[0]
    elif author_standby_count < 1 and promoted_author_standby_instances_count < 1:
        logger.error(
            "Found %s author-standby instances. Unhealthy stack.", author_standby_count
        )
        return None

    if (
        min_preview_publish_instance > 0
        and preview_publish_count >= min_preview_publish_instance
    ):
        preview_publish_instance_id = preview_publish_instances[0]
        paired_preview_publish_dispatcher_id = retrieve_tag_value(
            preview_publish_instance_id, "PairInstanceId"
        )
    elif min_preview_publish_instance == 0:
        preview_publish_instance_id = "False"
        paired_preview_publish_dispatcher_id = "False"
    else:
        logger.error(
            "Found %s preview-publish instances. Unhealthy stack.", preview_publish_count
        )

        return None
    logger.info("[%s] Finished checking Stack health successfully.", stack_prefix)

    return {
        "author-primary": author_primary_instance_id,
        "author-standby": author_standby_instance_id,
        "publish": publish_instance_id,
        "publish-dispatcher": paired_publish_dispatcher_id,
        "preview-publish": preview_publish_instance_id,
        "preview-publish-dispatcher": paired_preview_publish_dispatcher_id,
    }


def put_state_in_dynamodb(
    table_name, command_id, environment, task, state, timestamp, message_id, **kwargs
):
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
    ttl = (datetime.datetime.now() - datetime.datetime.fromtimestamp(0)).total_seconds()
    ttl += datetime.timedelta(days=1).total_seconds()

    item = {
        "command_id": {"S": command_id},
        "environment": {"S": environment},
        "task": {"S": task},
        "state": {"S": state},
        "timestamp": {"S": timestamp},
        "ttl": {"N": str(ttl)},
        "message_id": {"S": message_id},
    }

    if "InstanceInfo" in kwargs and kwargs["InstanceInfo"] is not None:
        item["instance_info"] = {"M": kwargs["InstanceInfo"]}

    if "LastCommand" in kwargs and kwargs["LastCommand"] is not None:
        item["last_command"] = {"S": kwargs["LastCommand"]}

    if "ExternalId" in kwargs and kwargs["ExternalId"] is not None:
        item["externalId"] = {"S": kwargs["ExternalId"]}

    # the following three attributes are exclusively for compacting remaining
    # publish instances
    if "PublishIds" in kwargs and kwargs["PublishIds"] is not None:
        item["publish_ids"] = {"SS": kwargs["PublishIds"]}

    if "DispatcherIds" in kwargs and kwargs["DispatcherIds"] is not None:
        item["dispatcher_ids"] = {"SS": kwargs["DispatcherIds"]}

    if "PreviewPublishIds" in kwargs and kwargs["PreviewPublishIds"] is not None:
        item["preview_publish_ids"] = {"SS": kwargs["PreviewPublishIds"]}

    if "PreviewDispatcherIds" in kwargs and kwargs["PreviewDispatcherIds"] is not None:
        item["preview_dispatcher_ids"] = {"SS": kwargs["PreviewDispatcherIds"]}

    if "SubState" in kwargs and kwargs["SubState"] is not None:
        item["sub_state"] = {"S": kwargs["SubState"]}

    dynamodb.put_item(TableName=table_name, Item=item)


# dynamodb is used to host state information
def get_state_from_dynamodb(table_name, command_id):
    item = dynamodb.get_item(
        TableName=table_name,
        Key={"command_id": {"S": command_id}},
        ConsistentRead=True,
        ReturnConsumedCapacity="NONE",
    )

    return item


def update_state_in_dynamodb(table_name, command_id, new_state, timestamp):
    item_update = {
        "TableName": table_name,
        "Key": {"command_id": {"S": command_id}},
        "UpdateExpression": "SET #S = :sval, #T = :tval",
        "ExpressionAttributeNames": {"#S": "state", "#T": "timestamp"},
        "ExpressionAttributeValues": {
            ":sval": {"S": new_state},
            ":tval": {"S": timestamp},
        },
    }

    dynamodb.update_item(**item_update)


# currently just forward the notification message from EC2 Run command.
def publish_status_message(topic, message):
    payload = {"default": message}

    sns.publish(TopicArn=topic, MessageStructure="json", Message=json.dumps(payload))


def get_remaining_publish_dispatcher_pairs(
    stack_prefix: str, completed_publish_id: str
):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["publish"]},
    ]

    publish_ids = instance_ids_by_tags(filters)
    publish_ids.remove(completed_publish_id)
    publish_dispatcher_ids = []
    for publish_id in publish_ids:
        publish_dispatcher_id = retrieve_tag_value(publish_id, "PairInstanceId")
        publish_dispatcher_ids.append(publish_dispatcher_id)

    return publish_ids, publish_dispatcher_ids


def get_remaining_preview_publish_dispatcher_pairs(
    stack_prefix: str, completed_preview_publish_id: str
):
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["preview-publish"]},
    ]

    preview_publish_ids = instance_ids_by_tags(filters)
    preview_publish_ids.remove(completed_preview_publish_id)
    preview_publish_dispatcher_ids = []
    for preview_publish_id in preview_publish_ids:
        preview_publish_dispatcher_id = retrieve_tag_value(
            preview_publish_id, "PairInstanceId"
        )
        preview_publish_dispatcher_ids.append(preview_publish_dispatcher_id)

    return preview_publish_ids, preview_publish_dispatcher_ids


def get_stack_manager_config():
    # reading in config info from either s3 or within bundle
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    if bucket is not None and prefix is not None:
        config_file = "/tmp/config.json"
        s3.download_file(bucket, f"{prefix}/config.json", config_file)
    else:
        logger.warn("Unable to locate config.json in S3, searching within bundle")

    with open(config_file, "r") as open_file:
        content = "".join(open_file.readlines()).replace("\n", "")
        logger.debug("config file: %s", str(content))
        config = json.loads(content)

    return config


def get_ssm_command_common_parameters(stack_manager_config):
    run_command = stack_manager_config["ec2_run_command"]
    offline_snapshot_config = stack_manager_config["offline_snapshot"]

    ssm_common_params = {
        "TimeoutSeconds": 120,
        "OutputS3BucketName": run_command["cmd-output-bucket"],
        "OutputS3KeyPrefix": run_command["cmd-output-prefix"],
        "ServiceRoleArn": run_command["ssm-service-role-arn"],
        "NotificationConfig": {
            "NotificationArn": offline_snapshot_config["sns-topic-arn"],
            "NotificationEvents": ["Success", "Failed"],
            "NotificationType": "Command",
        },
    }

    return ssm_common_params


def get_ssm_command_parameter_manage_service(
    instance_ids: list, aem_id: str, action: str
):
    config = get_stack_manager_config()
    ssm_params = get_ssm_command_common_parameters(config)

    task_document_mapping = config["document_mapping"]

    ssm_command_parameters = {
        "InstanceIds": instance_ids,
        "DocumentName": task_document_mapping["manage-service"],
        "Comment": f"Stop AEM service on  {aem_id} instances",
        "Parameters": {
            "aemid": [aem_id],
            "action": [action],
            "executionTimeout": ["3600"],
        },
    }

    ssm_params.update(ssm_command_parameters)

    return ssm_params


def get_ssm_command_parameter_offline_compaction_snapshot(instance_ids: list):
    config = get_stack_manager_config()
    ssm_params = get_ssm_command_common_parameters(config)

    task_document_mapping = config["document_mapping"]
    ssm_command_parameters = {
        "InstanceIds": instance_ids,
        "DocumentName": task_document_mapping["offline-compaction-snapshot-full-set"],
        "Comment": "Run offline compaction on all remaining publisher instances",
        "Parameters": {"executionTimeout": ["14400"]},
    }

    ssm_params.update(ssm_command_parameters)

    return ssm_params


def get_ssm_command_parameter_offline_snapshot(instance_ids: list):
    config = get_stack_manager_config()
    ssm_params = get_ssm_command_common_parameters(config)

    task_document_mapping = config["document_mapping"]
    ssm_command_parameters = {
        "InstanceIds": instance_ids,
        "DocumentName": task_document_mapping["offline-snapshot-full-set"],
        "Comment": "Run offline snapshot on Author and a select publish instances",
        "Parameters": {"executionTimeout": ["14400"]},
    }

    ssm_params.update(ssm_command_parameters)

    return ssm_params


def get_ssm_command_parameter_wait_until_ready(instance_ids: list):
    config = get_stack_manager_config()
    ssm_params = get_ssm_command_common_parameters(config)

    task_document_mapping = config["document_mapping"]
    ssm_command_parameters = {
        "InstanceIds": instance_ids,
        "DocumentName": task_document_mapping["wait-until-ready"],
        "Comment": "Wait Until AEM Service is properly up on the selected publish instance",
        "Parameters": {"executionTimeout": ["7200"]},
    }

    ssm_params.update(ssm_command_parameters)

    return ssm_params


def log_command_info(
    send_command=None,
    stack_prefix=None,
    instance_id=None,
    aem_component=None,
    command_id=None,
):
    if command_id is not None:
        logger.info(
            "[%s/%s] Command ID: %s ...", stack_prefix, aem_component, command_id
        )
        logger.info(
            "[%s/%s] Finished sending command %s",
            stack_prefix,
            aem_component,
            send_command,
        )
    elif stack_prefix is not None and send_command is not None:
        logger.info(
            "[%s/%s] Start sending command %s for instance ids:",
            stack_prefix,
            aem_component,
            send_command,
        )
        logger.info("[%s/%s] %s ...", stack_prefix, aem_component, instance_id)
    else:
        logger.info("[%s/%s] %s ", stack_prefix, aem_component, send_command)


def sns_message_processor(event, context):
    """
    offline snapshot is a complicated process, requiring a few things happen in the
    right order. This function will kick start the process. The bulk of operations
    will happen in another lambada function.
    """
    logger.debug("Event:")
    logger.debug(event)
    logger.debug("Context:")
    logger.debug(context)

    config = get_stack_manager_config()

    run_command = config["ec2_run_command"]
    offline_snapshot_config = config["offline_snapshot"]
    dynamodb_table = run_command["dynamodb-table"]

    for record in event["Records"]:
        message_text = record["Sns"]["Message"]
        message_id = record["Sns"]["MessageId"]
        logger.info(message_text)
        message = json.loads(message_text.replace("'", '"'))

        supplement = {}
        external_id = None

        # message that start-offline snapshot has a task key
        if "task" in message and (
            message["task"] == "offline-snapshot-full-set"
            or message["task"] == "offline-compaction-snapshot-full-set"
        ):
            stack_prefix = message["stack_prefix"]
            task = message["task"]
            if "externalId" in message:
                external_id = message["externalId"]

            # enclosed in try is sanity check: stack health and no concurrent
            # runs
            try:
                min_publish_instance = int(
                    offline_snapshot_config["min-publish-instances"]
                )
                min_preview_publish_instance = int(
                    offline_snapshot_config["min-preview-publish-instances"]
                )
                instances = stack_health_check(
                    stack_prefix, min_publish_instance, min_preview_publish_instance
                )
                if instances is None:
                    raise RuntimeError("Unhealthy Stack")

                # try to acquire stack lock
                locked = manage_lock_for_environment(
                    dynamodb_table, stack_prefix + "_backup_lock", "trylock"
                )
                if not locked:
                    logger.warn(
                        "Cannot have two offline snapshots/compactions run in parallel"
                    )
                    raise RuntimeError(
                        "Another offline snapshot backup/compaction backup is running"
                    )

            except RuntimeError:
                # if externalId is present, it means other parties are interested in the status.
                # Need to put a record in daynamodb with this id duplicated to
                # command_id
                command_id = "none"
                if external_id is not None:
                    supplement = {"ExternalId": external_id}
                    command_id = external_id

                put_state_in_dynamodb(
                    dynamodb_table,
                    command_id,
                    stack_prefix,
                    task,
                    "Failed",
                    datetime.datetime.utcnow().isoformat()[:-3] + "Z",
                    message_id,
                    **supplement,
                )

                # rethrow to fail the execution
                raise

            # place both author-dispather in standby mode after health check
            manage_autoscaling_standby(
                stack_prefix, "enter", byComponent="author-dispatcher"
            )
            # place publish-dispatcher in standby mode
            manage_autoscaling_standby(
                stack_prefix, "enter", byInstanceIds=[instances["publish-dispatcher"]]
            )
            # place preview-publish-dispatcher in standby mode
            if instances["preview-publish-dispatcher"] != "False":
                manage_autoscaling_standby(
                    stack_prefix,
                    "enter",
                    byInstanceIds=[instances["preview-publish-dispatcher"]],
                )

            # Check if Author Standby is promoted to primary
            if instances["author-standby"] != "Promoted":
                instance_ids = [instances["author-standby"]]
                # Defining DynamoDB State
                put_state = "STOP_AUTHOR_STANDBY"
                aem_component = "author-standby"
            # If Author Standby is promoted to Author Primary skip stop for
            # Author Standby
            else:
                author_primary_id = instances["author-primary"]
                instance_ids = [author_primary_id]
                # Defining DynamoDB State
                put_state = "STOP_AUTHOR_PRIMARY"
                aem_component = "author-primary"

            message["eventTime"] = datetime.datetime.utcnow().isoformat()[:-3] + "Z"
            cmd_id = ""
            instance_info = {
                key: {"S": value} for (key, value) in list(instances.items())
            }

            ssm_params = get_ssm_command_parameter_manage_service(
                instance_ids, "author", "stop"
            )
        else:
            cmd_id = message["commandId"]

            # get back the state of this task
            item = get_state_from_dynamodb(dynamodb_table, cmd_id)

            stack_prefix = item["Item"]["environment"]["S"]
            task = item["Item"]["task"]["S"]
            state = item["Item"]["state"]["S"]
            instance_info = item["Item"]["instance_info"]["M"]

            if "externalId" in item["Item"]:
                external_id = item["Item"]["externalId"]["S"]

            author_primary_id = instance_info["author-primary"]["S"]
            author_standby_id = instance_info["author-standby"]["S"]
            publish_id = instance_info["publish"]["S"]
            publish_dispatcher_id = instance_info["publish-dispatcher"]["S"]
            preview_publish_id = instance_info["preview-publish"]["S"]
            preview_publish_dispatcher_id = instance_info["preview-publish-dispatcher"][
                "S"
            ]

            if message["status"] == "Failed":
                logger.error(
                    "[%s] ERROR: While running offline snapshot process ...",
                    stack_prefix,
                )
                logger.error("[%s] ERROR: Command ID: %s.", stack_prefix, cmd_id)
                logger.error(
                    "[%s] ERROR: SSM Document: %s.",
                    stack_prefix,
                    str(message["documentName"]),
                )
                logger.error(
                    "[%s] ERROR: Instance ID: {}.",
                    (stack_prefix, str(message["instanceIds"])),
                )
                logger.error("[%s] ERROR: Offline Snapshotting failed.", stack_prefix)

                update_state_in_dynamodb(
                    dynamodb_table, cmd_id, "Failed", message["eventTime"]
                )
                # move author-dispatcher instances out of standby
                manage_autoscaling_standby(
                    stack_prefix, "exit", byComponent="author-dispatcher"
                )
                # move publish-dispatcher instnace out of standby
                manage_autoscaling_standby(
                    stack_prefix, "exit", byInstanceIds=[publish_dispatcher_id]
                )
                # place preview-publish-dispatcher in active mode
                if preview_publish_dispatcher_id != "False":
                    manage_autoscaling_standby(
                        stack_prefix,
                        "exit",
                        byInstanceIds=[preview_publish_dispatcher_id],
                    )

                manage_lock_for_environment(
                    dynamodb_table, stack_prefix + "_backup_lock", "unlock"
                )

                raise RuntimeError(f"Command {cmd_id} failed.")

            if state == "STOP_AUTHOR_STANDBY":
                instance_ids = [author_primary_id]
                aem_component = "author-primary"
                put_state = "STOP_AUTHOR_PRIMARY"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "author", "stop"
                )

            elif state == "STOP_AUTHOR_PRIMARY":
                instance_ids = [publish_id]
                aem_component = "publish"
                # Defining DynamoDB State
                put_state = "STOP_PUBLISH"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "publish", "stop"
                )

            elif state == "STOP_PUBLISH" and preview_publish_id != "False":
                instance_ids = [preview_publish_id]
                aem_component = "preview-publish"
                # Defining DynamoDB State
                put_state = "STOP_PREVIEW_PUBLISH"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "publish", "stop"
                )

            elif (
                state == "STOP_PUBLISH"
                and task == "offline-compaction-snapshot-full-set"
                or (
                    state == "STOP_PREVIEW_PUBLISH"
                    and task == "offline-compaction-snapshot-full-set"
                )
            ):
                # Defining DynamoDB State
                put_state = "OFFLINE_COMPACTION"
                instance_ids = [author_primary_id, publish_id]
                aem_component = "author-primary/publish"

                if author_standby_id != "Promoted":
                    instance_ids.append(author_standby_id)
                    aem_component = "author-standby/" + aem_component

                if preview_publish_id != "False":
                    instance_ids.append(preview_publish_id)
                    aem_component = aem_component + "/preview-publish"

                ssm_params = get_ssm_command_parameter_offline_compaction_snapshot(
                    instance_ids
                )

            elif (
                state == "OFFLINE_COMPACTION"
                or (state == "STOP_PUBLISH" and task == "offline-snapshot-full-set")
                or (
                    state == "STOP_PREVIEW_PUBLISH"
                    and task == "offline-snapshot-full-set"
                )
            ):
                # Defining DynamoDB State
                put_state = "OFFLINE_BACKUP"
                instance_ids = [author_primary_id, publish_id]
                aem_component = "author-primary/publish"
                if author_standby_id != "Promoted":
                    instance_ids.append(author_standby_id)
                    aem_component = "author-standby/" + aem_component

                if preview_publish_id != "False":
                    instance_ids.append(preview_publish_id)
                    aem_component = aem_component + "/preview-publish"

                ssm_params = get_ssm_command_parameter_offline_snapshot(instance_ids)

            elif state == "OFFLINE_BACKUP":
                instance_ids = [author_primary_id]
                aem_component = "author-primary"
                # Defining DynamoDB State
                put_state = "START_AUTHOR_PRIMARY"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "author", "start"
                )

            elif state == "START_AUTHOR_PRIMARY" and author_standby_id != "Promoted":
                instance_ids = [author_standby_id]
                aem_component = "author-standby"
                # Defining DynamoDB State
                put_state = "START_AUTHOR_STANDBY"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "author", "start"
                )

            elif state == "START_AUTHOR_STANDBY" or (
                state == "START_AUTHOR_PRIMARY" and author_standby_id == "Promoted"
            ):
                instance_ids = [publish_id]
                aem_component = "publish"

                # Defining DynamoDB State
                put_state = "START_PUBLISH"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "publish", "start"
                )

            elif state == "START_PUBLISH" and preview_publish_id != "False":
                instance_ids = [preview_publish_id]
                aem_component = "preview-publish"
                # Defining DynamoDB State
                put_state = "START_PREVIEW_PUBLISH"

                ssm_params = get_ssm_command_parameter_manage_service(
                    instance_ids, "publish", "start"
                )

            elif (
                state == "START_PUBLISH"
                and task == "offline-snapshot-full-set"
                or (
                    state == "START_PREVIEW_PUBLISH"
                    and task == "offline-snapshot-full-set"
                )
            ):
                # move author-dispatcher instances out of standby
                manage_autoscaling_standby(
                    stack_prefix, "exit", byComponent="author-dispatcher"
                )
                # move publish-dispatcher instance out of standby
                manage_autoscaling_standby(
                    stack_prefix, "exit", byInstanceIds=[publish_dispatcher_id]
                )
                # place preview-publish-dispatcher in active mode
                if state == "START_PREVIEW_PUBLISH":
                    manage_autoscaling_standby(
                        stack_prefix,
                        "exit",
                        byInstanceIds=[preview_publish_dispatcher_id],
                    )

                # this is the success notification message
                update_state_in_dynamodb(
                    dynamodb_table, cmd_id, "Success", message["eventTime"]
                )

                manage_lock_for_environment(
                    dynamodb_table, stack_prefix + "_backup_lock", "unlock"
                )

                logger.info("[%s] Offline backup finished successfully", stack_prefix)

                response = {"status": "Success"}

                return response

            elif (
                state == "START_PUBLISH"
                and task == "offline-compaction-snapshot-full-set"
                or (
                    state == "START_PREVIEW_PUBLISH"
                    and task == "offline-compaction-snapshot-full-set"
                )
            ):
                # move author-dispatcher instances out of standby
                manage_autoscaling_standby(
                    stack_prefix, "exit", byComponent="author-dispatcher"
                )
                # move publish-dispatcher instance out of standby
                manage_autoscaling_standby(
                    stack_prefix, "exit", byInstanceIds=[publish_dispatcher_id]
                )

                # need to continue with compact other publish instances
                # start with checking the selected publish instance is
                # ready after compaction

                # Defining DynamoDB State
                put_state = "COMPACT_REMAINING_PUBLISHERS"
                sub_state = "PUBLISH_READY"
                instance_ids = [publish_id]
                aem_component = "publish"

                remaining_pub_disp_pairs = get_remaining_publish_dispatcher_pairs(
                    stack_prefix, publish_id
                )
                supplement = {
                    "SubState": sub_state,
                    "PublishIds": remaining_pub_disp_pairs[0],
                    "DispatcherIds": remaining_pub_disp_pairs[1],
                }

                # place preview-publish-dispatcher in active mode
                if state == "START_PREVIEW_PUBLISH":
                    manage_autoscaling_standby(
                        stack_prefix,
                        "exit",
                        byInstanceIds=[preview_publish_dispatcher_id],
                    )

                    remaining_preview_pub_disp_pairs = (
                        get_remaining_preview_publish_dispatcher_pairs(
                            stack_prefix, preview_publish_id
                        )
                    )

                    instance_ids.append(preview_publish_id)
                    aem_component = aem_component + "/preview-publish"
                    sub_state = "PUBLISH_PREVIEW_READY"
                    supplement_preview = {
                        "SubState": sub_state,
                        "PreviewPublishIds": remaining_preview_pub_disp_pairs[0],
                        "PreviewDispatcherIds": remaining_preview_pub_disp_pairs[1],
                    }
                    supplement = {**supplement, **supplement_preview}

                ssm_params = get_ssm_command_parameter_wait_until_ready(instance_ids)

            elif state == "COMPACT_REMAINING_PUBLISHERS":
                dispatcher_ids = item["Item"]["dispatcher_ids"]["SS"]
                publish_instance_ids = item["Item"]["publish_ids"]["SS"]
                
                supplement = {
                    "PublishIds": publish_instance_ids,
                    "DispatcherIds": dispatcher_ids,
                }

                instance_ids = publish_instance_ids

                put_state = state
                sub_state = item["Item"]["sub_state"]["S"]

                aem_id = "publish"
                aem_component = "publish"

                if preview_publish_id != "False":
                    preview_dispatcher_instance_ids = item["Item"][
                        "preview_dispatcher_ids"
                    ]["SS"]
                    preview_publish_instance_ids = item["Item"]["preview_publish_ids"][
                        "SS"
                    ]
                    instance_ids = instance_ids + preview_publish_instance_ids
                    aem_component = aem_component + "/preview-publish"
                    
                    supplement_preview = {
                        "PreviewPublishIds": item["Item"]["preview_publish_ids"]["SS"],
                        "PreviewDispatcherIds": item["Item"]["preview_dispatcher_ids"]["SS"],
                        
                    }
                    supplement = {**supplement, **supplement_preview}

                if sub_state == "PUBLISH_READY" or sub_state == "PUBLISH_PREVIEW_READY":
                    manage_autoscaling_standby(
                        stack_prefix, "enter", byInstanceIds=dispatcher_ids
                    )
                    if preview_publish_id != "False":
                        manage_autoscaling_standby(
                            stack_prefix,
                            "enter",
                            byInstanceIds=preview_dispatcher_instance_ids,
                        )

                    ssm_params = get_ssm_command_parameter_manage_service(
                        instance_ids, aem_id, "stop"
                    )
                    # Defining DynamoDB Sub State
                    put_sub_state = "STOP_PUBLISH"
                elif sub_state == "STOP_PUBLISH":
                    ssm_params = get_ssm_command_parameter_offline_compaction_snapshot(
                        instance_ids
                    )
                    # Defining DynamoDB Sub State
                    put_sub_state = "COMPACT_PUBLISH"
                elif sub_state == "COMPACT_PUBLISH":
                    ssm_params = get_ssm_command_parameter_manage_service(
                        instance_ids, aem_id, "start"
                    )
                    # Defining DynamoDB Sub State
                    put_sub_state = "START_PUBLISH"
                elif sub_state == "START_PUBLISH":
                    manage_autoscaling_standby(
                        stack_prefix, "exit", byInstanceIds=dispatcher_ids
                    )
                    if preview_publish_id != "False":
                        manage_autoscaling_standby(
                            stack_prefix,
                            "exit",
                            byInstanceIds=preview_dispatcher_instance_ids,
                        )
                    update_state_in_dynamodb(
                        dynamodb_table,
                        cmd_id,
                        "Success",
                        message["eventTime"],
                    )

                    manage_lock_for_environment(
                        dynamodb_table, stack_prefix + "_backup_lock", "unlock"
                    )

                    logger.info(
                        "[%s] Offline compaction backup successfully", stack_prefix
                    )

                    response = {"status": "Success"}

                    return response

                supplement_sub_state = {
                    "SubState": put_sub_state
                    
                }
                supplement = {**supplement, **supplement_sub_state}

            else:
                raise RuntimeError(f"Unexpected state {state} for {cmd_id}")

        # Logging pre-infos
        log_command_info(
            send_command=put_state,
            stack_prefix=stack_prefix,
            instance_id=instance_ids,
            aem_component=aem_component,
        )
        # Sending out ssm command
        response = send_ssm_cmd(ssm_params)
        command_id = response["Command"]["CommandId"]

        # Logging post-infos
        log_command_info(
            send_command=put_state,
            stack_prefix=stack_prefix,
            instance_id=instance_ids,
            aem_component=aem_component,
            command_id=command_id,
        )

        put_state_in_dynamodb(
            dynamodb_table,
            command_id,
            stack_prefix,
            task,
            put_state,
            message["eventTime"],
            message_id,
            ExternalId=external_id,
            InstanceInfo=instance_info,
            LastCommand=cmd_id,
            **supplement,
        )

    return response
