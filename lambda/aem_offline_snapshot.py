# -*- coding: utf8 -*-
# pylint: disable=too-many-lines

"""
Lambda function that takes offline snapshots of an AEM stack.

An offline snapshot has to stop AEM, snapshot the repository volumes and then
start AEM again, in a fixed order across several instances. Each step runs as
an EC2 Run Command. The Run Command completion notification, published to the
offline snapshot SNS topic, calls this function again to start the next step.
So the process is a state machine, and the current state of each run is kept
in DynamoDB under the ID of the command that is running.

Two tasks are supported:

* ``offline-snapshot-full-set``
* ``offline-compaction-snapshot-full-set``, which also compacts the AEM
  repositories before the snapshot, and afterwards compacts the remaining
  publish instances.

States of ``offline-snapshot-full-set``, in order:

#. ``STOP_AUTHOR_STANDBY`` (skipped if the standby has been promoted)
#. ``STOP_AUTHOR_PRIMARY``
#. ``STOP_PUBLISH``
#. ``STOP_PREVIEW_PUBLISH`` (only if the stack has preview publish)
#. ``OFFLINE_BACKUP``
#. ``START_AUTHOR_PRIMARY``
#. ``START_AUTHOR_STANDBY`` (skipped if the standby has been promoted)
#. ``START_PUBLISH``
#. ``START_PREVIEW_PUBLISH`` (only if the stack has preview publish)

``offline-compaction-snapshot-full-set`` adds ``OFFLINE_COMPACTION`` before
``OFFLINE_BACKUP``. After the start states, it moves to
``COMPACT_REMAINING_PUBLISHERS``, which goes through these sub-states for the
remaining publish (and preview publish) instances: ``PUBLISH_READY`` or
``PUBLISH_PREVIEW_READY``, ``STOP_PUBLISH``, ``COMPACT_PUBLISH`` and
``START_PUBLISH``.

While the process runs, the dispatchers in front of the instances being worked
on are put into Auto Scaling standby, and a DynamoDB lock stops two offline
snapshots of the same stack running at once.
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


#: Module logger. Its level comes from the ``LOG_LEVEL`` environment variable
#: and defaults to ``logging.INFO``.
logger = logging.getLogger(__name__)
logger.setLevel(int(os.getenv("LOG_LEVEL", str(logging.INFO))))


#: AWS region of the clients below, taken from ``AWS_REGION``.
aws_region = os.getenv("AWS_REGION")
#: SSM client, used to send EC2 Run Commands.
ssm = boto3.client("ssm", region_name=aws_region)
#: EC2 client, used to find instances by tag.
ec2 = boto3.client("ec2", region_name=aws_region)
#: S3 client, used to download ``config.json``.
s3 = boto3.client("s3", region_name=aws_region)
#: DynamoDB client, used for process state and the stack lock.
dynamodb = boto3.client("dynamodb", region_name=aws_region)
#: SNS client, used by :func:`publish_status_message`.
sns = boto3.client("sns", region_name=aws_region)


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
            return o.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        return json.JSONEncoder.default(self, o)


def instance_ids_by_tags(filters):
    """Return the IDs of the EC2 instances that match the given filters.

    :param filters: Filters in the format of
        :meth:`EC2.Client.describe_instances`, e.g.
        ``[{"Name": "tag:Component", "Values": ["publish"]}]``.
    :type filters: list[dict]
    :returns: IDs of the matching instances, across all reservations.
    :rtype: list[str]
    """
    response = ec2.describe_instances(Filters=filters)
    response2 = json.loads(json.dumps(response, cls=MyEncoder))

    instance_ids = []
    for reservation in response2["Reservations"]:
        instance_ids += [
            instance["InstanceId"] for instance in reservation["Instances"]
        ]
    return instance_ids


def send_ssm_cmd(cmd_details):
    """Send an EC2 Run Command.

    :param cmd_details: Keyword arguments for :meth:`SSM.Client.send_command`.
        Must include ``DocumentName``, which is logged.
    :type cmd_details: dict
    :returns: The ``send_command`` response, with datetimes converted to
        strings by :class:`MyEncoder`.
    :rtype: dict
    """
    ssm_document = cmd_details["DocumentName"]
    logger.info("Start calling SSM Document %s ...", ssm_document)
    response = json.loads(json.dumps(ssm.send_command(**cmd_details), cls=MyEncoder))
    logger.info("Finished calling SSM Document %s .", ssm_document)
    return response


def get_author_primary_ids(stack_prefix):
    """Return the IDs of the running ``author-primary`` instances of a stack.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :returns: Instance IDs. A healthy stack has exactly one.
    :rtype: list[str]
    """
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["author-primary"]},
    ]

    return instance_ids_by_tags(filters)


def get_author_standby_ids(stack_prefix):
    """Return the IDs of the running ``author-standby`` instances of a stack.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :returns: Instance IDs. Empty if the standby has been promoted.
    :rtype: list[str]
    """
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["author-standby"]},
    ]

    return instance_ids_by_tags(filters)


def get_promoted_author_standby_ids(stack_prefix):
    """Return the IDs of the running author-standby instances that were promoted.

    A promoted instance is found by its ``Name`` tag, ``AEM Author - Primary -
    Promoted from Standby``.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :returns: Instance IDs. Empty if no standby has been promoted.
    :rtype: list[str]
    """
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
    """Return the IDs of the running ``publish`` instances of a stack.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :returns: Instance IDs.
    :rtype: list[str]
    """
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["publish"]},
    ]

    return instance_ids_by_tags(filters)


def get_preview_publish_ids(stack_prefix):
    """Return the IDs of the running ``preview-publish`` instances of a stack.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :returns: Instance IDs. Empty if the stack has no preview publish.
    :rtype: list[str]
    """
    filters = [
        {"Name": "tag:StackPrefix", "Values": [stack_prefix]},
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Component", "Values": ["preview-publish"]},
    ]

    return instance_ids_by_tags(filters)


def manage_autoscaling_standby(stack_prefix, action, **kwargs):
    """Move instances of an Auto Scaling group into or out of standby.

    Standby takes the instances out of service, so they stop taking traffic
    and are not replaced while AEM is stopped.

    For ``enter``:

    #. Suspend the ``AlarmNotification`` and ``AZRebalance`` processes.
    #. Lower the group's ``MinSize`` by the number of instances (not below 0).
    #. Put the instances into standby, lowering the desired capacity.

    For ``exit``:

    #. Take the instances out of standby.
    #. Raise the group's ``MinSize`` by the number of instances (not above
       ``MaxSize``).
    #. Resume the ``AlarmNotification`` and ``AZRebalance`` processes.

    Any other ``action`` does nothing.

    Exactly one of ``byComponent`` and ``byInstanceIds`` should be given. If
    both are given, ``byComponent`` is used.

    :param stack_prefix: Value of the ``StackPrefix`` tag. Used with
        ``byComponent`` and in log messages.
    :type stack_prefix: str
    :param action: ``enter`` or ``exit``.
    :type action: str
    :keyword byComponent: Use all running instances of the stack with this
        ``Component`` tag, e.g. ``author-dispatcher``.
    :kwtype byComponent: str
    :keyword byInstanceIds: Use these instances. The group is looked up from
        the first one only, so they must all be in the same group.
    :kwtype byInstanceIds: list[str]
    :raises ValueError: If neither ``byComponent`` nor ``byInstanceIds`` is
        given.
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
        raise ValueError("neither byComponent or byInstanceIds found in arguments")

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

    # read the current min and max size of the autoscaling group
    autoscaling = boto3.client("autoscaling")
    asg_dcrb = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    asg_min_size = asg_dcrb["AutoScalingGroups"][0]["MinSize"]
    asg_max_size = asg_dcrb["AutoScalingGroups"][0]["MaxSize"]

    # lower the min size while the instances are in standby, otherwise the
    # group would launch replacements for them
    min_size = max(asg_min_size - len(instance_ids), 0)

    # move the instances into or out of standby
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
    """Return the value of a tag on an EC2 instance.

    :param instance_id: ID of the instance.
    :type instance_id: str
    :param tag_key: Key of the tag, e.g. ``PairInstanceId``.
    :type tag_key: str
    :returns: The tag value, or ``None`` if the instance has no such tag.
    :rtype: str or None
    """
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
    """Acquire or release a lock that stops two backups of a stack running at once.

    The lock is a DynamoDB item whose ``command_id`` is the lock name. It is
    created with a condition that no item with that key exists, so only one
    caller can acquire it. The item expires after one day, so a lock left
    behind by a failed run is eventually released.

    :param table_name: Name of the DynamoDB state table.
    :type table_name: str
    :param lock: Name of the lock, e.g. ``<stack_prefix>_backup_lock``.
    :type lock: str
    :param action: ``trylock`` to acquire the lock, or ``unlock`` to release
        it.
    :type action: str
    :returns: ``True`` if the lock was acquired or released, ``False`` if it
        is already held or ``action`` is not recognised.
    :rtype: bool
    """

    succeeded = False

    if action == "trylock":
        try:
            # record when the lock was taken
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
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Error Updating Dynamo %s",
                str(err),
            )
            succeeded = False

    elif action == "unlock":
        logger.info("Unlock DynamoDB Table %s", table_name)
        dynamodb.delete_item(TableName=table_name, Key={"command_id": {"S": lock}})
        succeeded = True

    return succeeded


def stack_health_check(
    stack_prefix, min_publish_instances
):
    """Check the stack is fit for an offline snapshot and pick the instances to use.

    The check is based only on the number of running instances of each
    component. The stack is healthy when:

    * it has at least ``min_publish_instances`` publish instances, and the
      first one has a ``PairInstanceId`` tag naming its dispatcher, and
    * it has exactly one author-primary instance, or exactly one author-standby
      instance that was promoted to primary, and
    * it has at least one author-standby instance, or a promoted one.

    Preview publish is optional. If the stack has one, the first preview
    publish instance and the dispatcher in its ``PreviewPairInstanceId`` tag
    are used.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :param min_publish_instances: Minimum number of publish instances. A
        numeric string is accepted.
    :type min_publish_instances: int or str
    :returns: ``None`` if the stack is unhealthy. Otherwise the instance ID
        to use for each of these keys: ``author-primary``,
        ``author-standby``, ``publish``, ``publish-dispatcher``,
        ``preview-publish`` and ``preview-publish-dispatcher``.
        ``author-standby`` is the string ``"Promoted"`` if the standby was
        promoted and there is no other standby. The preview values are the
        string ``"False"`` if there is no preview publish.
    :rtype: dict or None
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
        logger.error("Found %s publish instances. Unhealthy stack.", publish_count)
        return None

    author_standby_instance_id = None
    if promoted_author_standby_instances_count == 1:
        author_primary_instance_id = promoted_author_standby_instances[0]
        author_standby_instance_id = "Promoted"
        logger.warning(
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
        preview_publish_count >= 1
    ):
        preview_publish_instance_id = preview_publish_instances[0]
        paired_preview_publish_dispatcher_id = retrieve_tag_value(
            preview_publish_instance_id, "PreviewPairInstanceId"
        )
    else:
        preview_publish_instance_id = "False"
        paired_preview_publish_dispatcher_id = "False"

    logger.info("[%s] Finished checking Stack health successfully.", stack_prefix)

    return {
        "author-primary": author_primary_instance_id,
        "author-standby": author_standby_instance_id,
        "publish": publish_instance_id,
        "publish-dispatcher": paired_publish_dispatcher_id,
        "preview-publish": preview_publish_instance_id,
        "preview-publish-dispatcher": paired_preview_publish_dispatcher_id,
    }


def put_state_in_dynamodb(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    table_name, command_id, environment, task, state, timestamp, message_id, **kwargs
):
    """Write the state of an offline snapshot step to DynamoDB.

    Any existing item with the same ``command_id`` is replaced.

    Item schema (DynamoDB attribute types in brackets):

    ==========================  ====  ========================================
    Attribute                   Type  Description
    ==========================  ====  ========================================
    ``command_id``              S     Partition key. The EC2 Run Command ID,
                                      or the external ID if no command has
                                      run yet.
    ``environment``             S     Usually the stack prefix.
    ``task``                    S     ``offline-snapshot-full-set`` or
                                      ``offline-compaction-snapshot-full-set``.
    ``state``                   S     Process state, e.g.
                                      ``STOP_AUTHOR_STANDBY``, or ``Success``
                                      or ``Failed`` when the process ends.
    ``timestamp``               S     Time of the state, e.g.
                                      ``2017-05-16T01:57:05.9Z``.
    ``ttl``                     N     Expiry time in epoch seconds, one day
                                      from now.
    ``message_id``              S     ID of the SNS message that caused the
                                      write.
    ``instance_info``           M     Optional. Instance ID by component, as
                                      returned by :func:`stack_health_check`.
    ``last_command``            S     Optional. ID of the previous Run
                                      Command in the chain. Mostly for
                                      debugging.
    ``externalId``              S     Optional. ID from an external caller,
                                      e.g. a Jenkins or Bamboo job ID.
    ``publish_ids``             SS    Optional. Remaining publish instances
                                      to compact.
    ``dispatcher_ids``          SS    Optional. Dispatchers paired with
                                      ``publish_ids``.
    ``preview_publish_ids``     SS    Optional. Remaining preview publish
                                      instances to compact.
    ``preview_dispatcher_ids``  SS    Optional. Dispatchers paired with
                                      ``preview_publish_ids``.
    ``sub_state``               S     Optional. Step within
                                      ``COMPACT_REMAINING_PUBLISHERS``.
    ==========================  ====  ========================================

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
    :param kwargs: Optional attributes, each written only when given and not
        ``None``: ``InstanceInfo`` (*dict*), ``LastCommand`` (*str*),
        ``ExternalId`` (*str*), ``PublishIds`` (*list[str]*),
        ``DispatcherIds`` (*list[str]*), ``PreviewPublishIds``
        (*list[str]*), ``PreviewDispatcherIds`` (*list[str]*) and
        ``SubState`` (*str*). Other keys are ignored.
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

    # the following attributes are only used when compacting the remaining
    # publish and preview publish instances
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


def get_state_from_dynamodb(table_name, command_id):
    """Read the state of a command from DynamoDB with a consistent read.

    :param table_name: Name of the DynamoDB table.
    :type table_name: str
    :param command_id: The ``command_id`` key to read.
    :type command_id: str
    :returns: The raw :meth:`DynamoDB.Client.get_item` response. The item,
        with all the attributes described in :func:`put_state_in_dynamodb`,
        is under ``Item``. That key is missing if no item was found.
    :rtype: dict
    """
    item = dynamodb.get_item(
        TableName=table_name,
        Key={"command_id": {"S": command_id}},
        ConsistentRead=True,
        ReturnConsumedCapacity="NONE",
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


def publish_status_message(topic, message):
    """Publish a message to an SNS topic.

    Intended to forward EC2 Run Command notification messages. Not called
    anywhere in this module.

    :param topic: ARN of the SNS topic.
    :type topic: str
    :param message: Message body. It is sent as the ``default`` message for
        all protocols.
    :type message: str
    """
    payload = {"default": message}

    sns.publish(TopicArn=topic, MessageStructure="json", Message=json.dumps(payload))


def get_remaining_publish_dispatcher_pairs(
    stack_prefix: str, completed_publish_id: str
):
    """Return the publish instances still to compact, and their dispatchers.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :param completed_publish_id: The publish instance already compacted. It
        must be one of the running publish instances.
    :type completed_publish_id: str
    :returns: A ``(publish_ids, dispatcher_ids)`` tuple. ``dispatcher_ids[i]``
        is the ``PairInstanceId`` tag of ``publish_ids[i]``, or ``None`` if
        it is not set.
    :rtype: tuple[list[str], list[str]]
    :raises ValueError: If ``completed_publish_id`` is not a running publish
        instance.
    """
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
    """Return the preview publish instances still to compact, and their dispatchers.

    :param stack_prefix: Value of the ``StackPrefix`` tag.
    :type stack_prefix: str
    :param completed_preview_publish_id: The preview publish instance already
        compacted. It must be one of the running preview publish instances.
    :type completed_preview_publish_id: str
    :returns: A ``(preview_publish_ids, dispatcher_ids)`` tuple.
        ``dispatcher_ids[i]`` is the ``PreviewPairInstanceId`` tag of
        ``preview_publish_ids[i]``, or ``None`` if it is not set.
    :rtype: tuple[list[str], list[str]]
    :raises ValueError: If ``completed_preview_publish_id`` is not a running
        preview publish instance.
    """
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
            preview_publish_id, "PreviewPairInstanceId"
        )
        preview_publish_dispatcher_ids.append(preview_publish_dispatcher_id)

    return preview_publish_ids, preview_publish_dispatcher_ids


def get_stack_manager_config():
    """Load the Stack Manager configuration.

    ``config.json`` is downloaded from ``s3://$S3_BUCKET/$S3_PREFIX/`` to
    ``/tmp/config.json`` when both environment variables are set.

    .. note::
       The warning message says the bundled ``config.json`` will be used when
       the S3 variables are not set, but no fallback path is assigned, so
       ``open()`` fails with :exc:`UnboundLocalError` in that case.

    :returns: The parsed configuration. The sections used by this module are
        ``ec2_run_command``, ``offline_snapshot`` and ``document_mapping``.
    :rtype: dict
    """
    # reading in config info from either s3 or within bundle
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    if bucket is not None and prefix is not None:
        config_file = "/tmp/config.json"
        s3.download_file(bucket, f"{prefix}/config.json", config_file)
    else:
        logger.warning("Unable to locate config.json in S3, searching within bundle")

    with open(config_file, "r", encoding="utf-8") as open_file:
        content = "".join(open_file.readlines()).replace("\n", "")
        logger.debug("config file: %s", str(content))
        config = json.loads(content)

    return config


def get_ssm_command_common_parameters(stack_manager_config):
    """Build the ``send_command`` arguments shared by all offline snapshot steps.

    Completion notifications for ``Success`` and ``Failed`` go to the offline
    snapshot SNS topic, which calls :func:`sns_message_processor` again for
    the next step.

    :param stack_manager_config: Configuration from
        :func:`get_stack_manager_config`.
    :type stack_manager_config: dict
    :returns: Arguments for :meth:`SSM.Client.send_command`, without
        ``InstanceIds``, ``DocumentName``, ``Comment`` or ``Parameters``.
    :rtype: dict
    """
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
    """Build ``send_command`` arguments to start or stop the AEM service.

    Uses the ``manage-service`` SSM document with a one hour timeout.

    :param instance_ids: Instances to run the command on.
    :type instance_ids: list[str]
    :param aem_id: AEM instance type, ``author`` or ``publish``.
    :type aem_id: str
    :param action: ``start`` or ``stop``.
    :type action: str
    :returns: Arguments for :meth:`SSM.Client.send_command`.
    :rtype: dict
    """
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
    """Build ``send_command`` arguments to compact the AEM repositories offline.

    Uses the ``offline-compaction-snapshot-full-set`` SSM document with a
    four hour timeout.

    :param instance_ids: Instances to run the command on.
    :type instance_ids: list[str]
    :returns: Arguments for :meth:`SSM.Client.send_command`.
    :rtype: dict
    """
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
    """Build ``send_command`` arguments to snapshot the AEM repository volumes.

    Uses the ``offline-snapshot-full-set`` SSM document with a four hour
    timeout.

    :param instance_ids: Instances to run the command on.
    :type instance_ids: list[str]
    :returns: Arguments for :meth:`SSM.Client.send_command`.
    :rtype: dict
    """
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
    """Build ``send_command`` arguments to wait until AEM is ready.

    Uses the ``wait-until-ready`` SSM document with a two hour timeout.

    :param instance_ids: Instances to run the command on.
    :type instance_ids: list[str]
    :returns: Arguments for :meth:`SSM.Client.send_command`.
    :rtype: dict
    """
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
    """Log the progress of sending a Run Command for a step.

    The message depends on the arguments given:

    * ``command_id`` set -- the command was sent. Logs its ID.
    * ``stack_prefix`` and ``send_command`` set -- the command is about to be
      sent. Logs the target instances.
    * otherwise -- logs ``send_command`` alone.

    :param send_command: Name of the step, e.g. ``STOP_PUBLISH``.
    :type send_command: str or None
    :param stack_prefix: Stack prefix, used as a log prefix.
    :type stack_prefix: str or None
    :param instance_id: Target instance IDs.
    :type instance_id: list[str] or None
    :param aem_component: Target components, e.g. ``author-primary/publish``,
        used as a log prefix.
    :type aem_component: str or None
    :param command_id: ID of the command that was sent.
    :type command_id: str or None
    """
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


# The process is a state machine driven by SNS messages, hence the size.
# pylint: disable-next=too-many-branches,too-many-statements
def sns_message_processor(event, context):
    """Lambda entry point: start an offline snapshot or move it to the next step.

    Each SNS record is one of two kinds of message:

    * **Task request** -- has ``task`` set to ``offline-snapshot-full-set`` or
      ``offline-compaction-snapshot-full-set``, and ``stack_prefix``, plus an
      optional ``externalId``. The stack is checked with
      :func:`stack_health_check`, the stack lock is taken with
      :func:`manage_lock_for_environment`, the author and publish
      dispatchers are put into standby, and the first stop command is sent.
    * **Run Command notification** -- has ``commandId``, ``status`` and
      ``eventTime``. The state of the finished command is read from DynamoDB,
      and the command for the next state is sent. See the module
      documentation for the order of the states. When the last step is done,
      the dispatchers are taken out of standby, the state is set to
      ``Success`` and the lock is released.

    Every command sent is recorded with :func:`put_state_in_dynamodb`, keyed
    by its command ID, so the next notification can find it.

    If a notification has the status ``Failed``, the state is set to
    ``Failed``, the dispatchers are taken out of standby and the lock is
    released.

    .. warning::
       Some failure paths log the error but do not return. These are an
       unhealthy stack, a lock that is already held, a ``Failed``
       notification and an unexpected state. Processing then goes on as if
       the check had passed, and in some of these cases fails later with an
       exception.

    :param event: SNS event with one or more ``Records``.
    :type event: dict
    :param context: Lambda context object. Only logged.
    :type context: object
    :returns: ``{"status": "Success"}`` when the process completes.
        Otherwise the ``send_command`` response for the command sent for the
        last record.
    :rtype: dict
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
        # TODO: the failure paths below (unhealthy stack, failed lock, SSM
        # "Failed" status, unexpected state) fall through instead of returning
        # early; these defaults only keep that behavior well defined.
        put_state = None
        put_sub_state = None
        ssm_params = None
        instance_ids = None
        aem_component = None

        # a message with a task key starts a new offline snapshot; anything
        # else is a Run Command notification for a step in progress
        if "task" in message and (
            message["task"] == "offline-snapshot-full-set"
            or message["task"] == "offline-compaction-snapshot-full-set"
        ):
            stack_prefix = message["stack_prefix"]
            task = message["task"]
            if "externalId" in message:
                external_id = message["externalId"]

            # sanity checks: the stack is healthy and no other offline
            # snapshot of it is running
            try:
                min_publish_instance = int(
                    offline_snapshot_config["min-publish-instances"]
                )
                instances = stack_health_check(
                    stack_prefix, min_publish_instance
                )
                if instances is None:
                    raise RuntimeError("Unhealthy Stack")

                # try to acquire stack lock
                locked = manage_lock_for_environment(
                    dynamodb_table, stack_prefix + "_backup_lock", "trylock"
                )
                if not locked:
                    logger.error(
                        "Cannot have two offline snapshots/compactions run in parallel"
                    )

            except RuntimeError:
                # an externalId means an external party will query the status,
                # so record the failure in DynamoDB with the externalId as the
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

            # place all author-dispatcher instances in standby mode after the
            # health check
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

            # the first step is to stop author-standby, unless it has been
            # promoted to primary, in which case start with author-primary
            if instances["author-standby"] != "Promoted":
                instance_ids = [instances["author-standby"]]
                # Defining DynamoDB State
                put_state = "STOP_AUTHOR_STANDBY"
                aem_component = "author-standby"
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

            # look up the state that the finished command was sent for
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
                # move publish-dispatcher instance out of standby
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

                logger.error("Command %s failed.", cmd_id)

            # choose the next step from the state of the finished command
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

                # go on to compact the other publish instances, starting by
                # waiting until the compacted publish instance is ready

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
                # the remaining publish instances go through their own
                # sub-states: *_READY -> STOP_PUBLISH -> COMPACT_PUBLISH ->
                # START_PUBLISH -> done
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
                        "PreviewDispatcherIds": item["Item"]["preview_dispatcher_ids"][
                            "SS"
                        ],
                    }
                    supplement = {**supplement, **supplement_preview}

                if sub_state in ("PUBLISH_READY", "PUBLISH_PREVIEW_READY"):
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

                supplement_sub_state = {"SubState": put_sub_state}
                supplement = {**supplement, **supplement_sub_state}

            else:
                logger.error("Unexpected state %s for command %s.", state, cmd_id)

        # send the command for the next step and record its state, keyed by
        # the new command ID, for the next notification to pick up
        log_command_info(
            send_command=put_state,
            stack_prefix=stack_prefix,
            instance_id=instance_ids,
            aem_component=aem_component,
        )
        response = send_ssm_cmd(ssm_params)
        command_id = response["Command"]["CommandId"]

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
