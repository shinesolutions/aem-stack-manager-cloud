"""
Helpers that build mocked AWS infrastructure for the Lambda unit tests.

:class:`AwsHelper` creates the Stack Manager resources (S3 config, SNS topic,
DynamoDB table, SSM document and IAM role) and a tagged AEM stack of EC2
instances and Auto Scaling groups. It is meant to run inside a ``moto``
``mock_aws`` context.

These environment variables must be set before use:

* ``AWS_REGION``
* ``S3_BUCKET`` and ``S3_PREFIX`` -- location of ``config.json``
* ``DDB_TABLE_NAME`` -- Stack Manager state table
* ``STACK_PREFIX`` -- value of the ``StackPrefix`` tag on the instances
"""
# pylint: disable=C0301
# pylint: disable=C0302
# pylint: disable=R0801
# pylint: disable=R0904
# pylint: disable=R0914
import os
import json

import boto3


class AwsHelper:
    """
    Builds mocked AWS resources for tests.

    Every method makes a new boto3 client for ``AWS_REGION`` each time it is
    called, so the helper keeps no state.
    """

    def __init__(self):
        """
        Create the helper. Nothing is set up until a ``setup_*`` method is
        called.
        """

    # Boto3 Client Helpers
    def asg_client(self):
        """
        Create an Auto Scaling client.

        :returns: boto3 ``autoscaling`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("autoscaling", region_name=aws_region)

    def ec2_client(self):
        """
        Create an EC2 client.

        :returns: boto3 ``ec2`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("ec2", region_name=aws_region)

    def elb_client(self):
        """
        Create an Elastic Load Balancing v2 client.

        :returns: boto3 ``elbv2`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("elbv2", region_name=aws_region)

    def iam_client(self):
        """
        Create an IAM client.

        :returns: boto3 ``iam`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("iam", region_name=aws_region)

    def ssm_client(self):
        """
        Create an SSM client.

        :returns: boto3 ``ssm`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("ssm", region_name=aws_region)

    def sns_client(self):
        """
        Create an SNS client.

        :returns: boto3 ``sns`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("sns", region_name=aws_region)

    def s3_client(self):
        """
        Create an S3 client.

        :returns: boto3 ``s3`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("s3", region_name=aws_region)

    def ddb_client(self):
        """
        Create a DynamoDB client.

        :returns: boto3 ``dynamodb`` client.
        :rtype: botocore.client.BaseClient
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("dynamodb", region_name=aws_region)

    def create_ssm_document(self, document_name):
        """Create an SSM Command document that just echoes a message.

        It accepts the ``aemid``, ``action`` and ``executionTimeout``
        parameters that the offline snapshot Lambda sends, so it can stand
        in for every document in the Stack Manager config.

        :param document_name: Name of the document.
        :type document_name: str
        :returns: Name of the created document.
        :rtype: str
        """
        document_content = """
        {
          "schemaVersion": "2.2",
          "description": "My SSM document",
          "parameters": {
                "aemid": {
                    "type": "String",
                    "description": "Name of the person"
                },
                "action": {
                    "type": "String",
                    "description": "Age of the person"
                },
                "executionTimeout": {
                    "type": "String",
                    "description": "Location of the person"
                }
            },
          "mainSteps": [
            {
              "action": "aws:runShellScript",
              "name": "RunShellScript",
              "inputs": {
                "runCommand": [
                  "echo 'Hello, World!'"
                ]
              }
            }
          ]
        }
        """
        response = self.ssm_client().create_document(
            Content=document_content,
            Name=document_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )

        document_name = response["DocumentDescription"]["Name"]

        return document_name

    def create_ddb_table(self, table_name):
        """Create the Stack Manager state table.

        The partition key is ``command_id``. The ``ExternalQuery`` global
        secondary index (``externalId`` with sort key ``timestamp``) is the
        one :mod:`aem_task_query` uses.

        :param table_name: Name of the table.
        :type table_name: str
        :returns: The ``create_table`` response.
        :rtype: dict
        """
        response = self.ddb_client().create_table(
            AttributeDefinitions=[
                {"AttributeName": "command_id", "AttributeType": "S"},
                {"AttributeName": "externalId", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "command_id", "KeyType": "HASH"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "ExternalQuery",
                    "KeySchema": [
                        {"AttributeName": "externalId", "KeyType": "HASH"},
                        {"AttributeName": "timestamp", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": [
                            "state",
                        ],
                    },
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        return response

    def create_s3_bucket(self, bucket_name):
        """Create an S3 bucket in ``ap-southeast-2``.

        :param bucket_name: Name of the bucket.
        :type bucket_name: str
        :returns: The ``create_bucket`` response.
        :rtype: dict
        """
        response = self.s3_client().create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        return response

    def create_sns_topic(self, topic_name):
        """Create an SNS topic.

        :param topic_name: Name of the topic.
        :type topic_name: str
        :returns: ARN of the topic.
        :rtype: str
        """
        response = self.sns_client().create_topic(Name=topic_name)

        return response["TopicArn"]

    def create_sm_config(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        s3_bucket_name,
        s3_key_prefix,
        sns_topic_arn,
        sm_service_iam_role_arn,
        ddb_table_name,
        ssm_document_name,
    ):
        """Build a Stack Manager ``config.json`` for tests.

        Every task in ``document_mapping`` is mapped to the same SSM
        document. ``min-publish-instances`` is ``1``. The same SNS topic is
        used for Run Command status and for the offline snapshot process.

        :param s3_bucket_name: Bucket for Run Command output.
        :type s3_bucket_name: str
        :param s3_key_prefix: Key prefix for Run Command output.
        :type s3_key_prefix: str
        :param sns_topic_arn: ARN of the status and offline snapshot topic.
        :type sns_topic_arn: str
        :param sm_service_iam_role_arn: ARN of the SSM service role.
        :type sm_service_iam_role_arn: str
        :param ddb_table_name: Name of the state table.
        :type ddb_table_name: str
        :param ssm_document_name: SSM document to use for every task.
        :type ssm_document_name: str
        :returns: The configuration.
        :rtype: dict
        """
        config = {
            "ec2_run_command": {
                "cmd-output-bucket": s3_bucket_name,
                "cmd-output-prefix": s3_key_prefix,
                "status-topic-arn": sns_topic_arn,
                "ssm-service-role-arn": sm_service_iam_role_arn,
                "dynamodb-table": ddb_table_name,
            },
            "document_mapping": {
                "deploy-artifacts": ssm_document_name,
                "test-readiness-consolidated": ssm_document_name,
                "disable-saml": ssm_document_name,
                "run-adhoc-puppet": ssm_document_name,
                "export-packages": ssm_document_name,
                "import-package": ssm_document_name,
                "live-snapshot": ssm_document_name,
                "offline-compaction-snapshot-consolidated": ssm_document_name,
                "offline-snapshot-full-set": ssm_document_name,
                "export-package": ssm_document_name,
                "promote-author": ssm_document_name,
                "reconfigure-aem": ssm_document_name,
                "upgrade-repository-migration": ssm_document_name,
                "wait-until-ready": ssm_document_name,
                "enable-saml": ssm_document_name,
                "flush-dispatcher-cache": ssm_document_name,
                "manage-service": ssm_document_name,
                "deploy-artifact": ssm_document_name,
                "install-aem-profile": ssm_document_name,
                "offline-compaction-snapshot-full-set": ssm_document_name,
                "list-packages": ssm_document_name,
                "test-readiness-full-set": ssm_document_name,
                "upgrade-unpack-jar": ssm_document_name,
                "schedule-snapshot": ssm_document_name,
                "run-toughday2-performance-test": ssm_document_name,
                "enable-crxde": ssm_document_name,
                "offline-snapshot-consolidated": ssm_document_name,
                "run-aem-upgrade": ssm_document_name,
                "disable-crxde": ssm_document_name,
            },
            "offline_snapshot": {
                "min-publish-instances": "1",
                "sns-topic-arn": sns_topic_arn,
            },
            "cw_stream_s3": {
                "s3-bucket-cw-stream": "overwrite-me",
                "s3-prefix-cw-stream": "overwrite-me",
            },
        }

        return config

    def upload_sm_config(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        s3_bucket_name,
        s3_key_prefix,
        sns_topic_arn,
        sm_service_iam_role_arn,
        ddb_table_name,
        ssm_document_name,
    ):
        """Upload a test ``config.json`` to ``<s3_bucket_name>/<s3_key_prefix>/``.

        The parameters are the same as for :meth:`create_sm_config`.

        :returns: The ``put_object`` response.
        :rtype: dict
        """
        sm_config = self.create_sm_config(
            s3_bucket_name,
            s3_key_prefix,
            sns_topic_arn,
            sm_service_iam_role_arn,
            ddb_table_name,
            ssm_document_name,
        )

        response = self.s3_client().put_object(
            Body=json.dumps(sm_config),
            Bucket=s3_bucket_name,
            Key=s3_key_prefix + "/config.json",
        )

        return response

    # Infrastructure helper
    def setup_stack_manager_infrastructure(self):
        """Create the Stack Manager resources and upload its config.

        Creates the ``S3_BUCKET`` bucket, the ``aoc-sm`` SNS topic, the
        ``DDB_TABLE_NAME`` table, the ``aoc-sm-ssm-doc`` SSM document and the
        SSM service role, then uploads ``config.json`` to ``S3_PREFIX``.
        """
        s3_bucket_name = os.getenv("S3_BUCKET")
        s3_key_prefix = os.getenv("S3_PREFIX")
        s3_key_prefix = os.getenv("S3_PREFIX")
        ddb_table_name = os.getenv("DDB_TABLE_NAME")
        ssm_document_name = "aoc-sm-ssm-doc"
        self.create_s3_bucket(s3_bucket_name)

        sns_topic_arn = self.create_sns_topic("aoc-sm")
        self.create_ddb_table(ddb_table_name)

        ssm_document_name = self.create_ssm_document(ssm_document_name)

        sm_service_iam_role_arn = self.create_sm_service_iam_role()

        self.upload_sm_config(
            s3_bucket_name,
            s3_key_prefix,
            sns_topic_arn,
            sm_service_iam_role_arn,
            ddb_table_name,
            ssm_document_name,
        )

    def setup_stack_manager_preview_infrastructure(self):
        """Create the Stack Manager resources for a preview stack.

        The same as :meth:`setup_stack_manager_infrastructure`, but the SNS
        topic is ``aoc-sm-preview`` and the SSM document is
        ``aoc-sm--preview-ssm-doc``.
        """
        s3_bucket_name = os.getenv("S3_BUCKET")
        s3_key_prefix = os.getenv("S3_PREFIX")
        ddb_table_name = os.getenv("DDB_TABLE_NAME")
        ssm_document_name = "aoc-sm--preview-ssm-doc"
        self.create_s3_bucket(s3_bucket_name)

        sns_topic_arn = self.create_sns_topic("aoc-sm-preview")
        self.create_ddb_table(ddb_table_name)

        ssm_document_name = self.create_ssm_document(ssm_document_name)

        sm_service_iam_role_arn = self.create_sm_service_iam_role()

        self.upload_sm_config(
            s3_bucket_name,
            s3_key_prefix,
            sns_topic_arn,
            sm_service_iam_role_arn,
            ddb_table_name,
            ssm_document_name,
        )

    def setup_standard_infrastructure(self):
        """
        Create a standard AEM stack, tagged with ``STACK_PREFIX``.

        * 1 author-primary instance
        * 1 author-standby instance
        * 1 author-dispatcher target group and ASG with 1 instance, attached
          to the target group
        * 1 publish-dispatcher target group and ASG with 2 instances,
          attached to the target group
        * 1 publish ASG with 2 instances, each paired with a
          publish-dispatcher

        Every instance is tagged as having finished orchestration.

        :returns: The created instance IDs under these keys:
            ``Author-Primary`` and ``Author-Standby`` (*str*), and
            ``AuthorDispatcher``, ``PublishDispatcher`` and ``Publish``
            (*list[str]*).
        :rtype: dict
        """
        # Preparation
        # Environment preparation
        stack_prefix = os.getenv("STACK_PREFIX")

        # Author-Dispatcher preparation
        author_dispatcher_instance_name = "author-dispatcher"
        author_dispatcher_asg_name = f"{author_dispatcher_instance_name}-asg"
        author_dispatcher_asg_desired_capacity = 1

        # Publish-Dispatcher preparation
        publish_dispatcher_instance_name = "publish-dispatcher"
        publish_dispatcher_asg_name = f"{publish_dispatcher_instance_name}-asg"
        publish_dispatcher_asg_desired_capacity = 2

        # Publish preparation
        publish_instance_name = "publish"
        publish_asg_name = f"{publish_instance_name}-asg"
        publish_asg_desired_capacity = 2

        # Author preparation
        author_primary_instance_name = "author-primary"
        author_standby_instance_name = "author-standby"

        responses = {}

        # Create Author-Primary
        response = self.setup_author_primary_infrastructure(
            author_primary_instance_name,
            stack_prefix,
        )
        responses["Author-Primary"] = response

        # Create Author-Standby
        response = self.setup_author_standby_infrastructure(
            author_standby_instance_name,
            stack_prefix,
        )
        responses["Author-Standby"] = response

        # Create author_dispatcher Auto Scaling Group
        response = self.setup_author_dispatcher_infrastructure(
            author_dispatcher_asg_name,
            author_dispatcher_instance_name,
            author_dispatcher_asg_desired_capacity,
            stack_prefix,
        )
        responses["AuthorDispatcher"] = response

        # Create publish_dispatcher Auto Scaling Group
        response = self.setup_publish_dispatcher_infrastructure(
            publish_dispatcher_asg_name,
            publish_dispatcher_instance_name,
            publish_dispatcher_asg_desired_capacity,
            stack_prefix,
        )
        responses["PublishDispatcher"] = response

        # Create Publisher Auto Scaling Group
        response = self.setup_publish_infrastructure(
            publish_asg_name,
            publish_instance_name,
            publish_asg_desired_capacity,
            stack_prefix,
            responses["PublishDispatcher"],
        )
        responses["Publish"] = response

        return responses

    def setup_standard_infrastructure_promoted_standby(self):
        """Create a standard stack whose author-standby was promoted to primary.

        Builds :meth:`setup_standard_infrastructure`, terminates the
        author-primary instance and retags the author-standby with
        :meth:`promote_author_standby_to_primary`.
        """
        standard_infrastructure = self.setup_standard_infrastructure()
        stack_prefix = os.getenv("STACK_PREFIX")
        author_primary_instance_id = standard_infrastructure["Author-Primary"]
        self.ec2_client().terminate_instances(
            InstanceIds=[
                author_primary_instance_id,
            ]
        )

        self.promote_author_standby_to_primary(
            standard_infrastructure["Author-Standby"], stack_prefix, "author-primary"
        )

    def setup_preview_infrastructure(self):
        """Create a standard stack with preview publish components added.

        Adds to :meth:`setup_standard_infrastructure`:

        * 1 preview-publish-dispatcher target group and ASG with 2 instances
        * 1 preview-publish ASG with 2 instances, each paired with a
          preview-publish-dispatcher

        :returns: The instance IDs from
            :meth:`setup_standard_infrastructure`, plus
            ``PreviewPublishDispatcher`` and ``PreviewPublish``
            (*list[str]*).
        :rtype: dict
        """
        responses = self.setup_standard_infrastructure()
        stack_prefix = os.getenv("STACK_PREFIX")

        # preview-publish-Dispatcher preparation
        publish_dispatcher_instance_name = "preview-publish-dispatcher"
        publish_dispatcher_asg_name = f"{publish_dispatcher_instance_name}-asg"
        publish_dispatcher_asg_desired_capacity = 2

        # preview-publish preparation
        publish_instance_name = "preview-publish"
        publish_asg_name = f"{publish_instance_name}-asg"
        publish_asg_desired_capacity = 2

        # Create preview-publish-dispatcher Auto Scaling Group
        response = self.setup_preview_publish_dispatcher_infrastructure(
            publish_dispatcher_asg_name,
            publish_dispatcher_instance_name,
            publish_dispatcher_asg_desired_capacity,
            stack_prefix,
        )
        responses["PreviewPublishDispatcher"] = response

        # Create preview-publish Auto Scaling Group
        response = self.setup_preview_publish_infrastructure(
            publish_asg_name,
            publish_instance_name,
            publish_asg_desired_capacity,
            stack_prefix,
            responses["PreviewPublishDispatcher"],
        )
        responses["PreviewPublish"] = response

        return responses

    def setup_preview_infrastructure_promoted_standby(self):
        """Create a preview stack whose author-standby was promoted to primary.

        Builds :meth:`setup_preview_infrastructure`, terminates the
        author-primary instance and retags the author-standby with
        :meth:`promote_author_standby_to_primary`.
        """
        standard_infrastructure = self.setup_preview_infrastructure()
        stack_prefix = os.getenv("STACK_PREFIX")
        author_primary_instance_id = standard_infrastructure["Author-Primary"]
        self.ec2_client().terminate_instances(
            InstanceIds=[
                author_primary_instance_id,
            ]
        )

        self.promote_author_standby_to_primary(
            standard_infrastructure["Author-Standby"], stack_prefix, "author-primary"
        )

    def elbv2_register_target(self, target_group_arn, instance_id):
        """Register an EC2 instance with a target group.

        :param target_group_arn: ARN of the target group.
        :type target_group_arn: str
        :param instance_id: ID of the instance.
        :type instance_id: str
        :returns: The ``register_targets`` response.
        :rtype: dict
        """
        response = self.elb_client().register_targets(
            TargetGroupArn=target_group_arn,
            Targets=[
                {
                    "Id": instance_id,
                }
            ],
        )

        return response

    def setup_author_dispatcher_infrastructure(
        self, asg_name, instance_name, desired_capacity, stack_prefix
    ):
        """Create the author-dispatcher target group, ASG and instances.

        Each instance is registered with the target group and tagged with
        ``Component=author-dispatcher`` and orchestration completed.

        :param asg_name: Name of the Auto Scaling group.
        :type asg_name: str
        :param instance_name: Value of the ``Name`` tag. Also used to name the
            target group ``<instance_name>-tg``.
        :type instance_name: str
        :param desired_capacity: Number of instances.
        :type desired_capacity: int
        :param stack_prefix: Value of the ``StackPrefix`` tag.
        :type stack_prefix: str
        :returns: IDs of the healthy instances in the group.
        :rtype: list[str]
        """
        target_group_arn = self.create_target_group(instance_name)

        # Create Dispatcher Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Attach Target Group to ASG
        self.attach_load_balancer_target_groups(asg_name, target_group_arn)
        # Modify each instance in ASG
        for instance in instances:
            # Register Dispatcher EC2 instance to Target Group
            self.elbv2_register_target(target_group_arn, instance)
            # Tag EC2 instance with Orchestration Completion Tag
            self.ec2_tagger_orchestration_completed(instance)
            # Tag EC2 instance with EC2 information
            self.ec2_tagger_ec2_information(
                instance, instance_name, stack_prefix, "author-dispatcher"
            )


        return instances

    def setup_publish_dispatcher_infrastructure(
        self, asg_name, instance_name, desired_capacity, stack_prefix
    ):
        """Create the publish-dispatcher target group, ASG and instances.

        The same as :meth:`setup_author_dispatcher_infrastructure`, with
        ``Component=publish-dispatcher``.

        :returns: IDs of the healthy instances in the group.
        :rtype: list[str]
        """
        target_group_arn = self.create_target_group(instance_name)

        # Create Dispatcher Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Attach Target Group to ASG
        self.attach_load_balancer_target_groups(asg_name, target_group_arn)
        # Modify each instance in ASG
        for instance in instances:
            # Register Dispatcher EC2 instance to Target Group
            self.elbv2_register_target(target_group_arn, instance)
            # Tag EC2 instance with Orchestration Completion Tag
            self.ec2_tagger_orchestration_completed(instance)
            # Tag EC2 instance with EC2 information
            self.ec2_tagger_ec2_information(
                instance, instance_name, stack_prefix, "publish-dispatcher"
            )

        return instances

    def setup_preview_publish_dispatcher_infrastructure(
        self, asg_name, instance_name, desired_capacity, stack_prefix
    ):
        """Create the preview-publish-dispatcher target group, ASG and instances.

        The same as :meth:`setup_author_dispatcher_infrastructure`, with
        ``Component=preview-publish-dispatcher``.

        :returns: IDs of the healthy instances in the group.
        :rtype: list[str]
        """
        target_group_arn = self.create_target_group(instance_name)

        # Create Dispatcher Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Attach Target Group to ASG
        self.attach_load_balancer_target_groups(asg_name, target_group_arn)
        # Modify each instance in ASG
        for instance in instances:
            # Register Dispatcher EC2 instance to Target Group
            self.elbv2_register_target(target_group_arn, instance)
            # Tag EC2 instance with Orchestration Completion Tag
            self.ec2_tagger_orchestration_completed(instance)
            # Tag EC2 instance with EC2 information
            self.ec2_tagger_ec2_information(
                instance, instance_name, stack_prefix, "preview-publish-dispatcher"
            )

        return instances

    def setup_author_primary_infrastructure(self, instance_name, stack_prefix):
        """Create the author-primary instance.

        The instance gets a data volume on ``/dev/sdc`` and is tagged with
        ``Component=author-primary`` and orchestration completed.

        :param instance_name: Value of the ``Name`` tag.
        :type instance_name: str
        :param stack_prefix: Value of the ``StackPrefix`` tag.
        :type stack_prefix: str
        :returns: ID of the instance.
        :rtype: str
        """
        # Create Author-Primary EC2 instance
        instance = self.create_instance(instance_name)

        # Attach Data Volume to each instance
        self.attach_sdc_volume(instance)
        # Tag EC2 instance with Orchestration Completion Tag
        self.ec2_tagger_orchestration_completed(instance)
        # Tag EC2 instance with EC2 information
        self.ec2_tagger_ec2_information(
            instance, instance_name, stack_prefix, "author-primary"
        )

        return instance

    def setup_author_standby_infrastructure(self, instance_name, stack_prefix):
        """Create the author-standby instance.

        The same as :meth:`setup_author_primary_infrastructure`, with
        ``Component=author-standby``.

        :returns: ID of the instance.
        :rtype: str
        """
        # Create Author-Standby EC2 instance
        instance = self.create_instance(instance_name)

        # Attach Data Volume to each instance
        self.attach_sdc_volume(instance)
        # Tag EC2 instance with Orchestration Completion Tag
        self.ec2_tagger_orchestration_completed(instance)
        # Tag EC2 instance with EC2 information
        self.ec2_tagger_ec2_information(
            instance, instance_name, stack_prefix, "author-standby"
        )

        return instance

    def setup_publish_infrastructure(
        self,
        asg_name,
        instance_name,
        desired_capacity,
        stack_prefix,
        publish_dispatcher_instance_list,
    ):
        """Create the publish ASG and instances.

        Each instance gets a data volume on ``/dev/sdc`` and is tagged with
        ``Component=publish``, orchestration completed and a
        ``PairInstanceId`` naming its dispatcher.

        :param asg_name: Name of the Auto Scaling group.
        :type asg_name: str
        :param instance_name: Value of the ``Name`` tag.
        :type instance_name: str
        :param desired_capacity: Number of instances.
        :type desired_capacity: int
        :param stack_prefix: Value of the ``StackPrefix`` tag.
        :type stack_prefix: str
        :param publish_dispatcher_instance_list: Dispatcher to pair with each
            instance, in order. Must have at least ``desired_capacity`` items.
        :type publish_dispatcher_instance_list: list[str]
        :returns: IDs of the healthy instances in the group.
        :rtype: list[str]
        """
        # Create Publish Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Modify each instance in ASG
        i = 0
        for instance in instances:
            # Attach Data Volume to each instance
            self.attach_sdc_volume(instance)
            # Tag EC2 instance with Orchestration Completion Tag
            self.ec2_tagger_orchestration_completed(instance)
            # Tag EC2 instance with EC2 information
            self.ec2_tagger_publish(
                instance,
                instance_name,
                stack_prefix,
                "publish",
                publish_dispatcher_instance_list[i],
            )
            i = i + 1

        return instances

    def setup_preview_publish_infrastructure(
        self,
        asg_name,
        instance_name,
        desired_capacity,
        stack_prefix,
        preview_publish_dispatcher_instance_list,
    ):
        """Create the preview-publish ASG and instances.

        The same as :meth:`setup_publish_infrastructure`, with
        ``Component=preview-publish`` and the dispatcher in a
        ``PreviewPairInstanceId`` tag.

        :returns: IDs of the healthy instances in the group.
        :rtype: list[str]
        """
        # Create Publish Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Modify each instance in ASG
        i = 0
        for instance in instances:
            # Attach Data Volume to each instance
            self.attach_sdc_volume(instance)
            # Tag EC2 instance with Orchestration Completion Tag
            self.ec2_tagger_orchestration_completed(instance)
            # Tag EC2 instance with EC2 information
            self.ec2_tagger_preview_publish(
                instance,
                instance_name,
                stack_prefix,
                "preview-publish",
                preview_publish_dispatcher_instance_list[i],
            )
            i = i + 1

        return instances

    # Auto Scaling Group helper
    def attach_load_balancer_target_groups(self, asg_name, target_group_arn):
        """
        Attach a target group to an Auto Scaling group.

        :param asg_name: Name of the Auto Scaling group.
        :type asg_name: str
        :param target_group_arn: ARN of the target group.
        :type target_group_arn: str
        :returns: The ``attach_load_balancer_target_groups`` response.
        :rtype: dict
        """
        # Creating ASG Connection
        client = self.asg_client()

        response = client.attach_load_balancer_target_groups(
            AutoScalingGroupName=asg_name,
            TargetGroupARNs=[
                target_group_arn,
            ],
        )
        return response

    def create_asg(self, asg_name, desired_capacity):
        """
        Create an Auto Scaling group across three availability zones.

        A launch configuration ``lc-<asg_name>`` is created first, using the
        first available AMI, an ``m5.xlarge`` instance type and 100 GB
        volumes on ``/dev/sdb`` and ``/dev/sdc``. ``MinSize`` and
        ``DesiredCapacity`` are ``desired_capacity`` and ``MaxSize`` is 3.

        :param asg_name: Name of the Auto Scaling group.
        :type asg_name: str
        :param desired_capacity: Number of instances.
        :type desired_capacity: int
        :returns: Description of the group, from
            ``describe_auto_scaling_groups``.
        :rtype: dict
        """
        aws_region = os.getenv("AWS_REGION")

        # Creating ASG Connection
        ec2_client = self.ec2_client()

        # Retreieve available AMI IDs
        image_response = ec2_client.describe_images()

        # Create ASG Connection
        asg_client = self.asg_client()

        # Create Launch configuration
        lc_config = {
            "LaunchConfigurationName": f"lc-{asg_name}",
            "ImageId": image_response["Images"][0]["ImageId"],
            "KeyName": "mock_key",
            "InstanceType": "m5.xlarge",
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/sdb",
                    "Ebs": {
                        "VolumeSize": 100,
                    },
                },
                {
                    "DeviceName": "/dev/sdc",
                    "Ebs": {
                        "VolumeSize": 100,
                    },
                },
            ],
        }
        responses = []
        responses.append(asg_client.create_launch_configuration(**lc_config))
        responses.append(
            asg_client.create_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                MinSize=desired_capacity,
                MaxSize=3,
                DesiredCapacity=desired_capacity,
                LaunchConfigurationName=f"lc-{asg_name}",
                AvailabilityZones=[
                    f"{aws_region}a",
                    f"{aws_region}b",
                    f"{aws_region}c",
                ],
            )
        )

        response = asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )

        return response["AutoScalingGroups"][0]

    # EC2 Helper
    def create_instance(self, instance_name):
        """
        Create an EC2 instance from the first available AMI.

        The instance has 100 GB volumes on ``/dev/sdb`` and ``/dev/sdc``.

        :param instance_name: Value of the ``Name`` tag.
        :type instance_name: str
        :returns: ID of the instance.
        :rtype: str
        """
        client = self.ec2_client()

        # Retreieve available AMI IDs
        image_response = client.describe_images()

        response = client.run_instances(
            ImageId=image_response["Images"][0]["ImageId"],
            MinCount=1,
            MaxCount=1,
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sdb",
                    "Ebs": {
                        "VolumeSize": 100,
                    },
                },
                {
                    "DeviceName": "/dev/sdc",
                    "Ebs": {
                        "VolumeSize": 100,
                    },
                },
            ],
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": instance_name,
                        }
                    ],
                }
            ],
        )

        return response["Instances"][0]["InstanceId"]

    def ec2_tagger_ec2_information(
        self, instance_id, instance_name, stack_prefix, component
    ):
        """
        Tag an EC2 instance with its name, stack prefix and component.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :param instance_name: Value of the ``Name`` tag.
        :type instance_name: str
        :param stack_prefix: Value of the ``StackPrefix`` tag.
        :type stack_prefix: str
        :param component: Value of the ``Component`` tag, e.g.
            ``author-dispatcher``.
        :type component: str
        :returns: The ``create_tags`` response for the last tag.
        :rtype: dict
        """
        tag_map = {
            "Name": instance_name,
            "StackPrefix": stack_prefix,
            "Component": component,
        }
        for key, value in tag_map.items():
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": key, "Value": value}]
            )

        return response

    def promote_author_standby_to_primary(self, instance_id, stack_prefix, component):
        """
        Retag an author-standby instance as promoted to author-primary.

        Sets the ``Name`` tag to ``AEM Author - Primary - Promoted from
        Standby``, which is how the offline snapshot Lambda finds promoted
        instances.

        :param instance_id: ID of the author-standby instance.
        :type instance_id: str
        :param stack_prefix: Value of the ``StackPrefix`` tag.
        :type stack_prefix: str
        :param component: Value of the ``Component`` tag, normally
            ``author-primary``.
        :type component: str
        :returns: The ``create_tags`` response for the last tag.
        :rtype: dict
        """
        tag_map = {
            "Name": "AEM Author - Primary - Promoted from Standby",
            "StackPrefix": stack_prefix,
            "Component": component,
        }
        for key, value in tag_map.items():
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": key, "Value": value}]
            )

        return response

    def ec2_tagger_publish(
        self,
        instance_id,
        instance_name,
        stack_prefix,
        component,
        publish_dispatcher_instance_id,
    ):
        """
        Tag a publish instance, including its paired dispatcher.

        Sets the same tags as :meth:`ec2_tagger_ec2_information`, plus
        ``PairInstanceId``.

        :param instance_id: ID of the publish instance.
        :type instance_id: str
        :param instance_name: Value of the ``Name`` tag.
        :type instance_name: str
        :param stack_prefix: Value of the ``StackPrefix`` tag.
        :type stack_prefix: str
        :param component: Value of the ``Component`` tag.
        :type component: str
        :param publish_dispatcher_instance_id: Value of the
            ``PairInstanceId`` tag.
        :type publish_dispatcher_instance_id: str
        :returns: The ``create_tags`` response for the last tag.
        :rtype: dict
        """
        tag_map = {
            "Name": instance_name,
            "StackPrefix": stack_prefix,
            "Component": component,
            "PairInstanceId": publish_dispatcher_instance_id,
        }
        for key, value in tag_map.items():
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": key, "Value": value}]
            )

        return response

    def ec2_tagger_preview_publish(
        self,
        instance_id,
        instance_name,
        stack_prefix,
        component,
        publish_dispatcher_instance_id,
    ):
        """
        Tag a preview publish instance, including its paired dispatcher.

        The same as :meth:`ec2_tagger_publish`, but the dispatcher goes in
        the ``PreviewPairInstanceId`` tag.

        :returns: The ``create_tags`` response for the last tag.
        :rtype: dict
        """
        tag_map = {
            "Name": instance_name,
            "StackPrefix": stack_prefix,
            "Component": component,
            "PreviewPairInstanceId": publish_dispatcher_instance_id,
        }
        for key, value in tag_map.items():
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": key, "Value": value}]
            )

        return response

    def ec2_tagger_orchestration_completed(self, instance_id):
        """
        Tag an EC2 instance as having finished orchestration.

        Sets ``ComponentInitStatus=Success``.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :returns: The ``create_tags`` response.
        :rtype: dict
        """
        response = self.ec2_client().create_tags(
            Resources=[instance_id],
            Tags=[{"Key": "ComponentInitStatus", "Value": "Success"}],
        )

        return response

    # ELB Helper
    def create_target_group(self, name):
        """
        Create an HTTPS (port 443) instance target group in the first VPC.

        :param name: Base name. The group is named ``<name>-tg``.
        :type name: str
        :returns: ARN of the target group.
        :rtype: str
        """
        # Creating ELBv2 Connection
        client = self.elb_client()

        vpc_id = self.get_vpc_id()

        response = client.create_target_group(
            Name=f"{name}-tg",
            Port=443,
            Protocol="HTTPS",
            VpcId=vpc_id,
            TargetType="instance",
        )

        return response["TargetGroups"][0]["TargetGroupArn"]

    def create_sm_service_iam_role(self):
        """
        Create the ``Mock-SM-Role`` IAM role used as the SSM service role.

        The trust policy is only a placeholder. moto does not check it.

        :returns: ARN of the role.
        :rtype: str
        """
        # placeholder policy document; moto does not validate it
        data = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }

        client = self.iam_client()
        # Create the Stack Manager service role
        response = client.create_role(
            AssumeRolePolicyDocument=json.dumps(data),
            Path="/",
            RoleName="Mock-SM-Role",
        )

        return response["Role"]["Arn"]

    # Network Helper
    def get_vpc_id(self):
        """
        Return the ID of the first VPC, normally the default VPC.

        :returns: ID of the VPC.
        :rtype: str
        """
        # Creating EC2 Connection
        client = self.ec2_client()
        vpcs = client.describe_vpcs()

        return vpcs["Vpcs"][0]["VpcId"]

    # Volume Helper
    def attach_sdc_volume(self, instance_id):
        """
        Attach a data volume to an EC2 instance on ``/dev/sdc``.

        The volume is created from the first snapshot found, in the
        instance's availability zone.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :returns: The instance information after the volume is attached, as
            returned by :meth:`get_ec2_info_by_id`.
        :rtype: dict
        """
        response = self.get_ec2_info_by_id(instance_id)
        availability_zone = response["AvailabilityZone"]

        # Get EC2 Snapshot to create the volume from
        snapshot_id = self.get_snapshot_id()

        # Creating Volume so we can attach it
        response = self.create_volume(snapshot_id, availability_zone)
        volume_id = response["VolumeId"]

        # Attach the volume to the instance
        response = self.attach_volume(instance_id, volume_id, "/dev/sdc")

        return self.get_ec2_info_by_id(instance_id)

    def get_snapshot_id(self):
        """
        Return the ID of the first EBS snapshot found.

        :returns: ID of the snapshot.
        :rtype: str
        """
        client = self.ec2_client()
        snapshot_ids = client.describe_snapshots()

        return snapshot_ids["Snapshots"][0]["SnapshotId"]

    def _gen_instances_id_list(self, asg_instances):
        """Return the IDs of the healthy instances of an Auto Scaling group.

        :param asg_instances: The ``Instances`` of an Auto Scaling group
            description.
        :type asg_instances: list[dict]
        :returns: IDs of the instances whose ``HealthStatus`` is ``Healthy``.
        :rtype: list[str]
        """
        instances_id = []

        for asg_instance in asg_instances:
            if asg_instance["HealthStatus"] == "Healthy":
                instances_id.append(asg_instance["InstanceId"])
        return instances_id

    def ec2_information(self, instance_id, tag_map):
        """Tag an EC2 instance with every key and value in a map.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :param tag_map: Tags to add, by key.
        :type tag_map: dict[str, str]
        :returns: The ``create_tags`` response for each tag.
        :rtype: list[dict]
        """
        response = []
        for key, value in tag_map.items():
            response.append(
                self.ec2_client().create_tags(
                    Resources=[instance_id], Tags=[{"Key": key, "Value": value}]
                )
            )

        return response

    def get_ec2_info_by_id(self, instance_id):
        """Return a summary of an EC2 instance.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :returns: The keys ``InstanceId``, ``Tags`` (dict of tag values by
            key), ``Name``, ``PrivateIpAddress``, ``Volumes`` (block device
            mappings without ``AttachTime``) and ``AvailabilityZone``.
        :rtype: dict
        """
        ec2_information = self._get_ec2_information_by_id(instance_id)

        tags = self._create_tag_mapping(ec2_information["Tags"])

        ec2_name = tags.get("Name", None)

        private_ip = ec2_information["PrivateIpAddress"]
        volumes = ec2_information["BlockDeviceMappings"]

        # Remove AttachTime from MetaData as it can't be converted
        # into string
        for volume in volumes:
            volume["Ebs"].pop("AttachTime", None)
        availability_zone = ec2_information["Placement"]["AvailabilityZone"]

        output = {
            "InstanceId": instance_id,
            "Tags": tags,
            "Name": ec2_name,
            "PrivateIpAddress": private_ip,
            "Volumes": volumes,
            "AvailabilityZone": availability_zone,
        }

        return output

    def _get_ec2_information_by_id(self, instance_id):
        """Return the ``describe_instances`` entry of an EC2 instance.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :rtype: dict
        """
        response = self.ec2_client().describe_instances(InstanceIds=[instance_id])

        return response["Reservations"][0]["Instances"][0]

    def _create_tag_mapping(self, tags):
        """Convert a list of EC2 ``Key``/``Value`` tags to a dict.

        :param tags: Tags as returned by the EC2 API.
        :type tags: list[dict]
        :returns: Tag values by key.
        :rtype: dict[str, str]
        """
        tag_mapping = {}
        for tag in tags:
            tag_mapping[tag["Key"]] = tag["Value"]

        return tag_mapping

    def create_volume(self, snapshot_id, availability_zone):
        """Create an EBS volume from a snapshot.

        :param snapshot_id: ID of the snapshot.
        :type snapshot_id: str
        :param availability_zone: Availability zone of the volume.
        :type availability_zone: str
        :returns: The ``create_volume`` response.
        :rtype: dict
        """
        response = self.ec2_client().create_volume(
            AvailabilityZone=availability_zone, SnapshotId=snapshot_id
        )

        return response

    def attach_volume(self, instance_id, volume_id, device_name):
        """Attach an EBS volume to an EC2 instance.

        :param instance_id: ID of the instance.
        :type instance_id: str
        :param volume_id: ID of the volume.
        :type volume_id: str
        :param device_name: Device name, e.g. ``/dev/sdc``.
        :type device_name: str
        :returns: The ``attach_volume`` response.
        :rtype: dict
        """
        response = self.ec2_client().attach_volume(
            Device=device_name, InstanceId=instance_id, VolumeId=volume_id
        )
        return response
