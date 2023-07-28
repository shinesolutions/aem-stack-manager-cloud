"""
    tests.aws_helper
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Class supports the other Test classes
"""
# pylint: disable=C0301
# pylint: disable=R0201
# pylint: disable=R0801
# pylint: disable=R0904
# pylint: disable=R0914
import os
import json

import boto3


class AwsHelper:
    """
    Test Helper Class.
    """

    def __init__(self):
        """
        Initiate Test Helper Class.
        :return new Test Helper Resources
        """

    # Boto3 Client Helpers
    def asg_client(self):
        """
        Create asg boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("autoscaling", region_name=aws_region)

    def ec2_client(self):
        """
        Create ec2 boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("ec2", region_name=aws_region)

    def elb_client(self):
        """
        Create elb boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("elbv2", region_name=aws_region)

    def iam_client(self):
        """
        Create iam boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("iam", region_name=aws_region)

    def ssm_client(self):
        """
        Create ssm boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("ssm", region_name=aws_region)

    def sns_client(self):
        """
        Create sns boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("sns", region_name=aws_region)

    def s3_client(self):
        """
        Create s3 boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("s3", region_name=aws_region)

    def ddb_client(self):
        """
        Create dynamodb boto3 client
        """
        aws_region = os.getenv("AWS_REGION")
        return boto3.client("dynamodb", region_name=aws_region)

    def create_ssm_document(self, document_name):
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
        response = self.s3_client().create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        return response

    def create_sns_topic(self, topic_name):
        response = self.sns_client().create_topic(Name=topic_name)

        return response["TopicArn"]

    def create_sm_config(
        self,
        s3_bucket_name,
        s3_key_prefix,
        sns_topic_arn,
        sm_service_iam_role_arn,
        ddb_table_name,
        ssm_document_name,
        min_preview_instances,
    ):
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
                "min-preview-publish-instances": min_preview_instances,
                "sns-topic-arn": sns_topic_arn,
            },
            "cw_stream_s3": {
                "s3-bucket-cw-stream": "overwrite-me",
                "s3-prefix-cw-stream": "overwrite-me",
            },
        }

        return config

    def upload_sm_config(
        self,
        s3_bucket_name,
        s3_key_prefix,
        sns_topic_arn,
        sm_service_iam_role_arn,
        ddb_table_name,
        ssm_document_name,
        min_preview_instances,
    ):
        sm_config = self.create_sm_config(
            s3_bucket_name,
            s3_key_prefix,
            sns_topic_arn,
            sm_service_iam_role_arn,
            ddb_table_name,
            ssm_document_name,
            min_preview_instances,
        )

        response = self.s3_client().put_object(
            Body=json.dumps(sm_config),
            Bucket=s3_bucket_name,
            Key=s3_key_prefix + "/config.json",
        )

        return response

    # Infrastructure helper
    def setup_stack_manager_infrastructure(self):
        s3_bucket_name = os.getenv("S3_BUCKET")
        s3_key_prefix = os.getenv("S3_PREFIX")
        s3_key_prefix = os.getenv("S3_PREFIX")
        ddb_table_name = os.getenv("DDB_TABLE_NAME")
        ssm_document_name = "aoc-sm-ssm-doc"
        response = self.create_s3_bucket(s3_bucket_name)

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
            0,
        )

    def setup_stack_manager_preview_infrastructure(self):
        s3_bucket_name = os.getenv("S3_BUCKET")
        s3_key_prefix = os.getenv("S3_PREFIX")
        ddb_table_name = os.getenv("DDB_TABLE_NAME")
        ssm_document_name = "aoc-sm--preview-ssm-doc"
        response = self.create_s3_bucket(s3_bucket_name)

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
            1,
        )

    def setup_standard_infrastructure(self):
        """
        Setup AWS infrastructure

        * 1 Author-Dispatcher Target Group
        * Author-Dispatcher ASG
            * 1 instances finished Orchestration & attached to Target Group

        * 1 Publish-Dispatcher Target Group
        * Publish-Dispatcher ASG
            * 2 instances finished Orchestration & attached to Target Group

        * Publish ASG
            * 2 instances finished Orchestration
        """
        # Preparation
        # Environment preparation
        stack_prefix = os.getenv("STACK_PREFIX")

        # Author-Dispatcher preparation
        author_dispatcher_instance_name = "author-dispatcher"
        author_dispatcher_asg_name = "%s-asg" % author_dispatcher_instance_name
        author_dispatcher_asg_desired_capacity = 1

        # Publish-Dispatcher preparation
        publish_dispatcher_instance_name = "publish-dispatcher"
        publish_dispatcher_asg_name = "%s-asg" % publish_dispatcher_instance_name
        publish_dispatcher_asg_desired_capacity = 2

        # Publish preparation
        publish_instance_name = "publish"
        publish_asg_name = "%s-asg" % publish_instance_name
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

        # Create Author-Primary
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
        standard_infrastructure = self.setup_standard_infrastructure()
        stack_prefix = os.getenv("STACK_PREFIX")
        responses = {}
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
        responses = self.setup_standard_infrastructure()
        stack_prefix = os.getenv("STACK_PREFIX")

        # preview-publish-Dispatcher preparation
        publish_dispatcher_instance_name = "preview-publish-dispatcher"
        publish_dispatcher_asg_name = "%s-asg" % publish_dispatcher_instance_name
        publish_dispatcher_asg_desired_capacity = 2

        # preview-publish preparation
        publish_instance_name = "preview-publish"
        publish_asg_name = "%s-asg" % publish_instance_name
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
        standard_infrastructure = self.setup_preview_infrastructure()
        stack_prefix = os.getenv("STACK_PREFIX")
        responses = {}
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
        target_group_arn = self.create_target_group(instance_name)

        # Create Dispatcher Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Attach Target Group to ASG
        self.attach_load_balancer_target_groups(asg_name, target_group_arn)
        i = 0
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

        response = self.asg_client().describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )

        return instances

    def setup_publish_dispatcher_infrastructure(
        self, asg_name, instance_name, desired_capacity, stack_prefix
    ):
        target_group_arn = self.create_target_group(instance_name)

        # Create Dispatcher Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Attach Target Group to ASG
        self.attach_load_balancer_target_groups(asg_name, target_group_arn)
        i = 0
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

        response = self.asg_client().describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )

        return instances

    def setup_preview_publish_dispatcher_infrastructure(
        self, asg_name, instance_name, desired_capacity, stack_prefix
    ):
        target_group_arn = self.create_target_group(instance_name)

        # Create Dispatcher Auto Scaling Group
        asg_response = self.create_asg(asg_name, desired_capacity)
        instances = self._gen_instances_id_list(asg_response["Instances"])

        # Attach Target Group to ASG
        self.attach_load_balancer_target_groups(asg_name, target_group_arn)
        i = 0
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

        response = self.asg_client().describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )

        return instances

    def setup_author_primary_infrastructure(self, instance_name, stack_prefix):
        # Create Publish Auto Scaling Group
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
        # Create Publish Auto Scaling Group
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
        Attach Target Group to Auto Scaling Group
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
        Create Auto Scaling Group
        """
        aws_region = os.getenv("AWS_REGION")

        # Creating ASG Connection
        ec2_client = self.ec2_client()

        # Retreieve available AMI IDs
        image_response = ec2_client.describe_images()

        # Create ASG Connection
        asg_client = self.asg_client()

        # Create Launch configuration
        lc_config = dict(
            LaunchConfigurationName="lc-%s" % asg_name,
            ImageId=image_response["Images"][0]["ImageId"],
            KeyName="mock_key",
            InstanceType="m5.xlarge",
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
        )
        responses = []
        responses.append(asg_client.create_launch_configuration(**lc_config))
        responses.append(
            asg_client.create_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                MinSize=desired_capacity,
                MaxSize=3,
                DesiredCapacity=desired_capacity,
                LaunchConfigurationName="lc-%s" % asg_name,
                AvailabilityZones=[
                    "%sa" % aws_region,
                    "%sb" % aws_region,
                    "%sc" % aws_region,
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
        Create EC2 Instance
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
        Tag EC2 information to an EC2 instance
        """
        tag_map = {
            "Name": instance_name,
            "StackPrefix": stack_prefix,
            "Component": component,
        }
        for tag in tag_map:
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": tag, "Value": tag_map[tag]}]
            )

        return response

    def promote_author_standby_to_primary(self, instance_id, stack_prefix, component):
        """
        Tag EC2 information to an EC2 instance
        """
        tag_map = {
            "Name": "AEM Author - Primary - Promoted from Standby",
            "StackPrefix": stack_prefix,
            "Component": component,
        }
        for tag in tag_map:
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": tag, "Value": tag_map[tag]}]
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
        Tag EC2 information to an EC2 instance
        """
        tag_map = {
            "Name": instance_name,
            "StackPrefix": stack_prefix,
            "Component": component,
            "PairInstanceId": publish_dispatcher_instance_id,
        }
        for tag in tag_map:
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": tag, "Value": tag_map[tag]}]
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
        Tag EC2 information to an EC2 instance
        """
        tag_map = {
            "Name": instance_name,
            "StackPrefix": stack_prefix,
            "Component": component,
            "PreviewPairInstanceId": publish_dispatcher_instance_id,
        }
        for tag in tag_map:
            response = self.ec2_client().create_tags(
                Resources=[instance_id], Tags=[{"Key": tag, "Value": tag_map[tag]}]
            )

        return response

    def ec2_tagger_orchestration_completed(self, instance_id):
        """
        Tag Orchestration Completed to an EC2 instance
        """
        response = self.ec2_client().create_tags(
            Resources=[instance_id],
            Tags=[{"Key": "ComponentInitStatus", "Value": "Success"}],
        )

        return response

    # ELB Helper
    def create_target_group(self, name):
        """
        Create Target Group
        """
        # Creating ELBv2 Connection
        client = self.elb_client()

        vpc_id = self.get_vpc_id()

        response = client.create_target_group(
            Name="%s-tg" % name,
            Port=443,
            Protocol="HTTPS",
            VpcId=vpc_id,
            TargetType="instance",
        )

        return response["TargetGroups"][0]["TargetGroupArn"]

    def create_sm_service_iam_role(self):
        """
        Create SM IAM Role
        """
        # IAM R53 role Policy
        data = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }

        client = self.iam_client()
        # Create IAM role to execute Step Functions
        response = client.create_role(
            AssumeRolePolicyDocument=json.dumps(data),
            Path="/",
            RoleName="Mock-SM-Role",
        )

        return response["Role"]["Arn"]

    # Network Helper
    def get_vpc_id(self):
        """
        Get VPC ID
        """
        # Creating EC2 Connection
        client = self.ec2_client()
        vpcs = client.describe_vpcs()

        return vpcs["Vpcs"][0]["VpcId"]

    # Volume Helper
    def attach_sdc_volume(self, instance_id):
        """
        Attach Data volume to EC2 instance
        """
        response = self.get_ec2_info_by_id(instance_id)
        availability_zone = response["AvailabilityZone"]

        # Get EC2 Snapshot to create the volume from
        snapshot_id = self.get_snapshot_id()

        # Creating Volume so we can attach it
        response = self.create_volume(snapshot_id, availability_zone)
        volume_id = response["VolumeId"]

        # Let EC2 Volume Mnager attach the volume
        response = self.attach_volume(instance_id, volume_id, "/dev/sdc")

        return self.get_ec2_info_by_id(instance_id)

    def create_volume(self, instance_id):
        """
        Create EC2 Volume
        """
        response = self.get_ec2_info_by_id(instance_id)
        availability_zone = response["AvailabilityZone"]

        # Get EC2 Snapshot to create the volume from
        snapshot_id = self.get_snapshot_id()
        response = self.create_volume(snapshot_id, availability_zone)

        return response["VolumeId"]

    def get_snapshot_id(self):
        """
        Get a Snapshot ID
        """
        client = self.ec2_client()
        snapshot_ids = client.describe_snapshots()

        return snapshot_ids["Snapshots"][0]["SnapshotId"]

    def _gen_instances_id_list(self, asg_instances):
        """Method to generate a list of EC2 instance ids
        based on the retrieved ASG instances
        :param asg_instances: Dict containing the retrieved ASG instances
        :return A list of ec2 instances in the Auto Scaling Group
        """
        instances_id = []

        for asg_instance in asg_instances:
            if asg_instance["HealthStatus"] == "Healthy":
                instances_id.append(asg_instance["InstanceId"])
        return instances_id

    def ec2_information(self, instance_id, tag_map):
        """Method to tag EC2 resource with values provided as map
        :param tag_map: Key/Value map of Tags to add to the EC2 instances
        :return List of Boto3 EC2 client create_tags responses
        """
        response = []
        for tag in tag_map:
            response.append(
                self.ec2.create_tags(instance_id, tag, tag_map[tag])
            )  # noqa

        return response

    def get_ec2_info_by_id(self, instance_id):
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
        response = self.ec2_client().describe_instances(InstanceIds=[instance_id])

        return response["Reservations"][0]["Instances"][0]

    def _create_tag_mapping(self, tags):
        tag_mapping = {}
        for tag in tags:
            tag_mapping[tag["Key"]] = tag["Value"]

        return tag_mapping

    def create_volume(self, snapshot_id, availability_zone):
        response = self.ec2_client().create_volume(
            AvailabilityZone=availability_zone, SnapshotId=snapshot_id
        )

        return response

    def attach_volume(self, instance_id, volume_id, device_name):
        response = self.ec2_client().attach_volume(
            Device=device_name, InstanceId=instance_id, VolumeId=volume_id
        )
        return response
