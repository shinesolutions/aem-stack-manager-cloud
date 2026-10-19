# pylint: disable=C0301
# pylint: disable=C0302
# pylint: disable=R0904
# pylint: disable=R0913
# pylint: disable=R0914
# pylint: disable=R0915
# pylint: disable=R1703
# pylint: disable=W0703
"""
End-to-end tests of the :mod:`aem_offline_snapshot` state machine.

Each test builds a mocked stack with :class:`test_aws_helper.AwsHelper`
under ``moto``, sends the SNS task request to
:func:`aem_offline_snapshot.sns_message_processor`, and then drives the
process one step at a time. For each step it finds the command ID recorded
for the expected state in DynamoDB and sends a ``Success`` Run Command
notification for it. The last notification must return
``{"status": "Success"}``.
"""
import os
import unittest
from moto import mock_aws # pylint: disable=import-error
from test_aws_helper import AwsHelper
os.environ["AWS_REGION"] = "ap-southeast-2"

def aws_credentials():
    """Set dummy AWS credentials and region in the environment for ``moto``.

    Called at import time, before :mod:`aem_offline_snapshot` is imported,
    because that module creates its boto3 clients on import.
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

aws_credentials()

# Must be imported after aws_credentials() as the module reads AWS config at import time
from aem_offline_snapshot import sns_message_processor  # pylint: disable=wrong-import-position

class TestUtils(unittest.TestCase):
    """
    Offline snapshot tests against mocked AWS services.

    The tests cover both tasks (``offline-snapshot-full-set`` and
    ``offline-compaction-snapshot-full-set``) on four stack layouts: standard
    and preview publish, each with and without a promoted author-standby.
    """
    def aws_credentials(self):
        """Set dummy AWS credentials and region in the environment for ``moto``."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

    @mock_aws
    def test_offline_snapshot_standard_architecture(self):
        """Run ``offline-snapshot-full-set`` on a standard stack.

        Expected states: ``STOP_AUTHOR_STANDBY``, ``STOP_AUTHOR_PRIMARY``,
        ``STOP_PUBLISH``, ``OFFLINE_BACKUP``, ``START_AUTHOR_PRIMARY``,
        ``START_AUTHOR_STANDBY``, ``START_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm"
        os.environ["S3_BUCKET"] = "aoc"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")

        # Create Infrastructure
        AwsHelper().setup_stack_manager_infrastructure()
        AwsHelper().setup_standard_infrastructure()
        context = {}


        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }
        # event = {}

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Author Primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-standby
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        assert response == {"status": "Success"}

    @mock_aws
    def test_offline_snapshot_preview_architecture(self):
        """Run ``offline-snapshot-full-set`` on a stack with preview publish.

        Expected states: ``STOP_AUTHOR_STANDBY``, ``STOP_AUTHOR_PRIMARY``,
        ``STOP_PUBLISH``, ``STOP_PREVIEW_PUBLISH``, ``OFFLINE_BACKUP``,
        ``START_AUTHOR_PRIMARY``, ``START_AUTHOR_STANDBY``,
        ``START_PUBLISH``, ``START_PREVIEW_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs-preview"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm-preview"
        os.environ["S3_BUCKET"] = "aoc-preview"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")


        # Create Infrastructure
        AwsHelper().setup_stack_manager_preview_infrastructure()
        AwsHelper().setup_preview_infrastructure()


        context = {}

        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Author Primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Preview-Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-standby
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start Preview publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Offline Snapshot completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)
        assert response == {"status": "Success"}

    @mock_aws
    def test_offline_snapshot_standard_architecture_promoted_standby(self):
        """Run ``offline-snapshot-full-set`` on a standard stack with a promoted standby.

        The author-standby steps are skipped. Expected states:
        ``STOP_AUTHOR_PRIMARY``, ``STOP_PUBLISH``, ``OFFLINE_BACKUP``,
        ``START_AUTHOR_PRIMARY``, ``START_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs-promoted-author"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm-promoted-author"
        os.environ["S3_BUCKET"] = "aoc-preview"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")


        # Create Infrastructure
        AwsHelper().setup_stack_manager_infrastructure()
        AwsHelper().setup_standard_infrastructure_promoted_standby()


        context = {}

        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }

        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Preview-Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-standby
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start Preview publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        assert response == {"status": "Success"}

    @mock_aws
    def test_offline_snapshot_preview_architecture_promoted_standby(self):
        """Run ``offline-snapshot-full-set`` on a preview stack with a promoted standby.

        The author-standby steps are skipped. Expected states:
        ``STOP_AUTHOR_PRIMARY``, ``STOP_PUBLISH``, ``STOP_PREVIEW_PUBLISH``,
        ``OFFLINE_BACKUP``, ``START_AUTHOR_PRIMARY``, ``START_PUBLISH``,
        ``START_PREVIEW_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs-preview-promoted-standby"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm-preview-promoted-standby"
        os.environ["S3_BUCKET"] = "aoc-preview"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")


        # Create Infrastructure
        AwsHelper().setup_stack_manager_preview_infrastructure()
        AwsHelper().setup_preview_infrastructure_promoted_standby()


        context = {}

        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Preview-Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start Preview publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Offline Snapshot completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)
        assert response == {"status": "Success"}


    @mock_aws
    def test_offline_compaction_snapshot_standard_architecture(self):
        """Run ``offline-compaction-snapshot-full-set`` on a standard stack.

        Expected states: ``STOP_AUTHOR_STANDBY``, ``STOP_AUTHOR_PRIMARY``,
        ``STOP_PUBLISH``, ``OFFLINE_COMPACTION``, ``OFFLINE_BACKUP``,
        ``START_AUTHOR_PRIMARY``, ``START_AUTHOR_STANDBY``,
        ``START_PUBLISH``, then ``COMPACT_REMAINING_PUBLISHERS`` with the
        sub-states ``PUBLISH_READY``, ``STOP_PUBLISH``, ``COMPACT_PUBLISH``
        and ``START_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm"
        os.environ["S3_BUCKET"] = "aoc"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")

        # Create Infrastructure
        AwsHelper().setup_stack_manager_infrastructure()
        AwsHelper().setup_standard_infrastructure()
        context = {}


        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-compaction-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }
        # event = {}

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Author Primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_COMPACTION",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-standby
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # compact remaining publishers completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Stop remaining publisher for compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "PUBLISH_READY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Compact remaining publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Start publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Offline Snapshot Compaction completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        assert response == {"status": "Success"}

    @mock_aws
    def test_offline_compaction_snapshot_preview_architecture(self):
        """Run ``offline-compaction-snapshot-full-set`` on a stack with preview publish.

        Expected states: ``STOP_AUTHOR_STANDBY``, ``STOP_AUTHOR_PRIMARY``,
        ``STOP_PUBLISH``, ``STOP_PREVIEW_PUBLISH``, ``OFFLINE_COMPACTION``,
        ``OFFLINE_BACKUP``, ``START_AUTHOR_PRIMARY``,
        ``START_AUTHOR_STANDBY``, ``START_PUBLISH``,
        ``START_PREVIEW_PUBLISH``, then ``COMPACT_REMAINING_PUBLISHERS`` with
        the sub-states ``PUBLISH_PREVIEW_READY``, ``STOP_PUBLISH``,
        ``COMPACT_PUBLISH`` and ``START_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs-preview"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm-preview"
        os.environ["S3_BUCKET"] = "aoc-preview"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")


        # Create Infrastructure
        AwsHelper().setup_stack_manager_preview_infrastructure()
        AwsHelper().setup_preview_infrastructure()


        context = {}

        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-compaction-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Author Primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Preview-Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_COMPACTION",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-standby
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_STANDBY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start Preview publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Offline Snapshot completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # compact remaining publishers completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Stop remaining publisher for compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "PUBLISH_PREVIEW_READY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Compact remaining publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Start publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Offline Snapshot Compaction completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        assert response == {"status": "Success"}

    @mock_aws
    def test_offline_compaction_snapshot_standard_architecture_promoted_standby(self):
        """Run ``offline-compaction-snapshot-full-set`` on a standard stack with a promoted standby.

        The author-standby steps are skipped. Expected states:
        ``STOP_AUTHOR_PRIMARY``, ``STOP_PUBLISH``, ``OFFLINE_COMPACTION``,
        ``OFFLINE_BACKUP``, ``START_AUTHOR_PRIMARY``, ``START_PUBLISH``, then
        ``COMPACT_REMAINING_PUBLISHERS`` with the sub-states
        ``PUBLISH_READY``, ``STOP_PUBLISH``, ``COMPACT_PUBLISH`` and
        ``START_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs-promoted-author"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm-promoted-author"
        os.environ["S3_BUCKET"] = "aoc-preview"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")


        # Create Infrastructure
        AwsHelper().setup_stack_manager_infrastructure()
        AwsHelper().setup_standard_infrastructure_promoted_standby()


        context = {}

        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-compaction-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }

        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Preview-Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_COMPACTION",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-standby
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # compact remaining publishers
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Stop remaining publisher for compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "PUBLISH_READY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Compact remaining publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Start publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Offline Snapshot Compaction completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        assert response == {"status": "Success"}

    @mock_aws
    def test_offline_compaction_snapshot_preview_architecture_promoted_standby(self):
        """Run ``offline-compaction-snapshot-full-set`` on a preview stack with a promoted standby.

        The author-standby steps are skipped. Expected states:
        ``STOP_AUTHOR_PRIMARY``, ``STOP_PUBLISH``, ``STOP_PREVIEW_PUBLISH``,
        ``OFFLINE_COMPACTION``, ``OFFLINE_BACKUP``, ``START_AUTHOR_PRIMARY``,
        ``START_PUBLISH``, ``START_PREVIEW_PUBLISH``, then
        ``COMPACT_REMAINING_PUBLISHERS`` with the sub-states
        ``PUBLISH_PREVIEW_READY``, ``STOP_PUBLISH``, ``COMPACT_PUBLISH`` and
        ``START_PUBLISH``.
        """
        # Preparation
        self.aws_credentials()
        stack_prefix = "aoc-fs-preview-promoted-standby"
        os.environ["DDB_TABLE_NAME"] = "aoc-sm-preview-promoted-standby"
        os.environ["S3_BUCKET"] = "aoc-preview"
        os.environ["S3_PREFIX"] = "stack-manager"
        os.environ["RUNTIME_ENV"] = "dev"
        os.environ["AWS_REGION"] = "ap-southeast-2"
        os.environ["STACK_PREFIX"] = stack_prefix

        ddb_table_name = os.getenv("DDB_TABLE_NAME")


        # Create Infrastructure
        AwsHelper().setup_stack_manager_preview_infrastructure()
        AwsHelper().setup_preview_infrastructure_promoted_standby()


        context = {}

        # Event to start the offlien process
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "123333-c83b-56f6-9589-bd41a3a3329b",
                        "Message": "{'task': 'offline-compaction-snapshot-full-set','stack_prefix': '%s'}"
                        % stack_prefix,
                    }
                }
            ]
        }

        # Start Offline Snapshot process
        sns_message_processor(event, context)

        # Start stopping of Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start stopping of Preview-Publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start offline snapshot
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_COMPACTION",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start author-primary
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "OFFLINE_BACKUP",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_AUTHOR_PRIMARY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # Start Preview publish
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        sns_message_processor(event, context)

        # compact remaining publishers completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PREVIEW_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Stop remaining publisher for compaction
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "PUBLISH_PREVIEW_READY",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Compact remaining publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "STOP_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Start publisher
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)

        # Offline Snapshot Compaction completed
        scan_filter_dict = {
            "TableName": ddb_table_name,
            "Limit": 100,
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": "command_id",
            "ScanFilter": {
                "state": {
                    "AttributeValueList": [
                        {
                            "S": "COMPACT_REMAINING_PUBLISHERS",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                },
            "sub_state": {
                    "AttributeValueList": [
                        {
                            "S": "START_PUBLISH",
                        }
                    ],
                    "ComparisonOperator": "EQ",
                }
            },
        }
        response = AwsHelper().ddb_client().scan(**scan_filter_dict)
        cmd_id = response["Items"][0]["command_id"]["S"]
        event = {
            "Records": [
                {
                    "Sns": {
                        "MessageId": "fb7fac8c-62c8-52e7-9daa-f1ad09691eb4",
                        "Message": '{"commandId":"%s","eventTime":"2023-07-07T01:24:49.173Z","status":"Success"}'
                        % cmd_id,
                    }
                }
            ]
        }
        response = sns_message_processor(event, context)
        assert response == {"status": "Success"}
