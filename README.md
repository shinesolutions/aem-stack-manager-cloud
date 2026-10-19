[![Build Status](https://github.com/shinesolutions/aem-stack-manager-cloud/workflows/CI/badge.svg)](https://github.com/shinesolutions/aem-stack-manager-cloud/actions?query=workflow%3ACI)
[![Known Vulnerabilities](https://snyk.io/test/github/shinesolutions/aem-stack-manager-cloud/badge.svg)](https://snyk.io/test/github/shinesolutions/aem-stack-manager-cloud)

# aem-stack-manager-cloud

AEM Stack Manager Cloud Native Implementation

This is part of Shine Solutions Open Source AEM Solution offerings.

## What is AEM Stack Manager

AEM Stack Manager provides the ability to do the following:

* deploy-artifact: deploy individual AEM package
* deploy-artifacts: deploy AEM packages based on a descriptor file
* export-package: exporting an AEM package based on a set of filter rules
* import-package: import a previously exported package
* offline-snapshot-full-set: take an EBS snapshot of AEM repository volume after stopping AEM service
* offline-compaction-snapshot-full-set: take an EBS snapshot after stopping AEM service and compacting the repository
* promote-author: promote a standby Author instance to be the primary.
* enable-crxde: enable crxde on selected instances.
* run-adhoc-puppet: run adhoc puppet code provided in a tar ball.

In addition, a scheduled AEM Snapshots Purge function is provided as a separate Lambda function, triggered by AWS CloudWatch Events.

For more information, please refer to [aem-stack-manager](https://github.com/shinesolutions/aem-stack-manager).

## What is AEM Stack Manager Cloud

Shine Solutions has a Java implementation of the AEM Stack Manager.
This cloud implementation uses cloud native technologies to do the same things.
It is made of Python Lambda functions that use Amazon SNS, AWS Systems Manager Run Command, DynamoDB, EC2 Auto Scaling, S3 and Amazon CloudWatch.

To maintain compatibility with the Java version, this cloud version uses the same SNS interface to invoke the functions.
There is a separate repo, [aem-stack-manager-messenger](https://github.com/shinesolutions/aem-stack-manager-messenger), for sending the SNS messages that trigger the tasks.

The sequence of events is:

`SNS -> Lambda -> Run Command -> Scripts/Puppet Manifests on instances`

DynamoDB is used to keep the state of the tasks.

*Snapshots Purge* does not rely on this SNS interface.

This repository contains the Lambda function code only.
The CloudFormation templates, SSM documents and Ansible playbooks that create the AWS resources were moved to [aem-aws-stack-builder](https://github.com/shinesolutions/aem-aws-stack-builder) in 2018.

## Lambda Functions

| Module | Handler | Purpose |
| ------ | ------- | ------- |
| `aem_stack_manager` | `sns_message_processor` | Runs the SSM document mapped to a task on the target instances, and records the task status in DynamoDB. |
| `aem_offline_snapshot` | `sns_message_processor` | Runs `offline-snapshot-full-set` and `offline-compaction-snapshot-full-set` as a multi-step process (see below). |
| `aem_task_query` | `handler` | Returns the latest status of a task by the `externalId` given when the task was triggered. |
| `purge_snapshots` | `handler` | Deletes AEM EBS snapshots older than a given age. |
| `cloudwatch_logs_s3_stream` | `handler` | Stores CloudWatch Logs subscription data on S3. |

### Offline Snapshot

An offline snapshot stops AEM, snapshots the repository volumes, and then starts AEM again, in a fixed order across the instances.
Each step runs as a Run Command, and the command's completion notification triggers the next step.
The current step is kept in DynamoDB.

* The author and publish dispatchers are put into Auto Scaling standby while the process runs, and taken out of standby when it ends.
* A lock in DynamoDB prevents two offline snapshots of the same stack running at the same time.
* `offline-compaction-snapshot-full-set` also compacts the AEM repositories before the snapshot.
  It then compacts the remaining publish instances one step at a time.
* Preview publish instances are included when the stack has them.
* If the author-standby has been promoted to author-primary, the author-standby steps are skipped.

### Snapshots Purge

The function takes the following input:

* `SnapshotType`: `live`, `offline` or `orchestration`
* `Age`: minimum age of the snapshots to delete, as a number followed by `h` (hours), `d` (days) or `w` (weeks), e.g. `2w`
* `StackPrefix` (optional): only purge the snapshots of this stack

Only completed snapshots with an `AemId` tag of `author` or `publish` are deleted.

## Configuration

The functions read their settings from a `config.json` file.
It is downloaded from `s3://<S3_BUCKET>/<S3_PREFIX>/config.json` when both environment variables are set.
Otherwise the `config.json` bundled with the function is used.
`aem_offline_snapshot` requires the S3 location, as it does not fall back to the bundled file.

| Section | Keys |
| ------- | ---- |
| `ec2_run_command` | `cmd-output-bucket`, `cmd-output-prefix`, `status-topic-arn`, `ssm-service-role-arn`, `dynamodb-table` |
| `document_mapping` | Task name to SSM document name, e.g. `"deploy-artifacts": "<document name>"` |
| `offline_snapshot` | `min-publish-instances`, `sns-topic-arn` |
| `cw_stream_s3` | `s3-bucket-cw-stream`, `s3-prefix-cw-stream` |

Environment variables:

* `S3_BUCKET`, `S3_PREFIX`: location of `config.json`
* `LOG_LEVEL`: Python logging level, defaults to `INFO`
* `AWS_REGION`: set by the Lambda runtime

## Usage

* To deploy the functions and the AWS resources they need, use [aem-aws-stack-builder](https://github.com/shinesolutions/aem-aws-stack-builder).
* To invoke the individual tasks, please refer to [aem-stack-manager-messenger](https://github.com/shinesolutions/aem-stack-manager-messenger).
  It is usually just like the following:

  ```bash
  make deploy-artifacts
  ```

* To wait for a task to finish, pass the name of the `aem_task_query` Lambda function and the task's `externalId` to `scripts/task_status_query.sh`.
  It polls the task status every 5 seconds for up to one hour, and needs the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and [jq](https://jqlang.org/).

  ```bash
  scripts/task_status_query.sh <function-name> <externalId>
  ```

`scripts/create-stack.sh`, `scripts/delete-stack.sh` and `scripts/manage-stack.sh` run Ansible playbooks from an `ansible/` directory that is no longer in this repository, so they do not work on their own.

## Development

Requirements:

* Python 3
* [ShellCheck](https://www.shellcheck.net/) and [markdownlint-cli2](https://github.com/DavidAnson/markdownlint-cli2), used by `make lint`
* [GitHub CLI](https://cli.github.com/), used by `make publish`

Makefile targets:

| Target | Description |
| ------ | ----------- |
| `make deps` | Create `.venv` and install the Python dependencies from `requirements.txt` |
| `make deps-upgrade` | Upgrade the pinned dependencies in `requirements.txt` from `requirements.in` |
| `make lint` | Run ShellCheck, Pylint, yamllint, actionlint, markdownlint and a JSON check |
| `make test` | Run the unit tests with pytest, against AWS services mocked by moto |
| `make package` | Create `stage/aem-stack-manager-cloud-<version>.zip` |
| `make ci` | Run `clean`, `deps`, `lint`, `test` and `package` |
| `make release` | Release a new version with [RTK](https://github.com/cliffano/rtk), updating `Makefile` and `CHANGELOG.md` |
| `make publish` | Create a GitHub release for the version and upload the package to it |

CI runs `make ci` on every push and pull request, inside the `shinesolutions/aem-platform-buildenv` Docker image.

## Dependencies

The EC2 instances are assumed to have the AWS Systems Manager (SSM) Agent installed and properly configured.
Please refer to [amazon_ssm_agent](https://github.com/shinesolutions/amazon_ssm_agent) for a simple, easy-to-use Puppet module that supports using a proxy.

## Going Forward

Two of the tasks, `offline-snapshot-full-set` and `offline-compaction-snapshot-full-set`, need several steps to happen in the right order and share state between them.
This is implemented with Lambda and DynamoDB because AWS Step Functions was not available in the Sydney region when this work started.
