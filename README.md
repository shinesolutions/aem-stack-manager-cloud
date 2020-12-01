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

In addition, scheduled AEM Snapshots Purge function is also provided in a separate Lambada function, which uses AWS CloudWatch Events to trigger the execution. It provides a sensible default to start with.

 For more information, please refer to:  [aem-stack-manager](https://github.com/shinesolutions/aem-stack-manager)

## What is AEM Stack Manager Cloud
 Shine Solutions has a Java implementation of the AEM Stack Manger. This (cloud )
 implementation use cloud native technologies to do the same things. The AWS services used in this implementation includes Lambda, EC2 Run Command, DynamoDB, AWS CloudWatch. Python is used as the language for the Lambda functions.

 To maintain compatibility with the Java version, this cloud version uses the same SNS interface to invoke the functions. There is a separate repo: [aem-stack-manager-messenger](https://github.com/shinesolutions/aem-stack-manager-messenger) for sending the SNS messages that trigger the tasks.

 the sequence of events:
 `SNS -> Lambda -> EC2 Run Command -> Scripts/Puppet Manifests on instances`
DynamoDB is used to keep the state of the Tasks.

*Snapshots Purge* does not reply on this SNS interface.

## How to Get Start
Under *cloudformation*, it has the CloudFormation template used to create the resources: the SSM Documents, Lambda Functions, SNS Topics, DynamoDB, and necessary IAM Roles. Please take note of the Stack Manager Topic name, Backup Topic name, as those will be used with *AEM Stack Manager Messenger*; they have the form of AemStackManger*version*, AemOfflineBackup*version*. Please also take note of the task status query Lambda Function name if you plan passing in an identifier when invoking a function, and use it to query the status of the task. It is usually in the form: AemTaskQuery*version*. *version* is a parameter in the Cloud Formation template.

Similarly CloudFormation Template for *Snapshots Purge* resources can also be found under *cloudformation*.

*Ansible* is used to orchestrate the creation of the stack, such as zip up the Python code and upload them to S3, and provide the parameters used in the CloudFormation template.

Under *scripts*, `generate.sh` is used to create the CloudFormation Template for creating the SSM Documents from a set of include files, `manage-stack.sh` enlist *Ansible* to create the CloudFormation Stacks, and `task_status_query.sh` query the task status by using AWS CLI. `manage_document_permisson.py` help sharing the SSM documents to other accounts, while `output_task_doc_mapping.py` generate a AEM Stack Manager task to SSM document name mapping, to be used with AEM Stack Manager Messenger and configure the Lambda Functions.

### Installation

* Run `make deps` to install [AWS CLI](http://docs.aws.amazon.com/cli/latest/userguide/installing.html), [Ansible](http://docs.ansible.com/ansible/intro_installation.html), and [Boto 3](https://boto3.readthedocs.io/en/latest/).
* [Configure](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-quick-configuration) AWS CLI.

### Usage
* To create the Lambda functions, DynamoDB, and other resources:


    make create-stack-manager-cloud [config_path=path]


  A sample yaml config file can be found under
    `ansible/group_vars/aem-stack-manager-cloud.yaml`

* To create Snapthos Purge related resources:

    make creaet-snapshots-purge-cloud [config_path=path]

* To invoke the individual tasks, please refer to [aem-stack-manager-messenger](https://github.com/shinesolutions/aem-stack-manager-messenger). It is usually just like the following:

  `make deploy-artifacts`

## Dependencies

The EC2 instances are assumed to have EC2 System Manager Agent installed and properly configured. Please refer to [amazon_ssm_agent](https://github.com/shinesolutions/amazon_ssm_agent) for a simple, easy-to-use Puppet module that supports using a proxy.

## Going Forward
Lambda function is stateless, while two of the tasks, `offline-snapshot-full-set`, `offline-compaction-snapshot-full-set` requires a few things happen in the right order and share state information between the steps. Using a combination of Lambda and DynamoDB can work, but is a less optimal choice due to *AWS Step Function* is not available in Sydney region when this work started.

A Step Function implementation is planed and this cloud implementation will switch to that once it is available in Sydney region.
