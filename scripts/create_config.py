# -*- coding: utf8 -*-

"""
Create configuration file to be use by the AEM Stack Manager and upload to S3 Bucket
"""

import boto3
import json
import os
import sys
import logging

__author__ = 'Michael Bloch (michael.bloch@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'

if len(sys.argv) != 10:
    print('Error, Usage: {} stack_prefix stack_name s3_bucket s3_prefix statustopicarn ssmservicerolearn cmdoutputbucket backuparn dynamodbtable'.format(sys.argv[0]) )
    exit(1)
else:
    stack_prefix = sys.argv[1]
    stack_name = sys.argv[2]
    s3_bucket = sys.argv[3]
    s3_prefix = sys.argv[4]
    statustopicarn = sys.argv[5]
    ssmservicerolearn = sys.argv[6]
    cmdoutputbucket = sys.argv[7]
    backuparn = sys.argv[8]
    dynamodbtable = sys.argv[9]


# Set variables
tmp_file = '/tmp/templist.json'

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS resources
s3_client = boto3.resource('s3')
cloudformation_client = boto3.resource('cloudformation')
s3_bucket = s3_client.Bucket(s3_bucket) 


# Dict ec2 run commands
def dict_ec2_run_command(cmdoutputbucket, statustopicarn, ssmservicerolearn, dynamodbtable):
    ec2_run_command = {
            "ec2_run_command": {\
                    "cmd-output-bucket": cmdoutputbucket,\
                    "cmd-output-prefix": "SSMOutput",\
                    "status-topic-arn": statustopicarn,\
                    "ssm-service-role-arn": ssmservicerolearn,\
                    "dynamodb-table": dynamodbtable\
                    }}
    return ec2_run_command


# Dict Messenger -> Task Mapping
def dict_task_mapping():
    messenger_task_mapping = {
            "DeployArtifacts": "deploy-artifacts",
            "ManageService": "manage-service",
            "OfflineSnapshot": "offline-snapshot",
            "ExportPackage": "export-package",
            "DeployArtifact": "deploy-artifact",
            "OfflineCompaction": "offline-compaction",
            "PromoteAuthor": "promote-author",
            "ImportPackage": "import-package",
            'WaitUntilReady': "wait-until-ready",
            "EnableCrxde": "enable-crxde",
            "RunAdhocPuppet": "run-adhoc-puppet",
            "SSMStackName" : "StackName"
            }
    return messenger_task_mapping

# Dict Offline Snapshots
def dict_offline_snapshot(backuparn):
    offline_snapshot ={
            "offline_snapshot": {
                "min-publish-instances": 2,\
                "sns-topic-arn": backuparn
                }}
    return offline_snapshot

def logging_handler(log_text):
    logger.info('got event{}'.format(log_text))
    return 'Logging!'  

def aws_cf(stack_name, stack_prefix):
    try:
        stack_outputs = cloudformation_client.Stack(stack_name + '-' + stack_prefix).outputs
        return stack_outputs
    except Exception, e:
        responses = 'Error: Could not read Stack Description'
        status = "FAILED"
        print(responses)
        exit(1)
    
def messenger_mapping_list(stack_outputs, messenger_task_mapping):
    messenger_config_list = {
        messenger_task_mapping[output['OutputKey']]: output['OutputValue']
        for output in stack_outputs
    }
    messenger_dict = dict()
    messenger_dict['document_mapping'] = messenger_config_list
    return messenger_dict

def create_config(messenger_dict, offline_snapshot):
    # Update ec2_run_command dict
    ec2_run_command.update(messenger_dict)
    ec2_run_command.update(offline_snapshot)
    return ec2_run_command

def save_dict(tmp_file, messenger_dict):
        # Update ec2_run_command dict
    ec2_run_command.update(messenger_dict)
    ec2_run_command.update(offline_snapshot)
    try:
        with open(tmp_file, 'w') as file:
            json.dump( ec2_run_command, file, indent=2)
    except Exception, e:
        responses = 'Error: Could not create json config'
        print(responses)
        exit(1)

def s3_upload(tmp_file, stack_prefix):
    config_path = 'stack-manager/config.json'
    upload_path = stack_prefix + '/' + config_path
    s3_bucket.upload_file(tmp_file, upload_path)

def script_clean():
    os.remove(tmp_file)


# Execution
stack_outputs = aws_cf(stack_prefix, stack_name)

# Set Dicts
ec2_run_command = dict_ec2_run_command(cmdoutputbucket, statustopicarn, ssmservicerolearn, dynamodbtable)
messenger_task_mapping = dict_task_mapping()
offline_snapshot = dict_offline_snapshot(backuparn)

messenger_dict = messenger_mapping_list(stack_outputs, messenger_task_mapping)
create_config(messenger_dict, offline_snapshot)
save_dict(tmp_file, messenger_dict)

try:
    s3_upload(tmp_file, stack_prefix)
    script_clean()
    print("Succesfully Uploaded Config file")
except:
    responses = 'Error: Could not upload config.json'
    print(responses)
    exit(1)
