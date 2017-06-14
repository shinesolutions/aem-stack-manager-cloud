# -*- coding: utf8 -*-

"""
Add/Remove sharing permissions on documents created in the cloudformation template
"""

import sys
import boto3
import json

__author__ = 'Andy Wang (andy.wang@shinesolutions.com)'
__copyright__ = 'Shine Solutions'
__license__ = 'Apache License, Version 2.0'


def get_document_names(stack_name):
    stack_outputs = boto3.resource('cloudformation').Stack(stack_name).outputs

    # dumps the document to stand out for import as configuration
    config = {output['OutputKey']:output['OutputValue'] for output in stack_outputs}
    print(json.dumps(config, indent=2))

    # for use with stack manager messenger
    messenger_task_mapping = {
      "DeployArtifacts": "deploy-artifacts",
      "ManageService": "manage-service",
      "OfflineSnapshot": "offline-snapshot",
      "ExportPackage": "export-package",
      "DeployArtifact": "deploy-artifact",
      "OfflineCompaction": "offline-compaction",
      "PromoteAuthor": "promote-author",
      "ImportPackage": "import-package",
      'WaitUntilReady': "wait-until-ready"
    }

    sts = boto3.client('sts')
    aws_acct_id = sts.get_caller_identity().get('Account')
    region = sts.meta.region_name
    prefix = 'arn:aws:ssm:{}:{}:document'.format(
        region,
        aws_acct_id
    )
    messenger_config = {
        messenger_task_mapping[output['OutputKey']]: '{}/{}'.format(prefix, output['OutputValue'])
        for output in stack_outputs
    }
    print(json.dumps(messenger_config, indent=2))


    return [output['OutputValue'] for output in stack_outputs]


def authorize_documents(documents, acct_ids):
    ssm = boto3.client('ssm')
    for document in documents:
        ssm.modify_document_permission(
            Name=document,
            PermissionType='Share',
            AccountIdsToAdd=acct_ids
        )


def deauthorize_documents(documents, acct_ids):
    ssm = boto3.client('ssm')
    for document in documents:
        ssm.modify_document_permission(
            Name=document,
            PermissionType='Share',
            AccountIdsToRemove=acct_ids
        )


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Error, Usage: {} stack_name [add|remove] acct[, acct]'.format(sys.argv[0]))
        exit(1)

    stack_name = sys.argv[1]
    action_type = sys.argv[2]
    acct_ids = sys.argv[3:]

    documents = get_document_names(stack_name)

    print('{} sharing permissions on documents {} to accounts {}'.format(
        action_type,
        documents,
        acct_ids
    ))

    if action_type == 'add':
        authorize_documents(documents, acct_ids)
    elif action_type == 'remove':
        deauthorize_documents(documents, acct_ids)
