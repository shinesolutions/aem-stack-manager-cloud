# -*- coding: utf8 -*-

"""
Lambda function to store cloudwatch log streams on S3
"""

import os
import json
import tempfile
import logging
import uuid
from datetime import date
import boto3

# setting up logger
logger = logging.getLogger(__name__)
log_level = os.getenv('LOG_LEVEL', str(logging.INFO))
logger.setLevel(int(log_level) if log_level.isdigit() else log_level.upper())

# Set todays date
today = date.today()
today_date = today.strftime("%Y/%m/%d")

# Create s3 client connection
s3 = boto3.client('s3')

def handler(event, _ctx) -> None:
    """
    Lambda entry point: store a CloudWatch Logs subscription payload on S3.

    :param event: CloudWatch Logs event containing the 'awslogs' data
    :param _ctx: Lambda context (unused)
    """
    # reading in config info from either s3 or within bundle
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')

    if bucket is not None and prefix is not None:
        config_file = '/tmp/config.json'
        s3.download_file(bucket, f'{prefix}/config.json', config_file)
    else:
        logger.info('Unable to locate config.json in S3, searching within bundle')
        config_file = 'config.json'

    # Read configuration file
    with open(config_file, encoding='utf-8') as f:
        content = ''.join(f.readlines()).replace('\n', '')
        logger.debug('config file: %s', content)
        config = json.loads(content)
        cw_stream_s3_config = config['cw_stream_s3']
        cw_stream_s3_bucket = cw_stream_s3_config['s3-bucket-cw-stream']
        cw_stream_s3_prefix = cw_stream_s3_config['s3-prefix-cw-stream']

    # Generate random uuid
    message_uuid = str(uuid.uuid4())

    # Set s3 destionation path
    s3_destination = cw_stream_s3_prefix + '/' + today_date + '/' + message_uuid

    awslogs = event['awslogs']
    awslogs_data = awslogs['data']

    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file_name = tmp_file.name
        with open(tmp_file_name, 'w', encoding='utf-8') as file:
            file.write(awslogs_data)

        s3.upload_file(
            Bucket = cw_stream_s3_bucket,
            Filename=tmp_file_name,
            Key=s3_destination
        )
