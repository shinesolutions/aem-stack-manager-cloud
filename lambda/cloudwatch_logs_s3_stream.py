# -*- coding: utf8 -*-

"""
Lambda function that archives CloudWatch Logs subscription data to S3.

Each invocation writes the ``awslogs.data`` payload unchanged to a new object
at ``<s3-prefix-cw-stream>/YYYY/MM/DD/<uuid4>`` in the configured bucket. The
payload stays base64-encoded and gzip-compressed.
"""

import os
import json
import tempfile
import logging
import uuid
from datetime import date
import boto3

#: Module logger. ``LOG_LEVEL`` may be a numeric level or a level name such
#: as ``DEBUG``. Defaults to ``logging.INFO``.
logger = logging.getLogger(__name__)
#: Raw value of the ``LOG_LEVEL`` environment variable.
log_level = os.getenv('LOG_LEVEL', str(logging.INFO))
logger.setLevel(int(log_level) if log_level.isdigit() else log_level.upper())

#: Date of the cold start. A warm Lambda container keeps using this date in
#: the S3 key until it is recycled.
today = date.today()
#: :data:`today` formatted as ``YYYY/MM/DD`` for use in S3 keys.
today_date = today.strftime("%Y/%m/%d")

#: S3 client, used for the config download and the log upload.
s3 = boto3.client('s3')

def handler(event, _ctx) -> None:
    """
    Lambda entry point: store a CloudWatch Logs subscription payload on S3.

    ``config.json`` is downloaded from ``s3://$S3_BUCKET/$S3_PREFIX/`` when both
    environment variables are set. Otherwise the copy bundled with the
    function is used. The destination comes from the ``cw_stream_s3`` section
    of that file:

    * ``s3-bucket-cw-stream`` -- destination bucket
    * ``s3-prefix-cw-stream`` -- key prefix

    :param event: CloudWatch Logs subscription event in the form
        ``{"awslogs": {"data": "<base64 gzip>"}}``.
    :type event: dict
    :param _ctx: Lambda context object. Not used.
    :type _ctx: object
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

    # read the S3 destination for the log data from the config file
    with open(config_file, encoding='utf-8') as f:
        content = ''.join(f.readlines()).replace('\n', '')
        logger.debug('config file: %s', content)
        config = json.loads(content)
        cw_stream_s3_config = config['cw_stream_s3']
        cw_stream_s3_bucket = cw_stream_s3_config['s3-bucket-cw-stream']
        cw_stream_s3_prefix = cw_stream_s3_config['s3-prefix-cw-stream']

    # a random object name stops concurrent invocations overwriting each other
    message_uuid = str(uuid.uuid4())

    # S3 key: <prefix>/YYYY/MM/DD/<uuid>
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
