# -*- coding: utf-8 -*-
#
# crhelper.py
#
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##################################################################################################

from __future__ import print_function

import json
import logging
import threading

from botocore.vendored import requests


def log_config(event, loglevel=None, botolevel=None):
    if 'ResourceProperties' in event.keys():
        if 'loglevel' in event['ResourceProperties'] and not loglevel:
            loglevel = event['ResourceProperties']['loglevel']
        if 'botolevel' in event['ResourceProperties'] and not botolevel:
            loglevel = event['ResourceProperties']['botolevel']
    if not loglevel:
        loglevel = 'WARNING'
    if not botolevel:
        botolevel = 'ERROR'
    # Set log verbosity levels
    loglevel = getattr(logging, loglevel.upper(), 20)
    botolevel = getattr(logging, botolevel.upper(), 40)
    mainlogger = logging.getLogger()
    mainlogger.setLevel(loglevel)
    logging.getLogger('boto3').setLevel(botolevel)
    logging.getLogger('botocore').setLevel(botolevel)
    # Set log message format
    logfmt = '[%(requestid)s][%(asctime)s][%(levelname)s] %(message)s \n'
    mainlogger.handlers[0].setFormatter(logging.Formatter(logfmt))
    return logging.LoggerAdapter(mainlogger, {'requestid': event['RequestId']})


def send(event, context, response_status, response_data, physical_resource_id,
         logger, reason=None):
    response_url = event['ResponseURL']
    logger.debug(f"CFN response URL: {response_url}")

    response_body = {'Status': response_status}
    msg = f'See CloudWatch Log Stream: {context.log_stream_name}'
    response_body['Reason'] = str(reason)[:255] + f' ({msg})' if reason else msg
    response_body['PhysicalResourceId'] = physical_resource_id or 'NONE'
    response_body['StackId'] = event['StackId']
    response_body['RequestId'] = event['RequestId']
    response_body['LogicalResourceId'] = event['LogicalResourceId']
    if response_data and response_data != {} and response_data != [] and isinstance(response_data, dict):
        response_body['Data'] = response_data

    json_response_body = json.dumps(response_body)

    logger.debug(f"Response body:\n{json_response_body}")

    headers = {
        'content-type': '',
        'content-length': str(len(json_response_body))
    }

    try:
        response = requests.put(response_url,
                                data=json_response_body,
                                headers=headers)
        logger.info(f"CloudFormation returned status code: {response.reason}")
    except Exception as e:
        logger.error(f"send(..) failed executing requests.put(..): {e}")
        raise


# Function that executes just before lambda execution times out
def timeout(event, context, logger):
    logger.error("Execution is about to time out, sending failure message")
    send(event, context, "FAILED", {}, None, reason="Execution timed out",
         logger=logger)


# Handler function
def cfn_handler(event, context, create, update, delete, logger, init_failed):
    logger.info(
        f"Lambda RequestId: {context.aws_request_id} CloudFormation RequestId: {event['RequestId']}"
    )


    # Define an object to place any response information you would like to send
    # back to CloudFormation (these keys can then be used by Fn::GetAttr)
    response_data = {}

    # Define a physical_id for the resource, if the event is an update and the
    # returned physical_id changes, cloudformation will then issue a delete
    # against the old id
    physical_resource_id = None

    logger.debug(f"EVENT: {event}")
    # handle init failures
    if init_failed:
        send(event, context, "FAILED", response_data, physical_resource_id, logger, init_failed)
        raise Exception('FAILED')

    # Setup timer to catch timeouts
    t = threading.Timer((context.get_remaining_time_in_millis() / 1000.00) - 0.5,
                        timeout, args=[event, context, logger])
    t.start()

    try:
        # Execute custom resource handlers
        logger.info(f"Received a {event['RequestType']} Request")
        if event['RequestType'] == 'Create':
            physical_resource_id, response_data = create(event, context)
        elif event['RequestType'] == 'Update':
            physical_resource_id, response_data = update(event, context)
        elif event['RequestType'] == 'Delete':
            delete(event, context)

        # Send response back to CloudFormation
        logger.info("Completed successfully, sending response to cfn")
        send(event, context, "SUCCESS", response_data, physical_resource_id,
             logger=logger)

    except Exception as e:
        logger.error(e, exc_info=True)
        send(event, context, "FAILED", response_data, physical_resource_id, logger=logger, reason=e)
        raise Exception('FAILED')
    finally:
        # Cancel timer before exit
        t.cancel()
