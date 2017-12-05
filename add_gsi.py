""" This module is a Lambda Function used to create multiple
Global Secondary Indexes on a DynamoDB Table
"""

import logging
import boto3
import os
import time
import math
import json
import urllib

# parse logging level from environment variable 'logging_level' if set
LOGGING_LEVEL = os.environ.get("logging_level", "")
log_level = getattr(logging, LOGGING_LEVEL, logging.NOTSET)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
# quite down the botocore debug output because it can be quite noisy and distracting
logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

# gather the environment variables
TABLE_NAME = os.environ.get("TABLE_NAME")
GSI_1 = os.environ.get("GSI_1")
GSI_2 = os.environ.get("GSI_2")

# instantiate a dynamodb client
client = boto3.client('dynamodb')


def check_table_status(tablename: str):
    """ Check to see if a dynamo table is in an ACTIVE state'

    Dynamo tables are updated async, and an update needs to be completed
    before another update can be started otherwise an exception is raised.

    Args:
        tablename: name of the table to check status

    Returns:
        table_active: [bool] if the table is in an active state
    """
    response = client.describe_table(
        TableName=tablename
    )
    try:
        table_status = response['Table']['TableStatus']
        if table_status == 'ACTIVE':
            table_active = True
        else:
            table_active = False
    except:
        logger.debug('Table Status not returned')
        table_active = True
    logger.debug(table_status)
    return table_active


def check_gsi_status(tablename: str):
    """ Check to see that all GSIs on a dynamo table are in the ACTIVE state

    Args:
        tablename: name of the table to check status

    Returns:
        gsis_active: [bool] if the table is in an active state
    """
    response = client.describe_table(
        TableName=tablename
    )
    try:
        if "GlobalSecondaryIndexes" in response['Table']:
            GSIs = [n['IndexStatus'] for n in response['Table']['GlobalSecondaryIndexes']]
            if all([n == 'ACTIVE' for n in GSIs]):
                gsi_active = True
            else:
                gsi_active = False
        else:
            logger.info('No GSIs present, continuing...')
            gsi_active = True
    except:
        gsi_active = True
        logger.debug('Failed to get GSIs')
    return gsi_active

	
def table_active_wait(table_name: str, wait_seconds: int=15):
    """Wait for table and its GSIs to be active

    Poll dynamo table status and wait for it to have a state of ACTIVE.
    The wait time between iterations is an increasing backoff.

    Args:
        table_name: name of the dynamo table for which we want a status check
        wait_seconds: number of seconds to wait in between pollings

    Returns: N/A
    """
    table_active = False
    retry = 0

    while not table_active:
        if retry > wait_seconds:
            retry = wait_seconds
        exp_wait = math.floor(wait_seconds ** (retry / wait_seconds))
        logger.debug(f"Table [{table_name}] not active, waiting [{exp_wait}] seconds to poll again")
        time.sleep(exp_wait)
        table_active = check_table_status(table_name)
        logger.debug(table_active)
        retry += 1
    logger.debug(f"Table [{table_name}] is active")

    # check that the GSIs are all active
    gsi_active = False
    retry = 0
    while not gsi_active:
        if retry > wait_seconds:
            retry = wait_seconds
        exp_wait = math.floor(wait_seconds ** (retry / wait_seconds))
        logger.debug(f"Table [{table_name}] GSIs not active, waiting [{exp_wait}] seconds to poll again")
        time.sleep(exp_wait)
        gsi_active = check_gsi_status(table_name)
        logger.debug(gsi_active)
        retry += 1
    logger.debug(f"Table [{table_name}] GSIs are active")
    return


def function_retry(fn,):
    """Function retry wrapper
		
		This is used as a decorator to retry a function a set number of times with a set wait period between
		executions.

    Args:
        fn: wrapped function
        num_retries: number of times to retry function
        wait_seconds: number of seconds to wait in between function calls

    Returns:
        wrapper: The wrapped function
    """

    def wrapper(context, *args, num_retries=5, wait_seconds=30, **kwargs):
        retry = 0
        while retry <= num_retries:
            if context.get_remaining_time_in_millis() <= 10000:
                raise Exception('Function was about to timeout')
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logger.debug(f"Retrying function [{fn.__name__}] after waiting [{wait_seconds}] seconds.")
                logger.debug(f"Retries remaining: [{num_retries - retry}]")
                logger.debug(f"Retry caused by: {e}")
                if 'already exists' in str(e):
                    logger.info(f'GSI Already created for {fn.__name__}')
                    return
                time.sleep(wait_seconds)
                retry += 1
                if retry > num_retries:
                    logger.info(f"{fn.__name__} failed after {num_retries} retries")
    return wrapper


@function_retry
def create_gsi_1():
    """Create GSI 1

    Creates the Global Secondary Indexes for GSI 1
    in DynamoDB.

    Returns:
        N/A
    """
    try:
        logger.info("ADDING GSI 1")
        response = client.update_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'primary',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'gsikey',
                    'AttributeType': 'N'
                },
            ],
            TableName=TABLE_NAME,
            GlobalSecondaryIndexUpdates=[{
                'Create': {
                              'IndexName': GSI_1,
                              'KeySchema': [
                                  {
                                      'AttributeName': 'gsikey',
                                      'KeyType': 'HASH'
                                  },
                              ],
                              'Projection': {
                                  'ProjectionType': 'INCLUDE',
                                  'NonKeyAttributes': [
                                      'primary'
                                  ]
                              },
                              'ProvisionedThroughput': {
                                  'ReadCapacityUnits': 5,
                                  'WriteCapacityUnits': 5
                              }
                          }
            }
            ]
        )
        logger.info(f"GSI 1 added!")
    except Exception as e:
        raise e
        logger.debug(f"Failed to add GSI 1\n{e}")


@function_retry
def create_gsi_2():
    """Create GSI 2

    Creates the Global Secondary Indexes for GSI 2
    in DynamoDB with sort key.

    Returns:
        N/A
    """
    try:
        logger.info("ADDING GSI 2")
        response = client.update_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'primary',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'gsikey',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'gsisortkey',
                    'AttributeType': 'N'
                },
            ],
            TableName=TABLE_NAME,
            GlobalSecondaryIndexUpdates=[{
                'Create': {
                    'IndexName': GSI_2,
                    'KeySchema': [
                        {
                            'AttributeName': 'gsikey',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'gsisortkey',
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': [
                            'primary'
                        ]
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            }
            ]
        )
        logger.info(f"GSI 2 added!")
    except Exception as e:
        raise e
        logger.debug(f"Failed to add GSI 2\n{e}")


def send_response(context, event, status: str='SUCCESS', reason: str=None):
    """ Send status response to Cloud Formation
		
		Send a status message to Cloud Formation to let it know if the lambda function completed successfully

    Args:
        context: Lambda context object
        event: Lambda event object
        status: 'SUCCESS' or 'FAILURE' to discribe the state
        reason: reason for failure

    Returns:
        Sends a message to the pre-signed URL to indicate the state of the lambda execution
    """

    if not reason:
        reason = f"See the details in CloudWatch Log Stream: {context.log_stream_name}"
    response_body = json.dumps(
        {
            'Status': status,
            'Reason': reason,
            'PhysicalResourceId': context.log_stream_name,
            'StackId': event['StackId'],
            'RequestId': event['RequestId'],
            'LogicalResourceId': event['LogicalResourceId']
        }
    )
    encoded_body = response_body.encode()
    logger.info(f"Response: {response_body}")
    request = urllib.request.Request(url=event['ResponseURL'],
                                     method='PUT',
                                     data=encoded_body,
                                     headers={
                                         "content-type": "",
                                         "content-length": len(encoded_body)
                                     }
                                     )
    logger.info(f'Response Request: {request.__dict__}')
    logger.info('Sending status response...')
    try:
        urllib.request.urlopen(request)
        logger.info('Status response sent!')
    except Exception as e:
        logger.exception(f'Failed to send status response:\n{e}')


def lambda_handler(event, context):
    """Lambda Function to Update Table with GSIs

    Update specified (via environment variables) table with GSIs

    Args:
        event: Lambda Event object
        context: Lambda Context object

    Returns:
        N/A
    """
    logger.debug(f'EVENT: {event}')
    logger.debug(f'CONTEXT: {context.__dict__}')

    # GSI 1
    table_active_wait(TABLE_NAME)
    logger.info(f'Creating GSI 1 on {TABLE_NAME}')
    create_gsi_1(context)

    # GSI 2
    table_active_wait(TABLE_NAME)
    logger.info(f'Creating GSI 2 on {TABLE_NAME}')
    create_gsi_2(context)


    # send success message
    table_active_wait(TABLE_NAME)
    try:
        send_response(context, event, status='SUCCESS')
    except:
        logging.error('Failed to send the success message')
        send_response(context, event, status='FAILURE')
