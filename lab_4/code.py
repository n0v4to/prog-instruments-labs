######################################################################################################################
#  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License, Version 2.0 (the License). You may not use this file except in compliance    #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/LICENSE-2.0                                                                    #
#                                                                                                                    #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
#####################################################################################################################

import json
import os
import copy
import logging
import datetime
import traceback
from typing import Any, Dict, Union, Optional, List
from botocore.exceptions import ClientError
from botocore.client import Config

import boto3

MIN_TABLE_NAME_LENGTH = 4
INITIAL_LAST_BOTTOM = 1

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """
    AWS Lambda handler function to process events from SNS,
    extract Textract job information, and handle the processing request.

    Args:
        event (Dict[str, Any]): The event data containing SNS records.
        context (Any): The Lambda context object providing runtime information.
    """
    logger.info("Received event: %s", json.dumps(event))

    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        logger.info("Message: %s", message)

        request = {}

        request["job_id"] = message['job_id']
        request["jobTag"] = message['JobTag']
        request["jobStatus"] = message['Status']
        request["jobAPI"] = message['API']
        request["bucketName"] = message['DocumentLocation']['S3Bucket']
        request["objectName"] = message['DocumentLocation']['S3ObjectName']
        request["outputBucketName"] = os.environ['OUTPUT_BUCKET']

        logger.info("Full path of input file is %s/%s",
                    request["bucketName"], request["objectName"])

        try:
            process_request(request)
        except Exception as e:
            logger.error("Error processing request: %s", str(e))
            logger.debug(traceback.format_exc())


def get_job_results(api: str, job_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve the results of a Textract analysis job and parse them into a structured format.

    Args:
        api (str): The Textract API used for the job (e.g., "AnalyzeDocument").
        job_id (str): The ID of the Textract job to retrieve results for.

    Returns:
        List[Dict[str, Any]]: A list of parsed page contents with their page numbers.
    """
    logger.info("Getting job results for Job ID: %s using API: %s", job_id, api)
    text_tract_client = get_client('textract', 'us-east-1')
    blocks = []
    response = text_tract_client.get_document_analysis(
        JobId=job_id
    )
    analysis = copy.deepcopy(response)
    while True:
        for block in response["Blocks"]:
            blocks.append(block)
        if ("NextToken" not in response.keys()):
            break
        next_token = response["NextToken"]
        response = text_tract_client.get_document_analysis(
            JobId=job_id,
            NextToken=next_token
        )
    analysis.pop("NextToken", None)
    analysis["Blocks"] = blocks

    total_pages = response['DocumentMetadata']['Pages']
    final_json_all_page = []
    logger.info("Total pages found: %d", total_pages)

    for i in range(total_pages):
        this_page = i + 1
        this_page_json = parsejson_inorder_perpage(analysis, this_page)
        final_json_all_page.append({'Page': this_page, 'Content': this_page_json})
        logger.info("Page %d parsed", this_page)

    return final_json_all_page


def process_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a Textract job request, upload the results to S3,
    and save the metadata to DynamoDB.

    Args:
        request (Dict[str, Any]): A dictionary containing job details and S3 bucket information.

    Returns:
        Dict[str, Any]: A dictionary with the HTTP status code and the JSON response body.
    """
    s3_client = get_client('s3', 'us-east-1')

    logger.info("Processing request: %s", request)

    job_id = request['job_id']
    job_api = request['jobAPI']
    bucket_name = request['bucket_name']
    output_bucket_name = request['output_bucket_name']
    object_name = request['object_name']

    directory = object_name.split('/')

    upload_path = ''
    for subdirectory in directory:
        if subdirectory != directory[-1]:
            upload_path += (subdirectory + '/')

    file_name = directory[-1]

    file_name_no_ext = file_name.rsplit(".", 1)[0]

    upload_path = upload_path + file_name_no_ext + '/textract/'

    final_json_all_page = get_job_results(job_api, job_id)

    analyses_bucket_name = output_bucket_name
    analyses_bucket_key = "{}".format(object_name.replace('.PDF', '.json'))
    s3_client.put_object(
        Bucket=analyses_bucket_name,
        Key=upload_path + analyses_bucket_key,
        Body=json.dumps(final_json_all_page).encode('utf-8')
    )

    write_to_dynamo_db("pdf-to-json", object_name,
                     bucket_name + '/' + object_name, final_json_all_page)

    return {
        'statusCode': 200,
        'body': json.dumps(final_json_all_page)
    }


def find_value_block(key_block: Dict[str, Any], value_map: Dict[str, Any]) -> Dict[str, Any]:
    """
    Find the value block corresponding to a key block.

    Args:
        key_block (Dict[str, Any]): The block representing the key.
        value_map (Dict[str, Any]): A dictionary mapping block IDs to blocks.

    Returns:
        Dict[str, Any]: The value block corresponding to the key block.
    """
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result: Dict[str, Any], blocks_map: Dict[str, Any]) -> str:
    """
    Extract text content from a block result and its child blocks.

    Args:
        result (Dict[str, Any]): The block containing the parent result.
        blocks_map (Dict[str, Any]): A dictionary mapping block IDs to blocks.

    Returns:
        str: The concatenated text content of the block and its children.
    """
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '

    return text


def find_key_value_in_range(
    response: Dict[str, Any],
    top: float,
    bottom: float,
    this_page: int
) -> Dict[str, str]:
    """
    Find key-value pairs in a Textract response within a specified bounding box range.

    Args:
        response (Dict[str, Any]): The Textract response containing document blocks.
        top (float): The top boundary of the bounding box range.
        bottom (float): The bottom boundary of the bounding box range.
        this_page (int): The page number to filter blocks on.

    Returns:
        Dict[str, str]: A dictionary of key-value pairs within the specified range.
    """

    blocks = response['Blocks']
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        if block['Page'] == this_page:
            block_id = block['Id']
            block_map[block_id] = block
            if (block['BlockType'] == "KEY_VALUE_SET" or
                block['BlockType'] == 'KEY' or block['BlockType'] == 'VALUE'):
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block

    ## find key-value pair within given bounding box:
    key_value_pair = {}
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        if (value_block['Geometry']['BoundingBox']['Top'] >= top and
                value_block['Geometry']['BoundingBox']['Top'] +
                value_block['Geometry']['BoundingBox']['Height'] <=
                bottom):
            key_value_pair[key] = val
    return key_value_pair


def get_rows_columns_map(
    table_result: Dict[str, Any],
    blocks_map: Dict[str, Any]
) -> Dict[int, Dict[int, str]]:
    """
    Create a map of rows and columns from a Textract table block.

    Args:
        table_result (Dict[str, Any]): A table block containing relationships and cell data.
        blocks_map (Dict[str, Any]): A dictionary mapping block IDs to blocks.

    Returns:
        Dict[int, Dict[int, str]]: A nested dictionary where rows map to columns and their text content.
    """
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        rows[row_index] = {}
                    rows[row_index][col_index] = get_text(cell,
                                                          blocks_map)
    return rows


def write_to_dynamo_db(
    dd_table_name: str,
    id: str,
    full_file_path: str,
    full_pdf_json: Dict[str, Any]
) -> None:
    """
    Writes structured data to a DynamoDB table, creating the table if it doesn't exist.

    Args:
        dd_table_name (str): Name of the DynamoDB table.
        id (str): Unique identifier for the item.
        full_file_path (str): Full file path of the document.
        full_pdf_json (Dict[str, Any]): JSON data of the PDF to be stored in the database.

    Raises:
        Exception: If any error occurs during table creation or item insertion.
    """
    dynamodb = get_resource('dynamodb')

    dd_table_name = dd_table_name \
        .replace(" ", "-") \
        .replace("(", "-") \
        .replace(")", "-") \
        .replace("&", "-") \
        .replace(",", " ") \
        .replace(":", "-") \
        .replace('/', '--') \
        .replace("#", 'No') \
        .replace('"', 'Inch')

    if len(dd_table_name) <= MIN_TABLE_NAME_LENGTH:
        dd_table_name = dd_table_name + '-xxxx'

    print("DynamoDB table name is {}".format(dd_table_name))

    # Create the DynamoDB table.
    try:

        existing_tables = list([x.name for x in dynamodb.tables.all()])

        if dd_table_name not in existing_tables:
            table = dynamodb.create_table(
                TableName=dd_table_name,
                KeySchema=[
                    {
                        'AttributeName': 'Id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Id',
                        'AttributeType': 'S'
                    },
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            # Wait until the table exists, this will take a minute or so
            table.meta.client.get_waiter('table_exists').wait(TableName=dd_table_name)
            # Print out some data about the table.
            print("Table successfully created. Item count is: " +
                  str(table.item_count))
    except ClientError as e:
        if e.response['Error']['Code'] in ["ThrottlingException",
                                           "ProvisionedThroughputExceededException"]:
            msg = (f"DynamoDB ] Write Failed from DynamoDB, Throttling "
                   f"Exception [{e}] [{traceback.format_exc()}]")
            logging.warning(msg)
            raise e
        else:
            msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}] "
                   f"[{traceback.format_exc()}]")
            logging.error(msg)
            raise e

    except Exception as e:
        msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}] "
               f"[{traceback.format_exc()}]")
        logging.error(msg)
        raise Exception(e)

    table = dynamodb.Table(dd_table_name)

    try:
        table.put_item(Item=
        {
            'Id': id,
            'FilePath': full_file_path,
            'PdfJsonRegularFormat': str(full_pdf_json),
            'PdfJsonDynamoFormat': full_pdf_json,
            'DateTime': datetime.datetime.utcnow().isoformat(),
        }
        )
    except ClientError as e:
        if e.response['Error']['Code'] in ["ThrottlingException",
                                           "ProvisionedThroughputExceededException"]:
            msg = (f"DynamoDB ] Write Failed from DynamoDB, Throttling "
                   f"Exception [{e}] [{traceback.format_exc()}]")
            logging.warning(msg)
            raise e

        else:
            msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}] "
                   f"[{traceback.format_exc()}]")
            logging.error(msg)
            raise e

    except Exception as e:
        msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}] "
               f"[{traceback.format_exc()}]")
        logging.error(msg)
        raise Exception(e)