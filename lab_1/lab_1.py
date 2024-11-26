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
import urllib.parse
import time
import logging
import datetime
import traceback
from typing import Any, Dict, Union, Optional, List

import boto3
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError
from botocore.client import Config
from urllib.parse import unquote

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
    print("Received event: " + json.dumps(event))

    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        print("Message: {}".format(message))

        request = {}

        request["job_id"] = message['job_id']
        request["jobTag"] = message['JobTag']
        request["jobStatus"] = message['Status']
        request["jobAPI"] = message['API']
        request["bucketName"] = message['DocumentLocation']['S3Bucket']
        request["objectName"] = message['DocumentLocation']['S3ObjectName']
        request["outputBucketName"] = os.environ['OUTPUT_BUCKET']

        print("Full path of input file is {}/{}".format(
            request["bucketName"], request["objectName"]))

        process_request(request)


def get_job_results(api: str, job_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve the results of a Textract analysis job and parse them into a structured format.

    Args:
        api (str): The Textract API used for the job (e.g., "AnalyzeDocument").
        job_id (str): The ID of the Textract job to retrieve results for.

    Returns:
        List[Dict[str, Any]]: A list of parsed page contents with their page numbers.
    """
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
    print(total_pages)
    for i in range(total_pages):
        this_page = i + 1
        this_page_json = parsejson_inorder_perpage(analysis, this_page)
        final_json_all_page.append({'Page': this_page, 'Content': this_page_json})
        print(f"Page {this_page} parsed")

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

    print("Request : {}".format(request))

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


def get_tables_from_json_inrange(
    response: Dict[str, Any],
    top: float,
    bottom: float,
    this_page: int
) -> Optional[List[List[List[str]]]]:
    """
    Extract tables from a Textract response within a specified bounding box range.

    Args:
        response (Dict[str, Any]): The Textract response containing document blocks.
        top (float): The top boundary of the bounding box range.
        bottom (float): The bottom boundary of the bounding box range.
        this_page (int): The page number to filter blocks on.

    Returns:
        Optional[List[List[List[str]]]]: A list of tables, where each table is a list of rows,
                                         and each row is a list of cell text.
                                         Returns None if no tables are found.
    """
    # given respones and top/bottom corrdinate, return tables in the range
    blocks = response['Blocks']
    blocks_map = {}
    table_blocks = []
    for block in blocks:
        if block['Page'] == this_page:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                if (block['Geometry']['BoundingBox']['Top'] >= top and
                        block['Geometry']['BoundingBox']['Top'] +
                        block['Geometry']['BoundingBox']['Height'] <=
                        bottom):
                    table_blocks.append(block)

    if len(table_blocks) <= 0:
        return

    all_tables = []
    for table_result in table_blocks:
        table_matrix = []
        rows = get_rows_columns_map(table_result, blocks_map)
        for row_index, cols in rows.items():
            this_row = []
            for col_index, text in cols.items():
                this_row.append(text)
            table_matrix.append(this_row)
        all_tables.append(table_matrix)
    return all_tables


### get tables coordinate in range:
def get_tables_coord_in_range(
    response: Dict[str, Any],
    top: float,
    bottom: float,
    this_page: int
) -> Optional[List[Dict[str, float]]]:
    """
    Retrieve the bounding boxes of tables within a specified range in a Textract response.

    Args:
        response (Dict[str, Any]): The Textract response containing document blocks.
        top (float): The top boundary of the bounding box range.
        bottom (float): The bottom boundary of the bounding box range.
        this_page (int): The page number to filter blocks on.

    Returns:
        Optional[List[Dict[str, float]]]: A list of bounding boxes of tables within the range.
                                          Returns None if no tables are found.
    """
    blocks = response['Blocks']
    blocks_map = {}
    table_blocks = []
    for block in blocks:
        if block['Page'] == this_page:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                if (block['Geometry']['BoundingBox']['Top'] >= top and
                        block['Geometry']['BoundingBox']['Top'] +
                        block['Geometry']['BoundingBox']['Height'] <=
                        bottom):
                    table_blocks.append(block)

    if len(table_blocks) <= 0:
        return

    all_tables_coord = []
    for table_result in table_blocks:
        all_tables_coord.append(table_result['Geometry']['BoundingBox'])
    return all_tables_coord


def box_within_box(
    box1: Dict[str, float],
    box2: Dict[str, float]
) -> bool:
    """
    Check if one bounding box is completely within another.

    Args:
        box1 (Dict[str, float]): The inner bounding box with keys 'Width', 'Height', 'Left', 'Top'.
        box2 (Dict[str, float]): The outer bounding box with keys 'Width', 'Height', 'Left', 'Top'.

    Returns:
        bool: True if box1 is within box2, otherwise False.
    """
    if (box1['Top'] >= box2['Top'] and
            box1['Left'] >= box2['Left'] and
            box1['Top'] + box1['Height'] <= box2['Top'] +
            box2['Height'] and
            box1['Left'] + box1['Width'] <= box2['Left'] +
            box2['Width']):
        return True
    else:
        return False


def find_key_value_in_range_not_in_table(
    response: Dict[str, Any],
    top: float,
    bottom: float,
    this_page: int
) -> Dict[str, str]:
    """
    Find key-value pairs within a specified bounding box range that are not inside any table.

    Args:
        response (Dict[str, Any]): The Textract response containing document blocks.
        top (float): The top boundary of the bounding box range.
        bottom (float): The bottom boundary of the bounding box range.
        this_page (int): The page number to filter blocks on.

    Returns:
        Dict[str, str]: A dictionary of key-value pairs outside tables within the specified range.
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
                    block['BlockType'] == 'KEY' or
                    block['BlockType'] == 'VALUE'):
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block

    # get all table coordicates in range:
    all_tables_coord = get_tables_coord_in_range(response, top, bottom,
                                                 this_page)

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

            key_value_overlap_table_list = []
            if all_tables_coord is not None:
                for table_coord in all_tables_coord:
                    key_value_overlap_table_list.append(
                        box_within_box(value_block['Geometry']
                                       ['BoundingBox'], table_coord))
                if sum(key_value_overlap_table_list) == 0:
                    key_value_pair[key] = val
            else:
                key_value_pair[key] = val
    return key_value_pair

def parsejson_inorder_perpage(response: Dict[str, Any], this_page: int) -> List[Dict[str, Any]]:
    """
    Parses a Textract JSON response to extract information per page in a structured and ordered format.

    Args:
        response (Dict[str, Any]): The Textract response containing document blocks.
        this_page (int): The page number to process.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing parsed JSON data for the specified page.
    """
    id_list_key_value_table = []
    for block in response['Blocks']:
        if block['Page'] == this_page:
            if (block['BlockType'] == 'TABLE' or
                    block['BlockType'] == 'CELL' or
                    block['BlockType'] == 'KEY_VALUE_SET' or
                    block['BlockType'] == 'KEY' or
                    block['BlockType'] == 'VALUE' or
                    block['BlockType'] == 'SELECTION_ELEMENT'):

                key_value_id = block['Id']
                if key_value_id not in id_list_key_value_table:
                    id_list_key_value_table.append(key_value_id)

                child_id_list = []
                if 'Relationships' in block.keys():
                    for child in block['Relationships']:
                        child_id_list.append(child['Ids'])
                    flat_child_idlist = [
                        item for sublist in child_id_list for item in
                        sublist]
                    for childid in flat_child_idlist:
                        if childid not in id_list_key_value_table:
                            id_list_key_value_table.append(childid)
    text_list = []
    for block in response['Blocks']:
        if block['Page'] == this_page:
            if block['BlockType'] == 'LINE':

                thisline_idlist = []
                thisline_idlist.append(block['Id'])
                child_id_list = []
                if 'Relationships' in block.keys():
                    for child in block['Relationships']:
                        child_id_list.append(child['Ids'])
                    flat_child_idlist = [
                        item for sublist in child_id_list for item in
                        sublist]
                    for childid in flat_child_idlist:
                        thisline_idlist.append(childid)

                set_line_id = set(thisline_idlist)
                set_all_key_value_table_id = set(id_list_key_value_table)
                if len(set_line_id.intersection(set_all_key_value_table_id)) == 0:
                    this_dict = {'Line': block['Text'],
                                'Left': block['Geometry']['BoundingBox']['Left'],
                                'Top': block['Geometry']['BoundingBox']['Top'],
                                'Width': block['Geometry']['BoundingBox']['Width'],
                                'Height': block['Geometry']['BoundingBox']['Height']}
                    text_list.append(this_dict)

    final_json = []
    for i in range(len(text_list) - 1):
        this_text = text_list[i]['Line']
        this_top = text_list[i]['Top']
        this_bottom = text_list[i + 1]['Top'] + text_list[i + 1]['Height']
        this_text_kv = find_key_value_in_range(response, this_top,
                                               this_bottom, this_page)
        this_text_table = get_tables_from_json_inrange(response, this_top,
                                                      this_bottom, this_page)
        final_json.append({this_text: {'KeyValue': this_text_kv, 'Tables':
            this_text_table}})

    if (len(text_list) > 0):
        ## last line Text to bottom of page:
        last_text = text_list[len(text_list) - 1]['Line']
        last_top = text_list[len(text_list) - 1]['Top']
        last_bottom = INITIAL_LAST_BOTTOM
        this_text_kv = find_key_value_in_range(response, last_top,
                                               last_bottom, this_page)
        this_text_table = get_tables_from_json_inrange(response, last_top,
                                                      last_bottom, this_page)
        final_json.append({last_text: {'KeyValue': this_text_kv, 'Tables':
            this_text_table}})

    return final_json


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


def dict_to_item(raw: Union[Dict[str, Any], str, int]) -> Dict[str, Any]:
    """
    Converts a Python dictionary, string, or integer into a format suitable for DynamoDB `PutItem` requests.

    Args:
        raw (Union[Dict[str, Any], str, int]): Input data to be converted. Can be a dictionary, string, or integer.

    Returns:
        Dict[str, Any]: A DynamoDB-compatible dictionary structure.
    """
    if type(raw) is dict:
        resp = {}
        for k, v in raw.items():
            if type(v) is str:
                resp[k] = {
                    'S': v
                }
            elif type(v) is int:
                resp[k] = {
                    'I': str(v)
                }
            elif type(v) is dict:
                resp[k] = {
                    'M': dict_to_item(v)
                }
            elif type(v) is list:
                resp[k] = []
                for i in v:
                    resp[k].append(dict_to_item(i))

        return resp
    elif type(raw) is str:
        return {
            'S': raw
        }
    elif type(raw) is int:
        return {
            'I': str(raw)
        }


def get_client(name: str, aws_region: str = None) -> boto3.client:
    """
    Creates a Boto3 client for a specified AWS service.

    Args:
        name (str): Name of the AWS service (e.g., 's3', 'dynamodb').
        aws_region (str, optional): AWS region to configure the client. Defaults to None.

    Returns:
        boto3.client: A Boto3 client object for the specified AWS service.
    """
    config = Config(
        retries=dict(
            max_attempts=30
        )
    )
    if (aws_region):
        return boto3.client(name, region_name=aws_region, config=config)
    else:
        return boto3.client(name, config=config)


def get_resource(name: str, aws_region: str = None) -> boto3.resource:
    """
    Creates a Boto3 resource for a specified AWS service.

    Args:
        name (str): Name of the AWS service (e.g., 's3', 'dynamodb').
        aws_region (str, optional): AWS region to configure the resource. Defaults to None.

    Returns:
        boto3.resource: A Boto3 resource object for the specified AWS service.
    """
    config = Config(
        retries=dict(
            max_attempts=30
        )
    )

    if (aws_region):
        return boto3.resource(name, region_name=aws_region, config=config)
    else:
        return boto3.resource(name, config=config)