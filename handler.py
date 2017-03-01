from __future__ import print_function

import os
import boto3
import botocore
import json
import uuid
import datetime

DDB_TABLE = os.environ['DDB_TABLE']
TABLE_KEY = os.environ['TABLE_KEY']

def respondOK(res=None):
    return {
        'statusCode': '200',
        'body': json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def respondMethodNotAllowed(msg):
    return {
        'statusCode': '405',
        'body': json.dumps({'error': msg}),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def respondBadRequest(msg):
    return {
        'statusCode': '400',
        'body': json.dumps({'error': msg}),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def respondConflict(msg):
    return {
        'statusCode': '409',
        'body': json.dumps({'error': msg}),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def respondNotFound(msg):
    return {
        'statusCode': '404',
        'body': json.dumps({'error': msg}),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def serialize_item(i):
    try:
        nodes = i['nodes']
    except KeyError:
        nodes = []
    return {
        'created_at': i['created_at'],
        TABLE_KEY: i[TABLE_KEY],
        'nodes': [n for n in nodes]
    }

def lambda_handler(event, context):
    # Get the DDB table.
    dynamo = boto3.resource('dynamodb').Table(DDB_TABLE)
    
    # Get the key request parameters.
    operation = event['httpMethod']
    resource = event['pathParameters'] 
    queryStringParameters = event['queryStringParameters']
    
    # If there is no resource, then create a new disco ID.
    if resource is None:
        # Create a new disco ID.
        id = str(uuid.uuid1())
        i = {
            TABLE_KEY: id,
            'created_at': str(datetime.datetime.utcnow())
        }
            
        # Store the new ID.
        dynamo.put_item(Item=i)
        return respondOK(serialize_item(i))

    # A resource has been supplied -- access it.
    id = resource['proxy']
    try:
        i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)['Item']
    except KeyError:
        return respondNotFound('%s does not xx exist' % id)
        
    if operation == 'GET':
        return respondOK(serialize_item(i))
    elif operation == 'POST' or operation == 'DELETE':
        # Get the node address from the request.
        try:
            b = json.loads(event['body'])
        except:
            return respondBadRequest('bad request body')
            
        # Decode the node details.
        try:
            addr = b['addr']
        except KeyError:
            return respondBadRequest('address not specified')
                
        # All good, modify the list of nodes.
        key = {TABLE_KEY: i[TABLE_KEY]}
        
        if operation == 'POST':
            expr = 'add nodes :n'
        else:
            expr = 'delete nodes :n'
            
        dynamo.update_item(
            Key=key,
            UpdateExpression=expr,
            ExpressionAttributeValues={':n': set([addr])}
        )
        
        # Return the updated object.
        i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)['Item']
        return respondOK(serialize_item(i))

    return respondMethodNotAllowed('unsupported method "{}"'.format(operation))
