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
            'nodes': [],
            'created_at': str(datetime.datetime.utcnow())
        }
            
        # Store the new ID.
        dynamo.put_item(Item=i)
        return respondOK(i)

    # A resource has been supplied -- access it.
    id = resource['proxy']
    if operation == 'GET':
        i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)
        return respondNotFound('%s does not exist' % id)
    elif operation == 'POST':
        try:
            i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)['Item']
        except KeyError:
            return respondNotFound('%s does not exist' % id)
            
        # Get the address, and add it to the cluster.
        try:
            b = json.loads(event['body'])
        except:
            return respondBadRequest('bad request body')
            
        # Decode the node details.
        try:
            addr = b['addr']
        except KeyError:
            return respondBadRequest('address not specified')
                
        # All good, add to the list of nodes.
        key = {TABLE_KEY: i[TABLE_KEY]}
        try:
            dynamo.update_item(
                Key=key,
                UpdateExpression='SET nodes = list_append(nodes, :i)',
                ConditionExpression='not contains (nodes, :ix)',
                ExpressionAttributeValues={':i': [addr], ':ix': addr}
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # Make reregistration of an address idempotent.
                pass
            else:
                raise e
        
        
        # Return the updated object.
        i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)['Item']
        return respondOK(i)

    return respondMethodNotAllowed('unsupported method "{}"'.format(operation))
