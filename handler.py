import os
import boto3
import json
import uuid
import datetime

DDB_TABLE = os.environ['DDB_TABLE']
TABLE_KEY = os.environ['TABLE_KEY']

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
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
        if operation != 'POST':
            return respond(ValueError('Unsupported method "{}"'.format(operation)))
            
        # Create a new disco ID.
        id = str(uuid.uuid1())
        i = {
            TABLE_KEY: id,
            'nodes': [],
            'created_at': str(datetime.datetime.utcnow())
        }
            
        # Store the new ID.
        dynamo.put_item(Item=i)
        return respond(None, i)

    # A resource has been supplied -- access it.
    id = resource['proxy']
    if operation == 'GET':
        i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)
        return respond(None, i.get('Item', {}))
    elif operation == 'POST':
        try:
            i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)['Item']
        except KeyError:
            return respond(None, {})
            
        # Get the address, and add it to the cluster.
        try:
            b = json.loads(event['body'])
        except:
            return respond(ValueError('bad request body'))
            
        # Decode the node details.
        try:
            addr = b['addr']
        except KeyError:
            return respond(ValueError('address not specified'))
                
        # All good, add to the list of nodes.
        key = {TABLE_KEY: i[TABLE_KEY]}
        dynamo.update_item(Key=key, UpdateExpression='SET nodes = list_append(nodes, :i)', ExpressionAttributeValues={':i': [b],})
        
        # Return the updated object.
        i = dynamo.get_item(Key={TABLE_KEY: id}, ConsistentRead=True)['Item']
        return respond(None, i)

    return respond(ValueError('Unsupported method "{}"'.format(operation)))
