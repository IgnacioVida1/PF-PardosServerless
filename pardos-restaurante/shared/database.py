import boto3
import os
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

class DynamoDB:
    def __init__(self):
        self.client = boto3.client('dynamodb')
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
    
    def put_item(self, table_name, item):
        serialized_item = {k: self.serializer.serialize(v) for k, v in item.items()}
        return self.client.put_item(
            TableName=os.environ[f"{table_name.upper()}_TABLE"],
            Item=serialized_item
        )
    
    def get_item(self, table_name, key):
        serialized_key = {k: self.serializer.serialize(v) for k, v in key.items()}
        response = self.client.get_item(
            TableName=os.environ[f"{table_name.upper()}_TABLE"],
            Key=serialized_key
        )
        return {k: self.deserializer.deserialize(v) for k, v in response.get('Item', {}).items()}
    
    def update_item(self, table_name, key, update_expression, expression_values, expression_names=None):
        serialized_key = {k: self.serializer.serialize(v) for k, v in key.items()}
        serialized_values = {k: self.serializer.serialize(v) for k, v in expression_values.items()}
        
        params = {
            'TableName': os.environ[f"{table_name.upper()}_TABLE"],
            'Key': serialized_key,
            'UpdateExpression': update_expression,
            'ExpressionAttributeValues': serialized_values
        }
        
        if expression_names:
            params['ExpressionAttributeNames'] = expression_names
            
        return self.client.update_item(**params)
    
    def query(self, table_name, key_condition_expression, expression_attribute_values, limit=None, scan_index_forward=None):
        """Query corregido con parámetros válidos"""
        
        # Serializar valores de expresión
        serialized_values = {k: self.serializer.serialize(v) for k, v in expression_attribute_values.items()}
        
        params = {
            'TableName': os.environ[f"{table_name.upper()}_TABLE"],
            'KeyConditionExpression': key_condition_expression,
            'ExpressionAttributeValues': serialized_values
        }
        
        if limit is not None:
            params['Limit'] = limit
            
        if scan_index_forward is not None:
            params['ScanIndexForward'] = scan_index_forward
        
        response = self.client.query(**params)
        
        # Deserializar items
        items = [ {k: self.deserializer.deserialize(v) for k, v in item.items()} for item in response.get('Items', []) ]
        
        return {
            'Items': items,
            'Count': response.get('Count', 0)
        }
