import boto3
import json
import os

class EventBridge:
    def __init__(self):
        self.client = boto3.client('events')
    
    def publish_event(self, source, detail_type, detail):
        return self.client.put_events(
            Entries=[
                {
                    'Source': source,
                    'DetailType': detail_type,
                    'Detail': json.dumps(detail)
                }
            ]
        )
