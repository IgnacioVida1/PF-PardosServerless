import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

stepfunctions = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
steps_table = dynamodb.Table(os.environ['STEPS_TABLE'])

def cleanup_expired_tokens(event, context):
    """
    Función programada para limpiar tokens expirados
    """
    try:
        current_time = datetime.now().timestamp()
        
        # Buscar tokens expirados usando GSI (necesitarías crearlo)
        response = steps_table.scan(
            FilterExpression='ttl < :current_time AND attribute_exists(taskToken)',
            ExpressionAttributeValues={
                ':current_time': int(current_time)
            }
        )
        
        expired_count = 0
        
        for item in response.get('Items', []):
            try:
                # Notificar fallo a Step Functions
                stepfunctions.send_task_failure(
                    taskToken=item['taskToken'],
                    error='TokenExpired',
                    cause='El tiempo de espera para confirmación ha expirado'
                )
                
                # Marcar como expirado
                steps_table.update_item(
                    Key={
                        'PK': item['PK'],
                        'SK': item['SK']
                    },
                    UpdateExpression="SET #status = :status, expiredAt = :expiredAt",
                    ExpressionAttributeNames={
                        '#status': 'status'
                    },
                    ExpressionAttributeValues={
                        ':status': 'EXPIRED',
                        ':expiredAt': datetime.now().isoformat()
                    }
                )
                
                expired_count += 1
                
            except Exception as e:
                print(f"Error procesando token {item.get('taskToken', 'unknown')}: {str(e)}")
                continue
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Limpieza completada. {expired_count} tokens expirados procesados"
            })
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }
