import json
import boto3
import uuid
from datetime import datetime
from shared.database import DynamoDB
from shared.events import EventBridge

stepfunctions = boto3.client('stepfunctions')
dynamodb = DynamoDB()
events = EventBridge()

def iniciar_orquestacion(event, context):
    try:
        detail = event['detail']
        order_id = detail.get('orderId')
        customer_id = detail.get('customerId')
        tenant_id = "pardos"
        
        print(f"Iniciando orquestacion para orden: {order_id}, cliente: {customer_id}")
        
        if not order_id:
            print("Error: orderId no encontrado en el evento")
            return {'status': 'ERROR', 'reason': 'orderId missing'}
        
        # Por ahora solo hacemos log del evento recibido
        print(f"Evento OrderReceived recibido: {json.dumps(detail)}")
        
        # Publicar evento de workflow iniciado (sin Event Bus específico)
        events.publish_event(
            source="pardos.orquestador",
            detail_type="WorkflowStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'RECEIVED',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Evento recibido exitosamente',
                'orderId': order_id,
                'currentStage': 'RECEIVED'
            })
        }
        
    except Exception as e:
        print(f"Error en orquestacion: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
