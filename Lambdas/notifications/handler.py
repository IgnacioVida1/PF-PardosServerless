import json
import os
from datetime import datetime

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

try:
    from shared.database import DynamoDB
except ImportError:
    from Lambdas.shared.database import DynamoDB

# Inicialización lazy
dynamodb = None

def _get_dynamodb():
    global dynamodb
    if dynamodb is None:
        dynamodb = DynamoDB()
    return dynamodb

def send_order_notification(event, context):
    """
    Procesa eventos de cambios de estado de pedidos y envía notificaciones
    Triggered por EventBridge
    """
    try:
        # Extraer información del evento
        detail = event.get('detail', {})
        source = event.get('source', '')
        detail_type = event.get('detail-type', '')
        
        order_id = detail.get('orderId')
        tenant_id = detail.get('tenantId', 'pardos')
        customer_id = detail.get('customerId')
        stage = detail.get('stage', detail.get('step', ''))
        
        print(f"Procesando notificación: {detail_type} para pedido {order_id}")
        
        # Determinar el mensaje de notificación
        message = _get_notification_message(detail_type, stage)
        
        if message:
            # Guardar notificación en base de datos
            notification_record = {
                'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}",
                'SK': f"NOTIFICATION#{datetime.utcnow().isoformat()}",
                'orderId': order_id,
                'message': message,
                'type': detail_type,
                'stage': stage,
                'createdAt': datetime.utcnow().isoformat(),
                'read': False
            }
            
            # En una implementación real, aquí enviarías push notifications, SMS, email, etc.
            # Por ahora solo guardamos en DynamoDB
            _get_dynamodb().put_item(os.environ['NOTIFICATIONS_TABLE'], notification_record)
            
            print(f"Notificación guardada: {message}")
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Notification processed',
                'orderId': order_id,
                'stage': stage
            })
        }
        
    except Exception as e:
        print(f"Error en send_order_notification: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_customer_notifications(event, context):
    """
    Obtiene notificaciones para un cliente específico
    GET /notifications/{customerId}
    """
    try:
        customer_id = event['pathParameters']['customerId']
        tenant_id = 'pardos'
        
        response = _get_dynamodb().query(
            table_name=os.environ['NOTIFICATIONS_TABLE'],
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}",
                ':sk': 'NOTIFICATION#'
            }
        )
        
        notifications = response.get('Items', [])
        
        # Ordenar por fecha descendente
        notifications.sort(key=lambda x: x.get('createdAt', ''), reverse=True)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'notifications': notifications,
                'total': len(notifications)
            }, default=str)
        }
        
    except Exception as e:
        print(f"Error en get_customer_notifications: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def mark_notification_read(event, context):
    """
    Marca una notificación como leída
    PUT /notifications/{customerId}/{notificationId}/read
    """
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        customer_id = event['pathParameters']['customerId']
        notification_sk = body.get('notificationSK')
        tenant_id = 'pardos'
        
        _get_dynamodb().update_item(
            table_name=os.environ['NOTIFICATIONS_TABLE'],
            key={
                'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}",
                'SK': notification_sk
            },
            update_expression="SET #read = :read, readAt = :readAt",
            expression_names={'#read': 'read'},
            expression_values={
                ':read': True,
                ':readAt': datetime.utcnow().isoformat()
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Notification marked as read'})
        }
        
    except Exception as e:
        print(f"Error en mark_notification_read: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def _get_notification_message(detail_type, stage):
    """
    Genera mensaje de notificación basado en el tipo de evento
    """
    messages = {
        'OrderCreated': '¡Tu pedido ha sido confirmado! Comenzaremos a prepararlo pronto.',
        'OrderStageStarted': {
            'COOKING': ' ¡Estamos cocinando tu pedido! El aroma ya se siente.',
            'PACKAGING': 'Tu comida está lista, la estamos empacando cuidadosamente.',
            'DELIVERY': '¡Tu pedido está en camino! Nuestro repartidor ya salió.'
        },
        'StageStarted': {
            'COOKING': '¡Estamos cocinando tu pedido! El aroma ya se siente.',
            'PACKAGING': 'Tu comida está lista, la estamos empacando cuidadosamente.',
            'DELIVERY': '¡Tu pedido está en camino! Nuestro repartidor ya salió.'
        },
        'StageCompleted': {
            'COOKING': 'Tu pedido está listo para empacar.',
            'PACKAGING': 'Tu pedido está empacado y listo para delivery.',
            'DELIVERY': 'Tu pedido está en ruta.'
        },
        'OrderDelivered': '¡Tu pedido ha sido entregado! ¡Disfruta tu comida de Pardos!'
    }
    
    if detail_type in messages:
        if isinstance(messages[detail_type], dict):
            return messages[detail_type].get(stage, f'Tu pedido está en la etapa: {stage}')
        else:
            return messages[detail_type]
    
    return f'Actualización de tu pedido: {stage}'