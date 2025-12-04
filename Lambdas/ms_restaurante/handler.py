import json
import uuid
import os
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

try:
    from shared.database import DynamoDB
    from shared.events import EventBridge
except ImportError:
    from Lambdas.shared.database import DynamoDB
    from Lambdas.shared.events import EventBridge

STAGE_CONFIRMATION_TIMEOUT = int(os.environ.get('STAGE_CONFIRMATION_TIMEOUT', 86400))
MAX_DELIVERY_CAPACITY = 5  # Máximo 5 entregas simultáneas

# Inicialización lazy: No crear globales en import time
dynamodb = None
events = None
sqs_client = None

def _get_dynamodb():
    global dynamodb
    if dynamodb is None:
        dynamodb = DynamoDB()
    return dynamodb

def _get_events():
    global events
    if events is None:
        events = EventBridge()
    return events

def _get_sqs():
    global sqs_client
    if sqs_client is None:
        import boto3
        sqs_client = boto3.client('sqs')
    return sqs_client

def wait_stage_confirmation(event, context):
    """
    Espera confirmación manual de una etapa usando Task Token
    """
    try:
        task_token = event.get('taskToken')
        order_id = event['orderId']
        tenant_id = event['tenantId']
        stage = event['stage']
        
        if not task_token:
            raise ValueError("Task Token es requerido")
        
        # Guardar el token en DynamoDB para confirmación posterior
        current_time = datetime.now()
        expiration_time = current_time + timedelta(seconds=STAGE_CONFIRMATION_TIMEOUT)
        
        steps_table.put_item(Item={
            'PK': f'ORDER#{order_id}',
            'SK': f'TOKEN#{stage}',
            'taskToken': task_token,
            'orderId': order_id,
            'tenantId': tenant_id,
            'stage': stage,
            'status': 'PENDING_CONFIRMATION',
            'createdAt': current_time.isoformat(),
            'expiresAt': expiration_time.isoformat(),
            'ttl': int(expiration_time.timestamp())
        })
        
        # Publicar evento de espera de confirmación
        eventbridge = _get_events()
        eventbridge.put_events(
            Entries=[
                {
                    'Source': 'pardos.stepfunctions',
                    'DetailType': 'StageConfirmationPending',
                    'EventBusName': os.environ['EVENT_BUS_NAME'],
                    'Detail': json.dumps({
                        'orderId': order_id,
                        'stage': stage,
                        'tenantId': tenant_id,
                        'timestamp': current_time.isoformat(),
                        'timeout': STAGE_CONFIRMATION_TIMEOUT
                    })
                }
            ]
        )
        
        return {
            "status": "WAITING",
            "message": f"Esperando confirmación para etapa {stage}",
            "orderId": order_id,
            "stage": stage,
            "timeout": STAGE_CONFIRMATION_TIMEOUT
        }
        
    except Exception as e:
        # Si hay error, notificar a Step Functions
        if 'taskToken' in event:
            stepfunctions.send_task_failure(
                taskToken=event['taskToken'],
                error=str(type(e).__name__),
                cause=str(e)
            )
        raise e

def wait_delivery_capacity(event, context):
    """
    Espera capacidad disponible para delivery usando Task Token
    """
    try:
        task_token = event.get('taskToken')
        order_id = event['orderId']
        tenant_id = event['tenantId']
        
        if not task_token:
            raise ValueError("Task Token es requerido")
        
        # Verificar capacidad actual en la cola
        queue_attributes = sqs.get_queue_attributes(
            QueueUrl=delivery_queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        
        messages_in_flight = int(queue_attributes['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
        
        # Si hay capacidad disponible
        if messages_in_flight < MAX_DELIVERY_CAPACITY:
            # Reservar un slot
            message_id = str(uuid.uuid4())
            sqs.send_message(
                QueueUrl=delivery_queue_url,
                MessageBody=json.dumps({
                    'orderId': order_id,
                    'tenantId': tenant_id,
                    'action': 'RESERVE_DELIVERY_SLOT',
                    'timestamp': datetime.now().isoformat()
                }),
                MessageGroupId='delivery-capacity',
                MessageDeduplicationId=message_id
            )
            
            # Notificar éxito inmediato
            stepfunctions.send_task_success(
                taskToken=task_token,
                output=json.dumps({
                    "canProceed": True,
                    "message": "Capacidad disponible para delivery",
                    "queuePosition": messages_in_flight + 1,
                    "reservedAt": datetime.now().isoformat()
                })
            )
            
            return {
                "status": "CAPACITY_AVAILABLE",
                "canProceed": True
            }
        else:
            # Guardar token para notificación cuando haya capacidad
            current_time = datetime.now()
            expiration_time = current_time + timedelta(hours=1)
            
            steps_table.put_item(Item={
                'PK': f'ORDER#{order_id}',
                'SK': f'DELIVERY_CAPACITY_TOKEN',
                'taskToken': task_token,
                'orderId': order_id,
                'tenantId': tenant_id,
                'status': 'WAITING_CAPACITY',
                'createdAt': current_time.isoformat(),
                'expiresAt': expiration_time.isoformat(),
                'ttl': int(expiration_time.timestamp())
            })
            
            # Enviar heartbeat para mantener vivo el token
            stepfunctions.send_task_heartbeat(taskToken=task_token)
            
            return {
                "status": "WAITING_CAPACITY",
                "canProceed": False,
                "message": f"Esperando capacidad. {messages_in_flight}/{MAX_DELIVERY_CAPACITY} slots ocupados",
                "queuePosition": messages_in_flight + 1
            }
            
    except Exception as e:
        if 'taskToken' in event:
            stepfunctions.send_task_failure(
                taskToken=event['taskToken'],
                error=str(type(e).__name__),
                cause=str(e)
            )
        raise e

def process_cooking(event, context):
    try:
        order_id = event.get('detail', {}).get('orderId') or event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        _update_step(order_id, tenant_id, 'COOKING', 'IN_PROGRESS')
        return {'orderId': order_id, 'tenantId': tenant_id, 'stage': 'COOKING'}
    except Exception as e:
        print(f"Error en process_cooking: {str(e)}")
        raise

def process_packaging(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        _update_step(order_id, tenant_id, 'PACKAGING', 'IN_PROGRESS')
        return {'orderId': order_id, 'tenantId': tenant_id, 'stage': 'PACKAGING'}
    except Exception as e:
        print(f"Error en process_packaging: {str(e)}")
        raise

def process_delivery(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        _update_step(order_id, tenant_id, 'DELIVERY', 'IN_PROGRESS')
        return {'orderId': order_id,  'tenantId': tenant_id,'stage': 'DELIVERY'}
    except Exception as e:
        print(f"Error en process_delivery: {str(e)}")
        raise

def process_delivered(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        timestamp = datetime.utcnow().isoformat()
        
        print(f"🎯 Procesando entrega para orden {order_id}")
        
        # 1. Actualizar el estado del pedido a DELIVERED
        _get_dynamodb().update_item(
            table_name=os.environ['ORDERS_TABLE'],
            key={
                'PK': pk,
                'SK': 'INFO'
            },
            update_expression="SET currentStep = :step, #s = :status, updatedAt = :now",
            expression_names={'#s': 'status'},
            expression_values={
                ':step': 'DELIVERED',
                ':status': 'COMPLETED',
                ':now': timestamp
            }
        )
        
        # 2. Registrar etapa DELIVERED
        step_record = {
            'PK': pk,
            'SK': f"STEP#DELIVERED#{timestamp}",
            'stepName': 'DELIVERED',
            'status': 'DONE',
            'startedAt': timestamp,
            'finishedAt': timestamp,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        _get_dynamodb().put_item(os.environ['STEPS_TABLE'], step_record)
        
        # 3. Publicar evento
        _get_events().publish_event(
            source="pardos.etapas",
            detail_type="OrderDelivered",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': 'DELIVERED',
                'timestamp': timestamp
            }
        )
        
        # 4. REMOVER DE LA COLA SQS - VERSIÓN MEJORADA
        print(f"🔄 Intentando remover orden {order_id} de SQS")
        sqs = _get_sqs()
        queue_url = os.environ['DELIVERY_QUEUE_URL']
        
        removed = False
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                print(f"   Intento {attempt + 1} de {max_attempts}")
                
                # Buscar mensajes que contengan este orderId
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    VisibilityTimeout=10,
                    WaitTimeSeconds=0,
                    MessageAttributeNames=['All']
                )
                
                if 'Messages' in response:
                    for message in response['Messages']:
                        try:
                            message_body = json.loads(message['Body'])
                            print(f"   Mensaje encontrado: {message_body.get('orderId')}")
                            
                            if message_body.get('orderId') == order_id:
                                # Eliminar el mensaje de la cola
                                sqs.delete_message(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                                print(f"✅✅✅ MENSAJE REMOVIDO de SQS para orden {order_id}")
                                removed = True
                                break
                        except json.JSONDecodeError:
                            print(f"   ⚠️ Mensaje con formato inválido: {message['Body'][:50]}...")
                            continue
                
                if removed:
                    break
                    
            except Exception as e:
                print(f"   ⚠️ Error en intento {attempt + 1}: {str(e)}")
                continue
        
        if not removed:
            print(f"⚠️ No se pudo encontrar/remover mensaje para orden {order_id} en SQS")
            print(f"   Esto puede ser normal si el mensaje ya fue procesado o expiró")
        
        # 5. También verificar si hay mensajes viejos que limpiar
        try:
            clean_old_messages_from_queue(sqs, queue_url)
        except Exception as e:
            print(f"⚠️ Error en limpieza adicional: {str(e)}")
        
        return {
            'orderId': order_id, 
            'tenantId': tenant_id, 
            'stage': 'DELIVERED',
            'sqsRemoved': removed
        }
        
    except Exception as e:
        print(f"❌ Error en process_delivered: {str(e)}")
        raise

def clean_old_messages_from_queue(sqs, queue_url, max_age_minutes=30):
    """
    Limpia mensajes antiguos de la cola
    """
    try:
        print("🧹 Limpiando mensajes antiguos de SQS...")
        
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=5,
            WaitTimeSeconds=0
        )
        
        if 'Messages' not in response:
            print("   No hay mensajes para limpiar")
            return 0
        
        cleaned = 0
        current_time = datetime.utcnow()
        
        for message in response['Messages']:
            try:
                message_body = json.loads(message['Body'])
                added_time_str = message_body.get('addedToQueueAt')
                
                if added_time_str:
                    added_time = datetime.fromisoformat(added_time_str.replace('Z', '+00:00'))
                    age_minutes = (current_time - added_time).total_seconds() / 60
                    
                    if age_minutes > max_age_minutes:
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        cleaned += 1
                        print(f"   🗑️ Mensaje antiguo removido ({age_minutes:.1f} minutos)")
                        
            except Exception as e:
                print(f"   ⚠️ Error procesando mensaje: {str(e)}")
                continue
        
        print(f"   Total limpiado: {cleaned} mensajes antiguos")
        return cleaned
        
    except Exception as e:
        print(f"⚠️ Error en clean_old_messages_from_queue: {str(e)}")
        return 0

# ==============================================
# NUEVAS FUNCIONES PARA STEP FUNCTIONS MEJORADO
# ==============================================

def release_delivery_capacity(event, context):
    """
    Liberar capacidad de delivery cuando un pedido se completa
    """
    try:
        order_id = event['pathParameters']['orderId']
        
        # Eliminar mensaje de la cola (liberar capacidad)
        sqs.purge_queue(QueueUrl=delivery_queue_url)
        
        # Buscar tokens de capacidad pendientes
        response = steps_table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
            ExpressionAttributeValues={
                ':pk': f'ORDER#{order_id}',
                ':sk': 'DELIVERY_CAPACITY_TOKEN'
            }
        )
        
        # Notificar a todos los tokens pendientes
        for item in response.get('Items', []):
            try:
                stepfunctions.send_task_success(
                    taskToken=item['taskToken'],
                    output=json.dumps({
                        "capacityReleased": True,
                        "orderId": order_id,
                        "releasedAt": datetime.now().isoformat()
                    })
                )
                
                # Eliminar el token
                steps_table.delete_item(
                    Key={
                        'PK': item['PK'],
                        'SK': item['SK']
                    }
                )
            except:
                pass  # Token podría ya haber expirado
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Capacidad liberada exitosamente",
                "orderId": order_id
            })
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }
def confirm_stage(event, context):
    """
    Endpoint HTTP para confirmar etapa manualmente
    """
    try:
        order_id = event['pathParameters']['orderId']
        body = json.loads(event['body'])
        stage = body['stage']
        confirmed_by = body.get('confirmedBy', 'unknown')
        
        # Buscar el token en DynamoDB
        response = steps_table.get_item(
            Key={
                'PK': f'ORDER#{order_id}',
                'SK': f'TOKEN#{stage}'
            }
        )
        
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "error": "Token no encontrado o expirado",
                    "orderId": order_id,
                    "stage": stage
                })
            }
        
        item = response['Item']
        task_token = item['taskToken']
        
        # Enviar éxito a Step Functions
        stepfunctions.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                "confirmed": True,
                "stage": stage,
                "confirmedAt": datetime.now().isoformat(),
                "confirmedBy": confirmed_by,
                "orderId": order_id
            })
        )
        
        # Actualizar estado en DynamoDB
        steps_table.update_item(
            Key={
                'PK': f'ORDER#{order_id}',
                'SK': f'TOKEN#{stage}'
            },
            UpdateExpression="SET #status = :status, confirmedAt = :confirmedAt, confirmedBy = :confirmedBy",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'CONFIRMED',
                ':confirmedAt': datetime.now().isoformat(),
                ':confirmedBy': confirmed_by
            }
        )
        
        # Publicar evento de confirmación
        eventbridge = boto3.client('events')
        eventbridge.put_events(
            Entries=[
                {
                    'Source': 'pardos.stepfunctions',
                    'DetailType': 'StageConfirmed',
                    'EventBusName': os.environ['EVENT_BUS_NAME'],
                    'Detail': json.dumps({
                        'orderId': order_id,
                        'stage': stage,
                        'confirmedBy': confirmed_by,
                        'confirmedAt': datetime.now().isoformat()
                    })
                }
            ]
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Etapa confirmada exitosamente",
                "orderId": order_id,
                "stage": stage
            })
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "message": "Error al confirmar etapa"
            })
        }


# ==============================================
# FUNCIONES AUXILIARES (existentes)
# ==============================================

def _update_step(order_id, tenant_id, step, status="IN_PROGRESS"):
    pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
    timestamp = datetime.utcnow().isoformat()
    _get_dynamodb().update_item(
        table_name=os.environ['ORDERS_TABLE'],
        key={'PK': pk, 'SK': 'INFO'},
        update_expression="SET currentStep = :step, #s = :status",
        expression_names={'#s': 'status'},
        expression_values={':step': step, ':status': status}
    )
    step_record = {
        'PK': pk,
        'SK': f"STEP#{step}#{timestamp}",
        'stepName': step,
        'status': status,
        'startedAt': timestamp,
        'tenantId': tenant_id,
        'orderId': order_id
    }
    _get_dynamodb().put_item(os.environ['STEPS_TABLE'], step_record)
    _get_events().publish_event(
        source="pardos.orders",
        detail_type="OrderStageStarted" if status == "IN_PROGRESS" else "OrderStageCompleted",
        detail={
            'orderId': order_id,
            'step': step,
            'status': status
        }
    )

def calcular_duracion(inicio, fin):
    start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
    end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
    return int((end - start).total_seconds())
