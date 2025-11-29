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

# ==============================================
# FUNCIONES EXISTENTES (se mantienen igual)
# ==============================================

def iniciar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        assigned_to = body.get('assignedTo', 'Sistema')
        timestamp = datetime.utcnow().isoformat()
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        step_record = {
            'PK': pk,
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': 'IN_PROGRESS',
            'startedAt': timestamp,
            'assignedTo': assigned_to,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        _get_dynamodb().put_item(os.environ['STEPS_TABLE'], step_record)
        _get_dynamodb().update_item(
            table_name=os.environ['ORDERS_TABLE'],
            key={
                'PK': pk,
                'SK': 'INFO'
            },
            update_expression="SET currentStep = :step, updatedAt = :now",
            expression_values={
                ':step': stage,
                ':now': timestamp
            }
        )
        _get_events().publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'assignedTo': assigned_to,
                'timestamp': timestamp
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} iniciada',
                'stepRecord': step_record
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def completar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        response = _get_dynamodb().query(
            table_name=os.environ['STEPS_TABLE'],
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': pk,
                ':sk': f"STEP#{stage}"
            }
        )
        if not response.get('Items'):
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Etapa no encontrada'})
            }
        latest_step = max(response['Items'], key=lambda x: x['startedAt'])
        timestamp = datetime.utcnow().isoformat()
        _get_dynamodb().update_item(
            table_name=os.environ['STEPS_TABLE'],
            key={
                'PK': latest_step['PK'],
                'SK': latest_step['SK']
            },
            update_expression="SET #s = :status, finishedAt = :finished",
            expression_names={'#s': 'status'},
            expression_values={
                ':status': 'COMPLETED',
                ':finished': timestamp
            }
        )
        _get_events().publish_event(
            source="pardos.etapas",
            detail_type="StageCompleted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'startedAt': latest_step['startedAt'],
                'completedAt': timestamp,
                'duration': calcular_duracion(latest_step['startedAt'], timestamp)
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} completada',
                'duration': calcular_duracion(latest_step['startedAt'], timestamp)
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

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
        return {'orderId': order_id,  'tenantId': tenant_id,'stage': 'DELIVERED'}
    except Exception as e:
        print(f"Error en process_delivered: {str(e)}")
        raise

# ==============================================
# NUEVAS FUNCIONES PARA STEP FUNCTIONS MEJORADO
# ==============================================

def check_stage_confirmation(event, context):
    """
    Verifica en StepsTable si una etapa específica está confirmada (status: DONE)
    """
    try:
        order_id = event['orderId']
        tenant_id = event['tenantId']
        expected_stage = event['expectedStage']
        
        print(f"🔍 Checking confirmation for:")
        print(f"   PK: TENANT#{tenant_id}#ORDER#{order_id}")
        print(f"   SK prefix: STEP#{expected_stage}")
        # Buscar en StepsTable todas las entradas para esta etapa
        response = _get_dynamodb().query(
            table_name=os.environ['STEPS_TABLE'],
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
                ':sk': f"STEP#{expected_stage}"
            }
        )
        print(f"📊 Query found {len(response.get('Items', []))} items")
        # Verificar si hay alguna entrada de esta etapa con status DONE
        confirmed = False
        if response.get('Items'):
            for item in response['Items']:
                if item.get('status') == 'DONE':
                    confirmed = True
                    print(f"Stage {expected_stage} confirmed for order {order_id}")
                    break        
        print(f"Confirmation status: {confirmed}")
        
        return {
            'confirmed': confirmed,
            'orderId': order_id,
            'stage': expected_stage,
            'tenantId': tenant_id,
            'checkedAt': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error checking stage confirmation: {str(e)}")
        return {
            'confirmed': False,
            'error': str(e),
            'orderId': event.get('orderId', 'unknown'),
            'stage': event.get('expectedStage', 'unknown')
        }

def confirm_stage(event, context):
    """
    Confirma manualmente una etapa del pedido (llamado desde el frontend)
    """
    try:
        order_id = event['pathParameters']['orderId']
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        stage = body['stage']
        user_id = body.get('userId', 'system')
        tenant_id = body.get('tenantId', 'pardos')
        
        timestamp = datetime.utcnow().isoformat()
        
        print(f"Confirming stage {stage} for order {order_id} by user {user_id}")
        
        # Registrar en StepsTable que la etapa está confirmada/completada
        step_record = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': f"STEP#{stage}",
            'stepName': stage,
            'status': 'DONE',
            'startedAt': timestamp,
            'finishedAt': timestamp,
            'completedBy': user_id,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        _get_dynamodb().put_item(os.environ['STEPS_TABLE'], step_record)
        
        # Actualizar el estado actual en OrdersTable
        _get_dynamodb().update_item(
            table_name=os.environ['ORDERS_TABLE'],
            key={
                'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
                'SK': 'INFO'
            },
            update_expression="SET currentStep = :step, updatedAt = :now",
            expression_values={
                ':step': stage,
                ':now': timestamp
            }
        )
        
        # Publicar evento de etapa completada
        _get_events().publish_event(
            source="pardos.etapas",
            detail_type="StageCompleted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'completedBy': user_id,
                'completedAt': timestamp
            }
        )
        
        # Si es etapa de DELIVERY, agregar a la cola SQS
        if stage == 'DELIVERY':
            sqs = _get_sqs()
            sqs.send_message(
                QueueUrl=os.environ['DELIVERY_QUEUE_URL'],
                MessageBody=json.dumps({
                    'orderId': order_id,
                    'tenantId': tenant_id,
                    'stage': stage,
                    'addedToQueueAt': timestamp
                })
            )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Stage {stage} confirmed for order {order_id}',
                'orderId': order_id,
                'stage': stage,
                'confirmedBy': user_id,
                'confirmedAt': timestamp
            })
        }
        
    except Exception as e:
        print(f"Error confirming stage: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Failed to confirm stage: {str(e)}'
            })
        }

def check_delivery_queue(event, context):
    """
    Verifica la cola SQS para determinar si hay capacidad para nuevos deliveries
    """
    try:
        order_id = event['orderId']
        tenant_id = event.get('tenantId', 'pardos')
        
        print(f"Checking delivery queue capacity for order: {order_id}")
        
        # Obtener cantidad de mensajes en cola de delivery
        sqs = _get_sqs()
        response = sqs.get_queue_attributes(
            QueueUrl=os.environ['DELIVERY_QUEUE_URL'],
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        messages_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        max_capacity = 10  # Límite de 10 pedidos en delivery
        
        can_proceed = messages_count < max_capacity
        
        print(f"Queue status: {messages_count}/{max_capacity} - Can proceed: {can_proceed}")
        
        return {
            'canProceed': can_proceed,
            'currentQueueSize': messages_count,
            'maxCapacity': max_capacity,
            'orderId': order_id,
            'tenantId': tenant_id,
            'checkedAt': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error checking delivery queue: {str(e)}")
        return {
            'canProceed': False,
            'error': str(e),
            'currentQueueSize': 0,
            'maxCapacity': 10,
            'orderId': event.get('orderId', 'unknown')
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
