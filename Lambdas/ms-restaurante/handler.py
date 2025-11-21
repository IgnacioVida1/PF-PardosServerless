import json
import boto3
import uuid
import os
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from shared.database import DynamoDB
from shared.events import EventBridge

# Inicialización lazy: No crear globales en import time
dynamodb = None
events = None

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

# === FUNCIONES DE ETAPAS MANUALES (API para restaurante) ===
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

# === FUNCIONES DE WORKFLOW AUTOMÁTICO (para Step Functions) ===
def process_cooking(event, context):
    try:
        order_id = event.get('detail', {}).get('orderId') or event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        _update_step(order_id, tenant_id, 'COOKING', 'IN_PROGRESS')
        return {'orderId': order_id, 'stage': 'COOKING'}
    except Exception as e:
        print(f"Error en process_cooking: {str(e)}")
        raise

def process_packaging(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        _update_step(order_id, tenant_id, 'PACKAGING', 'IN_PROGRESS')
        return {'orderId': order_id, 'stage': 'PACKAGING'}
    except Exception as e:
        print(f"Error en process_packaging: {str(e)}")
        raise

def process_delivery(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        _update_step(order_id, tenant_id, 'DELIVERY', 'IN_PROGRESS')
        return {'orderId': order_id, 'stage': 'DELIVERY'}
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
        return {'orderId': order_id, 'stage': 'DELIVERED'}
    except Exception as e:
        print(f"Error en process_delivered: {str(e)}")
        raise

# Auxiliar: Actualizar paso (adaptado a lazy)
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

# === FUNCIONES AUXILIARES (compartidas) ===
def calcular_duracion(inicio, fin):
    start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
    end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
    return int((end - start).total_seconds())
