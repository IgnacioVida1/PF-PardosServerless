import json
import boto3
import requests
from datetime import datetime
from shared.database import DynamoDB
from shared.events import EventBridge

dynamodb = DynamoDB()
events = EventBridge()

def iniciar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        # Obtener customerId y stage del request
        customer_id = body['customerId']
        stage = body['stage']
        assigned_to = body.get('assignedTo', 'Sistema')
        tenant_id = "pardos"
        
        # Crear nuevo pedido en api-cliente y obtener orderId REAL
        order_data = crear_pedido_en_api_cliente(customer_id)
        order_id = order_data['orderId']
        
        timestamp = datetime.utcnow().isoformat()
        step_record = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': 'IN_PROGRESS',
            'startedAt': timestamp,
            'assignedTo': assigned_to,
            'tenantId': tenant_id,
            'orderId': order_id,
            'customerId': customer_id
        }
        
        dynamodb.put_item('steps', step_record)
        
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': stage,
                'assignedTo': assigned_to,
                'timestamp': timestamp
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} iniciada',
                'orderId': order_id,
                'customerId': customer_id,
                'stepRecord': step_record,
                'orderData': order_data
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def crear_pedido_en_api_cliente(customer_id):
    """
    Llama a api-cliente para crear un nuevo pedido y obtener orderId REAL
    REEMPLAZA LA URL CON LA REAL DE TU API-CLIENTE
    """
    try:
        # URL DEL API-CLIENTE - REEMPLAZA CON TU URL REAL
        api_cliente_url = "https://TU-API-CLIENTE-ID.execute-api.us-east-1.amazonaws.com/dev/orders"
        
        # Datos del pedido que se enviarán a api-cliente
        pedido_data = {
            "customerId": customer_id,
            "items": [
                {"productId": "pollo_1_4", "qty": 1, "price": 25.9},
                {"productId": "chicha", "qty": 1, "price": 8.5}
            ],
            "total": 34.4
        }
        
        print(f"Llamando a api-cliente: {api_cliente_url}")
        print(f"Datos del pedido: {pedido_data}")
        
        # LLAMADA REAL A API-CLIENTE
        response = requests.post(
            api_cliente_url,
            json=pedido_data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 201:
            result = response.json()
            print(f"Pedido creado exitosamente en api-cliente: {result}")
            
            # ORDERID REAL GENERADO POR API-CLIENTE
            order_id_real = result['orderId']
            return {
                'orderId': order_id_real,
                'message': 'Pedido creado exitosamente en api-cliente',
                'customerId': customer_id,
                'fullResponse': result
            }
        else:
            error_msg = f"Error creando pedido en api-cliente: {response.status_code} - {response.text}"
            print(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        print(f"Error llamando a api-cliente: {str(e)}")
        # Fallback: orderId local si api-cliente no está disponible
        fallback_order_id = f"o{int(datetime.utcnow().timestamp())}"
        print(f"Usando orderId de fallback: {fallback_order_id}")
        return {
            'orderId': fallback_order_id,
            'message': 'Pedido creado localmente - api-cliente no disponible',
            'customerId': customer_id
        }

def completar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        
        # Buscar etapa activa
        response = dynamodb.query(
            table_name='steps',
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
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
        
        dynamodb.update_item(
            table_name='steps',
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
        
        events.publish_event(
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

def cooking_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        
        print(f"Iniciando COOKING para orden: {order_id}")
        
        registrar_etapa(tenant_id, order_id, 'COOKING', 'IN_PROGRESS')
        
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'COOKING',
                'status': 'IN_PROGRESS',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        print(f"Cocinando pedido {order_id}...")
        
        return {
            'status': 'COMPLETED',
            'message': 'Cooking stage completed',
            'orderId': order_id,
            'stage': 'COOKING',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error en cooking_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def packaging_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        
        print(f"Iniciando PACKAGING para orden: {order_id}")
        
        completar_etapa_automatica(tenant_id, order_id, 'COOKING')
        registrar_etapa(tenant_id, order_id, 'PACKAGING', 'IN_PROGRESS')
        
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'PACKAGING',
                'status': 'IN_PROGRESS',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        print(f"Empacando pedido {order_id}...")
        
        return {
            'status': 'COMPLETED',
            'message': 'Packaging stage completed',
            'orderId': order_id,
            'stage': 'PACKAGING',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error en packaging_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def delivery_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        
        print(f"Iniciando DELIVERY para orden: {order_id}")
        
        completar_etapa_automatica(tenant_id, order_id, 'PACKAGING')
        registrar_etapa(tenant_id, order_id, 'DELIVERY', 'IN_PROGRESS')
        
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'DELIVERY',
                'status': 'IN_PROGRESS',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        print(f"Entregando pedido {order_id}...")
        
        return {
            'status': 'COMPLETED',
            'message': 'Delivery stage completed',
            'orderId': order_id,
            'stage': 'DELIVERY',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error en delivery_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def delivered_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        
        print(f"Completando DELIVERED para orden: {order_id}")
        
        completar_etapa_automatica(tenant_id, order_id, 'DELIVERY')
        registrar_etapa(tenant_id, order_id, 'DELIVERED', 'COMPLETED')
        actualizar_estado_final(tenant_id, order_id, 'COMPLETED')
        
        events.publish_event(
            source="pardos.etapas",
            detail_type="OrderCompleted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'DELIVERED',
                'status': 'COMPLETED',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        print(f"Pedido {order_id} completado exitosamente!")
        
        return {
            'status': 'COMPLETED',
            'message': 'Order delivered successfully',
            'orderId': order_id,
            'stage': 'DELIVERED',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error en delivered_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def registrar_etapa(tenant_id, order_id, stage, status):
    try:
        timestamp = datetime.utcnow().isoformat()
        step_record = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': status,
            'startedAt': timestamp,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        
        if status == 'COMPLETED':
            step_record['finishedAt'] = timestamp
            
        dynamodb.put_item('steps', step_record)
        print(f"Etapa {stage} registrada para orden {order_id}")
        
    except Exception as e:
        print(f"Error registrando etapa: {str(e)}")

def completar_etapa_automatica(tenant_id, order_id, stage):
    try:
        response = dynamodb.query(
            table_name='steps',
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
                ':sk': f"STEP#{stage}"
            }
        )
        
        if response.get('Items'):
            latest_step = max(response['Items'], key=lambda x: x['startedAt'])
            timestamp = datetime.utcnow().isoformat()
            
            dynamodb.update_item(
                table_name='steps',
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
            print(f"Etapa {stage} completada automaticamente")
            
    except Exception as e:
        print(f"Error completando etapa automatica: {str(e)}")

def actualizar_estado_final(tenant_id, order_id, status):
    print(f"Actualizando estado final del pedido {order_id} a {status}")

def calcular_duracion(inicio, fin):
    start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
    end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
    return int((end - start).total_seconds())