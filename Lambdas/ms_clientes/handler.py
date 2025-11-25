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

def create_order(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        customer_id = body['customerId']
        tenant_id = body.get('tenantId', 'pardos')
        order_id = str(uuid.uuid4())  # UUID para escalabilidad
        timestamp = datetime.utcnow().isoformat()

        # Convierte a Decimal para items y total
        if 'items' in body:
            body['items'] = [{k: Decimal(str(v)) if k == 'price' else v for k, v in item.items()} for item in body['items']]
        total = Decimal(str(body.get('total', '0')))

        # Crea el registro de order metadata en DynamoDB con SK="INFO"
        order_metadata = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': 'INFO',
            'orderId': order_id,
            'customerId': customer_id,
            'tenantId': tenant_id,
            'status': 'CREATED',
            'items': body.get('items', []),  # Lista de {productId, qty, price} con Decimal
            'total': total,  # Decimal para total
            'createdAt': timestamp,
            'currentStep': 'CREATED'  # Inicia en CREATED, el workflow lo moverá a COOKING
        }
        _get_dynamodb().put_item(os.environ['ORDERS_TABLE'], order_metadata)

        # Publica evento con detalles (convertir Decimal a float para JSON)
        items_for_event = [
            {
                "productId": item["productId"],
                "qty": int(item["qty"]),
                "price": float(item["price"])
            }
            for item in body.get('items', [])
        ]
        _get_events().publish_event(
            source="pardos.orders",
            detail_type="OrderCreated",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'total': float(total),
                'items': items_for_event,
                'timestamp': timestamp
            }
        )

        return {
            'statusCode': 201,
            'body': json.dumps({
                'orderId': order_id,
                'message': 'Order created and workflow initiated'
            })
        }
    except Exception as e:
        print(f"Error en create_order: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_orders_by_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        tenant_id = 'pardos'
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='customerId = :cid',
            expression_attribute_values={':cid': customer_id}
        )
        orders = [item for item in response.get('Items', []) if item.get('PK', '').startswith(f"TENANT#{tenant_id}")]
        return {
            'statusCode': 200,
            'body': json.dumps({'orders': orders}, default=str)  # default=str para Decimal
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def create_customer(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        customer_id = str(uuid.uuid4())
        tenant_id = body.get('tenantId', 'pardos')
        timestamp = datetime.utcnow().isoformat()
        customer = {
            'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}",
            'customerId': customer_id,
            'tenantId': tenant_id,
            'name': body.get('name'),
            'email': body.get('email'),
            'createdAt': timestamp
        }
        _get_dynamodb().put_item(os.environ['CUSTOMERS_TABLE'], customer)
        return {
            'statusCode': 201,
            'body': json.dumps({'customerId': customer_id, 'message': 'Customer created'})
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def get_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        tenant_id = 'pardos'
        response = _get_dynamodb().get_item(
            table_name=os.environ['CUSTOMERS_TABLE'],
            key={'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}"}
        )
        item = response.get('Item')
        if not item:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Customer not found'})}
        return {
            'statusCode': 200,
            'body': json.dumps(item, default=str)
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def get_order(event, context):
    try:
        order_id = event['pathParameters']['orderId']
        tenant_id = 'pardos'
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        
        # Obtener order metadata
        order_response = _get_dynamodb().query(
            table_name=os.environ['ORDERS_TABLE'],
            key_condition_expression=Key('PK').eq(pk) & Key('SK').eq('INFO')
        )
        
        items = order_response.get('Items', [])
        if not items:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Order not found'})}
        
        order = items[0]
        
        # Join con customer
        customer_pk = f"TENANT#{tenant_id}#CUSTOMER#{order['customerId']}"
        customer_response = _get_dynamodb().get_item(
            table_name=os.environ['CUSTOMERS_TABLE'],
            key={'PK': customer_pk}
        )
        customer = customer_response.get('Item', {})
        
        # Join con steps
        steps_response = _get_dynamodb().query(
            table_name=os.environ['STEPS_TABLE'],
            key_condition_expression=Key('PK').eq(pk)
        )
        steps = steps_response.get('Items', [])
        
        # Convertir items para JSON (Decimal to float)
        items_json = []
        for item in order.get('items', []):
            items_json.append({
                'productId': item.get('productId', ''),
                'qty': int(item.get('qty', 0)),
                'price': float(item.get('price', 0))
            })
        
        result = {
            'orderId': order_id,
            'status': order.get('status', 'CREATED'),
            'currentStep': order.get('currentStep', 'CREATED'),
            'total': float(order.get('total', 0)),
            'items': items_json,
            'createdAt': order.get('createdAt', ''),
            'customer': {
                'name': customer.get('name', 'N/A'), 
                'email': customer.get('email', 'N/A'),
                'phone': customer.get('phone', 'N/A')
            },
            'steps': [s.get('stepName') for s in steps if s.get('stepName')]
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        print(f"Error en get_order: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
