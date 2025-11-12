import os
import json
import boto3
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Clientes AWS
dynamodb = boto3.resource('dynamodb')
eventbridge = boto3.client('events')

# Tablas
customers_table = dynamodb.Table(os.environ.get('CUSTOMERS_TABLE', 'CustomersTable'))
orders_table = dynamodb.Table(os.environ.get('ORDERS_TABLE', 'OrdersTable'))
steps_table = dynamodb.Table(os.environ.get('STEPS_TABLE', 'StepsTable'))
EVENT_BUS_NAME = os.environ.get('EVENT_BUS_NAME', 'PardosEventBus')


# ========================
# 4 ENDPOINTS ORIGINALES (SIN CAMBIOS)
# ========================

# POST /orders
def create_order(event, context):
    try:
        body = json.loads(event.get('body', '{}'), parse_float=Decimal)
        order_id = f"o{int(datetime.utcnow().timestamp())}"
        pk = f"TENANT#pardos#ORDER#{order_id}"

        # === GUARDAR EN DYNAMODB (Decimal OK) ===
        item = {
            "PK": pk,
            "SK": "INFO",
            "customerId": body["customerId"],
            "status": "CREATED",
            "items": body["items"],  # ← Decimal OK
            "total": body["total"],  # ← Decimal OK
            "currentStep": "CREATED",
            "createdAt": datetime.utcnow().isoformat()
        }
        orders_table.put_item(Item=item)

        # === EVENTBRIDGE: Convertir a float ===
        items_for_event = [
            {
                "productId": item["productId"],
                "qty": item["qty"],
                "price": float(item["price"])
            }
            for item in body["items"]
        ]

        eventbridge.put_events(Entries=[{
            'Source': 'pardos.orders',
            'DetailType': 'OrderCreated',
            'Detail': json.dumps({
                "orderId": order_id,
                "customerId": body["customerId"],
                "total": float(body["total"]),
                "items": items_for_event
            }),
            'EventBusName': EVENT_BUS_NAME
        }])

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Order created", "orderId": order_id})
        }
    except Exception as e:
        print("ERROR:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

# GET /orders/{customerId}
def get_orders_by_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        response = orders_table.scan(FilterExpression=Key('customerId').eq(customer_id))
        orders = response.get('Items', [])
        return {
            "statusCode": 200 if orders else 404,
            "body": json.dumps({"orders": orders} if orders else {"message": "No orders"}, default=str)
        }
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

# POST /customers
def create_customer(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        customer_id = body["customerId"]
        pk = f"TENANT#pardos#CUSTOMER#{customer_id}"
        item = {
            "PK": pk,
            "name": body["name"],
            "email": body["email"],
            "createdAt": datetime.utcnow().isoformat()
        }
        customers_table.put_item(Item=item)
        return {"statusCode": 201, "body": json.dumps({"message": "Customer created", "customerId": customer_id})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

# GET /customers/{customerId}
def get_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        pk = f"TENANT#pardos#CUSTOMER#{customer_id}"
        response = customers_table.get_item(Key={"PK": pk})
        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"message": "Not found"})}
        return {"statusCode": 200, "body": json.dumps(response['Item'], default=str)}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

# (ENDPOINT ADICIONAL)

# GET /orders/{order_id}
def get_order(event, context):
    try:
        order_id = event['pathParameters']['orderId']
        pk = f"TENANT#pardos#ORDER#{order_id}"
        order = orders_table.get_item(Key={"PK": pk, "SK": "INFO"}).get('Item')
        if not order:
            return {"statusCode": 404, "body": json.dumps({"error": "Not found"})}
        
        customer = customers_table.get_item(Key={"PK": f"TENANT#pardos#CUSTOMER#{order['customerId']}"}).get('Item', {})
        steps = steps_table.query(KeyConditionExpression=Key('PK').eq(pk),ScanIndexForward=True).get('Items', [])
        
        result = {
            "orderId": order_id,
            "status": order["status"],
            "currentStep": order["currentStep"],
            "total": float(order["total"]),
            "customer": {"name": customer.get("name", "N/A")},
            "steps": [s["stepName"] for s in steps]
        }
        return {"statusCode": 200, "body": json.dumps(result)}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

# ========================
# Auxiliar: Actualizar paso
# ========================
def _update_step(order_id, step, status="IN_PROGRESS"):
    pk = f"TENANT#pardos#ORDER#{order_id}"
    now = datetime.utcnow().isoformat()
    
    # USAR #st para "status" (reservada)
    orders_table.update_item(
        Key={"PK": pk, "SK": "INFO"},
        UpdateExpression="SET currentStep = :step, #st = :status",
        ExpressionAttributeNames={"#st": "status"},  # ← Renombrar
        ExpressionAttributeValues={":step": step, ":status": status}
    )
    
    steps_table.put_item(Item={
        "PK": pk,
        "SK": f"STEP#{step}#{now}",
        "stepName": step,
        "status": status,
        "startedAt": now
    })
    
    eventbridge.put_events(Entries=[{
        'Source': 'pardos.orders',
        'DetailType': 'OrderStageStarted' if status == "IN_PROGRESS" else 'OrderStageCompleted',
        'Detail': json.dumps({"orderId": order_id, "step": step}),
        'EventBusName': EVENT_BUS_NAME
    }])


# ========================
# COOKING (PRIMERA) → Recibe EventBridge
# ========================
def process_cooking(event, context):
    try:
        order_id = event['detail']['orderId']  # ← CORREGIDO
        _update_step(order_id, "COOKING")
        return {"orderId": order_id}
    except Exception as e:
        print("ERROR in process_cooking:", str(e))
        raise


# ========================
# PACKAGING / DELIVERY → Reciben output anterior
# ========================
def process_packaging(event, context):
    try:
        order_id = event['orderId']  # ← CORREGIDO
        _update_step(order_id, "PACKAGING")
        return {"orderId": order_id}
    except Exception as e:
        print("ERROR in process_packaging:", str(e))
        raise


def process_delivery(event, context):
    try:
        order_id = event['orderId']
        _update_step(order_id, "DELIVERY")
        return {"orderId": order_id}
    except Exception as e:
        print("ERROR in process_delivery:", str(e))
        raise


# ========================
# DELIVERED (FINAL)
# ========================
def process_delivered(event, context):
    try:
        order_id = event['orderId']
        pk = f"TENANT#pardos#ORDER#{order_id}"
        now = datetime.utcnow().isoformat()
        
        # USAR #st para "status"
        orders_table.update_item(
            Key={"PK": pk, "SK": "INFO"},
            UpdateExpression="SET currentStep = :step, #st = :status",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":step": "DELIVERED", ":status": "COMPLETED"}
        )
        
        steps_table.put_item(Item={
            "PK": pk,
            "SK": f"STEP#DELIVERED#{now}",
            "stepName": "DELIVERED",
            "status": "DONE",
            "startedAt": now,
            "finishedAt": now
        })
        
        eventbridge.put_events(Entries=[{
            'Source': 'pardos.orders',
            'DetailType': 'OrderDelivered',
            'Detail': json.dumps({"orderId": order_id}),
            'EventBusName': EVENT_BUS_NAME
        }])
        
        return {"orderId": order_id}
    except Exception as e:
        print("ERROR in process_delivered:", str(e))
        raise
