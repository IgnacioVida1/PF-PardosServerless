import os
import json
import boto3
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Inicializamos DynamoDB
dynamodb = boto3.resource('dynamodb')

# Tablas
customers_table = dynamodb.Table(os.environ.get('CUSTOMERS_TABLE', 'CustomersTable'))
orders_table = dynamodb.Table(os.environ.get('ORDERS_TABLE', 'OrdersTable'))


# ========================
# 🧾 Crear un nuevo pedido
# ========================
def create_order(event, context):
    try:
        # Convierte automáticamente todos los floats del JSON a Decimal
        body = json.loads(event.get('body', '{}'), parse_float=Decimal)

        # Generamos un ID único para el pedido
        order_id = f"o{int(datetime.utcnow().timestamp())}"
        pk = f"TENANT#pardos#ORDER#{order_id}"

        # Construimos el ítem a guardar
        item = {
            "PK": pk,
            "SK": "INFO",
            "customerId": body["customerId"],
            "status": "CREATED",
            "items": body["items"],
            "total": body["total"],
            "currentStep": "CREATED",
            "createdAt": datetime.utcnow().isoformat()
        }

        # Guardamos en DynamoDB
        orders_table.put_item(Item=item)

        return {
            "statusCode": 201,
            "body": json.dumps({
                "message": "Order created successfully",
                "orderId": order_id
            })
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


# =====================================
# 🔍 Obtener pedidos por ID de cliente
# =====================================
def get_orders_by_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']

        # Buscar todos los pedidos que correspondan al customerId
        response = orders_table.scan(
            FilterExpression=Key('customerId').eq(customer_id)
        )

        orders = response.get('Items', [])

        if not orders:
            return {"statusCode": 404, "body": json.dumps({"message": "No orders found for this customer"})}

        return {
            "statusCode": 200,
            "body": json.dumps({"orders": orders}, default=str)
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


# =====================================
# 👤 Obtener información del cliente
# =====================================
def get_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        pk = f"TENANT#pardos#CUSTOMER#{customer_id}"

        response = customers_table.get_item(Key={"PK": pk})

        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"message": "Customer not found"})}

        return {
            "statusCode": 200,
            "body": json.dumps(response['Item'], default=str)
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


# ==========================
# 👤 Crear un nuevo cliente
# ==========================
def create_customer(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        customer_id = body["customerId"]
        pk = f"TENANT#pardos#CUSTOMER#{customer_id}"

        item = {
            "PK": pk,
            "SK": "INFO",
            "name": body["name"],
            "email": body["email"],
            "createdAt": datetime.utcnow().isoformat()
        }

        customers_table.put_item(Item=item)

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Customer created successfully", "customerId": customer_id})
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
