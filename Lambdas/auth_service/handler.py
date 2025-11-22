import json
import os
import uuid
import bcrypt
import jwt
from datetime import datetime, timedelta

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

def register(event, context):
    """
    Registrar nuevo usuario
    POST /auth/register
    """
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        username = body.get('username')
        email = body.get('email') 
        password = body.get('password')
        name = body.get('name', '')
        phone = body.get('phone', '')
        address = body.get('address', '')
        
        if not username or not email or not password:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Username, email y password son requeridos'})
            }
        
        tenant_id = 'pardos'
        user_pk = f"TENANT#{tenant_id}#USER#{username}"
        
        # Verificar si usuario ya existe
        existing_user = _get_dynamodb().get_item(
            table_name=os.environ['USERS_TABLE'],
            key={'PK': user_pk}
        )
        
        if existing_user.get('Item'):
            return {
                'statusCode': 409,
                'body': json.dumps({'error': 'Usuario ya existe'})
            }
        
        # Hash de la contraseña
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Crear customer ID
        customer_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Crear usuario
        user_data = {
            'PK': user_pk,
            'username': username,
            'email': email,
            'passwordHash': password_hash,
            'customerId': customer_id,
            'createdAt': timestamp
        }
        
        # Crear customer
        customer_data = {
            'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}",
            'customerId': customer_id,
            'userRef': user_pk,
            'name': name,
            'phone': phone,
            'address': address,
            'email': email,
            'createdAt': timestamp
        }
        
        # Guardar en DynamoDB
        _get_dynamodb().put_item(os.environ['USERS_TABLE'], user_data)
        _get_dynamodb().put_item(os.environ['CUSTOMERS_TABLE'], customer_data)
        
        return {
            'statusCode': 201,
            'body': json.dumps({
                'message': 'Usuario registrado exitosamente',
                'customerId': customer_id,
                'username': username
            })
        }
        
    except Exception as e:
        print(f"Error en register: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def login(event, context):
    """
    Iniciar sesión
    POST /auth/login
    """
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        username = body.get('username')
        password = body.get('password')
        
        if not username or not password:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Username y password son requeridos'})
            }
        
        tenant_id = 'pardos'
        user_pk = f"TENANT#{tenant_id}#USER#{username}"
        
        # Buscar usuario
        user_response = _get_dynamodb().get_item(
            table_name=os.environ['USERS_TABLE'],
            key={'PK': user_pk}
        )
        
        user = user_response.get('Item')
        if not user:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Credenciales inválidas'})
            }
        
        # Verificar contraseña
        if not bcrypt.checkpw(password.encode('utf-8'), user['passwordHash'].encode('utf-8')):
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Credenciales inválidas'})
            }
        
        # Generar JWT
        secret = os.environ.get('JWT_SECRET', 'pardos-secret-key')
        payload = {
            'username': username,
            'customerId': user['customerId'],
            'tenantId': tenant_id,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }
        
        token = jwt.encode(payload, secret, algorithm='HS256')
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Login exitoso',
                'token': token,
                'customerId': user['customerId'],
                'username': username
            })
        }
        
    except Exception as e:
        print(f"Error en login: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def validate(event, context):
    """
    Validar token JWT
    GET /auth/validate
    """
    try:
        # Obtener token del header Authorization
        headers = event.get('headers', {})
        auth_header = headers.get('Authorization') or headers.get('authorization')
        
        if not auth_header or not auth_header.startswith('Bearer '):
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Token no proporcionado'})
            }
        
        token = auth_header.split(' ')[1]
        secret = os.environ.get('JWT_SECRET', 'pardos-secret-key')
        
        try:
            payload = jwt.decode(token, secret, algorithms=['HS256'])
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'valid': True,
                    'username': payload['username'],
                    'customerId': payload['customerId'],
                    'tenantId': payload.get('tenantId', 'pardos')
                })
            }
        except jwt.ExpiredSignatureError:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Token expirado'})
            }
        except jwt.InvalidTokenError:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Token inválido'})
            }
        
    except Exception as e:
        print(f"Error en validate: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }