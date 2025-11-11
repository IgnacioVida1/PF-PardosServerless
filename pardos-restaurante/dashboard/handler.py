import json
from datetime import datetime, timedelta
from shared.database import DynamoDB

dynamodb = DynamoDB()

def obtener_resumen(event, context):
    """
    Obtiene resumen general para el dashboard
    """
    try:
        tenant_id = event.get('queryStringParameters', {}).get('tenantId', 'pardos')
        
        # Obtener métricas básicas
        total_pedidos = obtener_total_pedidos(tenant_id)
        pedidos_hoy = obtener_pedidos_hoy(tenant_id)
        pedidos_activos = obtener_pedidos_activos(tenant_id)
        tiempo_promedio = obtener_tiempo_promedio(tenant_id)
        
        resumen = {
            'totalPedidos': total_pedidos,
            'pedidosHoy': pedidos_hoy,
            'pedidosActivos': pedidos_activos,
            'tiempoPromedioEntrega': tiempo_promedio,
            'ultimaActualizacion': datetime.utcnow().isoformat()
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(resumen)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def obtener_metricas(event, context):
    """
    Obtiene métricas detalladas para gráficos
    """
    try:
        tenant_id = event.get('queryStringParameters', {}).get('tenantId', 'pardos')
        
        metricas = {
            'pedidosPorEstado': obtener_pedidos_por_estado(tenant_id),
            'tiemposPorEtapa': obtener_tiempos_por_etapa(tenant_id),
            'pedidosUltimaSemana': obtener_pedidos_ultima_semana(tenant_id),
            'productosPopulares': obtener_productos_populares(tenant_id)
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(metricas)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def obtener_pedidos(event, context):
    """
    Obtiene lista de pedidos para el dashboard - CORREGIDO
    """
    try:
        tenant_id = event.get('queryStringParameters', {}).get('tenantId', 'pardos')
        limit = int(event.get('queryStringParameters', {}).get('limit', 50))
        
        # NOTA: Esta función necesita acceso a la tabla de api-clientes
        # Por ahora devolverá datos de ejemplo hasta que se integre con api-clientes
        
        # Datos de ejemplo para demostración
        pedidos_ejemplo = [
            {
                'orderId': 'o123456789',
                'customerId': 'c1', 
                'status': 'COOKING',
                'total': 62.90,
                'items': [
                    {'name': 'Pollo a la brasa', 'price': 45.90, 'quantity': 1},
                    {'name': 'Inca Kola 1L', 'price': 8.50, 'quantity': 2}
                ],
                'createdAt': '2025-11-10T04:29:43.856279',
                'etapas': [
                    {'stepName': 'COOKING', 'status': 'IN_PROGRESS', 'startedAt': '2025-11-10T04:29:43.856279'},
                    {'stepName': 'PACKAGING', 'status': 'IN_PROGRESS', 'startedAt': '2025-11-10T04:31:19.195930'}
                ]
            }
        ]
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'pedidos': pedidos_ejemplo,
                'total': len(pedidos_ejemplo),
                'message': 'Datos de ejemplo - Integrar con api-clientes para datos reales'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Funciones auxiliares actualizadas
def obtener_total_pedidos(tenant_id):
    try:
        response = dynamodb.query(
            table_name='orders',
            key_condition='PK = :pk AND SK = :sk',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER",
                ':sk': 'INFO'
            },
            select='COUNT'
        )
        return response.get('Count', 0)
    except:
        return 0

def obtener_pedidos_hoy(tenant_id):
    try:
        hoy = datetime.utcnow().date().isoformat()
        response = dynamodb.query(
            table_name='orders',
            key_condition='PK = :pk AND SK = :sk',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER", 
                ':sk': 'INFO'
            }
        )
        
        pedidos_hoy = 0
        for pedido in response.get('Items', []):
            if pedido.get('createdAt', '').startswith(hoy):
                pedidos_hoy += 1
                
        return pedidos_hoy
    except:
        return 0

def obtener_pedidos_activos(tenant_id):
    try:
        response = dynamodb.query(
            table_name='orders',
            key_condition='PK = :pk AND SK = :sk',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER",
                ':sk': 'INFO'
            }
        )
        
        activos = 0
        estados_activos = ['CREATED', 'COOKING', 'PACKAGING', 'DELIVERY']
        for pedido in response.get('Items', []):
            if pedido.get('status') in estados_activos:
                activos += 1
                
        return activos
    except:
        return 0

def obtener_pedidos_por_estado(tenant_id):
    try:
        response = dynamodb.query(
            table_name='orders',
            key_condition='PK = :pk AND SK = :sk',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER",
                ':sk': 'INFO'
            }
        )
        
        distribucion = {'CREATED': 0, 'COOKING': 0, 'PACKAGING': 0, 'DELIVERY': 0, 'DELIVERED': 0}
        for pedido in response.get('Items', []):
            estado = pedido.get('status', 'CREATED')
            distribucion[estado] = distribucion.get(estado, 0) + 1
            
        return distribucion
    except:
        return {'CREATED': 0, 'COOKING': 0, 'PACKAGING': 0, 'DELIVERY': 0, 'DELIVERED': 0}

def obtener_tiempos_por_etapa(tenant_id):
    # Por ahora valores estáticos, se puede implementar cálculo real
    return {
        'COOKING': 15,
        'PACKAGING': 5, 
        'DELIVERY': 25
    }

def obtener_pedidos_ultima_semana(tenant_id):
    # Datos de ejemplo para gráficos
    return [25, 30, 28, 32, 35, 40, 38]

def obtener_productos_populares(tenant_id):
    # Por implementar: análisis de items en pedidos
    return [
        {'producto': 'Pollo a la Brasa', 'cantidad': 120},
        {'producto': 'Chicha Morada', 'cantidad': 95},
        {'producto': 'Ensalada Fresca', 'cantidad': 80}
    ]

def obtener_tiempo_promedio(tenant_id):
    return 45  # minutos

def obtener_etapas_pedido(tenant_id, order_id):
    try:
        response = dynamodb.query(
            table_name='steps',
            key_condition='PK = :pk AND begins_with(SK, :sk)',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
                ':sk': 'STEP#'
            }
        )
        return response.get('Items', [])
    except:
        return []

