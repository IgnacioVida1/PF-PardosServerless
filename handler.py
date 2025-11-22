"""
Handler principal unificado para Pardos Serverless
Importa y expone todas las funciones de los diferentes microservicios
"""

# Fix imports para deployment en AWS Lambda
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

# Importar funciones de autenticación
from Lambdas.auth_service.handler import (
    register,
    login, 
    validate
)

# Importar funciones de clientes
from Lambdas.ms_clientes.handler import (
    create_order,
    get_orders_by_customer, 
    create_customer,
    get_customer,
    get_order
)

# Importar funciones de dashboard  
from Lambdas.ms_dashboard.handler import (
    obtener_resumen,
    obtener_metricas,
    obtener_pedidos
)

# Importar funciones de restaurante/workflow
from Lambdas.ms_restaurante.handler import (
    iniciar_etapa,
    completar_etapa,
    process_cooking,
    process_packaging,
    process_delivery,
    process_delivered
)

# Importar funciones de notificaciones
from Lambdas.notifications.handler import (
    send_order_notification,
    get_customer_notifications,
    mark_notification_read
)

# Exponer todas las funciones para que serverless.yml pueda encontrarlas
__all__ = [
    'register',
    'login',
    'validate',
    'create_order',
    'get_orders_by_customer', 
    'create_customer',
    'get_customer',
    'get_order',
    'obtener_resumen',
    'obtener_metricas',
    'obtener_pedidos',
    'iniciar_etapa',
    'completar_etapa',
    'process_cooking',
    'process_packaging',
    'process_delivery',
    'process_delivered',
    'send_order_notification',
    'get_customer_notifications',
    'mark_notification_read'
]