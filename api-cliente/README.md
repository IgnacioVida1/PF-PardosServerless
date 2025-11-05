🧾 Proyecto: API de Clientes y Pedidos (Serverless AWS)
📘 Descripción

Esta API permite gestionar clientes y pedidos utilizando un enfoque Serverless en AWS.
Todo se ejecuta en servicios administrados: AWS Lambda, API Gateway y DynamoDB.

⚙️ Arquitectura

Servicios utilizados:

🧠 AWS Lambda → funciones para crear y obtener clientes/pedidos.

🌐 API Gateway → expone las funciones Lambda como endpoints HTTP.

💾 DynamoDB → almacena los datos de clientes y pedidos.

🪪 IAM Role (LabRole) → permisos para acceso a DynamoDB.

🗂️ Estructura del proyecto
api-cliente/
│
├── handler.py          # Código principal con las funciones Lambda
├── serverless.yml      # Configuración del proyecto Serverless Framework
├── requirements.txt    # Dependencias locales (opcional)
└── README.md           # Documentación del proyecto

🧩 Endpoints disponibles
Método	Endpoint	Descripción
POST	/customers	Crea un nuevo cliente
GET	/customers/{customerId}	Obtiene los datos de un cliente
POST	/orders	Crea un nuevo pedido
GET	/orders/{customerId}	Obtiene todos los pedidos de un cliente

⚠️ Reemplaza <tu-api> con el dominio real de tu API desplegada, por ejemplo:
https://2wmcf9zj7e.execute-api.us-east-1.amazonaws.com

🧠 Variables de entorno

Definidas en serverless.yml y disponibles dentro de cada Lambda:

environment:
  CUSTOMERS_TABLE: CustomersTable
  ORDERS_TABLE: OrdersTable

🚀 Despliegue en AWS

Asegúrate de tener instalado Serverless Framework:

npm install -g serverless


Despliega el proyecto completo:

serverless deploy


Actualiza solo una función:

serverless deploy function -f createOrder

💬 Ejemplos para Postman
🧍 Crear cliente

POST
https://<tu-api>.execute-api.us-east-1.amazonaws.com/customers

Body (JSON):

{
  "customerId": "c1",
  "name": "Juan Pérez",
  "email": "juan@example.com"
}


Respuesta:

{
  "message": "Customer created successfully",
  "customerId": "c1"
}

🔍 Obtener cliente

GET
https://<tu-api>.execute-api.us-east-1.amazonaws.com/customers/c1

Respuesta:

{
  "PK": "TENANT#pardos#CUSTOMER#c1",
  "SK": "INFO",
  "name": "Juan Pérez",
  "email": "juan@example.com",
  "createdAt": "2025-11-05T05:14:23.205934"
}

🧾 Crear pedido

POST
https://<tu-api>.execute-api.us-east-1.amazonaws.com/orders

Body (JSON):

{
  "customerId": "c1",
  "items": [
    {"name": "Pollo a la brasa entero", "price": 45.9, "quantity": 1},
    {"name": "Inca Kola 1L", "price": 8.5, "quantity": 2}
  ],
  "total": 62.9
}


Respuesta:

{
  "message": "Order created successfully",
  "orderId": "o1762318719"
}

📦 Obtener pedidos por cliente

GET
https://<tu-api>.execute-api.us-east-1.amazonaws.com/orders/c1

Respuesta:

{
  "orders": [
    {
      "PK": "TENANT#pardos#ORDER#o1762318719",
      "SK": "INFO",
      "customerId": "c1",
      "status": "CREATED",
      "items": [
        {"name": "Pollo a la brasa entero", "price": "45.9", "quantity": "1"},
        {"name": "Inca Kola 1L", "price": "8.5", "quantity": "2"}
      ],
      "total": "62.9",
      "createdAt": "2025-11-05T04:58:39.970480"
    }
  ]
}

🔎 Logs y monitoreo

Puedes ver los logs de cada Lambda con:

serverless logs -f createOrder


O directamente desde AWS CloudWatch Logs.

🧱 Tablas DynamoDB
CustomersTable
PK	SK	name	email	createdAt
TENANT#pardos#CUSTOMER#c1	INFO	Juan Pérez	juan@example.com
	...
OrdersTable
PK	SK	customerId	status	total	createdAt
TENANT#pardos#ORDER#o1762318719	INFO	c1	CREATED	62.9	...
