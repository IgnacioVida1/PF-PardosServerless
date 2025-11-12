# Pardos Chicken - Microservicio de Pedidos (Serverless)
# Arquitectura Event-Driven con AWS Step Functions, EventBridge y DynamoDB

## 🧩 Visión General
Este microservicio gestiona clientes y pedidos en un sistema **100% serverless**, inspirado en Taco Bell.  
Al crear un pedido:
- Se guarda en **DynamoDB**
- Se publica un evento en **EventBridge**
- **Step Functions** ejecuta el flujo:  
  `COOKING → PACKAGING → DELIVERY → DELIVERED`

---

## 🏗️ Arquitectura
[API Gateway] 
│
├── POST /orders → Lambda (create_order) → DynamoDB + EventBridge  
│
└── EventBridge → Rule → Step Functions (OrderWorkflow)
   │  
   ├── Lambda: process_cooking  
   ├── Lambda: process_packaging  
   ├── Lambda: process_delivery  
   └── Lambda: process_delivered → DynamoDB  

---

## 🌐 Endpoints (HTTP API)

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST   | /customers | Crear cliente |
| GET    | /customers/{customerId} | Ver cliente |
| POST   | /orders | Crear pedido (dispara flujo) |
| GET    | /orders/{customerId} | Ver pedidos del cliente |
| GET    | /order/{orderId} | Ver pedido + cliente + pasos |

---

## ☁️ Componentes AWS

| Servicio | Nombre | Función |
|----------|--------|---------|
| DynamoDB | CustomersTable | Clientes |
| DynamoDB | OrdersTable | Pedidos (PK, SK=INFO) |
| DynamoDB | StepsTable | Historial de pasos |
| EventBridge | PardosEventBus | Eventos |
| Step Functions | OrderWorkflow | Flujo de cocina |
| IAM Role | LabRole | Permisos |

---

## 🔄 Flujo de un Pedido

API Gateway  
→ Lambda create_order  
→ DynamoDB  
→ EventBridge  
→ Step Functions  
  → process_cooking  
  → process_packaging  
  → process_delivery  
  → process_delivered  
  → DynamoDB (status=COMPLETED)

---

## 🚀 Pruebas Rápidas (cURL)

# 1️⃣ Crear cliente
curl -X POST https://<api-gateway-url>/customers \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "c100",
    "name": "Ana López",
    "email": "ana@utec.edu.pe"
  }'

# 2️⃣ Crear pedido
curl -X POST https://<api-gateway-url>/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "c100",
    "items": [
      {"productId": "pollo_1_4", "qty": 1, "price": 25.9},
      {"productId": "chicha", "qty": 1, "price": 8.5}
    ],
    "total": 34.4
  }'

# → Respuesta: {"orderId": "o1738795678"}

# 3️⃣ Esperar 15 segundos (flujo automático)

# 4️⃣ Ver pedido completo
curl https://<api-gateway-url>/order/o1738795678

# ✅ Respuesta esperada:
# {
#   "orderId": "o1738795678",
#   "status": "COMPLETED",
#   "currentStep": "DELIVERED",
#   "total": 34.4,
#   "customer": {"name": "Ana López"},
#   "steps": ["COOKING", "PACKAGING", "DELIVERY", "DELIVERED"]
# }

---

## 🔍 Verificación Manual

# OrdersTable
aws dynamodb get-item --table-name OrdersTable \
  --key '{"PK": {"S": "TENANT#pardos#ORDER#o1738795678"}, "SK": {"S": "INFO"}}'

# StepsTable (4 pasos)
aws dynamodb query --table-name StepsTable \
  --key-condition-expression "PK = :pk" \
  --expression-attribute-values '{":pk": {"S": "TENANT#pardos#ORDER#o1738795678"}}'

# Step Functions
# Ver en consola AWS Step Functions → OrderWorkflow → ejecución con 4 pasos en verde

---

## ⚙️ Despliegue

npm install -g serverless
sls deploy --stage dev

---

## 📁 Estructura de Archivos

.
├── handler.py          ← Lógica de Lambdas  
├── serverless.yml      ← Infraestructura como código  
└── README.md           ← Este archivo  

---

## 🧠 Tecnologías Usadas

| Tecnología | Uso |
|------------|-----|
| Python 3.13 | Lambdas |
| AWS Lambda | Funciones |
| API Gateway (HTTP API) | Endpoints |
| DynamoDB | Base de datos NoSQL |
| EventBridge | Eventos |
| Step Functions | Orquestación |
| Serverless Framework | Despliegue |

---

## ⚠️ Notas Importantes

- `status` es palabra reservada → usar `#st` en UpdateExpression  
- `Decimal` → convertir a `float` solo para EventBridge  
- `GET /order/{id}` → evita conflicto con `/orders/{customerId}`  
- `ScanIndexForward=True` → ordena por SK (usa timestamp)

---

## 🏁 Estado Final
✅ Flujo automático  
✅ Sin errores  
✅ 100% serverless  
✅ Escalable y mantenible  

**Autor:** Amir Ykehara  
**Fecha:** 08 de noviembre de 2025  
**Estado:** COMPLETADO 🚀
