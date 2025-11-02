## PF-PardosServerless

# Enunciado del trabajo

Requerimientos:
El cliente coloca un pedido de comida desde una aplicación web de clientes, donde también puede ver el estado de atención de su pedido.
Flujo de Trabajo (Workflow) atendido desde una aplicación web para el restaurante:
  - El restaurante recibe el pedido de comida y lo atiende en orden de llegada.
  - Un cocinero toma un pedido de comida y cocina o prepara la comida.
  - Un despachador coloca la comida preparada en envases y lo empaca.
  - Un repartidor toma la comida empacada y la lleva al cliente. • El cliente recibe la comida empacada.
Conocer en todo momento, en la aplicación web para el restaurante, cual es el estado del Flujo de Trabajo de cada pedido de comida, los tiempos de inicio y fin de cada paso y quienes lo atendieron. También elaborar un dashboard resumen. 

Consideraciones para la solución:
  - Utilice Arquitectura Multi-tenancy, serverless y basada en eventos. Incluya como mínimo 3 microservicios. Implemente un Flujo de Trabajo. Utilice framework serverless para despliegue del backend.
  - Debe utilizar obligatoriamente como mínimo estos servicios de AWS: Amplify, Api Gateway, EventBridge (*), Step Functions (*), Lambda, DynamoDB y S3. (*): Investigue cómo se usan.

# Solucion

1. Tecnologias y Descripcion
2. Microservicios
  a. Auth MS (Ignacio): Se encarga del registro y autenticacion de los usuarios con python y JWT
     Endpoints clave:
     1) POST /register
     2) POST /login
     3) GET /validate

     Tabla DynamoDB:
     | Campo | Descripcion |
     |-------|-------------|
     | PK | TENANT#pardos#USER#<username |
     | username | nombre de usuario |
     | passwordHash | contraseña cifrada mediante una funcion Hash |
     | customerId | referencia a la tabla de clientes |
     | createdAt | Fecha de creacion |

     Ejemplo JSON:
     {
         "PK": "TENANT#pardos#USER#ignacio",
         "username": "ignacio",
         "passwordHash": "$2b$12$AbCdEf...",
         "role": "CLIENTE",
         "customerId": "c999",
         "createdAt": "2025-10-30T18:00:00Z"
     }
  b. 
