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
El trabajo consiste en hacer una web igual a la de Pardos Chicken (https://www.pardoschicken.pe/). Esto lo realizaremos con una arquitectura serverless usando Lambdas y un Event Bus. Esto sera complementado por Step Functions.
Tecnologias:
    1) Python 3
    2) AWS Amplify
    3) API Gateway
    4) EventBridge
    5) Step Functions
    6) Lambda
    7) DynamoDB
    8) S3
3. Eventos:
   a. Pedidos:
       1) OrderCreated
       2) OrderCancelled
   b. Etapas:
       1) OrderStageStarted
       2) OrderStageCompleted
       3) OrderStageUpdated
       4) OrderDelivered
   c. Cliente:
       1) CustomerRegistered
       2) CustomerLoggedIn (?)
   d. Otros:
       1) NotificationSent
       2) DashboardUpdated (?)
5. Microservicios
  a. Auth MS (Ignacio): Se encarga del registro y autenticacion de los usuarios con python y JWT
     Endpoints clave:
     1) POST /register
     2) POST /login
     3) GET /validate

     Tabla DynamoDB:
     UsersTable
     | Campo | Descripcion | Tipo |
     |-------|-------------|----|
     | PK | TENANT#pardos#USER#<username< | string |
     | username | nombre de usuario | string |
     | email | email del usuario | string |
     | passwordHash | contraseña cifrada mediante una funcion Hash | string |
     | customerId | referencia a la tabla de clientes | string |
     | createdAt | Fecha de creacion | timestamp |

     Ejemplo JSON:
     {
         "PK": "TENANT#pardos#USER#ignacio",
         "username": "ignacio",
         "email": "ignacio.vidal@utec.edu.pe",
         "passwordHash": "$2b$12$AbCdEf...",
         "customerId": "c1",
         "createdAt": "2025-10-30T18:00:00Z"
     }
   
  b. Cliente MS (Amir): Maneja el perfil del cliente y creacion de pedidos, ademas de encargarse de publicar eventos en el bus:
     Endpoints clave:
     1) POST /orders
     2) GET /orders/{customerId}
     3) GET /customers/{customerId}

Tabla DynamoDB:

 1. CustomersTable
      | Campo | Descripcion | Tipo | 
      |----------|---------------|--|
      | PK | TENANT#pardos#CUSTOMER#<customerId< | string |
      | userRef | Referencia a la tabla de usuario | string |
      | name | Nombre del cliente | string |
      | phone | Numero del cliente | string |
      | address | Direccion del cliente | string |
      | createdAt | Fecha de creacion | timestamp |
      
      Ejemplo JSON:
      {
        "PK": "TENANT#pardos#CUSTOMER#c1",
        "userRef": "TENANT#pardos#USER#ignacio",
        "name": "Ignacio Vidal",
        "phone": "999999999",
        "address": "Av. Javier Prado 123",
        "createdAt": "2025-10-25T15:00:00Z"
      }

 2. OrdersTable
	 | Campo | Descripcion | Tipo |
	 | ----- | ----- | ----- |
	 | PK | TENANT#pardos#ORDER#<orderId< | string |
	 | SK | INFO (marca que muestra info de la orden) | string |
	 | customerId | Identificador del cliente | string |
	 | status | Estado del pedido | string |
	 | items | Productos del pedido | lista de mapas |
	 | total | Monto del pedido | float |
	 | currentStep | Paso en el que se encuentra el pedido | string |
	 | createdAt | Fecha de creacion | timestamp |
     
     Subentidad:
     Items del pedido:
     | Campo | Descripcion | Tipo |
     |----------|---------------|-------|
     | productId | Id del producto | string |
     | qty | cantidad de este item | int |
     | price | precio del item | float |

     Ejemplo JSON:
     {
       "PK": "TENANT#pardos#ORDER#o1",
       "SK": "METADATA",
       "customerId": "c1",
       "status": "CREATED",
       "items": [
         {"productId": "pollo_1_4", "qty": 1, "price": 25.9},
         {"productId": "chicha_grande", "qty": 1, "price": 8.5}
       ],
       "total": 34.4,
       "currentStep": "CREATED",
       "createdAt": "2025-10-25T16:00:00Z"
     }

  c. Orquestador MS (Ignacio): Coordina el flujo de atencion del pedido
  Flujo de pedido (Step Functions):
  CREATED → COOKING → PACKAGING → DELIVERY → DELIVERED

  d. Estados MS (Ignacio): Ejecuta las tareas respecto de cada flujo, ademas de actualizar ordersTable y publica eventos.

Tabla DynamoDB:
StepsTable:
| Campo | Descripcion | Tipo |
|------|-----------|------|
| PK | TENANT#pardos#ORDER<orderId<| string |
| SK | STEP#<stepName<#<timeStamp< | string |
| stepName | COOKING / PACKAGING / DELIVERY | string |
| status | IN_PROGRESS / DONE | string |
| startedAt | Tiempo de inicio del pedido | timestamp |
| finishedAt | Tiempo final del pedido | timestamp |

Ejemplo JSON:
{
  "PK": "TENANT#pardos#ORDER#o1",
  "SK": "STEP#COOKING#20251025T160500Z",
  "stepName": "COOKING",
  "status": "DONE",
  "startedAt": "2025-10-25T16:05:00Z",
  "finishedAt": "2025-10-25T16:15:00Z",
}

   e. Dashboard MS (Ignacio): Genera metricas y reportes para el restaurante.
   Endpoints clave:
   1) GET /dashboard/summary
   2) GET /dashboard/orders
   3) GET /dashboard/stats

   f. Notificaciones MS (Ignacio): Envia notificaciones al cliente en base a los eventos que se publiquen.

3. Flujo completo del programa
    1) Un cliente con cuenta genera un pedido.
    2) Se publica un evento de pedido creado.
    3) El orquestador triggerea el step functions.
    4) Por cada etapa del pedido que se cumple, se actualiza su estado en las respectivas tablas.
    5) Se envian las notificaciones al cliente de cada etapa relevante del pedido.
    6) Se agregan las metricas al dashboard.
5. Frontend
Se realizaran 2 frontends, uno para el cliente y otro para el restaurante.
    1) Frontend Cliente (Julio): Este frontend debe ser igual a la pagina de Pardos Chicken (https://www.pardoschicken.pe/). Debe contar con una seccion para crear cuentas, una pestaña de la carta, una pestaña de promociones, y una pestaña de catering. En la parte de promociones no es necesario tener todas, con unas 2 o 3 bastan. Tambien debe tener una seccion de pedido, donde se veran las promos o productos de carta que se hayan agregado al carrito. Toda la logica de pago podemos saltarla, como poniendo una pequeña pagina intermedia entre la toma del pedido y la confirmacion del pago que diga algo como "Pedido siendo pagado", con fin de hacer la simulacion.Tambien debe haber una seccion de cuenta, donde el cliente podra cambiar sus datos de cuenta o revisarlos.
    2) Frontend Restaurante (Yuri): Este frontend es mas simple, debe contar con 2 pestañas principales, cualquier adicion se queda a criterio del encargado. La primera pestaña contenera un dashboard, dando info general sobre todos los pedidos mediante graficos y cosas asi, esta pestaña consumira el microservicio de Dashboard, el cual generara la mayoria de las metricas, la cosa es mas que nada como se muestra, preferiblemente en formato de graficos y tablas, obviamente que cuente con paginacion para que no explote la pagina. La segunda pestaña sera la pestaña de pedidos, ahi estaran todos los pedidos, con paginacion tambien. Aqui se podra cambiar el estado de cada pedido, marcandolo en cada etapa segun avance su preparacion. Los pedidos deben ordenarse en orden de llegada, osea que se ordenaran teniendo en cuenta cual lleva mas tiempo existiendo. En cada pedido, que aparecera en la pestaña de forma reducida, solo mostrando lo que contiene el pedido, debera poderse clickear para expandirlo y poder ver todos los detalles del pedido, desde los productos que contiene, hasta su costo, tiempos que se llevan manejando, estado actual, etc. En esta forma expandida tambien debera poder editarse el estado del pedido.


(Nota: Todos los MS que Ignacio esta trabajando son opcionales, sin contar el MS de autentiacion. Los ira trabajando por ahi, pero no seran necesarios para el trabajo final, es por amor al arte.)
