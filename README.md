# PF-PardosServerless

Requerimientos:
El cliente coloca un pedido de comida desde una aplicación web de clientes, donde también puede ver el estado de atención de su pedido.
Flujo de Trabajo (Workflow) atendido desde una aplicación web para el restaurante:
  - El restaurante recibe el pedido de comida y lo atiende en orden de llegada.
  - Un cocinero toma un pedido de comida y cocina o prepara la comida.
  - Un despachador coloca la comida preparada en envases y lo empaca.
  - Un repartidor toma la comida empacada y la lleva al cliente. • El cliente recibe la comida empacada.
Conocer en todo momento, en la aplicación web para el restaurante, cual es el estado del Flujo de Trabajo de cada pedido de comida, los tiempos de inicio y fin de cada paso y quienes lo atendieron. También elaborar un dashboard resumen. 

Consideraciones para la solución:
  • Utilice Arquitectura Multi-tenancy, serverless y basada en eventos. Incluya como mínimo 3 microservicios. Implemente un Flujo de Trabajo. Utilice framework serverless para despliegue del backend.
  • Debe utilizar obligatoriamente como mínimo estos servicios de AWS: Amplify, Api Gateway, EventBridge (*), Step Functions (*), Lambda, DynamoDB y S3. (*): Investigue cómo se usan.

Backend clientes: 
  - Microservicio de ordenes: crear pedidos y registrar en DynamoDB
  - Microservicio de estados: mantener actualizado el estado de los pedidos para el cliente
  - Microservicio de notificacion: enviar notificaciones al cliente (email, push o in-app) cuando cambie el estado de su pedido.
Backend restaurante:
  - Microservicio orquestador: coordinar el ciclo de vida del pedido
  - Microservicio etapas: actualizar el estado o ciclo de vida del pedido mediante los eventos q se usen
  - Microservicio Dashboard: generar métricas y resumen visual para el restaurante Recordar que ambos usaran estructuras multitenancy con dynamoDB.
Frontend Clientes: Igualito al de esta pagina https://www.pardoschicken.pe/
Frontend Restaurante: algo asi como el frontend de nuestro anterior trabajo, pero un poco mas entrado en la tematica de pardos
