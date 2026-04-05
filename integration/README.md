Subir o docker de raabitMQ: 
```
docker run -d --hostname rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Abra um terminal para cada módulo, nesta ordem:

Abra a pasta `integration/api`, no terminal da api rode: 

```
python -m uvicorn main:app --port 8000

```


Abra a pasta `integration/restaurant-worker`, no terminal rode: 

```
python .\restaurant_worker.py

```


Abra a pasta `integration/delivery_service`, no terminal rode: 

```
python -m uvicorn app:app --port 8001

```

Abra a pasta `integration/courier-worker`, no terminal rode: 

```
set COURIER_ID=1
python courier_worker.py

```

Poste um novo pedido com: curl -X POST http://localhost:8000/orders -H "Content-Type: application/json" -d "{\"client_id\":1,\"restaurant_id\":1,\"items\":[{\"name\":\"Pizza\",\"quantity\":1},{\"name\":\"Refrigerante\",\"quantity\":14}]}"