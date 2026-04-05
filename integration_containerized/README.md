# DijkstraFood containerizado

Este pacote já vem pronto para subir com Docker Compose.

## O que foi ajustado

- Adicionado `docker-compose.yml`
- Adicionados `Dockerfile` para todos os serviços
- Adicionado `Postgres` local no compose
- `RabbitMQ` já sobe no compose
- `USE_DYNAMO=false` por padrão para rodar sem AWS
- `routing-service` corrigido para funcionar em container
- lógica de rota ajustada para usar `networkx` diretamente, sem depender de `osmnx`
- workers com retry de conexão no RabbitMQ
- courier configurado para ignorar erro de localização quando Dynamo estiver desligado

## Estrutura

- `api/` -> API principal FastAPI
- `restaurant-worker/` -> worker do restaurante
- `delivery_service/` -> serviço que aloca entregador
- `delivery_service/routing_service/` -> serviço de rota
- `courier-worker/` -> worker do entregador
- `docker-compose.yml` -> sobe tudo junto

## Como rodar

### 1. Entre na pasta do projeto

```bash
cd integration_containerized
```

### 2. Suba tudo

```bash
docker compose up --build
```

### 3. Verifique se os containers subiram

```bash
docker compose ps
```

Você deve ver algo como:

- `dijkstrafood-postgres`
- `dijkstrafood-rabbitmq`
- `dijkstrafood-api`
- `dijkstrafood-restaurant-worker`
- `dijkstrafood-delivery-service`
- `dijkstrafood-routing-service`
- `dijkstrafood-courier-worker-1`

## Portas

- API: `http://localhost:8000`
- Delivery service: `http://localhost:8001`
- Routing service: `http://localhost:8002`
- RabbitMQ painel: `http://localhost:15672`
  - usuário: `guest`
  - senha: `guest`

## Teste rápido

### Criar pedido

No PowerShell:

```powershell
Invoke-RestMethod -Method POST "http://localhost:8000/orders" `
  -ContentType "application/json" `
  -Body '{"client_id":1,"restaurant_id":1,"items":[{"name":"Pizza","quantity":1},{"name":"Refrigerante","quantity":2}]}'
```

Ou com curl:

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"client_id":1,"restaurant_id":1,"items":[{"name":"Pizza","quantity":1},{"name":"Refrigerante","quantity":2}]}'
```

Resposta esperada:

```json
{
  "message": "Order created successfully",
  "order_id": 3
}
```

### Consultar pedido

```bash
curl http://localhost:8000/orders/3
```

O fluxo esperado é:

1. API cria pedido com status `pending`
2. restaurant-worker consome a fila e muda para:
   - `confirmed`
   - `preparing`
   - `ready_for_delivery`
3. delivery-service recebe o evento
4. API associa um courier
5. courier-worker consome a fila e muda para:
   - `picked_up`
   - `in_transit`
   - `delivered`

## Ver logs

Todos os logs:

```bash
docker compose logs -f
```

Só da API:

```bash
docker compose logs -f api
```

Só do entregador:

```bash
docker compose logs -f courier-worker-1
```

## Parar tudo

```bash
docker compose down
```

Para remover também o volume do Postgres:

```bash
docker compose down -v
```

## Observações

- Esse pacote está preparado para rodar localmente sem DynamoDB.
- O endpoint de localização continua existindo, mas o worker do courier ignora esse erro quando o Dynamo está desligado.
- Se depois você quiser plugar AWS de novo, basta ajustar as variáveis da API.
