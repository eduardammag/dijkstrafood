# DijkFood

## Visao Geral

O DijkFood e um sistema distribuido para demonstrar uma arquitetura de delivery em nuvem. A versao atual separa o que e sistema do que e simulacao:

- Servicos do sistema orquestram o fluxo do pedido por APIs HTTP.
- Simuladores executam comportamento artificial, como preparo, aceite operacional, movimentacao do entregador e mudancas temporizadas de status.
- A integracao de negocio entre componentes segue HTTP.
- Para observabilidade em tempo real, eventos de ciclo de vida de pedidos sao publicados em AWS Kinesis Data Streams.

## Arquitetura

Fluxo principal:

```text
Load Simulator
  -> API / Order Service
  -> Restaurant Simulator
  -> Delivery Service
  -> Restaurant Simulator (/deliveries)
  -> API / Order Service
  -> Kinesis Data Stream (eventos)
  -> Realtime Metrics Service (pipeline realtime)
  -> Redis Metrics Worker -> Redis (pipeline redis)
  -> Dashboard Web (tempo real)
```

Responsabilidades:

- `order-service`: API principal, persistencia, status, pedidos, usuarios, restaurantes, entregadores e localizacao.
- `restaurant-simulator`: simula aceite/rejeicao, preparo/tempo operacional, aciona o despacho e tambem simula o deslocamento do entregador (`/deliveries`).
- `delivery_service`: serviço de sistema que escolhe entregador, calcula rotas e atribui o pedido.
- `routing-service`: calcula rotas.
- `realtime-metrics-service`: consome eventos do Kinesis, agrega métricas em memória e expõe API + WebSocket para dashboard.
- `redis-metrics-worker`: consome o mesmo Kinesis em paralelo e grava snapshots no Redis/ElastiCache.
- `redis`: armazena snapshots do pipeline Redis para comparacao de latencia no dashboard.
- `simulator`: gerador de carga/populacao.

## Dashboard em Tempo Real

Foi adicionada uma camada isolada de observabilidade sem alterar a lógica de negócio existente.

Componente:

- `realtime-metrics-service` (FastAPI + boto3)
  - Consome continuamente eventos do Kinesis Data Stream.
  - Detecta automaticamente o formato dos eventos recebidos.
  - Mantém métricas em memória (sem DynamoDB para métricas realtime).
  - API HTTP:
    - `GET /metrics`
    - `GET /metrics/realtime-rollup`
    - `GET /health/redis`
    - `GET /dashboard`
  - WebSocket:
    - `GET /ws`

Indicadores implementados:

- Pedidos em `PREPARING`
- Pedidos aguardando entregador
- Pedidos em `DELIVERING` (mapeado de `PICKED_UP`, `IN_TRANSIT` e `DELIVERING`)
- Pedidos concluidos (`DELIVERED`)
- Pedidos criados por minuto
- Total de pedidos processados
- Latencia media de ingestao (evento -> consumer, janela de 1 minuto)
- Latencia ponta a ponta no dashboard (evento -> browser)

Observacao sobre autodeteccao de formato:

- O serviço identifica automaticamente o formato dos eventos do stream por heurística de campos.
- Formatos atualmente suportados:
  - `flat_order_event` (ex.: `order_id`, `event_type`, `from_status`, `to_status`)
  - `status_update` (ex.: `order_id`, `status`)
  - `nested_order` (ex.: `order.order_id`, `order.order_status`)
  - `courier_availability` (ex.: `courier_id`, `is_available`)
  - `courier_location` (ex.: `courier_id`, `latitude`, `longitude`, `order_id`)
- O resultado da autodeteccao fica disponível em `meta.detected_event_formats` dentro de `GET /metrics`.

## Deploy

O deploy automatizado usa:

```bash
python deploy.py --config config.json --run-simulator --scenario normal
```

## Pipeline analitico

Quando `analytics.enabled` esta `true` no `config.json`, o deploy tambem cria:

```text
Order Service
  -> Kinesis Data Stream
  -> Kinesis Firehose
  -> S3
  -> Glue Data Catalog
  -> Athena
```

O `order-service` publica cada registro de `order_events` no Kinesis. O Firehose grava esses eventos no S3 particionados por `year/month/day/hour`, e a tabela externa do Glue fica disponivel para consultas no Athena.

Exemplo de consulta:

```sql
SELECT
  event_status,
  count(*) AS total
FROM dijkfood_demo_analytics.order_events
WHERE year = '2026'
  AND month = '06'
  AND day = '02'
GROUP BY event_status
ORDER BY total DESC;
```

Imagens esperadas pelo `config.json`:

```text
marimarifr/dijkstrafood-api:latest
marimarifr/dijkstrafood-restaurant-simulator:latest
marimarifr/dijkstrafood-delivery-service:latest
marimarifr/dijkstrafood-routing-service:latest
```

Build/push das imagens:

```bash
docker build -t marimarifr/dijkstrafood-api:latest ./order-service
docker build -t marimarifr/dijkstrafood-restaurant-simulator:latest ./restaurant-simulator
docker build -t marimarifr/dijkstrafood-delivery-service:latest ./delivery-service
docker build -t marimarifr/dijkstrafood-routing-service:latest -f ./delivery-service/routing_service/Dockerfile ./delivery-service
docker build -t marimarifr/dijkstrafood-realtime-metrics-service:latest ./realtime_metrics_service

docker push marimarifr/dijkstrafood-api:latest
docker push marimarifr/dijkstrafood-restaurant-simulator:latest
docker push marimarifr/dijkstrafood-delivery-service:latest
docker push marimarifr/dijkstrafood-routing-service:latest
docker push marimarifr/dijkstrafood-realtime-metrics-service:latest
```

O ambiente local usa:

```bash
docker compose up --build
```

Observacao:

- O `routing-service` deve subir como modulo `routing_service.app:app`, entao os imports internos do pacote precisam ser relativos/qualificados pelo pacote.

No ambiente local, o `docker-compose.yml` inclui LocalStack para Kinesis, Redis para snapshots realtime e um container de inicializacao (`kinesis-init`) para criar o stream automaticamente.

Dashboard e API de métricas:

- Dashboard: `http://localhost:8010/dashboard`
- Métricas JSON: `http://localhost:8010/metrics`

Variáveis de ambiente para o `realtime-metrics-service`:

- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` (opcional)
- `KINESIS_STREAM_NAME`
- `KINESIS_ENDPOINT_URL` (opcional, usado no local com LocalStack)
- `KINESIS_ITERATOR_TYPE` (opcional, default `LATEST`)
- `KINESIS_POLL_INTERVAL_SECONDS` (opcional, default `1`)
- `KINESIS_RECORDS_LIMIT` (opcional, default `500`)
- `REDIS_URL` (opcional; local: `redis://redis:6379/0`)
- `REDIS_KEY_PREFIX` (opcional, default `dijkfood:realtime`)
- `REDIS_SNAPSHOT_TTL_SECONDS` (opcional, default `120`)

Variáveis de ambiente adicionais no `order-service` para publicação de eventos:

- `KINESIS_ENABLED` (default `true`)
- `KINESIS_STREAM_NAME`
- `KINESIS_ENDPOINT_URL` (opcional)

Para mandar somente 1 pedido pelo simulador:

```bash
cd simulator
python main.py --scenario teste
```

Para medir latencia realtime (p50/p95) automaticamente:

```bash
cd simulator
python latency_benchmark.py --scenario teste --sample-interval 1 --post-capture-seconds 15 --export-json
```

## AWS - Recursos Necessários

Para o dashboard realtime funcionar em EC2 ou ECS, os recursos abaixo precisam existir:

1. Kinesis Data Stream com nome igual à variável `KINESIS_STREAM_NAME`.
2. Credenciais/IAM com permissões mínimas:
  - `kinesis:DescribeStream`
  - `kinesis:ListShards`
  - `kinesis:GetShardIterator`
  - `kinesis:GetRecords`
3. Rede liberada para saída HTTPS do container para endpoint do Kinesis.

Deploy em EC2:

- Executar o container `realtime-metrics-service` com as variáveis acima.

Deploy em ECS:

- Publicar a imagem Docker do `realtime-metrics-service`.
- Criar task/service ECS com porta `8010`.
- Injetar as variáveis de ambiente e anexar role com permissões de leitura no Kinesis.

## Configuracao de Deploy

Os arquivos `config.json`, `config_req.json` e `config.json.example` agora suportam:

- `dockerhub_images.realtime_metrics_service`
- bloco `kinesis` (nome do stream, shard count e parametros do consumer)
- bloco `redis` (cria ElastiCache Redis por default; use `url` para um Redis externo)
- `ecs.desired_count_realtime_metrics_service`
- `ecs.desired_count_redis_metrics_worker`
- `autoscaling.realtime_metrics_service`
- `autoscaling.redis_metrics_worker`

O `deploy.py` cria automaticamente o stream Kinesis, o Redis/ElastiCache, o worker Kinesis -> Redis e publica o endpoint do dashboard em `http://<alb>:8010/dashboard`.
