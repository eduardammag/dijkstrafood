# DijkFood

## Visao Geral

O DijkFood e um sistema distribuido para demonstrar uma arquitetura de delivery em nuvem. A versao atual separa o que e sistema do que e simulacao:

- Servicos do sistema orquestram o fluxo do pedido por APIs HTTP.
- Simuladores executam comportamento artificial, como preparo, aceite operacional, movimentacao do entregador e mudancas temporizadas de status.
- A solucao final nao usa RabbitMQ, PubSub ou broker. Toda integracao entre componentes e HTTP.

## Arquitetura

Fluxo principal:

```text
Load Simulator
  -> API / Order Service
  -> Restaurant Simulator
  -> Delivery Service
  -> Courier Simulator
  -> API / Order Service
```

Responsabilidades:

- `order-service`: API principal, persistencia, status, pedidos, usuarios, restaurantes, entregadores e localizacao.
- `restaurant-simulator`: simula aceite/rejeicao, preparo/tempo operacional e aciona o despacho quando o pedido começa a ser preparado.
- `delivery_service`: serviço de sistema que escolhe entregador, calcula rotas e atribui o pedido.
- `routing-service`: calcula rotas.
- `courier-simulator`: simula deslocamento do entregador, atualiza localizacao e status de entrega.
- `simulator`: gerador de carga/populacao.

## Deploy

O deploy automatizado usa:

```bash
python deploy.py --config config.json
```

Imagens esperadas pelo `config.json`:

```text
marimarifr/dijkstrafood-api:latest
marimarifr/dijkstrafood-restaurant-simulator:latest
marimarifr/dijkstrafood-delivery-service:latest
marimarifr/dijkstrafood-routing-service:latest
marimarifr/dijkstrafood-courier-simulator:latest
```

Build/push das imagens:

```bash
docker build -t marimarifr/dijkstrafood-api:latest ./order-service
docker build -t marimarifr/dijkstrafood-restaurant-simulator:latest ./restaurant-simulator
docker build -t marimarifr/dijkstrafood-delivery-service:latest ./delivery-service
docker build -t marimarifr/dijkstrafood-routing-service:latest -f ./delivery-service/routing_service/Dockerfile ./delivery-service
docker build -t marimarifr/dijkstrafood-courier-simulator:latest ./courier-simulator

docker push marimarifr/dijkstrafood-api:latest
docker push marimarifr/dijkstrafood-restaurant-simulator:latest
docker push marimarifr/dijkstrafood-delivery-service:latest
docker push marimarifr/dijkstrafood-routing-service:latest
docker push marimarifr/dijkstrafood-courier-simulator:latest
```

O ambiente local usa:

```bash
docker compose up --build
```

Para mandar somente 1 pedido pelo simulador:

```bash
cd simulator
python main.py --scenario teste
```
