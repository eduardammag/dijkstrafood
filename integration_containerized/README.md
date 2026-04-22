# README – DijkFood

## Visão Geral

O DijkFood é um sistema distribuído que simula uma plataforma de delivery sob demanda, projetado para demonstrar conceitos de computação em nuvem, incluindo:

- arquitetura de microsserviços
- processamento assíncrono com mensageria
- escalabilidade horizontal
- tolerância a falhas
- uso de serviços gerenciados na AWS

O sistema conecta clientes, restaurantes e entregadores, executando todo o ciclo de vida de um pedido, desde a criação até a entrega final.

---

## Arquitetura

O sistema segue um modelo **orientado a eventos com comunicação híbrida**:

- **Assíncrona (RabbitMQ)** → processamento do pedido  
- **Síncrona (HTTP)** → consultas e localização  

### Deploy

O deploy automatizado e destruição dos recursos é feito via script:

```python deploy.py --config config.json --run-simulator --destroy-on-finish```

O script realiza:
* criação de recursos AWS
* build e deploy de containers
* configuração de serviços ECS
* setup de RDS e DynamoDB
* provisionamento do RabbitMQ

### Pipeline

```
Cliente → API → RabbitMQ → Restaurant Worker → RabbitMQ → Courier Worker → API
                                                  ↓
                                            Routing Service