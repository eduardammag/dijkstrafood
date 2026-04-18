# Deploy do DijkFood na AWS com boto3

Esse pacote foi montado em cima do que você já tem localmente: API FastAPI, workers, RabbitMQ, Postgres e DynamoDB opcional. A solução foi organizada para ficar próxima do enunciado: `deploy.py` sobe os recursos, publica os serviços e deixa uma URL pronta para a apresentação.

## O que este pacote sobe

- **ECS Fargate** para:
  - api
  - restaurant-worker
  - delivery-service
  - routing-service
  - courier-worker
- **ALB** público para expor a API
- **RDS PostgreSQL** para persistência relacional
- **DynamoDB** para localização do entregador
- **EC2** rodando RabbitMQ em container Docker
- **Cloud Map** para descoberta interna entre containers
- **CloudWatch Logs** para logs dos serviços
- **Auto Scaling** na API por CPU

## Arquitetura escolhida

- A API fica exposta por ALB.
- Os workers não ficam públicos; só conversam internamente com a API via Cloud Map.
- O RabbitMQ ficou em EC2 porque seu projeto já usa RabbitMQ diretamente. Para a apresentação, isso reduz refactor.
- O banco continua PostgreSQL porque seu código já sobe schema e seed no startup da API.
- O DynamoDB continua separado porque o seu código já espera esse uso para localização do courier.

## Arquivos

- `deploy.py`: sobe tudo
- `destroy.py`: apaga tudo
- `config.example.json`: modelo de configuração
- `deployment_state.json`: gerado automaticamente depois do deploy

## Antes de rodar

### 1. Configure as credenciais AWS

No Windows PowerShell:

```powershell
aws configure
```

As credenciais devem ficar em `~/.aws/credentials`, como o enunciado pede. fileciteturn1file1

### 2. Suba as imagens para o Docker Hub

Você disse que essa parte faz por fora. Então só precisa garantir que existam essas imagens:

- `SEU_USUARIO/dijkstrafood-api:latest`
- `SEU_USUARIO/dijkstrafood-restaurant-worker:latest`
- `SEU_USUARIO/dijkstrafood-delivery-service:latest`
- `SEU_USUARIO/dijkstrafood-routing-service:latest`
- `SEU_USUARIO/dijkstrafood-courier-worker:latest`

### 3. Crie seu config real

Copie o exemplo:

```powershell
copy config.example.json config.json
```

Depois ajuste:

- `dockerhub_images`
- `your_ip_cidr`
- senha do Postgres
- AMI da EC2, se necessário
- roles IAM, se seu laboratório bloquear criação automática

## Rodar deploy

```powershell
python deploy.py --config config.json
```

O script:

1. usa a VPC default
2. cria security groups
3. cria DynamoDB
4. cria RDS
5. sobe RabbitMQ em EC2
6. cria ECS cluster + Cloud Map
7. registra task definitions
8. cria serviços ECS
9. cria ALB
10. espera a API responder

No final ele grava `deployment_state.json` com tudo que foi criado.

## Rodar a demonstração

Quando o deploy terminar, pegue a URL em `api_url` no `deployment_state.json`.

Teste básico:

### Health

```powershell
Invoke-RestMethod -Method GET "http://SEU-ALB-DNS/"
```

### Criar pedido

```powershell
Invoke-RestMethod -Method POST "http://SEU-ALB-DNS/orders" `
  -ContentType "application/json" `
  -Body '{"client_id":1,"restaurant_id":1,"items":[{"name":"Pizza","quantity":1},{"name":"Refrigerante","quantity":2}]}'
```

### Consultar pedido

```powershell
Invoke-RestMethod -Method GET "http://SEU-ALB-DNS/orders/3"
```

## Logs

No console da AWS:

- ECS > cluster > services > tasks
- CloudWatch > Log groups

Ou por CLI:

```powershell
aws logs tail /ecs/dijkfood-demo/api --follow
```

## Destruir tudo

```powershell
python destroy.py
```

## Pontos importantes

O enunciado pede que o `deploy.py` crie os recursos, faça o deploy, execute o experimento de carga e destrua tudo ao final. fileciteturn1file1 Esse pacote cobre a parte de infraestrutura e deploy. O simulador de carga ainda precisa ser encaixado por você dentro desse fluxo, porque o zip enviado não veio com esse script pronto.

Também tem um detalhe prático: em conta de estudante/lab, algumas permissões IAM e RDS podem vir bloqueadas por política do laboratório. Se acontecer `AccessDenied`, o problema não é do Python em si; é limitação da conta. Nesse caso, o mais comum é:

- usar roles já existentes do laboratório em `config.json`
- reduzir recursos para `t3.micro` e `db.t3.micro`
- manter tudo em `us-east-1`

## O que falta para fechar 100%

Para ficar alinhado com o enunciado e com a apresentação, ainda falta encaixar:

- simulador de carga automático
- script de coleta de latência e throughput
- roteiro de teste de tolerância a falha
- relatório com arquitetura, custos e resultados

O enunciado cobra deploy automatizado, sistema implantado na hora da apresentação, escalabilidade e tolerância a falhas. fileciteturn1file2
