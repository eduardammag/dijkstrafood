import boto3
import os
from dotenv import load_dotenv

# -------------------------
# Carregar variáveis de ambiente do .env
# -------------------------
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")  # ✅ Necessário para assumed role

# -------------------------
# Criar cliente DynamoDB com token temporário
# -------------------------
dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

table_name = "OrdersRealtime"

try:
    # -------------------------
    # Verificar se a tabela já existe
    # -------------------------
    existing_tables = dynamodb.list_tables()["TableNames"]
    if table_name not in existing_tables:
        # -------------------------
        # Criar a tabela DynamoDB
        # -------------------------
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "order_id", "KeyType": "HASH"}  # Partition key
            ],
            AttributeDefinitions=[
                {"AttributeName": "order_id", "AttributeType": "S"}  # String
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )
        print("Criando tabela DynamoDB...", response)
    else:
        print("Tabela DynamoDB já existe")
except Exception as e:
    print("Erro ao criar/verificar tabela DynamoDB:", e)