import boto3
import os
from dotenv import load_dotenv

load_dotenv()

dynamodb = boto3.client(
    "dynamodb",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

table_name = "OrdersRealtime"

# Criar a tabela se não existir
existing_tables = dynamodb.list_tables()["TableNames"]
if table_name not in existing_tables:
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