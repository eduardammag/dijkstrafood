import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

table_name = "CourierLocation"

try:
    existing_tables = dynamodb.list_tables()["TableNames"]

    if table_name not in existing_tables:
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "courier_id", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "courier_id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        print("Criando tabela DynamoDB...", response)
    else:
        print("Tabela DynamoDB já existe")

except Exception as e:
    print("Erro ao criar/verificar tabela DynamoDB:", e)