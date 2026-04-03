import boto3
import os
from dotenv import load_dotenv

load_dotenv()

rds = boto3.client(
    "rds",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

db_identifier = "dijkstrafood-db"
db_name = "dijkstrafood"

try:
    response = rds.create_db_instance(
        DBInstanceIdentifier=db_identifier,
        AllocatedStorage=20,          # 20GB
        DBName=db_name,
        Engine="postgres",
        MasterUsername=os.getenv("DB_USER"),
        MasterUserPassword=os.getenv("DB_PASSWORD"),
        DBInstanceClass="db.t3.micro", # Free tier
        PubliclyAccessible=True
    )
    print("Criando RDS PostgreSQL...", response)
except rds.exceptions.DBInstanceAlreadyExistsFault:
    print("RDS já existe")