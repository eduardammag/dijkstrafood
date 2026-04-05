import boto3
import os
import requests
from dotenv import load_dotenv

# -------------------------
# Carregar variáveis de ambiente do .env
# -------------------------
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")  # ✅ Importante!

# -------------------------
# Criar clientes RDS e EC2 usando token temporário
# -------------------------
rds = boto3.client(
    "rds",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

ec2 = boto3.client(
    "ec2",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

# -------------------------
# Configurações do RDS
# -------------------------
db_identifier = "dijkstrafood-db"
db_name = "dijkstrafood"

try:
    # Pegar IP público da máquina local para liberar acesso
    ip = requests.get("https://checkip.amazonaws.com").text.strip()

    # Pegar a VPC default
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    # Criar Security Group
    sg = ec2.create_security_group(
        GroupName="rds-security",
        Description="Access RDS from local IP",
        VpcId=vpc_id
    )
    sg_id = sg["GroupId"]

    # Liberar porta 3306 para seu IP
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[{
            "IpProtocol": "tcp",
            "FromPort": 5432,
            "ToPort": 5432,
            "IpRanges": [{"CidrIp": f"{ip}/32"}]
        }]
    )

    # Criar instância RDS PostgreSQL
    response = rds.create_db_instance(
        DBInstanceIdentifier=db_identifier,
        AllocatedStorage=20,          # 20GB
        DBName=db_name,
        Engine="postgres",
        MasterUsername='mariana',
        MasterUserPassword='senha123',
        DBInstanceClass="db.t3.micro", # Free tier
        PubliclyAccessible=True,
        VpcSecurityGroupIds=[sg_id],
    )

    print("Criando RDS PostgreSQL...", response)

except rds.exceptions.DBInstanceAlreadyExistsFault:
    print("RDS já existe")

except Exception as e:
    print("Erro ao criar RDS:", e)