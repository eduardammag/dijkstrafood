import boto3
import os
import requests
from dotenv import load_dotenv

load_dotenv()

rds = boto3.client(
    "rds",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

ec2 = boto3.client(
    "ec2",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

db_identifier = "dijkstrafood-db"
db_name = "dijkstrafood"

try:

    ip = requests.get("https://checkip.amazonaws.com").text.strip()

    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    sg = ec2.create_security_group(
        GroupName="rds-security",
        Description="access",
        VpcId=vpc_id
    )
    sg_id = sg["GroupId"]

    ec2.authorize_security_group_ingress(
    GroupId=sg_id,
    IpPermissions=[{
        "IpProtocol": "tcp",
        "FromPort": 3306,
        "ToPort": 3306,
        "IpRanges": [{"CidrIp": f"{ip}/32"}]
    }]
)

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