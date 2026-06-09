import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
import subprocess
import requests

import boto3
from botocore.exceptions import ClientError, WaiterError


ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "deployment_state.json"



def wait_for_api_stable(api_base_url: str, timeout_seconds: int = 180):
    deadline = time.time() + timeout_seconds
    consecutive_success = 0

    # Endpoints que realmente existem e servem para health check
    endpoints = ["/", "/health/full"]

    while time.time() < deadline:
        all_ok = True
        last_errors = []

        for path in endpoints:
            url = f"{api_base_url.rstrip('/')}{path}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code != 200:
                    all_ok = False
                    last_errors.append(f"{path} -> HTTP {r.status_code}: {r.text[:200]}")
                    break
            except Exception as e:
                all_ok = False
                last_errors.append(f"{path} -> {type(e).__name__}: {e}")
                break

        if all_ok:
            consecutive_success += 1
            print(f"[deploy] API estável: {consecutive_success}/5")
            if consecutive_success >= 5:
                return
        else:
            consecutive_success = 0
            if last_errors:
                print(f"[deploy] Health check falhou: {last_errors[0]}")

        time.sleep(5)

    raise RuntimeError("API não estabilizou a tempo")

def log(msg: str):
    print(f"[deploy] {msg}", flush=True)


class Deployer:
    def __init__(self, config: Dict):
        self.config = config
        self.region = config["region"]
        self.project = config["project_name"]
        self.session = boto3.Session(region_name=self.region)
        self.ec2 = self.session.client("ec2")
        self.rds = self.session.client("rds")
        self.ecs = self.session.client("ecs")
        self.elbv2 = self.session.client("elbv2")
        self.logs = self.session.client("logs")
        self.iam = self.session.client("iam")
        self.sts = self.session.client("sts")
        self.sd = self.session.client("servicediscovery")
        self.ddb = self.session.client("dynamodb")
        self.kinesis = self.session.client("kinesis")
        self.s3 = self.session.client("s3")
        self.kinesis = self.session.client("kinesis")
        self.firehose = self.session.client("firehose")
        self.glue = self.session.client("glue")
        self.application_autoscaling = self.session.client("application-autoscaling")
        self.state = {"project": self.project, "region": self.region}

    def save_state(self):
        STATE_FILE.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def ensure_default_vpc(self):
        log("Buscando VPC default")
        resp = self.ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpcs = resp.get("Vpcs", [])
        if not vpcs:
            raise RuntimeError("Nenhuma VPC default encontrada.")
        vpc_id = vpcs[0]["VpcId"]
        self.state["vpc_id"] = vpc_id

        subnets = self.ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
        subnet_ids = [s["SubnetId"] for s in subnets]
        azs = sorted({s["AvailabilityZone"] for s in subnets})
        if len(subnet_ids) < 2:
            raise RuntimeError("Precisa de pelo menos 2 subnets na VPC default.")

        self.state["subnet_ids"] = subnet_ids
        self.state["availability_zones"] = azs
        log(f"VPC={vpc_id} | subnets={subnet_ids}")

    def _find_sg_by_name(self, group_name: str) -> Optional[str]:
        resp = self.ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [group_name]},
                {"Name": "vpc-id", "Values": [self.state["vpc_id"]]},
            ]
        )
        groups = resp.get("SecurityGroups", [])
        return groups[0]["GroupId"] if groups else None

    def _ensure_ingress(self, group_id: str, permissions: List[Dict]):
        for permission in permissions:
            try:
                self.ec2.authorize_security_group_ingress(GroupId=group_id, IpPermissions=[permission])
            except ClientError as e:
                if "InvalidPermission.Duplicate" not in str(e):
                    raise

    def create_security_groups(self):
        vpc_id = self.state["vpc_id"]
        your_ip_cidr = self.config["your_ip_cidr"]

        sgs = {}
        definitions = {
            "alb": "ALB publico do DijkFood",
            "ecs": "Tarefas ECS do DijkFood",
            "rds": "Banco RDS do DijkFood",
        }

        for key, desc in definitions.items():
            name = f"{self.project}-{key}-sg"
            sg_id = self._find_sg_by_name(name)
            if not sg_id:
                sg_id = self.ec2.create_security_group(
                    GroupName=name,
                    Description=desc,
                    VpcId=vpc_id,
                )["GroupId"]
            sgs[key] = sg_id

        self._ensure_ingress(
            sgs["alb"],
            [
                {"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": your_ip_cidr}]},
                {"IpProtocol": "tcp", "FromPort": 8000, "ToPort": 8010, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
                {"IpProtocol": "tcp", "FromPort": 8000, "ToPort": 8010, "UserIdGroupPairs": [{"GroupId": sgs["ecs"]}]},
            ],
        )
        self._ensure_ingress(
            sgs["ecs"],
            [{
                "IpProtocol": "tcp",
                "FromPort": 8000,
                "ToPort": 8010,
                "UserIdGroupPairs": [{"GroupId": sgs["alb"]}, {"GroupId": sgs["ecs"]}],
            }],
        )
        self._ensure_ingress(
            sgs["rds"],
            [{
                "IpProtocol": "tcp",
                "FromPort": 5432,
                "ToPort": 5432,
                "UserIdGroupPairs": [{"GroupId": sgs["ecs"]}],
            }],
        )
        self.state["security_groups"] = sgs
        log(f"Security groups: {sgs}")

    def create_dynamodb(self):
        table_name = self.config["dynamodb"]["table_name"]
        tables = self.ddb.list_tables()["TableNames"]
        if table_name not in tables:
            log(f"Criando DynamoDB {table_name}")
            self.ddb.create_table(
                TableName=table_name,
                BillingMode="PAY_PER_REQUEST",
                AttributeDefinitions=[
                    {"AttributeName": "courier_id", "AttributeType": "S"},
                    {"AttributeName": "timestamp", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "courier_id", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
            )
            waiter = self.ddb.get_waiter("table_exists")
            waiter.wait(TableName=table_name)
        else:
            desc = self.ddb.describe_table(TableName=table_name)["Table"]
            key_schema = {(k["AttributeName"], k["KeyType"]) for k in desc.get("KeySchema", [])}
            if key_schema != {("courier_id", "HASH"), ("timestamp", "RANGE")}:
                raise RuntimeError(
                    f"Tabela DynamoDB {table_name} existe com schema incompatível: {desc.get('KeySchema')}"
                )
            log(f"DynamoDB {table_name} já existe — limpando dados...")
            self._purge_dynamodb_table(table_name)
        self.state["dynamodb_table"] = table_name

    def create_kinesis_stream(self):
        kinesis_cfg = self.config.get("kinesis", {})
        stream_name = kinesis_cfg.get("stream_name", "dijkfood-order-events")
        shard_count = int(kinesis_cfg.get("shard_count", 1))

        try:
            self.kinesis.describe_stream_summary(StreamName=stream_name)
            log(f"Kinesis stream já existe: {stream_name}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code not in {"ResourceNotFoundException", "ValidationException"}:
                raise
            log(f"Criando Kinesis stream {stream_name}")
            self.kinesis.create_stream(StreamName=stream_name, ShardCount=shard_count)

        waiter = self.kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)

        self.state["kinesis_stream"] = {
            "name": stream_name,
            "shard_count": shard_count,
        }
        log(f"Kinesis stream pronto: {stream_name}")
    def _account_id(self) -> str:
        return self.sts.get_caller_identity()["Account"]

    def analytics_config(self) -> Dict:
        cfg = self.config.get("analytics", {})
        enabled = bool(cfg.get("enabled", False))
        bucket_name = cfg.get("s3_bucket") or (
            f"{self.project}-analytics-{self._account_id()}" if enabled else ""
        )
        prefix = cfg.get("s3_prefix", "order-events").strip("/")
        return {
            "enabled": enabled,
            "stream_name": cfg.get("stream_name") or f"{self.project}-order-events",
            "firehose_name": cfg.get("firehose_name") or f"{self.project}-order-events-firehose",
            "s3_bucket": bucket_name,
            "s3_prefix": prefix,
            "glue_database": cfg.get("glue_database") or f"{self.project.replace('-', '_')}_analytics",
            "glue_table": cfg.get("glue_table") or "order_events",
            "firehose_role_arn": cfg.get("firehose_role_arn", "").strip(),
        }

    def create_analytics_pipeline(self):
        cfg = self.analytics_config()
        if not cfg["enabled"]:
            log("Analytics desabilitado no config")
            self.state["analytics"] = {"enabled": False}
            return

        stream_arn = self.ensure_kinesis_stream(cfg["stream_name"])
        bucket_name, bucket_created = self.ensure_analytics_bucket(cfg["s3_bucket"])
        firehose_role_arn = cfg["firehose_role_arn"] or self.state["iam"]["ecs_task_role_arn"]
        firehose_arn = self.ensure_firehose_delivery_stream(
            cfg=cfg,
            stream_arn=stream_arn,
            bucket_name=bucket_name,
            role_arn=firehose_role_arn,
        )
        self.ensure_glue_table(cfg, bucket_name)

        self.state["analytics"] = {
            "enabled": True,
            "kinesis_stream_name": cfg["stream_name"],
            "kinesis_stream_arn": stream_arn,
            "firehose_name": cfg["firehose_name"],
            "firehose_arn": firehose_arn,
            "firehose_role_arn": firehose_role_arn,
            "s3_bucket": bucket_name,
            "s3_bucket_created": bucket_created,
            "s3_prefix": cfg["s3_prefix"],
            "glue_database": cfg["glue_database"],
            "glue_table": cfg["glue_table"],
            "athena_table": f"{cfg['glue_database']}.{cfg['glue_table']}",
        }
        log(f"Analytics pronto: Athena table {cfg['glue_database']}.{cfg['glue_table']}")

    def ensure_kinesis_stream(self, stream_name: str) -> str:
        try:
            desc = self.kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
            log(f"Kinesis stream existe: {stream_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            log(f"Criando Kinesis stream {stream_name}")
            self.kinesis.create_stream(StreamName=stream_name, ShardCount=1)
            waiter = self.kinesis.get_waiter("stream_exists")
            waiter.wait(StreamName=stream_name)
            desc = self.kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
        return desc["StreamARN"]

    def ensure_analytics_bucket(self, bucket_name: str) -> tuple[str, bool]:
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            log(f"S3 bucket existe: {bucket_name}")
            return bucket_name, False
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code not in ("404", "NoSuchBucket", "NotFound"):
                raise
            log(f"Criando S3 bucket {bucket_name}")
            kwargs = {"Bucket": bucket_name}
            if self.region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
            self.s3.create_bucket(**kwargs)
            return bucket_name, True

    def ensure_firehose_delivery_stream(self, cfg: Dict, stream_arn: str, bucket_name: str, role_arn: str) -> str:
        try:
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=cfg["firehose_name"])
            status = desc["DeliveryStreamDescription"]["DeliveryStreamStatus"]
            log(f"Firehose existe: {cfg['firehose_name']} ({status})")
            return desc["DeliveryStreamDescription"]["DeliveryStreamARN"]
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise

        data_prefix = (
            f"{cfg['s3_prefix']}/"
            "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
        )
        error_prefix = f"{cfg['s3_prefix']}/errors/!{{firehose:error-output-type}}/"

        log(f"Criando Firehose {cfg['firehose_name']} -> s3://{bucket_name}/{cfg['s3_prefix']}/")
        resp = self.firehose.create_delivery_stream(
            DeliveryStreamName=cfg["firehose_name"],
            DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={
                "KinesisStreamARN": stream_arn,
                "RoleARN": role_arn,
            },
            ExtendedS3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": f"arn:aws:s3:::{bucket_name}",
                "Prefix": data_prefix,
                "ErrorOutputPrefix": error_prefix,
                "BufferingHints": {
                    "SizeInMBs": 1,
                    "IntervalInSeconds": 60,
                },
                "CompressionFormat": "UNCOMPRESSED",
            },
        )
        return resp["DeliveryStreamARN"]

    def ensure_glue_table(self, cfg: Dict, bucket_name: str):
        database_name = cfg["glue_database"]
        table_name = cfg["glue_table"]
        location = f"s3://{bucket_name}/{cfg['s3_prefix']}/"

        try:
            self.glue.get_database(Name=database_name)
            log(f"Glue database existe: {database_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityNotFoundException":
                raise
            self.glue.create_database(DatabaseInput={"Name": database_name})
            log(f"Glue database criado: {database_name}")

        table_input = {
            "Name": table_name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "json",
                "projection.enabled": "true",
                "projection.year.type": "integer",
                "projection.year.range": "2024,2035",
                "projection.month.type": "integer",
                "projection.month.range": "1,12",
                "projection.month.digits": "2",
                "projection.day.type": "integer",
                "projection.day.range": "1,31",
                "projection.day.digits": "2",
                "projection.hour.type": "integer",
                "projection.hour.range": "0,23",
                "projection.hour.digits": "2",
                "storage.location.template": (
                    f"s3://{bucket_name}/{cfg['s3_prefix']}/"
                    "year=${year}/month=${month}/day=${day}/hour=${hour}/"
                ),
            },
            "PartitionKeys": [
                {"Name": "year", "Type": "string"},
                {"Name": "month", "Type": "string"},
                {"Name": "day", "Type": "string"},
                {"Name": "hour", "Type": "string"},
            ],
            "StorageDescriptor": {
                "Location": location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {"ignore.malformed.json": "true"},
                },
                "Columns": [
                    {"Name": "event_id", "Type": "int"},
                    {"Name": "order_id", "Type": "int"},
                    {"Name": "event_type", "Type": "string"},
                    {"Name": "from_status", "Type": "string"},
                    {"Name": "to_status", "Type": "string"},
                    {"Name": "event_status", "Type": "string"},
                    {"Name": "event_message", "Type": "string"},
                    {"Name": "latitude", "Type": "double"},
                    {"Name": "longitude", "Type": "double"},
                    {"Name": "created_at", "Type": "string"},
                ],
            },
        }

        try:
            self.glue.get_table(DatabaseName=database_name, Name=table_name)
            self.glue.update_table(DatabaseName=database_name, TableInput=table_input)
            log(f"Glue table atualizada: {database_name}.{table_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityNotFoundException":
                raise
            self.glue.create_table(DatabaseName=database_name, TableInput=table_input)
            log(f"Glue table criada: {database_name}.{table_name}")

    def _purge_dynamodb_table(self, table_name: str):
        """Remove todos os itens da tabela DynamoDB via scan + batch_write."""
        table = boto3.resource("dynamodb", region_name=self.region).Table(table_name)
        scan = table.scan(ProjectionExpression="courier_id, #ts", ExpressionAttributeNames={"#ts": "timestamp"})
        deleted = 0
        while True:
            items = scan.get("Items", [])
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={"courier_id": item["courier_id"], "timestamp": item["timestamp"]})
            deleted += len(items)
            if "LastEvaluatedKey" not in scan:
                break
            scan = table.scan(
                ProjectionExpression="courier_id, #ts",
                ExpressionAttributeNames={"#ts": "timestamp"},
                ExclusiveStartKey=scan["LastEvaluatedKey"],
            )
        log(f"DynamoDB: {deleted} itens removidos de {table_name}")

    def create_rds(self):
        db_cfg = self.config["database"]
        subnet_group_name = f"{self.project}-db-subnet-group"
        db_identifier = f"{self.project}-db"

        try:
            self.rds.describe_db_subnet_groups(
                DBSubnetGroupName=subnet_group_name
            )
            print(f"[deploy] DB subnet group já existe: {subnet_group_name}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "DBSubnetGroupNotFoundFault":
                self.rds.create_db_subnet_group(
                    DBSubnetGroupName=subnet_group_name,
                    DBSubnetGroupDescription="Subnet group do DijkFood",
                    SubnetIds=self.state["subnet_ids"][:2],
                )
                print(f"[deploy] DB subnet group criado: {subnet_group_name}")
            else:
                raise

        try:
            self.rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
            log(f"RDS {db_identifier} já existe")
        except ClientError as e:
            if "DBInstanceNotFound" not in str(e):
                raise
            log(f"Criando RDS {db_identifier}")
            self.rds.create_db_instance(
                DBInstanceIdentifier=db_identifier,
                DBName=db_cfg["db_name"],
                Engine="postgres",
                EngineVersion="15.12",
                MasterUsername=db_cfg["username"],
                MasterUserPassword=db_cfg["password"],
                DBInstanceClass=db_cfg["instance_class"],
                AllocatedStorage=db_cfg["allocated_storage"],
                PubliclyAccessible=db_cfg["publicly_accessible"],
                MultiAZ=db_cfg["multi_az"],
                VpcSecurityGroupIds=[self.state["security_groups"]["rds"]],
                DBSubnetGroupName=subnet_group_name,
                BackupRetentionPeriod=0,
                StorageType="gp2",
                DeletionProtection=False,
                AutoMinorVersionUpgrade=True,
            )

        log("Esperando RDS ficar disponível")
        waiter = self.rds.get_waiter("db_instance_available")
        waiter.wait(DBInstanceIdentifier=db_identifier)
        db = self.rds.describe_db_instances(DBInstanceIdentifier=db_identifier)["DBInstances"][0]
        endpoint = db["Endpoint"]["Address"]
        self.state["rds"] = {
            "identifier": db_identifier,
            "endpoint": endpoint,
            "port": db["Endpoint"]["Port"],
            "subnet_group": subnet_group_name,
            "db_name": db_cfg["db_name"],
            "username": db_cfg["username"],
        }
        log(f"RDS endpoint: {endpoint}")

    def ensure_log_group(self, name: str):
        groups = self.logs.describe_log_groups(logGroupNamePrefix=name).get("logGroups", [])
        if not any(g["logGroupName"] == name for g in groups):
            self.logs.create_log_group(logGroupName=name)

    def ensure_cluster(self):
        cluster_name = f"{self.project}-cluster"
        resp = self.ecs.list_clusters()
        if not any(cluster_name in arn for arn in resp.get("clusterArns", [])):
            self.ecs.create_cluster(clusterName=cluster_name)
        self.state["ecs_cluster"] = cluster_name
        log(f"Cluster ECS: {cluster_name}")

    def ensure_namespace(self):
        namespace_name = f"{self.project}.local"
        listed = self.sd.list_namespaces(Filters=[{"Name": "TYPE", "Values": ["DNS_PRIVATE"], "Condition": "EQ"}])
        namespace_id = None
        for item in listed.get("Namespaces", []):
            if item["Name"] == namespace_name:
                namespace_id = item["Id"]
                break
        if not namespace_id:
            try:
                op = self.sd.create_private_dns_namespace(
                    Name=namespace_name,
                    Vpc=self.state["vpc_id"]
                )
                operation_id = op["OperationId"]
                self.state["namespace_operation_id"] = operation_id
                while True:
                    operation = self.sd.get_operation(OperationId=operation_id)["Operation"]
                    status = operation["Status"]
                    if status == "SUCCESS":
                        namespace_id = operation["Targets"]["NAMESPACE"]
                        break
                    if status in ("FAIL", "ERROR"):
                        raise RuntimeError(f"Falha ao criar namespace Cloud Map: {operation}")
                    time.sleep(2)
                print(f"[deploy] Namespace criado: {namespace_name}")

            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "AccessDeniedException":
                    print("[deploy] Sem permissão para criar Cloud Map. Seguindo sem namespace privado.")
                    self.state["service_discovery_namespace_id"] = None
                    self.state["service_discovery_namespace_name"] = None
                    return
                raise

        self.state["service_discovery_namespace_id"] = namespace_id
        self.state["service_discovery_namespace_name"] = namespace_name

    def ensure_service_discovery_service(self, service_name):
        namespace_id = self.state.get("service_discovery_namespace_id")
        if not namespace_id:
            print(f"[deploy] Sem Cloud Map. Pulando service discovery para {service_name}")
            return None

        existing = self.sd.list_services(
            Filters=[
                {"Name": "NAMESPACE_ID", "Values": [namespace_id], "Condition": "EQ"},
                {"Name": "NAME", "Values": [service_name], "Condition": "EQ"},
            ]
        ).get("Services", [])
        if existing:
            return self._service_registry_arn(existing[0]["Id"])

        response = self.sd.create_service(
            Name=service_name,
            NamespaceId=namespace_id,
            DnsConfig={
                "DnsRecords": [{"Type": "A", "TTL": 10}],
                "RoutingPolicy": "MULTIVALUE",
            },
            HealthCheckCustomConfig={"FailureThreshold": 1},
        )
        return self._service_registry_arn(response["Service"]["Id"])

    def ensure_roles(self):
        iam_cfg = self.config.get("iam", {})
        execution_role_arn = iam_cfg.get("ecs_execution_role_arn", "").strip()
        task_role_arn = iam_cfg.get("ecs_task_role_arn", "").strip()

        if not execution_role_arn:
            role_name = f"{self.project}-ecs-execution-role"
            execution_role_arn = self._ensure_role(role_name, execution=True)
        if not task_role_arn:
            role_name = f"{self.project}-ecs-task-role"
            task_role_arn = self._ensure_role(role_name, execution=False)

        self.state["iam"] = {
            "ecs_execution_role_arn": execution_role_arn,
            "ecs_task_role_arn": task_role_arn,
        }
        log(f"Roles OK")

    def _ensure_role(self, role_name, execution=False):
        existing_role_name = self.config.get("existing_ecs_role_name", "LabRole")

        role = self.iam.get_role(RoleName=existing_role_name)["Role"]
        arn = role["Arn"]

        print(f"[deploy] Usando IAM role existente: {existing_role_name} -> {arn}")
        return arn

    def create_alb(self):
        name = f"{self.project}-alb"
        lbs = self.elbv2.describe_load_balancers().get("LoadBalancers", [])
        lb_arn = None
        dns_name = None
        for lb in lbs:
            if lb["LoadBalancerName"] == name:
                lb_arn = lb["LoadBalancerArn"]
                dns_name = lb["DNSName"]
                break
        if not lb_arn:
            resp = self.elbv2.create_load_balancer(
                Name=name,
                Subnets=self.state["subnet_ids"][:2],
                SecurityGroups=[self.state["security_groups"]["alb"]],
                Scheme="internet-facing",
                Type="application",
                IpAddressType="ipv4",
            )
            lb = resp["LoadBalancers"][0]
            lb_arn = lb["LoadBalancerArn"]
            dns_name = lb["DNSName"]
        self.state["alb"] = {"arn": lb_arn, "dns_name": dns_name}

        tg_name = f"{self.project[:20]}-api-tg"
        tgs = self.elbv2.describe_target_groups().get("TargetGroups", [])
        tg_arn = None
        for tg in tgs:
            if tg["TargetGroupName"] == tg_name:
                tg_arn = tg["TargetGroupArn"]
                break
        if not tg_arn:
            tg = self.elbv2.create_target_group(
                Name=tg_name,
                Protocol="HTTP",
                Port=8000,
                VpcId=self.state["vpc_id"],
                TargetType="ip",
                HealthCheckProtocol="HTTP",
                HealthCheckPath="/",
                Matcher={"HttpCode": "200"},
            )["TargetGroups"][0]
            tg_arn = tg["TargetGroupArn"]
        self.state["alb_target_group_arn"] = tg_arn

        listeners = self.elbv2.describe_listeners(LoadBalancerArn=lb_arn).get("Listeners", [])
        if not any(l["Port"] == 80 for l in listeners):
            self.elbv2.create_listener(
                LoadBalancerArn=lb_arn,
                Protocol="HTTP",
                Port=80,
                DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
            )
        if not any(l["Port"] == 8000 for l in listeners):
            self.elbv2.create_listener(
                LoadBalancerArn=lb_arn,
                Protocol="HTTP",
                Port=8000,
                DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
            )
        log(f"ALB DNS: http://{dns_name}")

    def create_internal_alb_targets(self):
        """Cria target groups e listeners ALB para os serviços HTTP internos."""
        lb_arn = self.state["alb"]["arn"]
        vpc_id = self.state["vpc_id"]
        listeners = self.elbv2.describe_listeners(LoadBalancerArn=lb_arn).get("Listeners", [])
        existing_ports = {l["Port"] for l in listeners}

        all_tgs = self.elbv2.describe_target_groups().get("TargetGroups", [])
        tg_by_name = {tg["TargetGroupName"]: tg["TargetGroupArn"] for tg in all_tgs}

        internal_services = [
            ("restaurant-simulator", "rsim", 8004),
            ("delivery-service", "ds", 8001),
            ("routing-service", "rt", 8002),
            ("realtime-metrics-service", "rms", 8010),
        ]

        internal_tgs = {}
        for svc_name, abbrev, port in internal_services:
            tg_name = f"{self.project[:20]}-{abbrev}-tg"
            tg_arn = tg_by_name.get(tg_name)
            if not tg_arn:
                tg = self.elbv2.create_target_group(
                    Name=tg_name,
                    Protocol="HTTP",
                    Port=port,
                    VpcId=vpc_id,
                    TargetType="ip",
                    HealthCheckProtocol="HTTP",
                    HealthCheckPath="/",
                    Matcher={"HttpCode": "200"},
                )["TargetGroups"][0]
                tg_arn = tg["TargetGroupArn"]
                log(f"Target group criado: {tg_name} (porta {port})")
            else:
                log(f"Target group existe: {tg_name}")
            internal_tgs[svc_name] = tg_arn

            if port not in existing_ports:
                self.elbv2.create_listener(
                    LoadBalancerArn=lb_arn,
                    Protocol="HTTP",
                    Port=port,
                    DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
                )
                log(f"Listener criado: porta {port} -> {svc_name}")
            else:
                log(f"Listener existe: porta {port}")

        self.state["internal_target_groups"] = internal_tgs
        log("Target groups internos configurados")

    def register_task_definition(self, name: str, image: str, port: Optional[int], env: Dict[str, str], command: Optional[List[str]] = None):
        family = f"{self.project}-{name}"
        log_group = f"/ecs/{self.project}/{name}"
        self.ensure_log_group(log_group)
        container = {
            "name": name,
            "image": image,
            "essential": True,
            "environment": [{"name": k, "value": str(v)} for k, v in env.items()],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": log_group,
                    "awslogs-region": self.region,
                    "awslogs-stream-prefix": "ecs",
                },
            },
        }
        if port:
            container["portMappings"] = [{"containerPort": port, "hostPort": port, "protocol": "tcp"}]
        if command:
            container["command"] = command

        resp = self.ecs.register_task_definition(
            family=family,
            requiresCompatibilities=["FARGATE"],
            networkMode="awsvpc",
            cpu=self.config["ecs"]["cpu"],
            memory=self.config["ecs"]["memory"],
            executionRoleArn=self.state["iam"]["ecs_execution_role_arn"],
            taskRoleArn=self.state["iam"]["ecs_task_role_arn"],
            runtimePlatform={"operatingSystemFamily": "LINUX", "cpuArchitecture": "X86_64"},
            containerDefinitions=[container],
        )
        arn = resp["taskDefinition"]["taskDefinitionArn"]
        self.state.setdefault("task_definitions", {})[name] = arn
        return arn

    def create_or_update_service(self, service_name: str, task_def_arn: str, desired_count: int, container_name: str, container_port: Optional[int], attach_to_alb: bool = False, target_group_arn: Optional[str] = None):
        cluster = self.state["ecs_cluster"]
        subnets = self.state["subnet_ids"][:2]
        ecs_sg = self.state["security_groups"]["ecs"]
        network_configuration = {
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": [ecs_sg],
                "assignPublicIp": "ENABLED" if self.config["ecs"].get("assign_public_ip", True) else "DISABLED",
            }
        }
        kwargs = {
            "cluster": cluster,
            "serviceName": service_name,
            "taskDefinition": task_def_arn,
            "desiredCount": desired_count,
            "launchType": "FARGATE",
            "networkConfiguration": network_configuration,
            "deploymentConfiguration": {
                "maximumPercent": 200,
                "minimumHealthyPercent": 50,
            },
        }

        if attach_to_alb:
            tg_arn = target_group_arn or self.state["alb_target_group_arn"]
            kwargs["loadBalancers"] = [{
                "targetGroupArn": tg_arn,
                "containerName": container_name,
                "containerPort": container_port,
            }]

        existing_service = None
        try:
            resp = self.ecs.describe_services(cluster=cluster, services=[service_name])
            services = resp.get("services", [])
            if services and services[0].get("status") == "ACTIVE":
                existing_service = services[0]
        except Exception:
            pass

        if existing_service:
            existing_lbs = existing_service.get("loadBalancers", [])
            if attach_to_alb and not existing_lbs:
                log(f"Recriando {service_name} para adicionar load balancer...")
                self.ecs.update_service(cluster=cluster, service=service_name, desiredCount=0)
                self.ecs.delete_service(cluster=cluster, service=service_name, force=True)
                time.sleep(20)
                self.ecs.create_service(**kwargs)
                log(f"Service recriado com ALB: {service_name}")
            else:
                self.ecs.update_service(cluster=cluster, service=service_name, taskDefinition=task_def_arn, desiredCount=desired_count)
                log(f"Service atualizado: {service_name}")
        else:
            self.ecs.create_service(**kwargs)
            log(f"Service criado: {service_name}")

    def delete_obsolete_services(self, service_names: List[str]):
        cluster = self.state["ecs_cluster"]
        for service_name in service_names:
            try:
                resp = self.ecs.describe_services(cluster=cluster, services=[service_name])
                services = resp.get("services", [])
                if not services or services[0].get("status") == "INACTIVE":
                    continue

                log(f"Removendo serviço obsoleto: {service_name}")
                self.ecs.update_service(cluster=cluster, service=service_name, desiredCount=0)
                self.ecs.delete_service(cluster=cluster, service=service_name, force=True)
            except Exception as exc:
                log(f"Não foi possível remover serviço obsoleto {service_name}: {exc}")

    def _service_registry_arn(self, service_id: str) -> str:
        service = self.sd.get_service(Id=service_id)["Service"]
        return service["Arn"]

    def configure_autoscaling_for_service(
        self,
        service_name: str,
        min_capacity: int,
        max_capacity: int,
        target_cpu: float = 60.0,
        target_memory: float = 70.0,
        target_requests_per_target: float | None = None,
        alb_resource_label: str | None = None,
    ):
        cluster = self.state["ecs_cluster"]
        resource_id = f"service/{cluster}/{service_name}"

        self.application_autoscaling.register_scalable_target(
            ServiceNamespace="ecs",
            ResourceId=resource_id,
            ScalableDimension="ecs:service:DesiredCount",
            MinCapacity=min_capacity,
            MaxCapacity=max_capacity,
        )

        self.application_autoscaling.put_scaling_policy(
            PolicyName=f"{self.project}-{service_name}-cpu-target",
            ServiceNamespace="ecs",
            ResourceId=resource_id,
            ScalableDimension="ecs:service:DesiredCount",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": target_cpu,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
                },
                "ScaleInCooldown": 120,
                "ScaleOutCooldown": 30,
            },
        )

        self.application_autoscaling.put_scaling_policy(
            PolicyName=f"{self.project}-{service_name}-memory-target",
            ServiceNamespace="ecs",
            ResourceId=resource_id,
            ScalableDimension="ecs:service:DesiredCount",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": target_memory,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "ECSServiceAverageMemoryUtilization"
                },
                "ScaleInCooldown": 120,
                "ScaleOutCooldown": 30,
            },
        )

        if target_requests_per_target is not None and alb_resource_label:
            self.application_autoscaling.put_scaling_policy(
                PolicyName=f"{self.project}-{service_name}-requests-target",
                ServiceNamespace="ecs",
                ResourceId=resource_id,
                ScalableDimension="ecs:service:DesiredCount",
                PolicyType="TargetTrackingScaling",
                TargetTrackingScalingPolicyConfiguration={
                    "TargetValue": target_requests_per_target,
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ALBRequestCountPerTarget",
                        "ResourceLabel": alb_resource_label,
                    },
                    "ScaleInCooldown": 120,
                    "ScaleOutCooldown": 30,
                },
            )

        log(f"Auto scaling configurado para {service_name}")

    @staticmethod
    def _alb_request_count_resource_label(
        load_balancer_arn: str,
        target_group_arn: str,
    ) -> str:
        load_balancer_suffix = load_balancer_arn.split("loadbalancer/", 1)[1]
        target_group_suffix = target_group_arn.split("targetgroup/", 1)[1]
        return f"{load_balancer_suffix}/targetgroup/{target_group_suffix}"

    def deploy_services(self):
        imgs = self.config["dockerhub_images"]
        rds = self.state["rds"]
        alb_dns = self.state["alb"]["dns_name"]
        alb_base = f"http://{alb_dns}"
        internal_api_base = f"http://{alb_dns}:8000"
        internal_tgs = self.state.get("internal_target_groups", {})

        self.delete_obsolete_services(["restaurant-service", "restaurant-worker", "courier-simulator"])

        print(f"[deploy] ALB_BASE           = {alb_base}")
        print(f"[deploy] INTERNAL_API_BASE  = {internal_api_base}")
        print(f"[deploy] DELIVERY_SERVICE_URL   = {alb_base}:8001")
        print(f"[deploy] ROUTING_URL            = {alb_base}:8002/rota")
        print(f"[deploy] RESTAURANT_SIMULATOR_URL = {alb_base}:8004")
        print(f"[deploy] REALTIME_METRICS_URL     = {alb_base}:8010")

        api_env = {
            "USE_DYNAMO": "True",
            "AWS_REGION": self.region,
            "DYNAMO_TABLE": self.state["dynamodb_table"],
            "DB_HOST": rds["endpoint"],
            "DB_NAME": self.config["database"]["db_name"],
            "DB_USER": self.config["database"]["username"],
            "DB_PASSWORD": self.config["database"]["password"],
            "DB_PORT": str(rds["port"]),
            "DB_SSLMODE": "require",
            "RESTAURANT_SIMULATOR_URL": f"{alb_base}:8004",
            "REQUEST_TIMEOUT_SECONDS": "15",
            "NOTIFY_TIMEOUT_SECONDS": "2",
            "NOTIFY_MAX_ATTEMPTS": "3",
            "NOTIFY_RETRY_BACKOFF_SECONDS": "0.2",
            "KINESIS_ENABLED": "true",
            "KINESIS_STREAM_NAME": self.state["kinesis_stream"]["name"],
            "UVICORN_WORKERS": str(self.config.get("api", {}).get("uvicorn_workers", 4)),
        }
        analytics_state = self.state.get("analytics", {})
        if analytics_state.get("enabled"):
            api_env.update(
                {
                    "ANALYTICS_ENABLED": "true",
                    "KINESIS_STREAM_NAME": analytics_state["kinesis_stream_name"],
                }
            )
        td_api = self.register_task_definition("api", imgs["api"], 8000, api_env)

        restaurant_simulator_env = {
            "API_URL": internal_api_base,
            "DELIVERY_SERVICE_URL": f"{alb_base}:8001",
            "REQUEST_TIMEOUT_SECONDS": "15",
            "ACCEPTANCE_RATE": "1.0",
            "CONFIRMED_DELAY_SECONDS": "0.4",
            "PREPARING_DELAY_SECONDS": "0.8",
        }
        td_restaurant_simulator = self.register_task_definition("restaurant-simulator", imgs["restaurant_simulator"], 8004, restaurant_simulator_env)

        routing_env = {}
        td_routing = self.register_task_definition("routing-service", imgs["routing_service"], 8002, routing_env)

        delivery_env = {
            "API_URL": internal_api_base,
            "ROUTING_URL": f"{alb_base}:8002/rota",
            "COURIER_SIMULATOR_URL": f"{alb_base}:8004",
            "REQUEST_TIMEOUT_SECONDS": "15",
        }
        td_delivery = self.register_task_definition("delivery-service", imgs["delivery_service"], 8001, delivery_env)

        realtime_env = {
            "API_URL": internal_api_base,
            "AWS_REGION": self.region,
            "KINESIS_STREAM_NAME": self.state["kinesis_stream"]["name"],
            "KINESIS_ITERATOR_TYPE": self.config.get("kinesis", {}).get("iterator_type", "LATEST"),
            "KINESIS_POLL_INTERVAL_SECONDS": str(self.config.get("kinesis", {}).get("poll_interval_seconds", 1)),
            "KINESIS_RECORDS_LIMIT": str(self.config.get("kinesis", {}).get("records_limit", 500)),
        }
        td_realtime = self.register_task_definition(
            "realtime-metrics-service",
            imgs["realtime_metrics_service"],
            8010,
            realtime_env,
        )

        self.create_or_update_service("api", td_api, self.config["ecs"]["desired_count_api"], "api", 8000, attach_to_alb=True)
        self.create_or_update_service("restaurant-simulator", td_restaurant_simulator, self.config["ecs"]["desired_count_restaurant_simulator"], "restaurant-simulator", 8004, attach_to_alb=True, target_group_arn=internal_tgs.get("restaurant-simulator"))
        self.create_or_update_service("routing-service", td_routing, self.config["ecs"]["desired_count_routing_service"], "routing-service", 8002, attach_to_alb=True, target_group_arn=internal_tgs.get("routing-service"))
        self.create_or_update_service("delivery-service", td_delivery, self.config["ecs"]["desired_count_delivery_service"], "delivery-service", 8001, attach_to_alb=True, target_group_arn=internal_tgs.get("delivery-service"))
        self.create_or_update_service("realtime-metrics-service", td_realtime, self.config["ecs"].get("desired_count_realtime_metrics_service", 1), "realtime-metrics-service", 8010, attach_to_alb=True, target_group_arn=internal_tgs.get("realtime-metrics-service"))

        autoscaling = self.config.get("autoscaling", {})

        self.configure_autoscaling_for_service(
            "api",
            autoscaling["api"]["min"],
            autoscaling["api"]["max"],
            autoscaling["api"]["target_cpu"],
            autoscaling["api"].get("target_memory", 70.0),
        )

        self.configure_autoscaling_for_service(
            "restaurant-simulator",
            autoscaling["restaurant_simulator"]["min"],
            autoscaling["restaurant_simulator"]["max"],
            autoscaling["restaurant_simulator"]["target_cpu"],
            autoscaling["restaurant_simulator"].get("target_memory", 70.0),
        )

        self.configure_autoscaling_for_service(
            "routing-service",
            autoscaling["routing_service"]["min"],
            autoscaling["routing_service"]["max"],
            autoscaling["routing_service"]["target_cpu"],
            autoscaling["routing_service"].get("target_memory", 70.0),
            autoscaling["routing_service"].get("target_requests_per_target"),
            self._alb_request_count_resource_label(
                self.state["alb"]["arn"],
                internal_tgs["routing-service"],
            ),
        )

        self.configure_autoscaling_for_service(
            "delivery-service",
            autoscaling["delivery_service"]["min"],
            autoscaling["delivery_service"]["max"],
            autoscaling["delivery_service"]["target_cpu"],
            autoscaling["delivery_service"].get("target_memory", 70.0),
        )

        if "realtime_metrics_service" in autoscaling:
            self.configure_autoscaling_for_service(
                "realtime-metrics-service",
                autoscaling["realtime_metrics_service"]["min"],
                autoscaling["realtime_metrics_service"]["max"],
                autoscaling["realtime_metrics_service"]["target_cpu"],
                autoscaling["realtime_metrics_service"].get("target_memory", 70.0),
            )
    def wait_for_api(self, timeout_seconds: int = 1200):
        dns = self.state["alb"]["dns_name"]
        url = f"http://{dns}/"
        import urllib.request
        log(f"Esperando API responder em {url}")
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:
                    body = resp.read().decode("utf-8", errors="ignore")
                    log(f"API respondeu: {body[:120]}")
                    self.state["api_url"] = f"http://{dns}"
                    return
            except Exception as e:
                time.sleep(15)
        raise TimeoutError("API não ficou pronta a tempo.")

    def deploy(self):
        self.ensure_default_vpc()
        self.create_security_groups()
        self.create_dynamodb()
        self.create_kinesis_stream()
        self.create_rds()
        self.ensure_cluster()
        try:
            self.ensure_namespace()
        except Exception as e:
            print(f"[deploy] Pulando Cloud Map: {e}")
            self.state["service_discovery_namespace_id"] = None
            self.state["service_discovery_namespace_name"] = None
        self.ensure_roles()
        self.create_analytics_pipeline()
        self.create_alb()
        self.create_internal_alb_targets()
        self.deploy_services()
        self.save_state()
        self.wait_for_api()
        self.save_state()
        log("Deploy concluído")
        print(json.dumps(self.state, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Caminho do config JSON")
    parser.add_argument("--scenario", choices=["normal", "peak", "special", 'teste'], default=None)
    parser.add_argument("--duration-seconds", type=int, default=None)
    parser.add_argument("--run-simulator", action="store_true")
    parser.add_argument("--destroy-on-finish", action="store_true")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    deployer = Deployer(config)

    try:
        deployer.deploy()
        wait_for_api_stable(deployer.state["api_url"])
        if args.run_simulator:
            config["api_url"] = deployer.state["api_url"]
            config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            simulator_dir = Path(__file__).resolve().parent / "simulator"
            simulator_cmd = [sys.executable, "main.py", "--scenario", args.scenario or "normal"]
            if args.duration_seconds is not None:
                simulator_cmd.extend(["--duration-seconds", str(args.duration_seconds)])
            subprocess.run(
                simulator_cmd,
                cwd=simulator_dir,
                check=True,
            )
    finally:
        if args.destroy_on_finish:
            state_file = Path(__file__).resolve().parent / "deployment_state.json"
            if state_file.exists():
                subprocess.run(
                    [sys.executable, str(Path(__file__).resolve().parent / "destroy.py"), "--config", str(config_path)],
                    check=False,
                )
            else:
                log("deployment_state.json ainda não existe; pulando destroy automático.")


if __name__ == "__main__":
    main()
