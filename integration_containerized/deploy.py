import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
import subprocess
import time
import requests

import boto3
from botocore.exceptions import ClientError, WaiterError


ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "deployment_state.json"



def wait_for_api_stable(api_base_url: str, timeout_seconds: int = 180):
    deadline = time.time() + timeout_seconds
    consecutive_success = 0

    endpoints = ["/", "/restaurants", "/couriers"]

    while time.time() < deadline:
        all_ok = True

        for path in endpoints:
            try:
                r = requests.get(f"{api_base_url.rstrip('/')}{path}", timeout=10)
                if r.status_code != 200:
                    all_ok = False
                    break
            except Exception:
                all_ok = False
                break

        if all_ok:
            consecutive_success += 1
            print(f"[deploy] API estável: {consecutive_success}/5")
            if consecutive_success >= 5:
                return
        else:
            consecutive_success = 0

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
        self.sd = self.session.client("servicediscovery")
        self.ddb = self.session.client("dynamodb")
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
        try:
            self.ec2.authorize_security_group_ingress(GroupId=group_id, IpPermissions=permissions)
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
            "rabbitmq": "EC2 RabbitMQ do DijkFood",
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
                {"IpProtocol": "tcp", "FromPort": 8000, "ToPort": 8000, "IpRanges": [{"CidrIp": your_ip_cidr}]},
            ],
        )
        self._ensure_ingress(
            sgs["ecs"],
            [{
                "IpProtocol": "tcp",
                "FromPort": 8000,
                "ToPort": 8002,
                "UserIdGroupPairs": [{"GroupId": sgs["alb"]}, {"GroupId": sgs["ecs"]}, {"GroupId": sgs["rabbitmq"]}],
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
        self._ensure_ingress(
            sgs["rabbitmq"],
            [
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5672,
                    "ToPort": 5672,
                    "UserIdGroupPairs": [{"GroupId": sgs["ecs"]}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 15672,
                    "ToPort": 15672,
                    "IpRanges": [{"CidrIp": your_ip_cidr}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": your_ip_cidr}],
                },
            ],
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
            log(f"DynamoDB {table_name} já existe")
        self.state["dynamodb_table"] = table_name

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

    def create_rabbitmq_ec2(self):
        cfg = self.config["rabbitmq"]
        instance_name = f"{self.project}-rabbitmq"
        existing = self.ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
            ]
        )
        reservations = existing.get("Reservations", [])
        if reservations:
            inst = reservations[0]["Instances"][0]
            instance_id = inst["InstanceId"]
            state = inst["State"]["Name"]
            if state == "stopped":
                self.ec2.start_instances(InstanceIds=[instance_id])
            log(f"RabbitMQ EC2 já existe: {instance_id}")
        else:
            user_data = f'''#!/bin/bash
set -eux
sudo dnf update -y || true
sudo dnf install -y docker || sudo yum install -y docker
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user || true
docker run -d --restart unless-stopped \
  --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
'''
            resp = self.ec2.run_instances(
                ImageId=cfg["ami_id"],
                InstanceType=cfg["instance_type"],
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[self.state["security_groups"]["rabbitmq"]],
                SubnetId=self.state["subnet_ids"][0],
                TagSpecifications=[{
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": instance_name}],
                }],
                UserData=user_data,
            )
            instance_id = resp["Instances"][0]["InstanceId"]
            log(f"Criado EC2 RabbitMQ: {instance_id}")

        waiter = self.ec2.get_waiter("instance_running")
        waiter.wait(InstanceIds=[instance_id])
        info = self.ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
        private_ip = info["PrivateIpAddress"]
        public_ip = info.get("PublicIpAddress")
        self.state["rabbitmq"] = {
            "instance_id": instance_id,
            "private_ip": private_ip,
            "public_ip": public_ip,
            "amqp_url": f"amqp://{cfg['username']}:{cfg['password']}@{private_ip}:5672/",
            "management_url": f"http://{public_ip}:15672" if public_ip else "",
        }
        log(f"RabbitMQ privado: {private_ip} | painel: {public_ip}:15672")

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
                self.state["namespace_operation_id"] = op["OperationId"]
                print(f"[deploy] Namespace criado: {namespace_name}")

            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "AccessDeniedException":
                    print("[deploy] Sem permissão para criar Cloud Map. Seguindo sem namespace privado.")
                    self.state["namespace_id"] = None
                    self.state["namespace_name"] = None
                    return
                raise

    def ensure_service_discovery_service(self, service_name):
        namespace_id = self.state.get("service_discovery_namespace_id")
        if not namespace_id:
            print(f"[deploy] Sem Cloud Map. Pulando service discovery para {service_name}")
            return None

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
        log(f"ALB DNS: http://{dns_name}")

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

    def create_or_update_service(self, service_name: str, task_def_arn: str, desired_count: int, container_name: str, container_port: Optional[int], attach_to_alb: bool = False):
        cluster = self.state["ecs_cluster"]
        service_discovery_id = None
        if self.state.get("service_discovery_namespace_id"):
            service_discovery_id = self.ensure_service_discovery_service(service_name)
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

        if service_discovery_id:
            kwargs["serviceRegistries"] = [{"registryArn": service_discovery_id}]
        if attach_to_alb:
            kwargs["loadBalancers"] = [{
                "targetGroupArn": self.state["alb_target_group_arn"],
                "containerName": container_name,
                "containerPort": container_port,
            }]

        try:
            self.ecs.describe_services(cluster=cluster, services=[service_name])["services"][0]
            self.ecs.update_service(cluster=cluster, service=service_name, taskDefinition=task_def_arn, desiredCount=desired_count)
            log(f"Service atualizado: {service_name}")
        except Exception:
            self.ecs.create_service(**kwargs)
            log(f"Service criado: {service_name}")

    def _service_registry_arn(self, service_id: str) -> str:
        service = self.sd.get_service(Id=service_id)["Service"]
        return service["Arn"]

    def configure_autoscaling_for_api(self):
        cluster = self.state["ecs_cluster"]
        service_name = "api"
        resource_id = f"service/{cluster}/{service_name}"
        self.application_autoscaling.register_scalable_target(
            ServiceNamespace="ecs",
            ResourceId=resource_id,
            ScalableDimension="ecs:service:DesiredCount",
            MinCapacity=1,
            MaxCapacity=4,
        )
        self.application_autoscaling.put_scaling_policy(
            PolicyName=f"{self.project}-api-cpu-target",
            ServiceNamespace="ecs",
            ResourceId=resource_id,
            ScalableDimension="ecs:service:DesiredCount",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": 60.0,
                "PredefinedMetricSpecification": {"PredefinedMetricType": "ECSServiceAverageCPUUtilization"},
                "ScaleInCooldown": 60,
                "ScaleOutCooldown": 60,
            },
        )
        log("Auto scaling configurado para api")

    def deploy_services(self):
        imgs = self.config["dockerhub_images"]
        ns = self.state.get("service_discovery_namespace_name")

        if ns:
            broker_host = f"rabbitmq.{ns}"
        else:
            broker_host = self.state["rabbitmq"]["private_ip"]

        api_base_url = self.state.get("alb_dns") or self.state.get("api_url")

        print(f"[deploy] BROKER_HOST = {broker_host}")
        print(f"[deploy] API_BASE_URL = {api_base_url}")

        rabbit_host = self.state["rabbitmq"]["private_ip"]
        rds = self.state["rds"]
        common_rabbit = {
            "RABBITMQ_HOST": rabbit_host,
            "RABBITMQ_PORT": "5672",
            "RABBITMQ_USER": self.config["rabbitmq"]["username"],
            "RABBITMQ_PASSWORD": self.config["rabbitmq"]["password"],
        }

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
            **common_rabbit,
        }
        td_api = self.register_task_definition("api", imgs["api"], 8000, api_env)

        api_base = f"http://{self.state['alb']['dns_name']}"

        restaurant_env = {
            "API_URL": api_base,
            "RESTAURANT_ID": "1",
            **common_rabbit,
        }
        td_restaurant = self.register_task_definition("restaurant-worker", imgs["restaurant_worker"], None, restaurant_env)

        routing_env = {}
        td_routing = self.register_task_definition("routing-service", imgs["routing_service"], 8002, routing_env)

        delivery_env = {
            "API_URL": api_base,
            "ROUTING_URL": f"http://routing-service.{ns}:8002/rota",
            **common_rabbit,
        }
        td_delivery = self.register_task_definition("delivery-service", imgs["delivery_service"], 8001, delivery_env)

        courier_env = {
            "API_URL": api_base,
            "COURIER_ID": "1",
            "MOVE_INTERVAL": "1",
            "INITIAL_LAT": "-22.1210",
            "INITIAL_LON": "-51.3880",
            "IGNORE_LOCATION_ERRORS": "true",
            **common_rabbit,
        }
        td_courier = self.register_task_definition("courier-worker", imgs["courier_worker"], None, courier_env)

        self.create_or_update_service("api", td_api, self.config["ecs"]["desired_count_api"], "api", 8000, attach_to_alb=True)
        self.create_or_update_service("restaurant-worker", td_restaurant, self.config["ecs"]["desired_count_restaurant_worker"], "restaurant-worker", None)
        self.create_or_update_service("routing-service", td_routing, self.config["ecs"]["desired_count_routing_service"], "routing-service", 8002)
        self.create_or_update_service("delivery-service", td_delivery, self.config["ecs"]["desired_count_delivery_service"], "delivery-service", 8001)
        self.create_or_update_service("courier-worker", td_courier, self.config["ecs"]["desired_count_courier_worker"], "courier-worker", None)

        self.configure_autoscaling_for_api()

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
        self.create_rds()
        self.create_rabbitmq_ec2()
        self.ensure_cluster()
        try:
            self.ensure_namespace()
        except Exception as e:
            print(f"[deploy] Pulando Cloud Map: {e}")
            self.state["namespace_id"] = None
            self.state["namespace_name"] = None
        self.ensure_roles()
        self.create_alb()
        self.deploy_services()
        self.save_state()
        self.wait_for_api()
        self.save_state()
        log("Deploy concluído")
        print(json.dumps(self.state, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Caminho do config JSON")
    parser.add_argument("--scenario", choices=["normal", "peak", "special"], default=None)
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
            subprocess.run(
                [sys.executable, "main.py", "--scenario", args.scenario or "normal"],
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
