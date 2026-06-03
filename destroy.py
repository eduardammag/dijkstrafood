import json
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "deployment_state.json"


def log(msg: str):
    print(f"[destroy] {msg}", flush=True)


def safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log(f"Ignorado: {e}")
        return None


def main():
    if not STATE_FILE.exists():
        raise SystemExit("deployment_state.json não encontrado")

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    session = boto3.Session(region_name=state["region"])
    ecs = session.client("ecs")
    elbv2 = session.client("elbv2")
    ec2 = session.client("ec2")
    rds = session.client("rds")
    ddb = session.client("dynamodb")
    kinesis = session.client("kinesis")
    sd = session.client("servicediscovery")
    logs = session.client("logs")
    iam = session.client("iam")

    cluster = state.get("ecs_cluster")
    if cluster:
        for service_name in [
            "api",
            "restaurant-worker",
            "restaurant-service",
            "restaurant-simulator",
            "routing-service",
            "delivery-service",
            "realtime-metrics-service",
            "courier-simulator",
        ]:
            try:
                ecs.update_service(cluster=cluster, service=service_name, desiredCount=0)
                time.sleep(2)
                ecs.delete_service(cluster=cluster, service=service_name, force=True)
                log(f"Service removido: {service_name}")
            except Exception as e:
                log(f"Service {service_name}: {e}")

        time.sleep(10)
        safe(ecs.delete_cluster, cluster=cluster)

    if state.get("alb", {}).get("arn"):
        lb_arn = state["alb"]["arn"]
        listeners = safe(elbv2.describe_listeners, LoadBalancerArn=lb_arn)
        if listeners:
            for l in listeners.get("Listeners", []):
                safe(elbv2.delete_listener, ListenerArn=l["ListenerArn"])
        safe(elbv2.delete_load_balancer, LoadBalancerArn=lb_arn)
        time.sleep(10)

    tg_arn = state.get("alb_target_group_arn")
    if tg_arn:
        safe(elbv2.delete_target_group, TargetGroupArn=tg_arn)

    for svc_tg_arn in state.get("internal_target_groups", {}).values():
        safe(elbv2.delete_target_group, TargetGroupArn=svc_tg_arn)

    rds_state = state.get("rds", {})
    if rds_state.get("identifier"):
        safe(
            rds.delete_db_instance,
            DBInstanceIdentifier=rds_state["identifier"],
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True,
        )
        time.sleep(5)
    if rds_state.get("subnet_group"):
        safe(rds.delete_db_subnet_group, DBSubnetGroupName=rds_state["subnet_group"])

    if state.get("dynamodb_table"):
        safe(ddb.delete_table, TableName=state["dynamodb_table"])

    kinesis_stream = state.get("kinesis_stream", {}).get("name")
    if kinesis_stream:
        safe(kinesis.delete_stream, StreamName=kinesis_stream, EnforceConsumerDeletion=True)

    ns_id = state.get("service_discovery_namespace_id")
    if ns_id:
        services = safe(sd.list_services, Filters=[{"Name": "NAMESPACE_ID", "Values": [ns_id], "Condition": "EQ"}])
        if services:
            for svc in services.get("Services", []):
                safe(sd.delete_service, Id=svc["Id"])
        safe(sd.delete_namespace, Id=ns_id)

    for name in [
        "/ecs/{}/{}".format(state["project"], s)
        for s in [
            "api",
            "restaurant-worker",
            "restaurant-service",
            "restaurant-simulator",
            "routing-service",
            "delivery-service",
            "realtime-metrics-service",
            "courier-simulator",
        ]
    ]:
        safe(logs.delete_log_group, logGroupName=name)

    for sg_id in state.get("security_groups", {}).values():
        safe(ec2.delete_security_group, GroupId=sg_id)

    iam_state = state.get("iam", {})
    for role_arn_key in ["ecs_task_role_arn", "ecs_execution_role_arn"]:
        arn = iam_state.get(role_arn_key)
        if arn and ":role/" in arn:
            role_name = arn.split("/")[-1]
            safe(iam.delete_role_policy, RoleName=role_name, PolicyName=f"{role_name}-inline")
            safe(iam.detach_role_policy, RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy")
            safe(iam.delete_role, RoleName=role_name)

    if STATE_FILE.exists():
        STATE_FILE.unlink()
    log("Destruição concluída")


if __name__ == "__main__":
    main()
