"""
EventBridge-triggered Lambda: stop or start one EC2 instance.
Event body: {"action": "stop"} or {"action": "start"}
Environment: INSTANCE_ID (required)
"""
import json
import os

import boto3


def handler(event, context):
    instance_id = os.environ.get("INSTANCE_ID", "").strip()
    if not instance_id:
        return {"statusCode": 500, "body": json.dumps({"error": "INSTANCE_ID not set"})}

    action = "stop"
    if isinstance(event, dict):
        action = str(event.get("action", "stop")).lower().strip()

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    ec2 = boto3.client("ec2", region_name=region)

    if action == "stop":
        ec2.stop_instances(InstanceIds=[instance_id])
        msg = {"action": "stop", "instanceId": instance_id, "state": "stopping"}
    elif action == "start":
        ec2.start_instances(InstanceIds=[instance_id])
        msg = {"action": "start", "instanceId": instance_id, "state": "starting"}
    else:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Unknown action: {action}"}),
        }

    return {"statusCode": 200, "body": json.dumps(msg)}
