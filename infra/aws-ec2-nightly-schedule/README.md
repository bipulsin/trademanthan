# EC2 nightly stop / morning start (cost saving)

Stops the TradeManthan EC2 instance every day at **23:00 IST** and starts it at **08:00 IST** using **Amazon EventBridge** (cron in UTC) and **AWS Lambda** (boto3 `StopInstances` / `StartInstances`).

Schedules in UTC (EventBridge):

| Local (IST) | UTC | Cron |
|-------------|-----|------|
| Stop 23:00 | 17:30 | `cron(30 17 * * ? *)` |
| Start 08:00 | 02:30 | `cron(30 2 * * ? *)` |

> **IST = UTC+5:30.** If you need another timezone, change the two `ScheduleExpression` values in `template.yaml` and redeploy.

### Quick reference (TradeManthan production)

| | |
|--|--|
| **Instance ID** | `i-031d2c8bb2447d767` |
| **Region** | Same as the instance (e.g. **ap-south-1** Mumbai) |

After `sam build`, deploy or update the stack with:

```bash
sam deploy --parameter-overrides InstanceId=i-031d2c8bb2447d767
```

(Use your AWS profile/region as needed, e.g. `--region ap-south-1`.)

## Prerequisites

- Your **EC2 instance ID** (this project: **`i-031d2c8bb2447d767`**) from **EC2 → Instances**.
- Work in the **same AWS region** as the instance (e.g. **Mumbai `ap-south-1`** — check the region selector in the top bar).

---

## Option A: AWS Management Console (no CLI)

### 1) IAM role for Lambda

1. Open **IAM → Roles → Create role**.
2. **Trusted entity:** AWS service → **Lambda** → Next.
3. **Add permissions:** choose **Create policy** (opens a new tab).
   - **JSON** tab, paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:StartInstances",
        "ec2:StopInstances",
        "ec2:DescribeInstances"
      ],
      "Resource": "*"
    }
  ]
}
```

   - Name the policy (e.g. `trademanthan-ec2-schedule-policy`) → **Create policy**.
4. Back on the role wizard, refresh the policy list, attach **trademanthan-ec2-schedule-policy** → Next.
5. Role name: e.g. `trademanthan-ec2-scheduler-role` → **Create role**.

### 2) Lambda function

1. Open **Lambda → Create function**.
2. **Author from scratch**, name e.g. `trademanthan-ec2-scheduler`, runtime **Python 3.12** (or latest supported).
3. **Change default execution role** → **Use existing role** → select `trademanthan-ec2-scheduler-role` → **Create function**.
4. **Configuration → Environment variables → Edit** → add:
   - Key `INSTANCE_ID` → value **`i-031d2c8bb2447d767`** (this project) → **Save**.
5. **Code** tab: replace the default file with the snippet below, then **Deploy**.
6. **Configuration → Runtime settings → Edit** → **Handler** must match your file and function. For the snippet below, use **`lambda_function.lambda_handler`** (default).

```python
import json
import os
import boto3

def lambda_handler(event, context):
    instance_id = os.environ.get("INSTANCE_ID", "").strip()
    if not instance_id:
        return {"statusCode": 500, "body": json.dumps({"error": "INSTANCE_ID not set"})}
    action = str(event.get("action", "stop")).lower().strip() if isinstance(event, dict) else "stop"
    ec2 = boto3.client("ec2", region_name=os.environ.get("AWS_REGION"))
    if action == "stop":
        ec2.stop_instances(InstanceIds=[instance_id])
        msg = {"action": "stop", "instanceId": instance_id}
    elif action == "start":
        ec2.start_instances(InstanceIds=[instance_id])
        msg = {"action": "start", "instanceId": instance_id}
    else:
        return {"statusCode": 400, "body": json.dumps({"error": f"Unknown action: {action}"})}
    return {"statusCode": 200, "body": json.dumps(msg)}
```

### 3) EventBridge rule — stop at 23:00 IST (17:30 UTC)

1. Open **Amazon EventBridge → Rules → Create rule**.
2. Name: e.g. `trademanthan-ec2-stop-2300-ist`.
3. **Rule type:** **Schedule**.
4. **Schedule pattern:** **A schedule that runs at a regular rate** is wrong — pick **Cron expression** (or “Schedule with a pattern” depending on UI).
5. **Cron expression:** `cron(30 17 * * ? *)`  
   Ensure the schedule uses **UTC** (EventBridge default). This is **23:00 IST**.
6. **Select targets** → **AWS service** → **Lambda function** → choose `trademanthan-ec2-scheduler`.
7. **Additional settings** → **Configure target input** → **Constant (JSON text)** → `{"action":"stop"}`.
8. Acknowledge resource policy if prompted so EventBridge may invoke the function → **Create**.

### 4) EventBridge rule — start at 08:00 IST (02:30 UTC)

Repeat step 3 with:

- Name: e.g. `trademanthan-ec2-start-0800-ist`.
- **Cron expression:** `cron(30 2 * * ? *)` (08:00 IST).
- Same Lambda target, constant JSON: `{"action":"start"}`.

### 5) Test (optional)

**Lambda → Test** with event JSON `{"action":"stop"}` (only if you are ready for the instance to stop). Prefer testing the **start** rule during a maintenance window.

### Disable later

**EventBridge → Rules** → select each rule → **Disable**.

---

## Option B: Deploy with SAM (CLI)

### Prerequisites (SAM)

- AWS CLI configured (`aws configure`) with permission to create Lambda, IAM, EventBridge, and EC2 start/stop on your instance.
- **AWS SAM CLI** (`sam`): [Install SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).

## Deploy

```bash
cd infra/aws-ec2-nightly-schedule
sam build
sam deploy --guided
```

When prompted:

- **Stack name:** e.g. `trademanthan-ec2-schedule`
- **Region:** same as your EC2 (e.g. `ap-south-1`)
- **Parameter InstanceId:** your instance ID
- Confirm IAM role creation for SAM

## Verify

- **Lambda:** Console → Lambda → function `trademanthan-ec2-scheduler-<InstanceId>`
- **Rules:** Console → EventBridge → Rules → `trademanthan-ec2-stop-2300-ist` and `trademanthan-ec2-start-0800-ist`
- **CloudWatch Logs:** Log group `/aws/lambda/trademanthan-ec2-scheduler-...` after the first run

## Important notes

1. **Elastic IP:** If you rely on a **fixed public IP**, associate an **Elastic IP** with the instance. After stop/start, a *non-EIP* public IP can change — update DNS and GitHub `EC2_HOST` if needed.
2. **GitHub Actions deploy:** If your workflow SSHs to an IP, use the Elastic IP or a DNS name that tracks the instance.
3. **Downtime:** The app is unavailable while the instance is stopped (roughly 23:00–08:00 IST).
4. **Disable schedule:** EventBridge → Rules → disable `trademanthan-ec2-stop-2300-ist` / `trademanthan-ec2-start-0800-ist`, or delete the CloudFormation stack.

## Remove

```bash
aws cloudformation delete-stack --stack-name trademanthan-ec2-schedule --region ap-south-1
```

(Use your actual stack name and region.)
