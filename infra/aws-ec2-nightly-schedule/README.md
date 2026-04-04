# EC2 nightly stop / morning start (cost saving)

Stops the TradeManthan EC2 instance every day at **23:30 IST** and starts it at **08:00 IST** using **Amazon EventBridge** (cron in UTC) and **AWS Lambda** (boto3 `StopInstances` / `StartInstances`).

Schedules in UTC (EventBridge):

| Local (IST) | UTC | Cron |
|-------------|-----|------|
| Stop 23:30 | 18:00 | `cron(0 18 * * ? *)` |
| Start 08:00 | 02:30 | `cron(30 2 * * ? *)` |

> **IST = UTC+5:30.** If you need another timezone, change the two `ScheduleExpression` values in `template.yaml` and redeploy.

## Prerequisites

- AWS CLI configured (`aws configure`) with permission to create Lambda, IAM, EventBridge, and EC2 start/stop on your instance.
- **AWS SAM CLI** (`sam`) for the simplest deploy: [Install SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).
- Your **EC2 instance ID** (e.g. `i-0abc123def4567890`) from EC2 console.
- Deploy the stack in the **same AWS region** as the instance (e.g. `ap-south-1`).

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
- **Rules:** Console → EventBridge → Rules → `trademanthan-ec2-stop-2330-ist` and `trademanthan-ec2-start-0800-ist`
- **CloudWatch Logs:** Log group `/aws/lambda/trademanthan-ec2-scheduler-...` after the first run

## Important notes

1. **Elastic IP:** If you rely on a **fixed public IP**, associate an **Elastic IP** with the instance. After stop/start, a *non-EIP* public IP can change — update DNS and GitHub `EC2_HOST` if needed.
2. **GitHub Actions deploy:** If your workflow SSHs to an IP, use the Elastic IP or a DNS name that tracks the instance.
3. **Downtime:** The app is unavailable while the instance is stopped (roughly 23:30–08:00 IST).
4. **Disable schedule:** EventBridge → Rules → disable `trademanthan-ec2-stop-2330-ist` / `trademanthan-ec2-start-0800-ist`, or delete the CloudFormation stack.

## Remove

```bash
aws cloudformation delete-stack --stack-name trademanthan-ec2-schedule --region ap-south-1
```

(Use your actual stack name and region.)
