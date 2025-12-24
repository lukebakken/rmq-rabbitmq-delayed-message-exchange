# AWS Deployment Guide

## Overview

This guide covers deploying the RabbitMQ Delayed Message Exchange plugin with DynamoDB storage backend to AWS.

**Status**: Phase 2 complete, ready for AWS deployment  
**Date**: 2025-12-24

---

## Prerequisites

### AWS Resources
- 3 EC2 instances (t3.medium or larger recommended)
- DynamoDB table in same region as EC2
- IAM instance role with DynamoDB permissions
- Security groups configured for RabbitMQ cluster

### Software Requirements
- RabbitMQ 4.2.0+
- Erlang 26.2+
- aws-erlang library (included in plugin dependencies)

---

## Step 1: Create DynamoDB Table

```bash
aws dynamodb create-table \
    --table-name rabbitmq_delayed_messages \
    --attribute-definitions \
        AttributeName=partition_key,AttributeType=S \
        AttributeName=sort_key,AttributeType=S \
    --key-schema \
        AttributeName=partition_key,KeyType=HASH \
        AttributeName=sort_key,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region us-west-2
```

**Verify table creation**:
```bash
aws dynamodb describe-table --table-name rabbitmq_delayed_messages --region us-west-2
```

Wait for `TableStatus` to be `ACTIVE`.

---

## Step 2: Configure IAM Role

### Create IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DynamoDBDelayedMessages",
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:DeleteItem",
                "dynamodb:DescribeTable"
            ],
            "Resource": "arn:aws:dynamodb:us-west-2:*:table/rabbitmq_delayed_messages"
        }
    ]
}
```

### Attach to EC2 Instance Role

```bash
aws iam create-policy \
    --policy-name RabbitMQDelayedMessageDynamoDB \
    --policy-document file://policy.json

aws iam attach-role-policy \
    --role-name YourEC2InstanceRole \
    --policy-arn arn:aws:iam::ACCOUNT_ID:policy/RabbitMQDelayedMessageDynamoDB
```

---

## Step 3: Update Plugin Code for AWS

### Modify `src/rabbit_delayed_message_storage_ddb.erl`

**Current (DynamoDB Local)**:
```erlang
Client = aws_client:make_local_client(
    <<"fakeMyKeyId">>,
    <<"fakeSecretAccessKey">>,
    <<"8000">>,
    <<"localhost">>
),
```

**Change to (Real AWS)**:
```erlang
%% Get region from config or default to us-west-2
Region = maps:get(region, Config, <<"us-west-2">>),

%% Use IAM instance role for credentials
Client = aws_client:make_client(Region),
```

**Remove or comment out**:
```erlang
%% Endpoint = application:get_env(rabbitmq_delayed_message_exchange, 
%%                                dynamodb_endpoint, 
%%                                <<"http://localhost:8000">>),
```

---

## Step 4: Build Plugin

On your build machine:

```bash
cd /home/lrbakken/development/rabbitmq/rabbitmq-server

make ADDITIONAL_PLUGINS=rabbitmq_delayed_message_exchange \
     ENABLED_PLUGINS='rabbitmq_management rabbitmq_top rabbitmq_delayed_message_exchange' \
     NODES=1 \
     start-cluster

# This builds the plugin
# Find the .ez file in deps/rabbitmq_delayed_message_exchange/plugins/
```

---

## Step 5: Deploy to EC2 Instances

### Copy Plugin to Each Node

```bash
# Find plugins directory
ssh ec2-user@node1 'rabbitmq-plugins directories -s'

# Copy .ez file
scp deps/rabbitmq_delayed_message_exchange/plugins/rabbitmq_delayed_message_exchange-*.ez \
    ec2-user@node1:/usr/lib/rabbitmq/plugins/

# Repeat for node2 and node3
```

### Create Configuration File

On each EC2 instance, create `/etc/rabbitmq/advanced.config`:

```erlang
[
    {rabbitmq_delayed_message_exchange, [
        {storage_backend, rabbit_delayed_message_storage_ddb},
        {storage_config, #{
            table_name => <<"rabbitmq_delayed_messages">>,
            region => <<"us-west-2">>
        }}
    ]}
].
```

---

## Step 6: Enable Plugin and Start Cluster

On each node:

```bash
# Enable plugin
sudo rabbitmq-plugins enable rabbitmq_delayed_message_exchange

# Start RabbitMQ
sudo systemctl start rabbitmq-server
```

### Form Cluster

On node2 and node3:
```bash
sudo rabbitmqctl stop_app
sudo rabbitmqctl join_cluster rabbit@node1
sudo rabbitmqctl start_app
```

### Verify Cluster
```bash
sudo rabbitmqctl cluster_status
```

---

## Step 7: Verify Installation

### Check Plugin Status
```bash
sudo rabbitmq-plugins list | grep delayed
```

Should show:
```
[E*] rabbitmq_delayed_message_exchange
```

### Check Gen_Server
```bash
sudo rabbitmqctl eval 'global:whereis_name(rabbit_delayed_message).'
```

Should return a PID like `<0.1234.0>`.

### Check DynamoDB Table
```bash
aws dynamodb describe-table --table-name rabbitmq_delayed_messages --region us-west-2
```

---

## Step 8: Test Delayed Messages

### Create Exchange
```bash
curl -u guest:guest -X PUT \
    http://node1:15672/api/exchanges/%2F/test-delayed-exchange \
    -H "Content-Type: application/json" \
    -d '{
        "type": "x-delayed-message",
        "durable": true,
        "arguments": {
            "x-delayed-type": "direct"
        }
    }'
```

### Create and Bind Queue
```bash
curl -u guest:guest -X PUT \
    http://node1:15672/api/queues/%2F/test-queue

curl -u guest:guest -X POST \
    http://node1:15672/api/bindings/%2F/e/test-delayed-exchange/q/test-queue \
    -H "Content-Type: application/json" \
    -d '{"routing_key": "test"}'
```

### Publish Delayed Message
```bash
curl -u guest:guest -X POST \
    http://node1:15672/api/exchanges/%2F/test-delayed-exchange/publish \
    -H "Content-Type: application/json" \
    -d '{
        "properties": {
            "headers": {
                "x-delay": 5000
            }
        },
        "routing_key": "test",
        "payload": "Hello delayed world!",
        "payload_encoding": "string"
    }'
```

### Verify Delivery
Wait 5 seconds, then:
```bash
curl -u guest:guest -X POST \
    http://node1:15672/api/queues/%2F/test-queue/get \
    -H "Content-Type: application/json" \
    -d '{"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"}'
```

---

## Monitoring

### Check Logs
```bash
sudo tail -f /var/log/rabbitmq/rabbit@node1.log
```

Look for:
- `Delayed message DynamoDB storage initialized`
- `Stored delayed message payload in DynamoDB`
- `Fetched delayed message payload from DynamoDB`
- `Deleted delayed message payload from DynamoDB`

### Check DynamoDB Items
```bash
aws dynamodb scan \
    --table-name rabbitmq_delayed_messages \
    --region us-west-2 \
    --max-items 10
```

### Check Khepri Metadata
```bash
sudo rabbitmqctl eval 'khepri:get_many([rabbitmq, delayed_messages, <<"**">>]).'
```

---

## Troubleshooting

### Plugin Won't Start

**Check logs**:
```bash
sudo tail -100 /var/log/rabbitmq/rabbit@node1.log
```

**Common issues**:
- IAM role not attached or missing permissions
- DynamoDB table doesn't exist
- Wrong region configured
- hackney not starting (should auto-start)

### Messages Not Delivered

1. **Check if stored in DynamoDB**:
   ```bash
   aws dynamodb scan --table-name rabbitmq_delayed_messages --region us-west-2
   ```

2. **Check gen_server is running**:
   ```bash
   sudo rabbitmqctl eval 'global:whereis_name(rabbit_delayed_message).'
   ```

3. **Check Khepri metadata**:
   ```bash
   sudo rabbitmqctl eval 'khepri:get_many([rabbitmq, delayed_messages, <<"**">>]).'
   ```

### DynamoDB Access Denied

**Verify IAM role**:
```bash
# On EC2 instance
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

Should return the role name. Then:
```bash
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/ROLE_NAME
```

Should return temporary credentials.

---

## Performance Tuning

### DynamoDB Capacity Mode

**On-demand** (default):
- Auto-scales based on traffic
- No capacity planning needed
- Pay per request

**Provisioned** (for predictable load):
```bash
aws dynamodb update-table \
    --table-name rabbitmq_delayed_messages \
    --billing-mode PROVISIONED \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=100 \
    --region us-west-2
```

### Enable Auto-Scaling (Provisioned Mode)
```bash
aws application-autoscaling register-scalable-target \
    --service-namespace dynamodb \
    --resource-id table/rabbitmq_delayed_messages \
    --scalable-dimension dynamodb:table:ReadCapacityUnits \
    --min-capacity 5 \
    --max-capacity 1000
```

---

## Cost Estimation

### DynamoDB Costs (On-Demand)
- Write: $1.25 per million requests
- Read: $0.25 per million requests
- Storage: $0.25 per GB-month

**Example** (1M messages/day, 1-hour avg delay):
- Writes: 1M × $1.25/million = $1.25/day
- Reads: 1M × $0.25/million = $0.25/day
- Storage: ~1GB × $0.25 = $0.25/month
- **Total: ~$45/month**

### EC2 Costs
- t3.medium × 3 nodes: ~$0.0416/hour × 3 = ~$90/month
- EBS storage: ~$10/month

**Total estimated cost**: ~$145/month for 1M delayed messages/day

---

## Security Considerations

### Network Security
- RabbitMQ cluster ports (4369, 5672, 15672, 25672) restricted to cluster nodes
- Management UI (15672) restricted to admin IPs
- DynamoDB accessed via VPC endpoint (no internet)

### Data Security
- DynamoDB encryption at rest (enabled by default)
- TLS for RabbitMQ connections
- IAM roles instead of access keys

### Least Privilege
- EC2 instance role has minimal DynamoDB permissions
- No CreateTable permission in production (table pre-created)

---

## Rollback Plan

If deployment fails:

1. **Disable plugin**:
   ```bash
   sudo rabbitmq-plugins disable rabbitmq_delayed_message_exchange
   sudo systemctl restart rabbitmq-server
   ```

2. **Revert to disk storage**:
   Edit `/etc/rabbitmq/advanced.config`:
   ```erlang
   {storage_backend, rabbit_delayed_message_storage_disk}
   ```

3. **Delete DynamoDB table** (if needed):
   ```bash
   aws dynamodb delete-table --table-name rabbitmq_delayed_messages --region us-west-2
   ```

---

## Next Steps After Deployment

1. Performance testing with realistic load
2. Failover testing (kill nodes, verify recovery)
3. Monitoring and alerting setup
4. Message size validation implementation
5. Retry logic and circuit breakers
6. Production readiness review
