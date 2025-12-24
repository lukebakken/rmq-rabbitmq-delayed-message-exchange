# Session Summary - 2025-12-24

## Current Status: Phase 2 Complete ✓

**Date**: December 24, 2025  
**Achievement**: DynamoDB storage backend fully functional

### What Works Now
- ✅ Khepri metadata storage (replicated across cluster)
- ✅ DynamoDB payload storage (distributed, durable)
- ✅ Message publishing with delays (1-10 seconds tested)
- ✅ Correct message delivery after delay expires
- ✅ Multi-node cluster operation (3 nodes tested)
- ✅ 20/20 messages delivered successfully in test

---

## Architecture (Current)

```
Publisher → Exchange → rabbit_delayed_message gen_server (single, via mirrored_supervisor)
                       ↓
                    Khepri (metadata - replicated)
                    /delayed_messages/<vhost>/<exchange>/<bucket>/<msg_id>
                       ↓
                    DynamoDB (payloads - distributed)
                    PK: broker_id#vhost#exchange#bucket
                    SK: timestamp#message_id
                       ↓
                    Erlang Timer → Deliver to Queue
```

---

## Files Modified Today (2025-12-24)

### New Files
1. `rabbit_delayed_message_storage_ddb.erl` - DynamoDB storage backend
2. `advanced.config` - Configuration for storage backend selection
3. `ELASTICACHE_VS_DYNAMODB.md` - Storage backend comparison analysis

### Modified Files
1. `rabbit_delayed_message_storage.erl` - Added metadata parameter to all callbacks
2. `rabbit_delayed_message_storage_disk.erl` - Updated to match new behavior signature
3. `rabbit_delayed_message.erl` - Read storage backend from config, pass metadata to storage operations
4. `rabbit_delayed_message_sup.erl` - Changed boot step to require `database` instead of `pre_flight`
5. `Makefile` - Changed aws_erlang dependency from git to hex 1.2.1
6. `CODING_RULES.md` - Added Rules #7-18 from today's lessons

---

## Key Technical Decisions (2025-12-24)

### 1. Storage Backend Abstraction
**Decision**: Pass message metadata to all storage operations  
**Rationale**: DynamoDB needs metadata (vhost, exchange, timestamp) to construct partition/sort keys  
**Impact**: Both disk and DynamoDB backends updated to match new signature

### 2. Hackney Initialization
**Decision**: DynamoDB backend starts hackney in its `init/1` function  
**Rationale**: Only DynamoDB needs hackney (for HTTP), disk backend doesn't  
**Impact**: Cleaner separation - each backend manages its own dependencies

### 3. AWS Error Handling
**Decision**: Match aws-erlang's 3-tuple error format `{error, ErrorMap, {StatusCode, Headers, Client}}`  
**Rationale**: aws-erlang returns decoded JSON error in ErrorMap with `__type` field  
**Impact**: Check `__type` field for `ResourceNotFoundException` to detect missing table

### 4. DynamoDB Schema
**Partition Key**: `broker_id#vhost#exchange#bucket` (15-minute buckets)  
**Sort Key**: `timestamp#message_id`  
**Rationale**: Enables efficient queries within time ranges, isolates brokers  
**Impact**: Good partition distribution, supports multi-broker deployments

### 5. Boot Step Dependencies
**Decision**: Supervisor requires `database` boot step  
**Rationale**: Ensures Khepri is available before mirrored_supervisor starts  
**Impact**: Proper initialization order, no race conditions

---

## Test Results (2025-12-24)

### Local 3-Node Cluster with DynamoDB Local
```bash
./test_dmx_basic.sh -n 20 -min 1 -max 10 -c localhost:15672 -c localhost:15673 -c localhost:15674
```

**Results**:
- ✅ 20 messages published with random delays (1-10 seconds)
- ✅ All messages stored in DynamoDB successfully
- ✅ All messages delivered after correct delay
- ✅ 20/20 messages received in queue
- ✅ No errors in logs

**DynamoDB Table Verification**:
```bash
$ aws dynamodb list-tables --endpoint-url http://localhost:8000
{
    "TableNames": [
        "rabbitmq_delayed_messages"
    ]
}
```

---

## Critical Lessons Learned (2025-12-24)

### Lesson 1: Application Dependencies vs Boot Steps
**Problem**: Even though `hackney` was in the dependency chain (`rabbitmq_delayed_message_exchange → aws_erlang → hackney`), it wasn't started before our code ran.

**Root Cause**: Application dependencies ensure apps are **loaded**, not **started** at the right time during boot steps.

**Solution**: DynamoDB backend explicitly calls `application:ensure_all_started(hackney)` in its `init/1`.

### Lesson 2: AWS Error Response Format
**Problem**: Assumed aws-erlang would return `{error, {<<"ResourceNotFoundException">>, _}}` but it actually returns `{error, ErrorMap, {StatusCode, Headers, Client}}`.

**Root Cause**: Didn't verify the actual return format from aws-erlang library.

**Solution**: Check the `<<"__type">>` field in ErrorMap for full exception name like `<<"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException">>`.

### Lesson 3: Boot Step Return Values
**Problem**: Boot step MFA called `application:ensure_all_started(hackney)` which returns `{ok, [Apps]}`, but boot steps expect `ok`.

**Root Cause**: Didn't understand boot step requirements.

**Solution**: Wrapper function that calls `ensure_all_started` and returns `ok`.

---

## Configuration

### Storage Backend Selection
Edit `advanced.config`:
```erlang
[
    {rabbitmq_delayed_message_exchange, [
        %% Choose backend: rabbit_delayed_message_storage_disk or rabbit_delayed_message_storage_ddb
        {storage_backend, rabbit_delayed_message_storage_ddb},
        {storage_config, #{
            table_name => <<"rabbitmq_delayed_messages">>
        }}
    ]}
].
```

### DynamoDB Local (Development)
Set environment variable:
```bash
export RABBITMQ_CONFIG_FILE=/path/to/advanced.config
```

Application environment (in code):
```erlang
application:get_env(rabbitmq_delayed_message_exchange, dynamodb_endpoint, <<"http://localhost:8000">>)
```

---

## Next Steps: AWS Deployment

### Prerequisites
1. **EC2 instances** - 3-node RabbitMQ cluster
2. **DynamoDB table** - Create in same region as EC2
3. **IAM role** - Attach to EC2 instances with DynamoDB permissions
4. **Security groups** - Allow RabbitMQ cluster communication

### DynamoDB Table Creation
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

### IAM Policy for EC2 Instances
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:DeleteItem",
                "dynamodb:DescribeTable",
                "dynamodb:CreateTable"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/rabbitmq_delayed_messages"
        }
    ]
}
```

### Configuration Changes for AWS
Update `rabbit_delayed_message_storage_ddb.erl`:
```erlang
%% Replace make_local_client with make_client for real AWS
Client = aws_client:make_client(Region),
```

Remove or comment out:
```erlang
%% Endpoint = application:get_env(..., dynamodb_endpoint, ...)
```

### Deployment Steps
1. Build plugin on EC2 instances
2. Copy `.ez` file to plugins directory
3. Enable plugin: `rabbitmq-plugins enable rabbitmq_delayed_message_exchange`
4. Configure storage backend in `advanced.config`
5. Start RabbitMQ cluster
6. Verify DynamoDB table exists
7. Run test script against cluster

---

## Testing Checklist for AWS

- [ ] Single message with 5s delay
- [ ] Multiple messages with different delays (verify ordering)
- [ ] Leader failover (kill node with gen_server, verify new leader delivers)
- [ ] Node restart (verify messages survive restart)
- [ ] DynamoDB replication (verify data in AWS console)
- [ ] Cross-AZ operation (if multi-AZ deployment)
- [ ] Performance test (100+ messages)
- [ ] Error handling (stop DynamoDB, verify graceful failure)

---

## Known Limitations

### Phase 2 Limitations
- ⚠️ No message size validation (should reject > 256KB)
- ⚠️ No retry logic for DynamoDB failures
- ⚠️ No circuit breaker for DynamoDB unavailability
- ⚠️ No cleanup of orphaned DynamoDB items
- ⚠️ Naive bucket scanning (lists all messages to find next)
- ⚠️ No observability beyond logs (no CloudWatch metrics)

### Acceptable for POC
These limitations are documented and acceptable for proof-of-concept validation. Production deployment would require addressing these issues.

---

## Git Status

**Branch**: (current branch)  
**Last Commit**: Add DynamoDB storage backend with metadata-aware operations

**Uncommitted Changes**: None (all changes committed)

---

## Quick Start Commands

### Start Local Cluster with DynamoDB Backend
```bash
cd /home/lrbakken/development/rabbitmq/rabbitmq-server

# Start DynamoDB Local (in separate terminal)
docker run -p 8000:8000 amazon/dynamodb-local

# Start RabbitMQ cluster
make ADDITIONAL_PLUGINS=rabbitmq_delayed_message_exchange \
     ENABLED_PLUGINS='rabbitmq_management rabbitmq_top rabbitmq_delayed_message_exchange' \
     NODES=3 \
     start-cluster
```

### Run Test
```bash
cd deps/rabbitmq_delayed_message_exchange
./test_dmx_basic.sh -n 20 -min 1 -max 10 -c localhost:15672 -c localhost:15673 -c localhost:15674
```

### Verify DynamoDB
```bash
aws dynamodb list-tables --endpoint-url http://localhost:8000
aws dynamodb scan --table-name rabbitmq_delayed_messages --endpoint-url http://localhost:8000
```

### Check Logs
```bash
tail -f /tmp/rabbitmq-test-instances/rabbit-1@*/log/*.log
```

### Stop Cluster
```bash
make stop-cluster
```

---

## What's Next

### Immediate: AWS Deployment
1. Set up 3-node EC2 cluster
2. Create DynamoDB table in AWS
3. Configure IAM roles
4. Update client initialization for real AWS (not DynamoDB Local)
5. Deploy and test

### Future Enhancements
1. Message size validation (reject > 256KB)
2. Retry logic and circuit breakers
3. CloudWatch metrics integration
4. Bucket scanning optimization
5. TTL-based cleanup in DynamoDB
6. Performance benchmarking
7. Production readiness review

---

## Summary

**Phase 1** (Khepri + Disk): ✓ Complete  
**Phase 2** (Khepri + DynamoDB): ✓ Complete  
**Next**: AWS deployment and validation

The plugin now uses distributed storage (Khepri + DynamoDB) instead of node-local Mnesia tables, addressing the fundamental limitation of the original implementation.
