# Development Guide

## Building and Testing

### Starting a 3-Node Cluster with Plugin Enabled

From the RabbitMQ server root directory (`/home/lrbakken/development/rabbitmq/rabbitmq-server`):

```bash
make ADDITIONAL_PLUGINS=rabbitmq_delayed_message_exchange \
     ENABLED_PLUGINS='rabbitmq_management rabbitmq_top rabbitmq_delayed_message_exchange' \
     NODES=3 \
     start-cluster
```

This will:
- Build the plugin as an additional plugin
- Enable the plugin along with management and top plugins
- Start a 3-node cluster on localhost (ports 5672, 5673, 5674)

### Running the Basic Test

Once the cluster is running:

```bash
cd deps/rabbitmq_delayed_message_exchange
./test_dmx_basic.sh
```

This tests basic delayed message functionality via the HTTP API.

**Phase 1 Status**: ✓ Test passing as of 2025-12-23

### Stopping the Cluster

```bash
make stop-cluster
```

## Development Workflow

### Phase 1: Khepri Migration ✓ COMPLETE (2025-12-23)
- ✅ Replaced Mnesia with Khepri for metadata storage
- ✅ Implemented disk file storage for message payloads
- ✅ Tested on 3-node local cluster
- ✅ Basic functionality validated

**Remaining**: Failover testing, node restart testing

### Phase 2: DynamoDB Integration (Next)
- Add DynamoDB storage backend
- Test on 3-node EC2 cluster
- Validate DynamoDB replication
```

## Development Workflow

### Phase 1: Khepri Migration ✓ COMPLETE (2025-12-23)
- ✅ Replaced Mnesia with Khepri for metadata storage
- ✅ Implemented disk file storage for message payloads
- ✅ Tested on 3-node local cluster
- ✅ Basic functionality validated

### Phase 2: DynamoDB Integration ✓ COMPLETE (2025-12-24)
- ✅ Implemented DynamoDB storage backend
- ✅ Tested on 3-node local cluster with DynamoDB Local
- ✅ 20/20 messages delivered successfully
- ✅ Error handling for aws-erlang responses

**Next**: Deploy to AWS EC2 cluster with real DynamoDB

---

## Configuration

### Storage Backend Selection

Create `advanced.config` in the plugin directory:

**For Disk Storage**:
```erlang
[
    {rabbitmq_delayed_message_exchange, [
        {storage_backend, rabbit_delayed_message_storage_disk},
        {storage_config, #{
            base_dir => <<"/tmp/rabbitmq-test-instances/delayed_messages">>
        }}
    ]}
].
```

**For DynamoDB Storage**:
```erlang
[
    {rabbitmq_delayed_message_exchange, [
        {storage_backend, rabbit_delayed_message_storage_ddb},
        {storage_config, #{
            table_name => <<"rabbitmq_delayed_messages">>
        }}
    ]}
].
```

### DynamoDB Local Setup
```bash
# Start DynamoDB Local
docker run -p 8000:8000 amazon/dynamodb-local

# Verify it's running
aws dynamodb list-tables --endpoint-url http://localhost:8000
```

---

## Testing

### Basic Test (Single Message)
```bash
cd deps/rabbitmq_delayed_message_exchange
./test_dmx_basic.sh
```

### Multiple Messages Test
```bash
./test_dmx_basic.sh -n 20 -min 1 -max 10 -c localhost:15672 -c localhost:15673 -c localhost:15674
```

Parameters:
- `-n` - Number of messages
- `-min` - Minimum delay in seconds
- `-max` - Maximum delay in seconds
- `-c` - RabbitMQ connection (can specify multiple)

---

## Verification

### Check DynamoDB
```bash
# List tables
aws dynamodb list-tables --endpoint-url http://localhost:8000

# Scan table contents
aws dynamodb scan --table-name rabbitmq_delayed_messages --endpoint-url http://localhost:8000
```

### Check Khepri Metadata
```bash
./sbin/rabbitmqctl -n rabbit-1 eval 'khepri:get_many([rabbitmq, delayed_messages, <<"**">>]).'
```

### Check Gen_Server
```bash
./sbin/rabbitmqctl -n rabbit-1 eval 'global:whereis_name(rabbit_delayed_message).'
```

---

## Useful Commands

### Check Plugin Status
```bash
rabbitmq-plugins list | grep delayed
```

### View Cluster Status
```bash
rabbitmqctl cluster_status
```

### View Delayed Messages
```bash
rabbitmqctl eval 'rabbit_delayed_message:messages_delayed(Exchange).'
```

## Documentation

- `DESIGN_QUESTIONS.md` - Design decisions and architecture
- `PHASE1_PLAN.md` - Phase 1 implementation plan
- `CODEBASE_SUMMARY.md` - Current codebase analysis
- `test_dmx_basic.sh` - Basic functionality test script
