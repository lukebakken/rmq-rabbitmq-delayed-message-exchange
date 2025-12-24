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

### Phase 1: Khepri Migration (Current)
- Replace Mnesia with Khepri for metadata storage
- Use disk files for message payloads
- Test on 3-node local cluster

### Phase 2: DynamoDB Integration (Future)
- Add DynamoDB storage backend
- Test on 3-node EC2 cluster
- Validate replication and failover

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
