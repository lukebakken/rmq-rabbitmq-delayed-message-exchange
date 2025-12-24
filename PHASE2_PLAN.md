# Phase 2 Implementation Plan: DynamoDB Storage Backend

**Goal**: Implement DynamoDB storage backend to replace disk storage

**Target**: 3-node RabbitMQ cluster on EC2 with DynamoDB access

---

## Architecture

### Phase 2 (Khepri + DynamoDB)
```
Publisher → Exchange → rabbit_delayed_message gen_server (single, via mirrored_supervisor)
                       ↓
                    Khepri (replicated metadata)
                    /delayed_messages/<vhost>/<exchange>/<bucket>/<msg_id>
                       ↓
                    DynamoDB (message payloads)
                    Table: delayed_messages
                    PK: broker_id#vhost#exchange#timestamp_bucket
                    SK: delivery_timestamp#message_id
                       ↓
                    Erlang Timer
                       ↓
                    Deliver to Queue
```

---

## Implementation Tasks

### 1. Create DynamoDB Storage Backend
**File**: `src/rabbit_delayed_message_storage_ddb.erl`

Implement the `rabbit_delayed_message_storage` behavior:

```erlang
-module(rabbit_delayed_message_storage_ddb).
-behaviour(rabbit_delayed_message_storage).

-export([init/1, store_message/3, fetch_message/2, delete_message/2, terminate/1]).

init(Config) ->
    %% Initialize AWS client
    %% Get region, table name from config
    %% Create client state
    ...

store_message(MessageId, Payload, State) ->
    %% Build DynamoDB PutItem request
    %% Call aws_dynamodb:put_item/2
    ...

fetch_message(MessageId, State) ->
    %% Build DynamoDB GetItem request
    %% Call aws_dynamodb:get_item/2
    ...

delete_message(MessageId, State) ->
    %% Build DynamoDB DeleteItem request
    %% Call aws_dynamodb:delete_item/2
    ...
```

### 2. DynamoDB Table Schema

**Table name**: `rabbitmq_delayed_messages` (configurable)

**Primary key**:
- Partition key: `partition_key` (STRING)
  - Format: `{broker_id}#{vhost}#{exchange}#{timestamp_bucket}`
  - Example: `localhost#/#test-exchange#2025-12-24-07-15`
- Sort key: `sort_key` (STRING)
  - Format: `{delivery_timestamp_ms}#{message_id_hex}`
  - Example: `1735059300000#a1b2c3d4e5f6...`

**Attributes**:
- `message_payload` (BINARY) - The serialized message
- `created_at` (NUMBER) - When message was published (milliseconds)

**TTL**: Optional - set on `delivery_timestamp` + grace period for auto-cleanup

### 3. Configuration

Add to RabbitMQ config:

```erlang
{rabbitmq_delayed_message_exchange, [
    {storage_backend, rabbit_delayed_message_storage_ddb},
    {ddb_config, #{
        region => <<"us-west-2">>,
        table_name => <<"rabbitmq_delayed_messages">>,
        endpoint => undefined  %% Use default AWS endpoint
    }}
]}
```

### 4. AWS Client Initialization

```erlang
init(Config) ->
    Region = maps:get(region, Config, <<"us-west-2">>),
    TableName = maps:get(table_name, Config, <<"rabbitmq_delayed_messages">>),
    
    %% Create AWS client (uses IAM instance role for credentials)
    Client = aws_client:make_client(Region),
    
    State = #{
        client => Client,
        table_name => TableName,
        broker_id => rabbit:cluster_name()
    },
    
    {ok, State}.
```

### 5. PutItem Implementation

```erlang
store_message(MessageId, Payload, State = #{client := Client, table_name := TableName}) ->
    HexId = binary:encode_hex(MessageId, lowercase),
    
    %% Build partition and sort keys
    %% (Need delivery timestamp from Khepri metadata - TODO: pass as parameter)
    PartitionKey = build_partition_key(State, ...),
    SortKey = build_sort_key(DeliveryTimestamp, HexId),
    
    Item = #{
        <<"partition_key">> => #{<<"S">> => PartitionKey},
        <<"sort_key">> => #{<<"S">> => SortKey},
        <<"message_payload">> => #{<<"B">> => base64:encode(Payload)},
        <<"created_at">> => #{<<"N">> => integer_to_binary(erlang:system_time(milli_seconds))}
    },
    
    Request = #{<<"TableName">> => TableName, <<"Item">> => Item},
    
    case aws_dynamodb:put_item(Client, Request) of
        {ok, _Response, _} ->
            {ok, State};
        {error, Reason} ->
            {error, {dynamodb_put_failed, Reason}}
    end.
```

### 6. GetItem Implementation

```erlang
fetch_message(MessageId, State = #{client := Client, table_name := TableName}) ->
    HexId = binary:encode_hex(MessageId, lowercase),
    
    %% Build keys (need to reconstruct from Khepri metadata)
    PartitionKey = build_partition_key(State, ...),
    SortKey = build_sort_key(DeliveryTimestamp, HexId),
    
    Key = #{
        <<"partition_key">> => #{<<"S">> => PartitionKey},
        <<"sort_key">> => #{<<"S">> => SortKey}
    },
    
    Request = #{<<"TableName">> => TableName, <<"Key">> => Key},
    
    case aws_dynamodb:get_item(Client, Request) of
        {ok, #{<<"Item">> := Item}, _} ->
            Payload = base64:decode(maps:get(<<"B">>, maps:get(<<"message_payload">>, Item))),
            {ok, Payload, State};
        {ok, #{}, _} ->
            {error, {not_found, MessageId}};
        {error, Reason} ->
            {error, {dynamodb_get_failed, Reason}}
    end.
```

### 7. DeleteItem Implementation

```erlang
delete_message(MessageId, State = #{client := Client, table_name := TableName}) ->
    HexId = binary:encode_hex(MessageId, lowercase),
    
    %% Build keys
    PartitionKey = build_partition_key(State, ...),
    SortKey = build_sort_key(DeliveryTimestamp, HexId),
    
    Key = #{
        <<"partition_key">> => #{<<"S">> => PartitionKey},
        <<"sort_key">> => #{<<"S">> => SortKey}
    },
    
    Request = #{<<"TableName">> => TableName, <<"Key">> => Key},
    
    case aws_dynamodb:delete_item(Client, Request) of
        {ok, _Response, _} ->
            {ok, State};
        {error, Reason} ->
            {error, {dynamodb_delete_failed, Reason}}
    end.
```

---

## Open Issues

### Issue 1: Partition/Sort Key Construction

**Problem**: The storage backend doesn't have access to delivery timestamp, exchange, vhost, etc. - only MessageId and Payload.

**Options**:
1. **Pass metadata as parameter** - Change storage behavior to accept metadata
2. **Store metadata in DynamoDB** - Duplicate what's in Khepri
3. **Encode in MessageId** - Make MessageId contain all needed info

**Recommendation**: Option 1 - extend storage behavior to pass metadata.

### Issue 2: Broker ID

**Problem**: Need broker_id for partition key to isolate different RabbitMQ clusters using same DynamoDB table.

**Solution**: Use `rabbit:cluster_name()` as broker_id (already decided in DESIGN_QUESTIONS.md).

### Issue 3: Error Handling

**Problem**: What to do when DynamoDB operations fail?

**Options**:
1. Return error to publisher (NACK message)
2. Retry with exponential backoff
3. Store in local fallback (disk)
4. Dead letter queue

**Recommendation**: For POC, return error to publisher. Add retry logic later.

---

## Performance Considerations

### Connection Pooling

**Current**: aws-erlang uses hackney's default connection pool.

**Future optimization**: Create dedicated DynamoDB pool for better control:

```erlang
%% In application startup or storage backend init
PoolName = dynamodb_pool,
PoolOptions = [
    {timeout, 60000},        %% Keep connections alive for 60s
    {max_connections, 50}    %% Max 50 concurrent connections to DynamoDB
],
ok = hackney_pool:start_pool(PoolName, PoolOptions),

%% In DynamoDB requests
Options = [{pool, dynamodb_pool}],
aws_dynamodb:put_item(Client, Request, Options).
```

**Benefits of dedicated pool**:
- Separate connection limits from other HTTP traffic
- Tune keepalive timeout for DynamoDB specifically
- Better observability via pool-specific metrics
- Prevent DynamoDB traffic from exhausting default pool

**When to implement**: During performance benchmarking phase, if default pool shows contention or if DynamoDB latency is high.

---

## Testing Strategy

### Unit Tests
- DynamoDB client initialization
- PutItem/GetItem/DeleteItem operations
- Error handling (network failures, throttling)
- Key construction (partition key, sort key)

### Integration Tests
1. **Single node**: Publish → store in DynamoDB → deliver
2. **3-node cluster**: Verify single process handles all operations
3. **Failover**: Kill node with process, verify migration and continued operation
4. **DynamoDB unavailable**: Verify error handling
5. **Large messages**: Test up to 256KB limit

### Performance Tests
- Throughput: Messages/second
- Latency: Publish to delivery time
- Concurrent load: Multiple publishers
- Connection pool efficiency: Monitor reuse vs new connections

---

## Success Criteria

Phase 2 is complete when:
1. ✅ DynamoDB storage backend implemented
2. ✅ Messages stored in DynamoDB with correct schema
3. ✅ Messages delivered correctly after delay
4. ✅ Works on 3-node EC2 cluster
5. ✅ Failover works (process migrates, continues delivery)
6. ✅ No disk storage dependency (except for POC testing)

---

## Next Steps After Phase 2

1. Performance benchmarking
2. Connection pool tuning (if needed)
3. Error handling improvements (retry logic, circuit breakers)
4. Monitoring and observability
5. Production readiness review
6. Consider migration to Ra state machine (long-term)
