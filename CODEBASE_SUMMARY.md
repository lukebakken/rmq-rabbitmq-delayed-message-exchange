# RabbitMQ Delayed Message Exchange - Codebase Summary

## Overview

The `rabbitmq_delayed_message_exchange` plugin implements a custom exchange type (`x-delayed-message`) that delays message delivery by a specified number of milliseconds. Messages are stored in Mnesia tables until their scheduled delivery time, at which point they are routed to bound queues.

**Critical Design Limitation**: This plugin is designed for short-term delays (seconds, minutes, hours, max 1-2 days). It is **NOT** suitable for long-term scheduling (days, weeks, months) or high-volume delayed messages (100k+ messages).

## Architecture

### Core Components

#### 1. `rabbit_delayed_message.erl` - Message Delay Manager (gen_server)
The heart of the plugin. A singleton gen_server process that:

- **Stores delayed messages** in two Mnesia tables (node-local, disc_copies):
  - `rabbit_delayed_message_<node>` - Main table storing message deliveries (type: bag)
  - `rabbit_delayed_message_<node>_index` - Index table for timestamp ordering (type: ordered_set)

- **Manages a single Erlang timer** that fires for the next message to deliver
  - Timer is dynamically adjusted when new messages arrive with earlier delivery times
  - Uses `erlang:start_timer/3` with max delay of `?ERL_MAX_T` (4294967295 ms ≈ 49.7 days)

- **Delivers messages** by:
  1. Reading all messages for the expired timestamp key from Mnesia
  2. Routing each message through `rabbit_exchange:route/2` using the original exchange
  3. Delivering to target queues via `rabbit_queue_type:deliver/4`
  4. Removing delay header (sets `x-delay` to negative value to prevent re-delay)
  5. Deleting records from both Mnesia tables

**Key Data Structures**:
```erlang
-record(delay_key, {
    timestamp,  %% Delivery timestamp in milliseconds (erlang:system_time(milli_seconds))
    exchange    %% rabbit_types:exchange() record
}).

-record(delay_entry, {
    delay_key,  %% delay_key record
    delivery,   %% mc:state() - the message
    ref         %% make_ref() - unique reference for bag semantics
}).

-record(delay_index, {
    delay_key,  %% delay_key record
    const       %% Always 'true' - dummy field for record structure
}).
```

**State Machine**:
- `not_set` - No timer active (no delayed messages)
- `reference()` - Active timer reference

**Recovery**: On startup, the plugin:
1. Recovers all durable `x-delayed-message` exchanges from the database
2. Recovers their bindings
3. Re-initializes the timer for the earliest delayed message

#### 2. `rabbit_exchange_type_delayed_message.erl` - Exchange Type Implementation
Implements the `rabbit_exchange_type` behavior as a **proxy exchange**:

- **Validates** exchange declaration:
  - Requires `x-delayed-type` argument (cannot be `x-delayed-message` itself)
  - Validates that `x-delayed-type` refers to an existing exchange type (direct, topic, fanout, etc.)

- **Routes messages**:
  - If message has `x-delay` header with valid delay (0 < delay ≤ 4294967295 ms):
    - Calls `rabbit_delayed_message:delay_message/3` to store message
    - Returns empty routing result (message not immediately routed)
  - If no delay or invalid delay:
    - Routes immediately using the underlying exchange type specified in `x-delayed-type`
    - For direct exchanges, uses legacy routing v1 (not v2) because index table only stores direct exchange bindings

- **Delegates all other operations** to the underlying exchange type:
  - `validate_binding/2`, `create/2`, `delete/2`, `policy_changed/2`
  - `add_binding/3`, `remove_bindings/3`, `assert_args_equivalence/2`

**Special Routing Note**: Direct exchange routing uses `rabbit_router:match_routing_key/2` (v1) instead of v2 because the `rabbit_index_route` table only contains bindings for actual direct exchanges, not proxy exchanges.

#### 3. `rabbit_delayed_message_utils.erl` - Utility Functions
Provides message header manipulation:

- **`get_delay/1`**: Extracts `x-delay` header from message
  - Accepts integer types: `long`, `ubyte`, `short`, `ushort`, `int`, `uint`
  - Converts string types: `utf8`, `binary` (via `rabbit_data_coercion:to_integer/1`)
  - Converts float types: `double`, `float` (via `trunc/1`)
  - Returns `{ok, Delay}` or `{error, nodelay}`

- **`swap_delay_header/1`**: Negates the `x-delay` value after delivery
  - Prevents re-delay if message is routed through another delayed exchange
  - Preserves header for downstream consumers to see original delay
  - Uses `mc:set_annotation/3` to update message metadata

#### 4. `rabbit_delayed_message_sup.erl` - Supervisor
Simple `one_for_one` supervisor that:
- Starts the `rabbit_delayed_message` gen_server as a transient worker
- Registered as part of the `rabbit_sup` supervision tree
- Boot step: `rabbit_delayed_message_supervisor`

#### 5. `rabbit_delayed_message_app.erl` - Application Callback
Minimal application behavior:
- Calls `rabbit_delayed_message:go()` on startup to initialize timer
- Returns empty supervisor spec (actual supervision via `rabbit_delayed_message_sup`)

## Boot Sequence

1. **`rabbit_delayed_message_sup`** - Supervisor starts (requires `pre_flight`)
2. **`rabbit_delayed_message:setup_mnesia/0`** - Creates Mnesia tables (requires `pre_flight`)
   - If Khepri is enabled: Ensures Mnesia is running first (creates local schema if needed)
   - Creates two tables: main table (bag) and index table (ordered_set)
3. **`rabbit_exchange_type_delayed_message`** - Registers exchange type (requires `rabbit_registry`, enables `recovery`)
4. **Application start** - Calls `go()` to initialize timer from persisted messages

**Khepri Compatibility**: 
- If Khepri is enabled before plugin: Works normally (starts Mnesia locally)
- If plugin enabled, then Khepri enabled: **Plugin must be restarted** or node rebooted

## Message Flow

### Publishing with Delay
```
1. Client publishes to x-delayed-message exchange with x-delay header
2. rabbit_exchange_type_delayed_message:route/3 called
3. get_delay/1 extracts delay value from header
4. If valid delay (0 < delay ≤ 4294967295):
   a. rabbit_delayed_message:delay_message/3 called
   b. gen_server:call to rabbit_delayed_message process
   c. internal_delay_message/4 writes to Mnesia:
      - Calculates DelayTS = Now + Delay (milliseconds)
      - Writes delay_index record to index table
      - Writes delay_entry record to main table
   d. Timer management:
      - If no timer: Start new timer for this message
      - If timer exists and new delay < current: Cancel and restart
      - If timer exists and new delay ≥ current: Keep existing timer
   e. Returns empty routing result (message not delivered yet)
5. If no delay or invalid: Route immediately via underlying exchange type
```

### Scheduled Delivery
```
1. Timer expires, sends {timeout, TimerRef, {deliver, Key}} to gen_server
2. handle_info/2 processes timeout:
   a. Read all delay_entry records for Key from main table
   b. For each message:
      - Extract message from delivery record
      - Call swap_delay_header/1 to negate x-delay
      - Route via rabbit_exchange:route/2
      - Get target queues via rabbit_db_queue:get_targets/1
      - Deliver via rabbit_queue_type:deliver/4
      - Bump routing statistics
   c. Delete records from both tables
   d. Call maybe_delay_first/0 to schedule next delivery
3. maybe_delay_first/0:
   a. Get first key from index table (ordered_set)
   b. If exists: Calculate delay and start new timer
   c. If empty: Set timer to not_set
```

### Edge Cases Handled
- **No messages for index key**: Timer fires, finds empty main table, deletes index entry, continues
- **Timer already expired**: `erlang:read_timer/1` returns `false`, keeps existing timer (handler will fire soon)
- **Negative delay in timer calculation**: `erlang:max(0, Delay)` ensures non-negative timer value

## Mnesia Schema

### Main Table: `rabbit_delayed_message_<node>`
- **Type**: `bag` (allows duplicate keys)
- **Storage**: `disc_copies` on local node only
- **Key**: `#delay_key{timestamp, exchange}`
- **Records**: `#delay_entry{delay_key, delivery, ref}`
- **Purpose**: Stores actual message deliveries, multiple messages can have same timestamp+exchange

### Index Table: `rabbit_delayed_message_<node>_index`
- **Type**: `ordered_set` (sorted by key, unique keys)
- **Storage**: `disc_copies` on local node only
- **Key**: `#delay_key{timestamp, exchange}`
- **Records**: `#delay_index{delay_key, const}`
- **Purpose**: Provides ordered iteration for finding next message to deliver

**Why Two Tables?**
- Index table is ordered_set for efficient `mnesia:dirty_first/1` to find earliest timestamp
- Main table is bag to allow multiple messages with same timestamp+exchange
- Separation allows efficient lookup and ordering without scanning all messages

## Limitations & Design Issues

### 1. **Single Node Storage**
- Messages stored only on the node where exchange is declared
- **No replication** across cluster nodes
- Losing the node = losing all delayed messages on that node
- Disabling plugin = **ALL delayed messages lost**

### 2. **Single Timer Architecture**
- Only one timer active at a time (for earliest message)
- High message volume causes frequent timer cancellation/restart
- Performance degrades with many delayed messages (see issue #72)

### 3. **Scalability Problems**
- Not designed for 100k+ delayed messages
- Mnesia table scans become expensive
- Single gen_server bottleneck for all delay operations

### 4. **Delivery Guarantees**
- **Single delivery attempt** only (publishing is local, so usually succeeds)
- No retry mechanism if queues are unavailable
- `mandatory` flag not supported (cannot guarantee queues exist at future delivery time)

### 5. **Time Constraints**
- Max delay: 4294967295 ms (≈49.7 days) due to Erlang timer limits
- Designed for seconds/minutes/hours, not days/weeks/months

### 6. **Routing Limitations**
- Direct exchange uses v1 routing (not v2) because proxy exchange bindings not in index table
- Cannot guarantee original connection alive for `basic.return` (mandatory flag)

## Statistics & Monitoring

### Exchange Info
- `messages_delayed` - Count of delayed messages for an exchange
- Calculated via `mnesia:dirty_select/2` on main table
- Available via `rabbit_exchange:info_all/1` or management API

### Core Metrics
- Routing statistics bumped when messages delivered (not when delayed)
- Uses fake channel PID (self()) to avoid GC issues with dead channel processes
- Metrics tracked: `channel_queue_exchange_metrics` table
- Only tracked when `collect_statistics` = `fine`

## Testing

### Test Coverage (`test/plugin_SUITE.erl`)
- **Routing tests**: Verify proxy behavior for direct, topic, fanout exchanges
- **Delay tests**: Verify messages delivered in correct order
- **E2E tests**: End-to-end with and without delays
- **Persistence tests**: Node restart before/after delay expires
- **Edge cases**: Missing messages for index keys, string delay headers
- **Statistics tests**: Fine-grained stats collection

### Test Patterns
- Uses AMQP client library for message publishing
- Declares durable exchanges/queues for restart tests
- Verifies message order by payload (messages contain their own delay value)
- Tests both immediate routing (delay=0) and delayed routing

## Configuration

### Exchange Declaration
```erlang
Args = [{<<"x-delayed-type">>, longstr, <<"direct">>}],
#'exchange.declare'{
    exchange = <<"my-exchange">>,
    type = <<"x-delayed-message">>,
    arguments = Args
}
```

### Message Publishing
```erlang
Headers = [{<<"x-delay">>, signedint, 5000}],  % 5 second delay
#'basic.publish'{exchange = <<"my-exchange">>},
#amqp_msg{props = #'P_basic'{headers = Headers}}
```

### Supported Delay Header Types
- **Integers**: `long`, `ubyte`, `short`, `ushort`, `int`, `uint`
- **Strings**: `utf8`, `binary` (converted via `to_integer/1`)
- **Floats**: `double`, `float` (truncated via `trunc/1`)

## Dependencies

### Required Applications
- `kernel`, `stdlib` - Erlang standard libraries
- `rabbit_common` - RabbitMQ common types and utilities
- `rabbit` - RabbitMQ broker core

### Key RabbitMQ Modules Used
- `rabbit_exchange` - Exchange routing and management
- `rabbit_exchange_type` - Exchange type behavior
- `rabbit_router` - Message routing (v1 for direct)
- `rabbit_db_queue` - Queue database operations
- `rabbit_queue_type` - Queue type abstraction
- `rabbit_binding` - Binding management
- `rabbit_registry` - Plugin registry
- `rabbit_event` - Event system for statistics
- `rabbit_core_metrics` - Core metrics tracking
- `rabbit_global_counters` - Global counter tracking
- `rabbit_mnesia` - Mnesia utilities
- `rabbit_khepri` - Khepri metadata store detection
- `mc` - Message container abstraction (modern message format)

### Mnesia Operations
- `mnesia:dirty_write/2` - Write without transaction
- `mnesia:dirty_read/2` - Read without transaction
- `mnesia:dirty_delete/2` - Delete without transaction
- `mnesia:dirty_first/1` - Get first key (ordered_set)
- `mnesia:dirty_select/2` - Pattern matching select

## Future Improvements Needed

The README explicitly states: "This plugin badly needs a new design and a reimplementation from the ground up."

### Known Issues
- Issue #72: Performance with high message counts
- Issue #229: New design discussion
- Single node storage (no replication)
- Single timer bottleneck
- No retry mechanism for failed deliveries
- Scalability limitations with large message volumes

### Potential Enhancements
- Distributed storage across cluster nodes
- Multiple timers or timer wheel for better scalability
- Retry mechanism for delivery failures
- Support for longer delays (external scheduler integration)
- Better handling of high-volume scenarios
- Replication for message durability

## Code Quality Notes

### Strengths
- Clear separation of concerns (exchange type, delay manager, utils)
- Comprehensive test coverage
- Good error handling for edge cases
- Proper use of Erlang/OTP patterns (gen_server, supervisor)
- Handles Mnesia/Khepri compatibility

### Areas for Improvement
- Single gen_server bottleneck limits scalability
- No message replication (durability risk)
- Timer management could be more sophisticated
- Limited observability (only message count metric)
- No backpressure mechanism for high message rates

## Version Information
- Current version: v4.2.x
- Requires RabbitMQ: 4.2.0+
- Requires Erlang: 26.2+
- License: MPL 2.0

---

## Phase 1 Implementation: Khepri + Disk Storage ✓ COMPLETE

**Completion Date**: 2025-12-23

### What Changed

The plugin has been successfully migrated from Mnesia to Khepri for metadata storage:

**Removed**:
- ❌ Node-local Mnesia tables (`rabbit_delayed_message_<node>`, `rabbit_delayed_message_<node>_index`)
- ❌ Mnesia setup and cleanup functions
- ❌ All `mnesia:*` operations

**Added**:
- ✅ `rabbit_delayed_message_storage` - Storage backend behavior
- ✅ `rabbit_delayed_message_storage_disk` - Disk storage implementation
- ✅ `rabbit_delayed_message_khepri` - Khepri operations wrapper
- ✅ Khepri metadata storage (replicated across cluster)
- ✅ Pluggable storage backend architecture

### Architecture

**Metadata Storage (Khepri)**:
```
Path: [rabbitmq, delayed_messages, VHost, Exchange, Bucket, MessageId]
Data: #{
  message_id => binary(),
  delivery_timestamp => integer(),
  routing_key => binary(),
  exchange => binary(),
  vhost => binary(),
  created_at => integer()
}
```

**Payload Storage (Disk)**:
```
Location: /tmp/rabbitmq-test-instances/delayed_messages/<message_id>.msg
Format: term_to_binary(mc:state())
```

### Test Results

✅ **Basic functionality test passed**:
- Message published with 5-second delay
- Stored in Khepri (metadata) and disk (payload)
- Delivered correctly after delay
- x-delay header swapped to negative
- Message routed to correct queue

### Known Limitations (Phase 1)

- ⚠️ Disk storage not replicated (payloads lost if node with file fails)
- ⚠️ Naive bucket scanning (lists all messages to find next)
- ⚠️ No cleanup of orphaned files
- ⚠️ Message size not validated before storage

These limitations are acceptable for Phase 1 and will be addressed in Phase 2 (DynamoDB).

### Next Steps

**Phase 1 Remaining**:
- Test leader failover
- Test node restart
- Test multiple messages with different delays

**Phase 2**:
- Implement DynamoDB storage backend
- Add broker_id to partition keys
- Test on EC2 3-node cluster
- Validate true distributed storage

---

## Modern RabbitMQ Features for Overcoming Limitations

### Analysis Date: 2025-12-23

The following modern RabbitMQ features could address the fundamental limitations of the current DMX implementation:

### 1. Ra (Raft Consensus) for Distributed Replication

**Location**: `deps/ra/` - RabbitMQ's Raft consensus library

**What it provides**:
- Multi-node replication with strong consistency guarantees
- Automatic leader election and failover
- Durable, replicated state machine via write-ahead log (WAL)
- Powers quorum queues (`rabbit_fifo` state machine)

**How it solves DMX limitations**:
- **Single-node storage** → Messages replicated across cluster nodes (3+ replicas)
- **Data loss on node failure** → Delayed messages survive on other replicas
- **No durability guarantees** → Ra's WAL ensures persistence across restarts
- **Single gen_server bottleneck** → Ra distributes load across cluster

**Implementation pattern** (from `rabbit_fifo.erl`):
```erlang
-behaviour(ra_machine).

%% State machine callbacks
init/1          - Initialize replicated state
apply/3         - Apply commands (enqueue delayed message)
state_enter/2   - Handle state transitions
tick/2          - Periodic callback for timer management
```

**Key insight**: `rabbit_fifo` demonstrates how to build a replicated queue with:
- `delivery_limit` for redelivery attempts (could adapt for delayed message retries)
- `msg_ttl` for message expiration
- Efficient message indexing via Ra log indexes
- Leader manages active operations, followers replicate

### 2. Quorum Queue Architecture Pattern

**Location**: `deps/rabbit/src/rabbit_quorum_queue.erl`, `rabbit_fifo.erl`

**What it provides**:
- Replicated queues using Ra state machine
- Leader handles writes, followers replicate synchronously
- Automatic failover if leader dies (new leader elected)
- Built-in metrics and observability

**How it solves DMX limitations**:
- **Scalability** → Multiple Ra clusters could shard by timestamp ranges
- **Single timer bottleneck** → Each Ra cluster manages its own timer
- **Limited observability** → Ra provides comprehensive metrics
- **No replication** → 3-5 node replication standard

**Sharding strategy example**:
- Ra cluster 1: Messages delayed 0-1 hour
- Ra cluster 2: Messages delayed 1-6 hours  
- Ra cluster 3: Messages delayed 6-24 hours
- Ra cluster 4: Messages delayed 1-7 days

### 3. Khepri for Metadata Storage

**Location**: `deps/rabbit/src/rabbit_khepri.erl`

**What it provides**:
- Raft-based metadata store (replacing Mnesia)
- Tree-structured key-value store built on Ra
- Cluster-wide replication and consistency
- Better performance than Mnesia for metadata operations

**How it solves DMX limitations**:
- **Node-local Mnesia tables** → Khepri replicates data cluster-wide
- **Metadata not replicated** → Exchange bindings and config automatically replicated
- **Mnesia scalability issues** → Khepri designed for better performance

**Tree structure for delayed messages**:
```
/delayed_messages/
  /<exchange_name>/
    /2025/12/23/14/30/
      /<message_id_1>
      /<message_id_2>
```

**Current DMX issue**: Creates node-local tables:
```erlang
mnesia:create_table(?TABLE_NAME, [..., {disc_copies, [node()]}])
```
This is why messages are lost if that node fails. Khepri would replicate automatically.

### 4. Stream Queues for High-Volume Storage

**Location**: `deps/rabbit/src/rabbit_stream_queue.erl`

**What it provides**:
- Append-only log backed by Osiris (disk-based)
- Offset-based consumption model
- Replication across nodes
- Designed for millions of messages
- Time-based and size-based retention

**How it solves DMX limitations**:
- **High message volume** → Efficient disk storage, not memory-bound
- **Scalability** → Handles 100k+ messages easily
- **Replication** → Messages replicated across cluster
- **Efficient scanning** → Offset-based reads

**Potential approach**:
1. Store delayed messages in stream with delivery timestamp in metadata
2. Consumer reads stream, checks timestamps via `mc:get_annotation/2`
3. Delivers messages when timestamp ≤ current time
4. Uses stream offsets for efficient scanning (no full table scan)

### 5. Message Container (mc) Annotations

**Location**: Message container abstraction throughout codebase

**What it provides**:
- `mc:set_annotation/3` - Set internal metadata on messages
- `mc:get_annotation/2` - Retrieve metadata
- `delivery_count` tracking (used in `rabbit_fifo`)
- Annotations are internal, not exposed to AMQP clients

**How it solves DMX limitations**:
- **Better than headers** → Annotations don't modify client-visible message
- **Delivery tracking** → Track redelivery attempts for delayed messages
- **Efficient** → No header manipulation overhead

**Current DMX usage**:
```erlang
swap_delay_header(Delivery) ->
    mc:set_annotation(<<"x-delay">>, -Delay, Delivery)
```

**Could be extended for**:
- `scheduled_delivery_time` - Absolute timestamp
- `delivery_attempts` - Retry counter
- `original_exchange` - For routing after delay

### 6. Dead Letter Exchange (DLX) + TTL Pattern

**Location**: `deps/rabbit/src/rabbit_amqqueue.erl`, queue TTL handling

**What exists**:
- Per-message TTL (`x-message-ttl` queue argument)
- Per-message TTL via `expiration` property
- Dead letter exchanges for expired messages
- Delivery limit tracking in quorum queues

**How it could be combined** (workaround, not ideal):
1. Publish message with TTL = delay time
2. Message sits in intermediate queue
3. When TTL expires → routed to DLX
4. DLX routes to final destination

**Limitations of this approach**:
- Messages consume queue resources while waiting
- Not true scheduling (queue must hold messages)
- Less efficient than timer-based approach
- Still subject to single-node issues if using classic queues

---

## Recommended Architectures for DMX v2

Based on analysis of modern RabbitMQ features, here are three viable approaches:

### Option A: Ra-Based Delay State Machine (Best for Reliability)

**Architecture**:
1. Create Ra state machine (behavior: `ra_machine`) for delayed messages
2. Store messages with delivery timestamps in replicated Ra log
3. Leader manages timer for next delivery (via `tick/2` callback)
4. On leader failover, new leader reconstructs timer from state
5. Shard by time range for scalability

**Implementation outline**:
```erlang
-module(rabbit_delayed_message_fifo).
-behaviour(ra_machine).

-record(state, {
    messages,           %% Map of timestamp -> [messages]
    next_delivery,      %% Next scheduled delivery time
    config              %% Configuration
}).

init(_Config) ->
    #state{messages = #{}, next_delivery = infinity}.

apply(_Meta, {enqueue, Timestamp, Msg}, State) ->
    %% Add message to replicated state
    %% Update next_delivery if this is sooner
    ...

tick(_TimeMs, State) ->
    %% Check for messages ready to deliver
    %% Return effects to route messages
    ...
```

**Sharding strategy**:
- Ra cluster per time bucket (e.g., hourly, daily)
- Route messages to appropriate cluster based on delay
- Each cluster manages its own timer independently

**Pros**:
- Solves all replication issues
- Cluster-wide durability guaranteed
- Proven pattern (quorum queues use this successfully)
- Automatic failover and leader election
- Strong consistency guarantees

**Cons**:
- Complex implementation (Ra state machine learning curve)
- Ra overhead for every delayed message (WAL writes)
- Need to implement sharding logic for scalability
- More moving parts (multiple Ra clusters)

### Option B: Khepri + Distributed Timer (Best for Simplicity)

**Architecture**:
1. Store delayed messages in Khepri tree structure
2. Each node runs a timer process (gen_server)
3. Use distributed coordination (e.g., leader election) to ensure single delivery
4. Leverage Khepri's tree structure for timestamp-based queries

**Tree structure**:
```
/rabbitmq/delayed_messages/
  /<vhost>/
    /<exchange>/
      /<year>/<month>/<day>/<hour>/<minute>/
        /<message_uuid> -> {message, metadata}
```

**Timer coordination**:
- Use Khepri's built-in Ra cluster for leader election
- Only leader node runs active timer
- Followers watch leader, take over on failure
- Query Khepri for next message to deliver

**Pros**:
- Simpler than full Ra state machine
- Leverages existing Khepri infrastructure
- Natural fit for metadata-like delayed messages
- Tree structure enables efficient timestamp queries
- Automatic replication via Khepri

**Cons**:
- Khepri not designed for high-throughput message storage
- Still need distributed timer coordination logic
- Query performance may degrade with many messages
- Not as battle-tested as Ra state machines

### Option C: Stream Queue + Consumer Pattern (Best for High Volume)

**Architecture**:
1. Store delayed messages in replicated stream queue
2. Specialized consumer process reads stream continuously
3. Checks message timestamps via annotations
4. Delivers messages when current_time ≥ scheduled_time
5. Uses stream offsets for efficient scanning

**Message flow**:
```
Publisher → DMX Exchange → Stream Queue (with timestamp annotation)
                              ↓
                         Consumer Process
                         (checks timestamps)
                              ↓
                         Route to destination
```

**Consumer implementation**:
- Maintains offset of last checked message
- Reads ahead in batches
- Sleeps until next message ready
- Multiple consumers possible (with coordination)

**Pros**:
- Handles millions of delayed messages efficiently
- Stream replication built-in (3+ replicas)
- Efficient disk-based storage (not memory-bound)
- Proven scalability (streams designed for high volume)
- Simple consumer logic

**Cons**:
- Consumer must continuously scan for ready messages
- Not true "timer fires" semantics (polling-based)
- Latency: Messages checked periodically, not immediately
- Ordering within same timestamp not guaranteed
- Consumer becomes single point of failure (needs HA)

---

## Key Technical Insights from Code Analysis

### 1. Ra State Machine Pattern (`rabbit_fifo.erl`)

The quorum queue implementation demonstrates the pattern for replicated state:

```erlang
%% Commands are replicated via Ra
apply(Meta, #enqueue{msg = Msg}, State) ->
    %% Add to replicated state
    {State2, ok, Effects}.

%% Periodic callback for housekeeping
tick(TimeMs, State) ->
    %% Could check for expired delays
    {State, Effects}.

%% Query without replication
query_messages_ready(State) ->
    %% Read-only query of current state
    length(State#state.messages).
```

**Key takeaway**: Ra handles replication automatically. State machine just defines operations.

### 2. Delivery Limit Tracking

Quorum queues track redelivery attempts:

```erlang
DeliveryLimit = maps:get(delivery_limit, Conf, undefined),

maybe_set_msg_delivery_count(Msg, Header) ->
    case mc:get_annotation(delivery_count, Msg) of
        undefined -> mc:set_annotation(delivery_count, 1, Msg);
        Count -> mc:set_annotation(delivery_count, Count + 1, Msg)
    end.
```

**Key takeaway**: Could adapt this for delayed message retry logic.

### 3. No Existing Timer Wheel

Search of codebase reveals:
- No hierarchical timer wheel implementation
- Erlang's `erlang:start_timer/3` used throughout
- Max timer: 4294967295 ms (≈49.7 days)

**Key takeaway**: Would need to build timer wheel for better scalability, or use Ra's `tick/2` for periodic checks.

### 4. Khepri Tree Structure

Khepri organizes data hierarchically:

```erlang
rabbit_khepri:put([rabbitmq, delayed_messages, VHost, Exchange, Timestamp],
                  Message).

rabbit_khepri:get_many([rabbitmq, delayed_messages, VHost, Exchange, 
                        {range, MinTimestamp, MaxTimestamp}]).
```

**Key takeaway**: Natural fit for timestamp-based organization and range queries.

### 5. Stream Offset-Based Consumption

Streams use offsets for efficient reading:

```erlang
-record(stream, {
    start_offset = 0 :: non_neg_integer(),
    listening_offset = 0 :: non_neg_integer(),
    last_consumed_offset :: non_neg_integer()
}).
```

**Key takeaway**: Could maintain "last_checked_offset" for delayed message scanning.

---

## Implementation Considerations

### Backward Compatibility

Any new implementation must consider:
1. **Migration path** from existing Mnesia-based storage
2. **Exchange declaration compatibility** (same arguments)
3. **Message format compatibility** (x-delay header)
4. **Metrics compatibility** (messages_delayed counter)

### Performance Requirements

Based on issue #72 and README warnings:
- Current design fails at 100k+ messages
- New design should target 1M+ messages
- Delivery latency: <100ms for messages ready to deliver
- Throughput: 10k+ delayed messages/second

### Failure Scenarios to Handle

1. **Leader failure**: New leader must reconstruct timer state
2. **Network partition**: Must not deliver messages twice
3. **Clock skew**: Use monotonic time or consensus on "current time"
4. **Node restart**: Delayed messages must survive
5. **Cluster membership change**: Rebalance delayed messages

### Testing Strategy

1. **Functional tests**: Message delivery order, timing accuracy
2. **Failure tests**: Node crashes, network partitions, leader changes
3. **Performance tests**: 1M+ messages, high throughput
4. **Upgrade tests**: Migration from v1 to v2
5. **Chaos tests**: Random failures during operation

---

## Critical Limitation: Ra Cannot Delete Individual Log Entries

### The Fundamental Problem with Ra for Delayed Messages

**Discovery Date**: 2025-12-23

Ra is a **write-ahead log (WAL)** system where:
1. Entries are written sequentially with monotonically increasing indexes
2. Log compaction only removes entries **before the release cursor** (smallest needed index)
3. **Individual entries cannot be deleted from the middle of the log**

### Why This Breaks Delayed Messages

**The core issue**: Delayed messages are delivered **out of order** relative to their Ra log index.

**Example scenario**:
```
Time 14:10 - Publish three messages:
  - Message A: delay 1 hour  → deliver at 15:10 (Ra index: 100)
  - Message B: delay 10 min  → deliver at 14:20 (Ra index: 101)
  - Message C: delay 2 hours → deliver at 16:10 (Ra index: 102)

Time 14:20 - Message B delivered (index 101)
  ❌ Cannot compact log - Message A (index 100) still pending

Time 15:10 - Message A delivered (index 100)
  ❌ Cannot compact log - Message C (index 102) still pending

Time 16:10 - Message C delivered (index 102)
  ✅ NOW can compact log (release cursor → 103)
```

### The Consequences

- **Log grows indefinitely** until ALL messages with earlier indexes are delivered
- **Disk space cannot be reclaimed** even after 99% of messages delivered
- **Worst case**: One message with 7-day delay blocks compaction for 7 days
- **Snapshots don't help**: Must retain all undelivered messages regardless of snapshot

### Why Quorum Queues Don't Have This Problem

Quorum queues deliver in **FIFO order** (same as Ra log index order):
- Ra log index = enqueue order = delivery order
- Once consumed, index can be released immediately
- `smallest_raft_index()` naturally advances as messages are consumed

**For delayed messages**: `smallest_raft_index()` would be stuck at the earliest undelivered message, regardless of how many later messages have been delivered.

### Code Evidence

From `rabbit_fifo.erl`:
```erlang
smallest_raft_index(#?STATE{messages = Messages,
                            ra_indexes = Indexes,
                            dlx = DlxState}) ->
    SmallestMsgsRaIdx = rabbit_fifo_q:get_lowest_index(Messages),
    SmallestRaIdx = rabbit_fifo_index:smallest(Indexes),
    min(SmallestMsgsRaIdx, SmallestRaIdx).

release_cursor(LastSmallest, Smallest) ->
    [{release_cursor, Smallest - 1}].  %% Can only release UP TO smallest
```

The release cursor can only advance to `Smallest - 1`, meaning everything before the oldest undelivered message. For delayed messages delivered out of order, this would rarely advance.

---

## Conclusion: Ra-Based Approach Not Viable

The current DMX plugin's limitations stem from its **single-node, single-timer architecture**. While modern RabbitMQ provides several paths forward:

1. ~~**Ra state machine**~~ - **NOT VIABLE**: Cannot delete delivered messages out of order
2. **Khepri storage** - Simpler, good for moderate volumes, can delete individual entries
3. **Stream queues** - Best for high volume, but polling-based (not true scheduling)

**Critical insight**: Any solution must support **random-access deletion** of individual messages, which Ra's append-only log cannot provide.

---

## AWS-Enhanced Architecture for Amazon MQ

### Context: Amazon MQ Environment

Amazon MQ is AWS's managed RabbitMQ service with access to:
- **DynamoDB**: Fully managed NoSQL database with single-digit millisecond latency
- **S3**: Object storage for large payloads
- **SQS**: Managed message queuing with delay queues (up to 15 minutes)
- **EventBridge Scheduler**: Managed scheduling service (up to 1 year delays)
- **Lambda**: Serverless compute for message delivery
- **EBS**: Block storage with snapshots and replication
- **CloudWatch**: Monitoring and metrics

### AWS-Native Solutions

#### Option 1: DynamoDB + EventBridge Scheduler (RECOMMENDED)

**Architecture**:
```
Publisher → DMX Exchange → DynamoDB (message storage)
                         → EventBridge Scheduler (timer)
                         → Lambda (delivery trigger)
                         → RabbitMQ (route to destination)
```

**How it works**:
1. **Message published** with x-delay header
2. **Store in DynamoDB**:
   - Partition key: `exchange_name#delivery_timestamp` (for range queries)
   - Sort key: `message_id`
   - Attributes: message payload, routing key, headers, metadata
   - TTL: delivery_timestamp + grace_period (auto-cleanup)
3. **Create EventBridge Schedule**:
   - One-time schedule at delivery timestamp
   - Target: Lambda function with message_id
   - Flexible time window (up to 1 year delay)
4. **Lambda triggered** at scheduled time:
   - Fetch message from DynamoDB
   - Publish to RabbitMQ via AMQP
   - Delete from DynamoDB
   - Handle retries if RabbitMQ unavailable

**Advantages**:
- ✅ **Solves all DMX limitations**:
  - Multi-region replication (DynamoDB global tables)
  - No single-node failure (fully managed)
  - Scales to millions of messages (DynamoDB + EventBridge)
  - Individual message deletion (DynamoDB delete)
  - No log compaction issues
- ✅ **True scheduling**: EventBridge handles timer management
- ✅ **Cost-effective**: Pay per request, no idle resources
- ✅ **Observability**: CloudWatch metrics and logs built-in
- ✅ **Durability**: DynamoDB 99.999999999% durability
- ✅ **Long delays**: Up to 1 year (EventBridge limit)

**Disadvantages**:
- ❌ **External dependency**: Requires AWS services
- ❌ **Network latency**: Lambda → RabbitMQ call adds ~10-50ms
- ❌ **Eventual consistency**: DynamoDB replication lag (typically <1s)
- ❌ **Cost**: DynamoDB + EventBridge + Lambda charges
- ❌ **Complexity**: More moving parts to monitor

**DynamoDB Schema**:
```
Table: delayed_messages
  Partition Key: delivery_bucket (STRING)  # "exchange_name#YYYY-MM-DD-HH"
  Sort Key: delivery_timestamp#message_id (STRING)  # "1703347200000#uuid"
  
  Attributes:
    - message_payload (BINARY) - Compressed message
    - routing_key (STRING)
    - headers (MAP)
    - exchange_name (STRING)
    - created_at (NUMBER)
    - delivery_timestamp (NUMBER)
    - ttl (NUMBER) - DynamoDB TTL for auto-cleanup
    
  GSI: message_id_index
    - Partition Key: message_id
    - For cancellation/updates
    
  TTL: ttl attribute (auto-delete after delivery + grace period)
```

**EventBridge Scheduler**:
```json
{
  "Name": "delayed-msg-{message_id}",
  "ScheduleExpression": "at(2025-12-23T15:30:00)",
  "Target": {
    "Arn": "arn:aws:lambda:region:account:function:dmx-deliver",
    "Input": "{\"message_id\": \"uuid\", \"delivery_bucket\": \"exchange#2025-12-23-15\"}"
  },
  "FlexibleTimeWindow": {
    "Mode": "OFF"  // Precise delivery
  }
}
```

**Lambda Handler** (pseudo-code):
```python
def handler(event):
    message_id = event['message_id']
    delivery_bucket = event['delivery_bucket']
    
    # Fetch from DynamoDB
    msg = dynamodb.get_item(
        Key={'delivery_bucket': delivery_bucket, 
             'delivery_timestamp#message_id': f"{timestamp}#{message_id}"}
    )
    
    # Publish to RabbitMQ
    try:
        rabbitmq.publish(
            exchange=msg['exchange_name'],
            routing_key=msg['routing_key'],
            body=msg['message_payload'],
            headers=msg['headers']
        )
        # Delete from DynamoDB
        dynamodb.delete_item(Key=...)
    except Exception as e:
        # Retry logic or DLQ
        raise
```

---

#### Option 2: DynamoDB + SQS Delay Queues (For Short Delays)

**Architecture**:
```
Publisher → DMX Exchange → DynamoDB (if delay > 15 min)
                         → SQS Delay Queue (if delay ≤ 15 min)
                         → Lambda (poll SQS)
                         → RabbitMQ
```

**How it works**:
1. **If delay ≤ 15 minutes**: Send to SQS with DelaySeconds
2. **If delay > 15 minutes**: Store in DynamoDB + EventBridge (Option 1)
3. **Lambda polls SQS** continuously
4. **Delivers to RabbitMQ** when message becomes visible

**Advantages**:
- ✅ **No EventBridge cost** for short delays (most common case)
- ✅ **Simpler** for delays under 15 minutes
- ✅ **Built-in retry**: SQS visibility timeout
- ✅ **FIFO option**: SQS FIFO queues for ordering

**Disadvantages**:
- ❌ **15-minute limit**: Need hybrid approach
- ❌ **Polling overhead**: Lambda continuously polling
- ❌ **Less precise**: SQS delay has ~second precision

---

#### Option 3: S3 + DynamoDB + Lambda (For Large Messages)

**Architecture**:
```
Publisher → DMX Exchange → S3 (message payload if > 256KB)
                         → DynamoDB (metadata + S3 key)
                         → EventBridge Scheduler
                         → Lambda (fetch from S3, deliver)
```

**When to use**:
- Messages > 256KB (DynamoDB item size limit)
- Need to store millions of large delayed messages
- Cost optimization (S3 cheaper than DynamoDB for large data)

**Advantages**:
- ✅ **Unlimited message size**: S3 supports up to 5TB objects
- ✅ **Cost-effective**: S3 storage much cheaper than DynamoDB
- ✅ **Lifecycle policies**: Auto-delete old messages

**Disadvantages**:
- ❌ **Higher latency**: S3 GET adds ~50-100ms
- ❌ **More complexity**: Two storage systems

---

#### Option 4: Hybrid - Khepri + DynamoDB (Best of Both Worlds)

**Architecture**:
```
Publisher → DMX Exchange → Khepri (metadata, bindings)
                         → DynamoDB (message storage)
                         → EventBridge Scheduler
                         → Lambda → RabbitMQ
```

**How it works**:
1. **Khepri stores**: Exchange configuration, bindings, routing rules
2. **DynamoDB stores**: Actual delayed messages
3. **EventBridge**: Scheduling
4. **Lambda**: Delivery logic using Khepri metadata

**Advantages**:
- ✅ **Leverages RabbitMQ infrastructure**: Khepri for metadata
- ✅ **Scales message storage**: DynamoDB for messages
- ✅ **Consistent with RabbitMQ**: Uses native metadata store
- ✅ **No Ra log compaction issue**: Messages in DynamoDB

**Disadvantages**:
- ❌ **Complexity**: Three storage systems
- ❌ **Consistency challenges**: Khepri vs DynamoDB sync

---

### Implementation Considerations for AWS

#### 1. Message Payload Handling

**Strategy**: Tiered storage based on size
```
< 4KB:    Store inline in DynamoDB
4KB-256KB: Compress and store in DynamoDB
> 256KB:   Store in S3, reference in DynamoDB
```

**Compression**: Use LZ4 or Snappy for fast compression/decompression

#### 2. Delivery Guarantees

**At-least-once delivery**:
- Lambda retries on failure (built-in)
- DynamoDB conditional delete (only if not already deleted)
- Idempotency token in message headers

**Exactly-once** (if needed):
- Use DynamoDB transactions
- Track delivery state in separate table
- Deduplication window

#### 3. Cancellation Support

**Feature**: Allow canceling scheduled messages

**Implementation**:
```
1. Client calls cancel API with message_id
2. Delete from DynamoDB (GSI lookup by message_id)
3. Delete EventBridge schedule
4. Return success/not_found
```

**DynamoDB GSI required**:
```
GSI: message_id_index
  Partition Key: message_id
  Projection: ALL
```

#### 4. Monitoring & Observability

**CloudWatch Metrics**:
- `DelayedMessagesScheduled` - Counter
- `DelayedMessagesDelivered` - Counter
- `DelayedMessagesFailed` - Counter
- `DeliveryLatency` - Histogram (scheduled vs actual)
- `DynamoDBReadLatency` - Histogram
- `RabbitMQPublishLatency` - Histogram

**CloudWatch Logs**:
- Lambda execution logs
- Failed deliveries with retry count
- Message lifecycle events

**X-Ray Tracing**:
- End-to-end trace: Publish → DynamoDB → EventBridge → Lambda → RabbitMQ

#### 5. Cost Optimization

**DynamoDB**:
- Use on-demand pricing for variable load
- Enable auto-scaling for provisioned capacity
- Use TTL for automatic cleanup (no cost)
- Compress large messages

**EventBridge Scheduler**:
- $1.00 per million schedules
- Free tier: 14 million schedules/month
- Batch schedule creation when possible

**Lambda**:
- Use ARM64 (Graviton2) for 20% cost savings
- Optimize memory allocation (128MB sufficient)
- Use reserved concurrency to control costs

**S3** (if used):
- Use S3 Intelligent-Tiering
- Lifecycle policy: Delete after delivery + 7 days
- Use S3 Select for partial object reads

**Estimated costs** (1M messages/day, avg 1-hour delay):
- DynamoDB: ~$25/month (on-demand, 1KB avg)
- EventBridge: ~$1/month (1M schedules)
- Lambda: ~$5/month (1M invocations, 128MB, 100ms avg)
- **Total: ~$31/month** for 1M delayed messages/day

Compare to:
- EC2 instance for RabbitMQ node: ~$50-100/month
- EBS storage: ~$10-20/month
- **Much more cost-effective at scale**

#### 6. High Availability

**Multi-AZ**:
- DynamoDB: Automatic multi-AZ replication
- Lambda: Runs in multiple AZs automatically
- EventBridge: Regional service, multi-AZ

**Multi-Region** (if needed):
- DynamoDB Global Tables
- EventBridge Scheduler in each region
- Lambda in each region
- Route53 health checks for failover

**Disaster Recovery**:
- DynamoDB point-in-time recovery (PITR)
- S3 versioning and cross-region replication
- EventBridge schedules backed up to S3

#### 7. Security

**Encryption**:
- DynamoDB: Encryption at rest (KMS)
- S3: Server-side encryption (SSE-KMS)
- Lambda: Environment variables encrypted
- RabbitMQ: TLS for AMQP connections

**IAM Roles**:
```
Lambda Execution Role:
  - dynamodb:GetItem, DeleteItem
  - s3:GetObject (if using S3)
  - logs:CreateLogGroup, PutLogEvents
  - xray:PutTraceSegments
  
EventBridge Role:
  - lambda:InvokeFunction
  
RabbitMQ Plugin Role:
  - dynamodb:PutItem
  - scheduler:CreateSchedule
```

**VPC**:
- Lambda in same VPC as RabbitMQ
- VPC endpoints for DynamoDB, S3 (no internet)
- Security groups restrict access

#### 8. Migration Path

**Phase 1**: Dual-write
- Write to both Mnesia and DynamoDB
- Deliver from Mnesia (existing logic)
- Validate DynamoDB writes

**Phase 2**: Dual-read
- Write to DynamoDB only
- Read from DynamoDB, fallback to Mnesia
- Monitor for discrepancies

**Phase 3**: DynamoDB-only
- Remove Mnesia code
- Clean up old Mnesia tables
- Full AWS-native implementation

---

### Recommended Architecture for Amazon MQ

**Primary Recommendation**: **Option 1 - DynamoDB + EventBridge Scheduler**

**Rationale**:
1. ✅ **Solves ALL current limitations**:
   - No single-node storage (DynamoDB multi-AZ)
   - No log compaction issues (individual deletes)
   - Scales to millions of messages
   - True scheduling (not polling)
   - Long delays supported (up to 1 year)

2. ✅ **AWS-native**: Fully managed, no infrastructure to maintain

3. ✅ **Cost-effective**: Pay-per-use, no idle costs

4. ✅ **Observable**: CloudWatch integration out-of-box

5. ✅ **Reliable**: 99.99% SLA for all components

**When to use alternatives**:
- **Option 2** (SQS): If 90%+ messages have delay < 15 minutes
- **Option 3** (S3): If messages regularly > 256KB
- **Option 4** (Khepri): If tight integration with RabbitMQ metadata required

---

### Performance Comparison

| Metric | Current DMX | Ra-Based | DynamoDB + EventBridge |
|--------|-------------|----------|------------------------|
| Max messages | ~100K | ~1M* | Unlimited |
| Replication | None | 3-5 nodes | Multi-AZ automatic |
| Delivery precision | ±10ms | ±10ms | ±1s |
| Max delay | 49 days | 49 days | 1 year |
| Storage cost | EBS | EBS | $0.25/GB-month |
| Ops overhead | High | Medium | Low (managed) |
| Failure recovery | Manual | Automatic | Automatic |
| Scalability | Poor | Good* | Excellent |

\* Ra-based limited by log compaction issue

---

### Conclusion: AWS Changes Everything

With access to AWS services, the **DynamoDB + EventBridge Scheduler** approach is clearly superior:

1. **Eliminates all DMX limitations** without Ra's log compaction issue
2. **Fully managed** - no infrastructure to maintain
3. **Scales effortlessly** to millions of messages
4. **Cost-effective** at scale
5. **Highly available** by default (multi-AZ)
6. **Observable** with CloudWatch integration

The key insight: **Don't try to solve distributed storage and scheduling within RabbitMQ when AWS provides purpose-built services for these problems.**

Implementation complexity shifts from:
- ❌ Building distributed consensus (Ra/Raft)
- ❌ Managing log compaction
- ❌ Implementing timer wheels
- ❌ Handling replication

To:
- ✅ AWS SDK integration
- ✅ Lambda function development
- ✅ Monitoring and alerting
- ✅ Cost optimization

This is a **much more maintainable and scalable solution** for Amazon MQ.
