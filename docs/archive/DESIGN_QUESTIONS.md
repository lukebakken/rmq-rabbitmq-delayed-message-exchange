# Khepri + DynamoDB Implementation - Design Questions

**Date**: 2025-12-23  
**Goal**: Redesign rabbitmq-delayed-message-exchange plugin using Khepri for metadata and DynamoDB for message storage

---

## Architecture & Design Questions

### 1. Khepri vs DynamoDB Responsibility Split
**Q**: What exactly should live in Khepri vs DynamoDB?
- **Assumption**: Khepri stores exchange metadata (bindings, configuration, routing rules), DynamoDB stores actual delayed message payloads?
- Should Khepri also track "scheduled message IDs" for consistency checks?
- Should we use Khepri as a distributed lock/coordination mechanism for anything?

**Answer**: 
✅ **DECIDED**: Khepri stores ALL message metadata needed to correctly store/retrieve message payloads from backend storage (DynamoDB). This includes:
- Message routing information (exchange, routing key, headers)
- Delivery timestamp
- Message ID/reference
- Any metadata needed for delivery

DynamoDB stores ONLY the message payload (body).

**Rationale**: Clean separation - Khepri handles distributed coordination and metadata, DynamoDB is pure payload storage.


---

### 2. EventBridge Scheduler Integration
**Q**: Are we using EventBridge Scheduler, or building our own timer mechanism?
- If EventBridge: How do we handle the RabbitMQ → AWS SDK calls? New Erlang dependency?
- If custom timers: Do we run one timer process per node, or coordinate via Khepri?
- What's the acceptable delivery precision? (EventBridge is ±1s, custom could be ±10ms)

**Answer**:
✅ **DECIDED**: Use existing Erlang timer mechanism from current DMX plugin (no EventBridge).
- Reuse `erlang:start_timer/3` approach
- Single timer for next message to deliver (same as current implementation)
- Precision: ±10ms (same as current)

**Rationale**: Simpler for POC, proven mechanism, no additional AWS service dependencies.


---

### 3. Message Delivery Path
**Q**: When a message's delay expires, how does it get back into RabbitMQ?
- **Option A**: Lambda calls RabbitMQ AMQP (external)
- **Option B**: RabbitMQ polls DynamoDB (internal process)
- **Option C**: EventBridge → SQS → RabbitMQ consumer
- Which approach do you prefer and why?

**Answer**:
✅ **DECIDED**: **Option B** - RabbitMQ internal process handles delivery.
- When Erlang timer expires, RabbitMQ fetches payload from DynamoDB
- Enqueues message to correct queue(s) via internal routing
- No external components (Lambda, SQS, EventBridge)

**Rationale**: Simplest architecture, all logic stays within RabbitMQ, no external triggers needed.


---

### 4. Backward Compatibility
**Q**: Do we need to support migration from existing Mnesia-based DMX?
- Should we support dual-write during transition?
- Can we break compatibility, or must existing delayed messages survive upgrade?
- What about the `messages_delayed` metric - must it work across both storage backends?

**Answer**:
✅ **DECIDED**: **NO backward compatibility required**.
- Clean break from Mnesia-based implementation
- No migration support needed
- No dual-write period
- Existing delayed messages will be lost on upgrade (acceptable for POC)

**Rationale**: POC focus, simplifies implementation significantly.


---

## Technical Implementation Questions

### 5. DynamoDB Schema Design
**Q**: What's your preferred partition key strategy?
- **Option A**: `exchange_name#timestamp_bucket` (recommended for even distribution)
- **Option B**: `exchange_name` only (simpler, but hot partitions)
- **Option C**: `vhost#exchange_name#timestamp_bucket` (multi-tenancy)
- How granular should timestamp buckets be? (hourly, 15-min, 5-min?)

**Answer**:
✅ **DECIDED**: **Option C** - `broker_id#vhost#exchange_name#timestamp_bucket`
- Partition key: `broker_id#vhost#exchange_name#timestamp_bucket`
- Sort key: `delivery_timestamp#message_id`
- Timestamp bucket granularity: **15 minutes**

**Schema**:
```
Table: delayed_messages
  Partition Key: partition_key (STRING)
    Format: "{broker_id}#{vhost}#{exchange}#{YYYY-MM-DD-HH-MM}"
    Example: "broker-123#/#my-exchange#2025-12-23-15-00"
  
  Sort Key: sort_key (STRING)
    Format: "{delivery_timestamp_ms}#{message_id}"
    Example: "1703347200000#550e8400-e29b-41d4-a716-446655440000"
  
  Attributes:
    - message_payload (BINARY) - The message body
    - created_at (NUMBER) - When message was published
```

**Rationale**: 
- Broker ID isolation for multi-broker deployments
- 15-minute buckets balance partition distribution vs query complexity
- Sort key enables efficient range queries within bucket

**Note**: Current DMX doesn't use timestamp bucketing - it uses a single ordered_set Mnesia table with timestamp as part of the key. We're improving on this.


---

### 6. DynamoDB Consistency Model
**Q**: Are we using DynamoDB's eventual consistency or strong consistency?
- For writes: Do we need to confirm write before returning to publisher?
- For reads: Can we tolerate stale reads when checking for ready messages?
- Should we use DynamoDB transactions for any operations?

**Answer**:
✅ **DECIDED**: 
- **Writes**: Confirm writes before returning to publisher (wait for DynamoDB PutItem response)
- **Reads**: Eventual consistency acceptable (stale reads OK for checking ready messages)
- **Transactions**: Not needed for POC

**Rationale**: 
- Write confirmation ensures message is persisted before ack to publisher
- Eventual consistency on reads is fine - worst case is slight delivery delay
- No transactions needed for single-item operations


---

### 7. AWS SDK Integration
**Q**: How do we integrate AWS SDK into Erlang/RabbitMQ?
- Use existing `rabbitmq_aws` dependency? (exists in deps/)
- Build our own AWS SDK wrapper?
- What about credentials management? (IAM roles, assume role, etc.)

**Answer**:
✅ **DECIDED**: Use **aws-erlang** library from `/home/lrbakken/development/aws-beam/aws-erlang`
- Add to `deps/rabbitmq_delayed_message_exchange/Makefile` as dependency
- Credentials: IAM instance role (EC2 instance profile)
- No custom wrapper needed initially

**Rationale**: Existing, maintained library. IAM roles are standard for EC2-based services.


---

### 8. Khepri Data Model
**Q**: What's the Khepri tree structure for DMX metadata?
- **Suggested**: `/rabbitmq/delayed_messages/<vhost>/<exchange>/config`
- Should we store anything about active scheduled messages in Khepri?
- How do we handle Khepri → DynamoDB consistency? (e.g., exchange deleted but messages still in DynamoDB)

**Answer**:
✅ **DECIDED**: Khepri tree structure (to be refined during implementation):
```
/rabbitmq/delayed_messages/
  /<vhost>/
    /<exchange>/
      /<delivery_timestamp_bucket>/
        /<message_id> -> {
          delivery_timestamp,
          routing_key,
          headers,
          exchange_name,
          vhost,
          message_id,
          dynamodb_key  % For payload retrieval
        }
```

**Open Questions** (to address later):
- Should we store active scheduled messages in Khepri? (Leaning YES for timer management)
- Khepri → DynamoDB consistency handling (NOTE: Deferred - needs design)

**Rationale**: Hierarchical structure enables efficient queries by timestamp bucket. Storing message metadata in Khepri allows timer reconstruction after failover.


---

## Operational Questions

### 9. Failure Scenarios
**Q**: How do we handle these failure modes?
- **DynamoDB unavailable**: Queue messages locally? Return error to publisher? Use circuit breaker?
- **EventBridge unavailable**: Fall back to local timers? Fail fast?
- **Partial failure**: Message in DynamoDB but EventBridge schedule failed - how to detect and recover?
- **Clock skew**: Between RabbitMQ nodes and AWS - how to handle?

**Answer**:
✅ **DECIDED**: For POC, assume everything succeeds.
- **Action**: Add TODO comments and logging for failure cases
- **Action**: Document failure scenarios in code comments
- **Action**: Return errors to publisher if DynamoDB write fails (NACK message)

**Deferred**: Proper error handling, circuit breakers, retry logic, recovery mechanisms.

**Note**: This is acceptable for POC but must be addressed before production.


---

### 10. Message Cancellation
**Q**: Do we need to support canceling scheduled messages?
- If yes: How does client identify the message? (message-id header? custom API?)
- Do we need a DynamoDB GSI for message_id lookups?
- What about updating delay time for already-scheduled messages?

**Answer**:
✅ **DECIDED**: **NOT SUPPORTED** in POC.
- No message cancellation
- No delay time updates
- No GSI needed for message_id lookups

**Rationale**: Simplifies POC. Can be added later if needed.


---

### 11. Large Messages
**Q**: What's the strategy for messages > 256KB (DynamoDB item limit)?
- Store in S3 with reference in DynamoDB?
- Reject at publish time?
- Compress and hope it fits?
- What's the expected message size distribution in Amazon MQ?

**Answer**:
✅ **DECIDED**: **Reject large messages** at publish time.
- Check message size before storing
- If > 256KB: NACK message with error
- Log warning about size limit

**Rationale**: Simplest for POC. S3 integration can be added later if needed.

**Implementation**: Add size check in exchange routing logic before Khepri/DynamoDB write.


---

### 12. Multi-Region
**Q**: Is this single-region or multi-region?
- If multi-region: DynamoDB Global Tables?
- How do we prevent duplicate delivery across regions?
- Should EventBridge schedules be regional or global?

**Answer**:
✅ **DECIDED**: **Single region** for POC.
- DynamoDB table in same region as RabbitMQ cluster
- Broker ID in partition key provides isolation for multi-broker scenarios
- No cross-region replication

**Rationale**: Simplifies POC. Multi-region can be added later with Global Tables.


---

## Performance & Scale Questions

### 13. Expected Load
**Q**: What are the target performance metrics?
- Messages per second (publish rate)?
- Concurrent delayed messages (total in system)?
- Typical delay distribution? (90% < 1 hour? 99% < 1 day?)
- This affects DynamoDB capacity planning and partition key design

**Answer**:
✅ **DECIDED**: **Not a concern for POC**.
- No specific performance targets
- Will test with realistic but modest load
- DynamoDB on-demand capacity mode (auto-scaling)

**Rationale**: POC focuses on correctness, not performance. Can optimize later.


---

### 14. Delivery Latency
**Q**: What's acceptable delivery latency?
- ±1 second? ±10 seconds? ±1 minute?
- This determines if we need EventBridge or can use polling
- Should we optimize for precision or throughput?

**Answer**:
✅ **DECIDED**: **Not a concern for POC**.
- Using Erlang timers (±10ms precision, same as current DMX)
- No specific latency requirements
- Focus on correctness over precision

**Rationale**: POC validation, not performance optimization.


---

### 15. Cost Constraints
**Q**: Are there cost targets or limits?
- DynamoDB on-demand vs provisioned capacity?
- Should we optimize for cost or performance?
- Is S3 storage acceptable for cost savings on large messages?

**Answer**:
✅ **DECIDED**: **No cost constraints for POC**.
- DynamoDB on-demand capacity mode (simplest, auto-scaling)
- No cost optimization needed
- No S3 integration (rejecting large messages instead)

**Rationale**: POC budget is not a concern. Optimize later if needed.


---

## Testing & Validation Questions

### 16. Testing Strategy
**Q**: What's the testing approach?
- Can we use LocalStack for DynamoDB in tests?
- Or do we need real AWS resources for integration tests?
- How do we test EventBridge scheduling without waiting for real delays?
- Should we build a "fast-forward time" test mode?

**Answer**:
✅ **DECIDED**: Testing in **real AWS environment** when ready.
- No LocalStack for POC
- Real DynamoDB table in test AWS account
- Real EC2 instances for 3-node cluster
- Manual testing initially, automated tests later

**Rationale**: POC focuses on real-world validation. LocalStack can be added later for CI/CD.


---

### 17. Observability
**Q**: What metrics and logging do we need?
- CloudWatch integration required?
- Should we emit RabbitMQ metrics (for existing dashboards)?
- What about distributed tracing (X-Ray)?
- Error tracking and alerting requirements?

**Answer**:
✅ **DECIDED**: **RabbitMQ logs only** for POC.
- Use existing RabbitMQ logging (`logger` module)
- Log key events: message stored, timer set, message delivered, errors
- No CloudWatch integration
- No X-Ray tracing
- No custom metrics (beyond existing RabbitMQ metrics)

**Rationale**: Simplifies POC. Observability can be enhanced later.


---

## Development Process Questions

### 18. Development Approach
**Q**: What's the implementation order?
- Start with DynamoDB integration (storage layer)?
- Start with Khepri integration (metadata layer)?
- Build end-to-end skeleton first, then optimize?
- Incremental feature rollout or big-bang?

**Answer**:
✅ **DECIDED**: **Two-phase approach**:

**Phase 1**: Migrate existing DMX to use Khepri (instead of Mnesia)
- Replace Mnesia tables with Khepri storage
- Keep message payloads on-disk (local storage)
- Test on 3-node cluster (all on same host)
- Validate Khepri replication and failover

**Phase 2**: Add DynamoDB backend
- Abstract storage layer (behavior for backends)
- Implement DynamoDB backend
- Test on 3-node cluster in EC2
- Validate DynamoDB integration

**Rationale**: 
- Phase 1 validates Khepri integration in isolation
- Phase 2 adds DynamoDB without Khepri complexity
- Incremental approach reduces risk


---

### 19. Code Organization
**Q**: How should we structure the code?
- New modules: `rabbit_delayed_message_dynamodb.erl`, `rabbit_delayed_message_khepri.erl`?
- Keep existing modules and add backends?
- Should we abstract storage layer (behavior) for future backends?

**Answer**:
✅ **DECIDED**: Maintain existing modules with abstracted storage layer.

**Module structure**:
```
rabbit_delayed_message.erl              - Main gen_server (timer management)
rabbit_exchange_type_delayed_message.erl - Exchange type implementation
rabbit_delayed_message_utils.erl        - Utility functions (header handling)

NEW:
rabbit_delayed_message_storage.erl      - Storage behavior definition
rabbit_delayed_message_storage_disk.erl - Disk backend (Phase 1)
rabbit_delayed_message_storage_ddb.erl  - DynamoDB backend (Phase 2)
rabbit_delayed_message_khepri.erl       - Khepri metadata operations
```

**Rationale**: 
- Clean separation of concerns
- Easy to swap backends
- Reuse existing exchange type and utils code
- Storage behavior allows future backends (S3, etc.)


---

### 20. Existing Code Reuse
**Q**: What can we reuse from current DMX?
- `rabbit_exchange_type_delayed_message.erl` - routing logic?
- `rabbit_delayed_message_utils.erl` - header manipulation?
- Test suite structure?
- Or clean slate rewrite?

**Answer**:
✅ **DECIDED**: **Reuse as much as feasible**:

**Reuse directly**:
- `rabbit_exchange_type_delayed_message.erl` - Exchange type proxy logic (minimal changes)
- `rabbit_delayed_message_utils.erl` - Header manipulation (get_delay, swap_delay_header)
- `rabbit_delayed_message_sup.erl` - Supervisor structure
- `rabbit_delayed_message_app.erl` - Application callback

**Modify significantly**:
- `rabbit_delayed_message.erl` - Replace Mnesia with Khepri + storage backend

**New modules**:
- Storage abstraction and backends
- Khepri operations

**Rationale**: 
- Exchange type logic is solid, no need to change
- Utils are pure functions, work with any backend
- Main gen_server needs rewrite for new storage model


---

## Critical Path Questions

### 21. MVP Definition
**Q**: What's the minimum viable product?
- Just basic delay + delivery?
- Must include cancellation?
- Must include metrics?
- Must support migration from old DMX?

**Answer**:
✅ **DECIDED**: MVP = **3-node cluster in EC2 with Khepri + DynamoDB**

**Must have**:
- ✅ Basic delay + delivery functionality
- ✅ Khepri for metadata storage (replicated across 3 nodes)
- ✅ DynamoDB for message payload storage
- ✅ Erlang timer mechanism (existing)
- ✅ Works on 3-node RabbitMQ cluster in EC2

**Explicitly NOT included**:
- ❌ Message cancellation
- ❌ Delay updates
- ❌ Custom metrics (beyond existing RabbitMQ metrics)
- ❌ Backward compatibility / migration
- ❌ CloudWatch integration
- ❌ S3 for large messages
- ❌ Multi-region support

**Success criteria**: Publish message with x-delay header → stored in Khepri + DynamoDB → delivered after delay expires → routed to correct queue(s).


---

### 22. Timeline & Priorities
**Q**: What's the timeline and what are the must-haves vs nice-to-haves?
- This helps prioritize which questions are most critical
- Are there any Amazon MQ release deadlines?

**Answer**:
✅ **DECIDED**: **No timeline pressure** - POC development.
- Take time to do it right
- Focus on correctness over speed
- Iterate and refine as needed
- No release deadlines

**Rationale**: POC is for learning and validation, not production release.


---

## Top 5 Most Critical Questions (Priority Order)

These answers will fundamentally shape the architecture:

1. **Message delivery path** (Q3): Lambda callback or RabbitMQ polling DynamoDB?
2. **EventBridge vs custom timers** (Q2): Which scheduling mechanism?
3. **DynamoDB partition key strategy** (Q5): How to avoid hot partitions?
4. **Backward compatibility** (Q4): Must support migration from Mnesia-based DMX?
5. **Expected load** (Q13): Messages/sec and typical delay distribution?

---

## Decision Log

As decisions are made, record them here with rationale:

### Decision 1: Two-Phase Implementation Approach
- **Date**: 2025-12-23
- **Decision**: Phase 1 = Khepri migration (disk storage), Phase 2 = DynamoDB backend
- **Rationale**: Isolates Khepri complexity from DynamoDB complexity, reduces risk, enables incremental validation
- **Alternatives Considered**: Big-bang implementation (rejected - too risky)
- **Impact**: Longer timeline but lower risk, easier debugging

### Decision 2: Storage Layer Abstraction
- **Date**: 2025-12-23
- **Decision**: Create `rabbit_delayed_message_storage` behavior with disk and DynamoDB backends
- **Rationale**: Clean separation, easy to swap backends, future-proof for S3 or other storage
- **Alternatives Considered**: Hardcode DynamoDB (rejected - not flexible)
- **Impact**: Slightly more code but much better architecture

### Decision 3: Khepri Stores Metadata, DynamoDB Stores Payloads
- **Date**: 2025-12-23
- **Decision**: Khepri = all message metadata (routing, timestamps, IDs), DynamoDB = message body only
- **Rationale**: Leverages Khepri's distributed coordination, DynamoDB's scalable storage
- **Alternatives Considered**: Everything in DynamoDB (rejected - loses Khepri benefits)
- **Impact**: Clean separation of concerns, optimal use of each system

### Decision 4: 15-Minute Timestamp Buckets
- **Date**: 2025-12-23
- **Decision**: DynamoDB partition key includes 15-minute timestamp bucket
- **Rationale**: Balances partition distribution vs query complexity
- **Alternatives Considered**: Hourly (too coarse), 5-minute (too fine)
- **Impact**: Good partition distribution for typical delay patterns

### Decision 5: Reject Messages > 256KB
- **Date**: 2025-12-23
- **Decision**: NACK messages larger than DynamoDB item limit
- **Rationale**: Simplest for POC, avoids S3 integration complexity
- **Alternatives Considered**: S3 storage (deferred to later)
- **Impact**: Size limitation but simpler implementation

### Decision 6: Broker ID from Cluster Name
- **Date**: 2025-12-23
- **Decision**: Use RabbitMQ cluster ID (via `rabbit:cluster_name/0`) as broker_id
- **Rationale**: Built-in, unique per cluster, no configuration needed
- **Alternatives Considered**: Config parameter, node name (rejected - not cluster-wide)
- **Impact**: Automatic, no manual configuration required

### Decision 7: Khepri Tree Structure
- **Date**: 2025-12-23
- **Decision**: `/delayed_messages/<vhost>/<exchange>/<timestamp_bucket>/<message_id>`
- **Rationale**: Hierarchical, enables efficient queries by bucket, clear organization
- **Alternatives Considered**: Flat structure (rejected - harder to query)
- **Impact**: Clean tree structure, efficient range queries

### Decision 8: Phase 1 Disk Storage - One File Per Message
- **Date**: 2025-12-23
- **Decision**: Store each message payload in separate file on disk
- **Rationale**: Simple, no Mnesia dependency, easy to implement and debug
- **Alternatives Considered**: ETS table (rejected - not persistent), Keep Mnesia (rejected - want clean break)
- **Impact**: Simple implementation, file I/O overhead acceptable for POC 

---

## Open Issues

Track unresolved questions or concerns here:

### 1. Khepri → DynamoDB Consistency
- **Issue**: What happens if exchange is deleted but messages still in DynamoDB?
- **Impact**: Orphaned messages, potential delivery to non-existent exchange
- **Priority**: Medium (deferred to Phase 2)
- **Proposed Solution**: Cleanup job or TTL-based expiration in DynamoDB

### 2. Timer Reconstruction After Failover
- **Issue**: When leader node fails, how does new leader reconstruct timer state?
- **Impact**: Critical for HA - messages might not be delivered if timer lost
- **Priority**: High (must solve in Phase 1)
- **Proposed Solution**: Query Khepri for next message to deliver on startup/failover

### 3. Broker ID Configuration
- **Issue**: How is broker_id determined and configured?
- **Impact**: Affects DynamoDB partition key, must be unique per broker
- **Priority**: High (must solve before Phase 2)
- **Proposed Solution**: ✅ **RESOLVED** - Use RabbitMQ cluster ID (available via `rabbit:cluster_name/0`)

### 4. Message Ordering Within Same Timestamp
- **Issue**: Multiple messages with same delivery timestamp - what order?
- **Impact**: Low - most use cases don't care about sub-second ordering
- **Priority**: Low
- **Proposed Solution**: Document as undefined behavior, or use message_id sort

### 5. DynamoDB Table Creation
- **Issue**: Who creates the DynamoDB table? Plugin? Manual? Terraform?
- **Impact**: Operational - must exist before plugin starts
- **Priority**: Medium
- **Proposed Solution**: Manual creation for POC, document table schema

### 6. Failure Recovery - Partial Writes
- **Issue**: Message in Khepri but not DynamoDB (or vice versa) - how to detect and recover?
- **Impact**: Data consistency, potential message loss
- **Priority**: Medium (deferred to production)
- **Proposed Solution**: Reconciliation job or eventual consistency acceptance

### 7. Clock Skew Between Nodes
- **Issue**: Different nodes may have slightly different system times
- **Impact**: Timer might fire early/late depending on which node is leader
- **Priority**: Low (acceptable for POC)
- **Proposed Solution**: Use monotonic time or accept small variance

### 8. DynamoDB Throttling
- **Issue**: What happens if DynamoDB throttles requests?
- **Impact**: Message publish failures, delivery delays
- **Priority**: Medium (deferred to production)
- **Proposed Solution**: Exponential backoff, circuit breaker, on-demand capacity mode
