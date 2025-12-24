# ElastiCache/Valkey vs DynamoDB for Delayed Message Storage

## Summary

Your coworker likely suggested **Amazon MemoryDB** (not ElastiCache), which is a durable, Valkey-compatible in-memory database. This analysis compares three AWS storage options for Phase 2.

---

## Option 1: DynamoDB (Current Plan)

### What It Is
- Fully managed NoSQL key-value/document database
- Serverless with on-demand or provisioned capacity
- Single-digit millisecond latency
- Unlimited scalability

### Durability Model
- **Multi-AZ replication** - Data replicated across 3 AZs automatically
- **99.999999999% (11 9's) durability**
- **Persistent storage** - Data on SSD, not in-memory
- **No data loss** on node failure

### API
- HTTP/HTTPS REST API with JSON
- Operations: PutItem, GetItem, DeleteItem, Query, Scan
- aws-erlang library provides Erlang bindings

### Pros for Delayed Messages
- ✅ **True persistence** - Data survives all failures
- ✅ **Flexible schema** - Can store any attributes
- ✅ **Query support** - Can query by partition key + sort key range
- ✅ **TTL support** - Auto-delete expired items
- ✅ **No capacity planning** - On-demand mode auto-scales
- ✅ **Simple operations** - Just PutItem/GetItem/DeleteItem

### Cons
- ❌ **Higher latency** - Single-digit milliseconds (vs microseconds)
- ❌ **HTTP overhead** - REST API adds latency
- ❌ **Cost** - Pay per request + storage
- ❌ **256KB item limit** - Need S3 for larger messages

### Cost Estimate (1M messages/day, 1-hour avg delay)
- Writes: 1M × $1.25/million = $1.25/day
- Reads: 1M × $0.25/million = $0.25/day
- Storage: ~1GB × $0.25/GB-month = $0.25/month
- **Total: ~$45/month**

---

## Option 2: Amazon MemoryDB (Likely What Coworker Meant)

### What It Is
- Valkey/Redis-compatible **durable** in-memory database
- **Not just a cache** - designed as a primary database
- Multi-AZ distributed transaction log for durability
- 99.99% availability SLA

### Durability Model
- **Multi-AZ transaction log** - Writes persisted across AZs before ack
- **Microsecond read, single-digit millisecond write latency**
- **Data survives node failures** - Transaction log enables recovery
- **Stronger than AOF** - Distributed log vs single-node file

### API
- **Valkey/Redis protocol** - Native binary protocol (RESP)
- **Direct TCP connection** - No HTTP overhead
- Commands: SET, GET, DEL, ZADD (sorted sets), etc.
- Erlang clients: eredis, eredis_cluster

### Pros for Delayed Messages
- ✅ **Much lower latency** - Microsecond reads, single-digit ms writes
- ✅ **Native protocol** - No HTTP/JSON overhead
- ✅ **Sorted sets** - Perfect for timestamp-based ordering (ZADD with score=timestamp)
- ✅ **Atomic operations** - ZPOPMIN for atomic get-and-delete
- ✅ **Pub/Sub** - Could use for notifications (if needed)
- ✅ **True durability** - Multi-AZ transaction log
- ✅ **Fast failover** - Under 20 seconds unplanned, 200ms planned

### Cons
- ❌ **More expensive** - Pay for instance hours + data written
- ❌ **Capacity planning** - Must choose node types and count
- ❌ **No TTL** - Must manually delete expired items
- ❌ **512MB value limit** - Larger than DynamoDB but still limited
- ❌ **Less flexible** - Key-value model, not document store

### Cost Estimate (1M messages/day, 1-hour avg delay)
- Instance: r7g.large (13.5GB memory) × 2 nodes (primary + replica) = ~$0.40/hour
- Data written: 1M messages × ~1KB × $0.20/GB = ~$0.20/day
- **Total: ~$300/month** (much more expensive than DynamoDB)

### Sorted Set Pattern for Delayed Messages
```erlang
%% Store message with delivery timestamp as score
ZADD delayed_messages {delivery_timestamp} {message_id}:{payload}

%% Get messages ready for delivery (score <= now)
ZRANGEBYSCORE delayed_messages 0 {current_timestamp}

%% Atomic get and remove
ZPOPMIN delayed_messages {count}
```

---

## Option 3: ElastiCache for Valkey (NOT Recommended)

### What It Is
- Managed Valkey/Redis cache service
- **Designed for caching**, not primary storage
- Optional persistence (RDB snapshots or AOF)

### Durability Model
- **RDB snapshots** - Point-in-time backups (every 5+ minutes)
  - Data loss: Last 5+ minutes if node fails
- **AOF (Append-Only File)** - Log every write
  - Data loss: Up to 1 second with `fsync everysec`
  - Single-node file (not distributed)
- **Both are single-AZ** - Data loss risk on AZ failure

### Why NOT Recommended
- ❌ **Not durable enough** - Can lose minutes of data (RDB) or seconds (AOF)
- ❌ **Single-AZ persistence** - AOF file on one node only
- ❌ **Designed for caching** - Not a primary database
- ❌ **Weaker guarantees** - Compared to MemoryDB's transaction log

**Verdict**: ElastiCache is for caching, not durable storage. Use MemoryDB if you want Valkey with durability.

---

## Comparison Matrix

| Feature | DynamoDB | MemoryDB | ElastiCache |
|---------|----------|----------|-------------|
| **Durability** | 11 9's | Multi-AZ log | Single-AZ AOF |
| **Data loss on failure** | None | None | Seconds to minutes |
| **Read latency** | Single-digit ms | Microseconds | Microseconds |
| **Write latency** | Single-digit ms | Single-digit ms | Microseconds |
| **Protocol** | HTTP/JSON | Valkey/RESP | Valkey/RESP |
| **Capacity planning** | None (on-demand) | Required | Required |
| **Cost (1M msg/day)** | ~$45/month | ~$300/month | ~$200/month |
| **Item/value size limit** | 256KB | 512MB | 512MB |
| **TTL support** | Yes (automatic) | No (manual) | No (manual) |
| **Query patterns** | Partition + sort key | Sorted sets, ranges | Sorted sets, ranges |
| **Atomic operations** | Conditional writes | ZPOPMIN, MULTI/EXEC | ZPOPMIN, MULTI/EXEC |
| **Best for** | Durable, scalable | Low latency + durable | Caching only |

---

## Recommendation

### For POC: **DynamoDB**

**Reasons**:
1. **Simpler** - Just HTTP API calls, no connection management
2. **Serverless** - No capacity planning needed
3. **Cost-effective** - Pay per request, ~$45/month for 1M messages/day
4. **True durability** - 11 9's, no data loss
5. **Already planned** - Phase 2 plan already written

### For Production (If Latency Critical): **MemoryDB**

**Reasons**:
1. **Much lower latency** - Microsecond reads vs milliseconds
2. **Native protocol** - No HTTP/JSON overhead
3. **Sorted sets** - Natural fit for timestamp-based ordering
4. **Atomic operations** - ZPOPMIN for get-and-delete
5. **True durability** - Multi-AZ transaction log

**Trade-offs**:
- 6-7x more expensive (~$300/month vs $45/month)
- Requires capacity planning (node types, shard count)
- More complex client (Valkey protocol vs HTTP)

### NOT Recommended: **ElastiCache**

ElastiCache is designed for **caching**, not durable storage. The persistence options (RDB/AOF) are single-AZ and can lose data on failures. If you want Valkey with durability, use MemoryDB instead.

---

## Implementation Complexity

### DynamoDB
- **Low** - HTTP client already in aws-erlang
- Just build JSON requests and parse responses
- No connection pooling concerns (hackney handles it)

### MemoryDB
- **Medium** - Need Valkey client library (eredis or eredis_cluster)
- Connection pooling required
- Must handle Valkey protocol (RESP)
- Sorted set operations for timestamp ordering

---

## Decision Factors

**Choose DynamoDB if**:
- Cost is a concern (~$45/month vs ~$300/month)
- Simplicity is important (HTTP API vs Valkey protocol)
- Latency requirements are relaxed (single-digit ms acceptable)
- You want serverless/auto-scaling

**Choose MemoryDB if**:
- Latency is critical (need microsecond reads)
- You're already using Valkey/Redis in your stack
- You need sorted set operations
- Budget allows for 6-7x higher cost

**Don't choose ElastiCache** - It's not durable enough for delayed messages.

---

## Recommendation for This POC

**Stick with DynamoDB** for Phase 2:
1. Simpler implementation (already planned)
2. Cost-effective for POC
3. True durability without complexity
4. Can always migrate to MemoryDB later if latency becomes an issue

MemoryDB is a valid alternative for production if latency requirements demand it, but DynamoDB is the pragmatic choice for validating the architecture.
