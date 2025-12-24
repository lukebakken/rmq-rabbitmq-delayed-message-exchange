# Coordination Analysis for Delayed Message Exchange

## Problem Statement

Currently, each node runs its own `rabbit_delayed_message` gen_server process. All three processes:
- Query the same Khepri data (replicated)
- Set timers for the same messages
- Attempt delivery when timers fire
- Result: ~3x duplicate delivery attempts (56 deliveries for 20 messages)

**Goal**: Ensure only ONE process in the cluster manages timers and deliveries.

---

## Option 1: Ra-Based State Machine

### What Ra Provides
- **Raft consensus protocol** - automatic leader election
- **Single leader** - only the leader processes commands
- **Automatic failover** - new leader elected if current leader dies
- **Replicated state** - all nodes have consistent view via write-ahead log
- **Built-in coordination** - no manual leader election needed

### How It Would Work
Replace the gen_server with a Ra state machine:

```erlang
-module(rabbit_delayed_message_fifo).
-behaviour(ra_machine).

-record(state, {
    messages :: #{message_id() => message_metadata()},
    next_delivery :: integer() | infinity
}).

%% Only the LEADER executes these
apply(_Meta, {enqueue, MessageId, Metadata}, State) ->
    %% Store metadata in replicated state (NOT the payload)
    %% Leader stores payload to disk/DynamoDB via storage backend
    %% Returns effects to set timer
    Messages2 = maps:put(MessageId, Metadata, State#state.messages),
    {State#state{messages = Messages2}, ok, []}.

apply(_Meta, {deliver, MessageId}, State) ->
    %% Remove from state - allows log compaction
    Messages2 = maps:remove(MessageId, State#state.messages),
    {State#state{messages = Messages2}, ok, [{deliver, MessageId}]}.

tick(TimeMs, State) ->
    %% Periodic callback - check for ready messages
    %% Only runs on leader
    Now = erlang:system_time(milli_seconds),
    ReadyMessages = maps:filter(
        fun(_Id, #{delivery_timestamp := TS}) -> TS =< Now end,
        State#state.messages),
    
    %% Return effects to deliver messages
    Effects = [{deliver, Id} || Id <- maps:keys(ReadyMessages)],
    {State, Effects}.
```

### Critical Insight: Log Compaction IS Viable

**Initial concern**: Ra's append-only log cannot delete individual entries, only compact before the release cursor.

**Reality**: Ra state machines store **state**, not individual messages. When we:
1. Add message to state via `maps:put` - log entry added
2. Remove message from state via `maps:remove` - log entry added
3. Take snapshot - current state captured
4. **Log entries before snapshot can be compacted**

The key: We're not trying to delete individual log entries. We're **modifying state** (removing delivered messages), and Ra's snapshot mechanism handles compaction naturally.

**Evidence**: `rabbit_fifo` (quorum queues) works this way:
- Messages added to state
- Messages removed from state when consumed
- No log compaction issues
- Handles millions of messages

### Disk Space Analysis

**Scenario**: 10 million messages, last one delayed 1 year

**Ra log entry size**:
- Ra overhead: ~150 bytes
- Message metadata: ~124 bytes (MessageId, DeliveryTS, RoutingKey, Exchange, VHost, StorageRef)
- **Total per entry**: ~274 bytes

**Storage calculation**:
- 10M enqueue commands: 10M × 274 bytes = 2.74 GB
- Periodic snapshots: ~1.24 GB (current state)
- As messages deliver, state shrinks
- Snapshots taken periodically (e.g., every 10K commands)
- Old log entries before snapshot: compacted

**Peak disk usage**: ~2-3 GB for 10M messages over 1 year

**Comparison to Khepri**:
- Khepri: 10M entries × ~200 bytes = 2 GB
- Khepri deletes entries immediately on delivery (more efficient)
- Ra: Holds entries until snapshot cycle (less efficient)

**Verdict**: ✅ VIABLE - Disk usage is acceptable (~2-3 GB for extreme case)

### Pros
- ✅ **Automatic leader election** - Raft consensus built-in
- ✅ **Single delivery point** - only leader delivers messages
- ✅ **No duplicate deliveries** - solves coordination problem completely
- ✅ **Proven pattern** - quorum queues use this successfully
- ✅ **Strong consistency** - Raft guarantees
- ✅ **Automatic failover** - new leader elected on failure
- ✅ **Log compaction works** - state modifications enable compaction

### Cons
- ❌ **Major rewrite** - gen_server → Ra state machine (significant effort)
- ❌ **Ra learning curve** - must understand Ra machine behavior
- ❌ **Less disk efficient** - holds log entries longer than Khepri
- ❌ **Snapshot overhead** - periodic snapshots of full state
- ❌ **More moving parts** - Ra cluster management

### Verdict: ✅ VIABLE AND ROBUST
The log compaction concern was based on a misunderstanding. Ra state machines work well for this use case, as proven by quorum queues. The disk overhead is acceptable for the coordination benefits gained.

---

## Option 2: Khepri Transactions for Leader Election

### What Khepri Provides
- **Built on Ra** - inherits Ra's leader election
- **Transactions** - atomic operations with compare-and-swap
- **Tree structure** - hierarchical key-value store
- **Cluster-wide replication** - consistent view across nodes

### How It Would Work
Use Khepri to implement a distributed lock/leader election:

```erlang
%% Each node attempts to claim leadership
claim_leadership() ->
    Path = [rabbitmq, delayed_messages, leader],
    NodeId = node(),
    
    %% Try to create leader node with our node ID
    case khepri:create(Path, #{node => NodeId, timestamp => erlang:system_time()}) of
        ok ->
            %% We are the leader
            {ok, leader};
        {error, {mismatching_node, _}} ->
            %% Someone else is leader
            {ok, follower}
    end.

%% Periodically refresh leadership claim
refresh_leadership() ->
    Path = [rabbitmq, delayed_messages, leader],
    NodeId = node(),
    
    %% Update timestamp to prove we're alive
    khepri:put(Path, #{node => NodeId, timestamp => erlang:system_time()}).

%% Check if we're still the leader
am_i_leader() ->
    Path = [rabbitmq, delayed_messages, leader],
    case khepri:get(Path) of
        {ok, #{node := NodeId}} when NodeId =:= node() ->
            true;
        _ ->
            false
    end.
```

### Implementation Approach
1. On startup, each gen_server attempts to claim leadership via Khepri
2. Only the leader sets timers and delivers messages
3. Followers monitor the leader node (via `erlang:monitor(process, {rabbit_delayed_message, LeaderNode})`)
4. If leader dies, followers detect it and race to claim leadership
5. New leader reconstructs timer state from Khepri

### Pros
- ✅ Leverages existing Khepri infrastructure
- ✅ No new dependencies
- ✅ Khepri already replicated and consistent
- ✅ Simple leadership model

### Cons
- ❌ Manual leader election logic (not automatic)
- ❌ Split-brain risk if network partitions
- ❌ Need heartbeat mechanism to detect dead leaders
- ❌ Race conditions during leadership transition
- ❌ More complex than using built-in coordination

### Verdict: ⚠️ POSSIBLE BUT COMPLEX
Requires careful implementation of leader election, heartbeats, and failover logic.

---

## Option 3: Mirrored Supervisor

### What Mirrored Supervisor Provides
- **Distributed supervisor** - one child process per cluster, not per node
- **Automatic migration** - child moves to another node if its node fails
- **Process group coordination** - uses `pg` for membership
- **Built-in failover** - surviving supervisors adopt orphaned children

### How It Would Work
Replace the current supervisor with a mirrored supervisor:

```erlang
-module(rabbit_delayed_message_sup).
-behaviour(mirrored_supervisor).

init([]) ->
    {ok, {{one_for_one, 10, 10},
          [{rabbit_delayed_message,
            {rabbit_delayed_message, start_link, []},
            transient,
            ?WORKER_WAIT,
            worker,
            [rabbit_delayed_message]}]}}.

%% In rabbit_delayed_message_app.erl or boot step:
start_link() ->
    mirrored_supervisor:start_link(
        {local, rabbit_delayed_message_sup},
        rabbit_delayed_message_sup,  %% Group name
        rabbit_delayed_message_sup,  %% Module
        []).
```

### Key Behavior
- **One child process** across the entire cluster (not one per node)
- **Runs on one node** at a time
- **Migrates on failure** - if the node dies, another supervisor starts the child
- **No coordination needed** - mirrored_supervisor handles it

### Implementation Changes
1. Change supervisor from `supervisor` to `mirrored_supervisor`
2. Add group name parameter
3. Ensure Mnesia tables exist (mirrored_supervisor uses Mnesia for coordination)
4. No changes to `rabbit_delayed_message` gen_server itself

### Pros
- ✅ **Minimal code changes** - just supervisor setup
- ✅ **Automatic coordination** - built-in leader election
- ✅ **Proven pattern** - used elsewhere in RabbitMQ
- ✅ **Handles failover** - automatic process migration
- ✅ **No manual leader election** - mirrored_supervisor does it

### Cons
- ❌ **Depends on Mnesia** - uses Mnesia for coordination state
- ❌ **Mnesia + Khepri** - mixing two metadata stores
- ❌ **Less control** - can't customize failover behavior
- ❌ **Deprecated?** - might be legacy, need to verify if still recommended

### Verdict: ✅ MOST PRACTICAL
Solves the problem with minimal changes and proven coordination mechanism.

---

## Option 4: Process Groups (pg)

### What pg Provides
- **Named process groups** - processes can join groups by name
- **Membership tracking** - query which processes are in a group
- **Eventually consistent** - membership view may temporarily diverge
- **No leader election** - just membership tracking

### How It Would Work
Use `pg` to track which gen_servers are running, then implement manual leader election:

```erlang
init([]) ->
    %% Join the delayed message process group
    pg:join(rabbit_delayed_messages, self()),
    
    %% Determine if we should be active
    Members = pg:get_members(rabbit_delayed_messages),
    SortedMembers = lists:sort(Members),
    AmLeader = hd(SortedMembers) =:= self(),
    
    State = #state{am_leader = AmLeader, ...},
    {ok, State}.

%% Only leader sets timers
maybe_delay_first() ->
    case am_i_leader() of
        true ->
            %% Set timer as normal
            ...;
        false ->
            %% Don't set timer
            not_set
    end.

am_i_leader() ->
    Members = pg:get_members(rabbit_delayed_messages),
    SortedMembers = lists:sort(Members),
    hd(SortedMembers) =:= self().
```

### Leadership Determination
Use **lowest PID** as leader (arbitrary but deterministic):
- All nodes query `pg:get_members(rabbit_delayed_messages)`
- Sort PIDs
- First PID is the leader
- Each node checks if it's the leader

### Handling Leader Failure
- Monitor all other members via `erlang:monitor(process, Pid)`
- When leader dies, all followers detect it
- Each re-evaluates: "Am I the new leader?"
- New leader (lowest remaining PID) starts timers

### Pros
- ✅ **Simple membership tracking** - pg is lightweight
- ✅ **No external dependencies** - pg is in Kernel
- ✅ **Eventually consistent** - handles network partitions gracefully
- ✅ **Flexible** - full control over leader election logic

### Cons
- ❌ **Manual leader election** - must implement ourselves
- ❌ **Race conditions** - multiple nodes might think they're leader during transitions
- ❌ **No automatic failover** - must detect and handle leader death
- ❌ **Eventually consistent** - temporary split-brain possible
- ❌ **More code** - need to implement monitoring, election, failover

### Verdict: ⚠️ POSSIBLE BUT REQUIRES CAREFUL IMPLEMENTATION
More flexible than mirrored_supervisor but requires more code and careful handling of edge cases.

---

## Comparison Matrix

| Feature | Ra State Machine | Khepri Transactions | Mirrored Supervisor | pg + Manual Election |
|---------|------------------|---------------------|---------------------|----------------------|
| **Automatic leader election** | ✅ Yes | ❌ No | ✅ Yes | ❌ No |
| **Handles failover** | ✅ Automatic | ⚠️ Manual | ✅ Automatic | ⚠️ Manual |
| **Code changes required** | 🔴 Major rewrite | 🟡 Moderate | 🟢 Minimal | 🟡 Moderate |
| **Out-of-order delivery** | ✅ Works (state-based) | ✅ Works | ✅ Works | ✅ Works |
| **Dependencies** | Ra | Khepri (Ra) | Khepri (via rabbit_db_msup) | Kernel (pg) |
| **Proven in RabbitMQ** | ✅ Quorum queues | ⚠️ Metadata only | ✅ Used historically | ❌ Not used |
| **Split-brain handling** | ✅ Raft consensus | ⚠️ Manual | ✅ Via Khepri/Ra | ❌ Eventually consistent |
| **Complexity** | 🔴 High | 🟡 Medium | 🟢 Low | 🟡 Medium |
| **Disk efficiency** | 🟡 Snapshots | ✅ Individual deletes | ✅ Individual deletes | ✅ Individual deletes |
| **Single delivery guarantee** | ✅ Leader only | ⚠️ Manual | ✅ One process | ⚠️ Manual |

---

## Recommendation: Mirrored Supervisor (Short Term) → Ra State Machine (Long Term)

### Phase 1: Mirrored Supervisor (Immediate Fix)

**Why start here**:
1. **Minimal code changes** - just supervisor setup
2. **Solves duplicate delivery problem** - immediately
3. **Proven solution** - already used in RabbitMQ
4. **Quick to implement** - can validate in hours
5. **Uses Khepri** - no additional dependencies (rabbit_db_msup uses Khepri when enabled)

**Implementation**: Change supervisor type, add group name, done.

### Phase 2: Ra State Machine (Production-Ready)

**Why migrate later**:
1. **Better architecture** - purpose-built for distributed coordination
2. **Stronger guarantees** - Raft consensus vs. process migration
3. **Proven at scale** - quorum queues handle millions of messages
4. **Single leader model** - cleaner than process migration
5. **Disk overhead acceptable** - ~2-3 GB for 10M messages over 1 year

**Implementation**: Rewrite as Ra state machine following `rabbit_fifo` pattern.

### Migration Path
1. **Now**: Fix duplicate deliveries with mirrored_supervisor
2. **Validate**: Test Phase 1 completion with single-process delivery
3. **Later**: Migrate to Ra state machine for production robustness
4. **Benefit**: Learn from Phase 1 experience before major rewrite

### Why Mirrored Supervisor is Best

1. **Minimal code changes**
   - Change supervisor type from `supervisor` to `mirrored_supervisor`
   - Add group name parameter
   - No changes to gen_server logic

2. **Proven coordination**
   - Used in RabbitMQ for years
   - Handles leader election automatically
   - Manages failover without manual intervention

3. **Solves the immediate problem**
   - Ensures only one gen_server runs cluster-wide
   - Eliminates duplicate deliveries
   - No timer coordination needed

4. **Works with current architecture**
   - Gen_server remains unchanged
   - Khepri storage works as-is
   - Disk storage works as-is

### Implementation Steps

1. **Update supervisor module**:
```erlang
-module(rabbit_delayed_message_sup).
-behaviour(mirrored_supervisor).

init([]) ->
    {ok, {{one_for_one, 10, 10},
          [{rabbit_delayed_message,
            {rabbit_delayed_message, start_link, []},
            transient,
            ?WORKER_WAIT,
            worker,
            [rabbit_delayed_message]}]}}.
```

2. **Update start_link call**:
```erlang
start_link() ->
    mirrored_supervisor:start_link(
        {local, ?MODULE},
        ?MODULE,  %% Group name
        ?MODULE,  %% Callback module
        []).
```

3. **Ensure Mnesia tables exist**:
```erlang
%% In boot step or application start
mirrored_supervisor:create_tables().
```

### Caveats

1. **Storage backend** - mirrored_supervisor uses `rabbit_db_msup` which supports both:
   - **Khepri** (when enabled) - uses Khepri for coordination state
   - **Mnesia** (fallback) - only if Khepri not available
   - Since we're already using Khepri, mirrored_supervisor will use Khepri too
   - **No additional Mnesia dependency** - everything stays in Khepri

2. **Migration on failure** - when a node dies, the child process restarts on another node
   - The new process will call `init/1` and reconstruct timer state from Khepri
   - This is already implemented in our current `init/1`

3. **Startup race** - multiple supervisors might try to start the child simultaneously
   - Mirrored_supervisor handles this via Khepri transactions
   - Only one succeeds, others see "already_in_store"

---

## Alternative: pg + Manual Leader Election

If mirrored_supervisor is deprecated or we want more control:

### Implementation Outline

```erlang
-record(state, {
    timer,
    stats_state,
    storage_backend,
    storage_state,
    am_leader :: boolean(),
    member_monitors :: #{pid() => reference()}
}).

init([]) ->
    %% Join process group
    ok = pg:join(rabbit_delayed_messages, self()),
    
    %% Monitor all other members
    Members = pg:get_members(rabbit_delayed_messages) -- [self()],
    Monitors = maps:from_list([{Pid, erlang:monitor(process, Pid)} || Pid <- Members]),
    
    %% Determine leadership
    AmLeader = is_leader(),
    
    %% Initialize storage
    {ok, StorageState} = ...,
    
    %% Only leader sets timer
    Timer = case AmLeader of
        true -> maybe_delay_first();
        false -> not_set
    end,
    
    {ok, #state{am_leader = AmLeader, member_monitors = Monitors, timer = Timer, ...}}.

is_leader() ->
    AllMembers = lists:sort(pg:get_members(rabbit_delayed_messages)),
    hd(AllMembers) =:= self().

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    %% Member died - remove from monitors
    Monitors2 = maps:remove(Pid, State#state.member_monitors),
    
    %% Check if we're now the leader
    AmLeader = is_leader(),
    
    %% If we just became leader, start timer
    Timer2 = case {State#state.am_leader, AmLeader} of
        {false, true} ->
            %% We just became leader
            maybe_delay_first();
        _ ->
            State#state.timer
    end,
    
    {noreply, State#state{am_leader = AmLeader, member_monitors = Monitors2, timer = Timer2}};

handle_info({timeout, _, _}, State = #state{am_leader = false}) ->
    %% We're not leader, ignore timer
    {noreply, State};

handle_info({timeout, TimerRef, {deliver, DeliveryTimestamp}}, 
            State = #state{am_leader = true, ...}) ->
    %% We're leader, deliver messages
    ...
```

### Pros
- ✅ No Mnesia dependency
- ✅ Full control over leader election
- ✅ Can customize failover behavior

### Cons
- ❌ More code to write and test
- ❌ Must handle all edge cases ourselves
- ❌ Eventually consistent (temporary split-brain possible)
- ❌ Need to monitor all members and handle 'DOWN' messages

---

## Final Recommendation

**Use mirrored_supervisor** for the following reasons:

1. **Proven solution** - already used in RabbitMQ
2. **Minimal changes** - just supervisor setup
3. **Automatic coordination** - no manual leader election
4. **Handles edge cases** - tested in production for years
5. **Quick to implement** - can validate in hours, not days

The Mnesia dependency is acceptable since it's only for coordination metadata, not message data. The messages remain in Khepri (metadata) and disk/DynamoDB (payloads).

If mirrored_supervisor is deprecated or problematic, the **pg + manual election** approach is the fallback, but requires significantly more code and careful testing of edge cases.

**Do NOT use Ra state machine** - the log compaction issue makes it fundamentally incompatible with delayed message delivery.
