# Coding Rules and Lessons Learned

**Date**: 2025-12-24  
**Context**: RabbitMQ Delayed Message Exchange POC Development

---

## CRITICAL RULE #1: NEVER INVENT FUNCTIONS

**Problem**: Wasted hours by calling functions that don't exist.

**Examples of failures**:
- `rabbit:cluster_name/0` - doesn't exist, should be `rabbit_nodes:cluster_name/0`
- `mirrored_supervisor:find_child/2` - doesn't exist, should be `rabbit_db_msup:find_mirror/2`
- `aws_client:make_client(#{...})` - doesn't exist, should be `aws_client:make_local_client/4`

**MANDATORY PROCESS**:
1. **Before writing ANY function call**, verify it exists using one of:
   - `grep "^function_name(" module.erl` - Find function definition
   - `grep "^-export" module.erl` - Check if exported
   - `code` tool with `search_symbols` or `goto_definition`
   - `erl -noshell -eval 'io:format("~p~n", [module:module_info(exports)]), halt().'`

2. **Verify the EXACT signature**:
   - Check arity (number of parameters)
   - Check parameter types
   - Check return type

3. **If uncertain, ASK** - Don't guess, don't assume

**NO EXCEPTIONS**: This applies to:
- Standard library functions (Erlang/OTP)
- RabbitMQ internal functions
- Third-party library functions (aws-erlang, hackney, etc.)
- Custom functions in the codebase

---

## Rule #2: Verify Module Names

**Problem**: Called `rabbit:cluster_name/0` when it's actually in `rabbit_nodes` module.

**Process**:
1. Search for function usage in codebase: `grep -r "cluster_name" deps/rabbit/src/*.erl`
2. Check which module defines it
3. Verify module is the correct one

---

## Rule #3: Check Function Exports

**Problem**: Function exists but isn't exported (like `mirrored_supervisor:find_mirror/2`).

**Process**:
1. Find function definition: `grep "^find_mirror(" module.erl`
2. Check exports: `grep "^-export" module.erl | grep find_mirror`
3. If not exported, find the public API that wraps it

---

## Rule #4: Understand Library Architecture

**Problem**: Didn't understand mirrored_supervisor's three-process structure.

**Process**:
1. Read module documentation/comments
2. Look at test suites for usage examples
3. Find real production usage in codebase
4. Understand the architecture before using it

**Example**: mirrored_supervisor has:
- Overall supervisor (what you call)
- Mirroring server (coordination)
- Delegate supervisor (actual children)
- `find_mirror` returns mirroring server PID, not child PID

---

## Rule #5: Test Function Calls Before Using

**Problem**: Assumed functions work without testing.

**Process**:
1. Test in `erl` shell or via `rabbitmqctl eval`
2. Verify return values match expectations
3. Check for errors or exceptions

**Example**:
```bash
# Test if function exists and works
./sbin/rabbitmqctl -n rabbit-1 eval 'rabbit_nodes:cluster_name().'
```

---

## Rule #6: Read Error Messages Carefully

**Problem**: Misinterpreted cluster formation errors as unrelated to our code.

**Lesson**: When cluster won't start after code changes, it's almost certainly the code changes causing it, even if the error message seems unrelated.

**Process**:
1. Add try-catch blocks to catch ALL exceptions
2. Log full stack traces
3. Read the actual error, not what you think it should be

---

## Rule #7: Command Substitution and Variable Scope

**Problem**: Modified variables in functions called via `$(...)`, changes were lost.

**Rule**: Variables modified in command substitution (subshells) don't persist to parent shell.

**Solution**: Separate read and write operations:
```bash
# WRONG
get_and_increment() {
    local val="${array[$counter]}"
    counter=$((counter + 1))  # Lost when subshell exits
    printf "%s" "$val"
}
result=$(get_and_increment)

# RIGHT
get_current() {
    printf "%s" "${array[$counter]}"
}
advance() {
    counter=$((counter + 1))
}
result=$(get_current)
advance
```

---

## Rule #8: Array Initialization with Optional Arguments

**Problem**: Complex logic to replace default values caused bugs.

**Solution**: Start with empty array, append everything, set default if empty:
```bash
# WRONG
declare -a hosts=("default")
if first_arg_matches_default; then
    hosts=("$arg")  # Replace
else
    hosts+=("$arg")  # Append
fi

# RIGHT
declare -a hosts
# ... parse all arguments ...
hosts+=("$arg")  # Always append
# ... after parsing ...
if (( ${#hosts[@]} == 0 )); then
    hosts=("default")
fi
```

---

## Rule #9: Check HTTP Response Codes, Not Curl Exit Codes

**Problem**: Curl returns 0 even for HTTP 4xx/5xx errors.

**Solution**: Always capture and check HTTP status code:
```bash
# WRONG
if curl -s -X POST "$url"; then
    echo "Success"  # Runs even for HTTP 500!
fi

# RIGHT
http_code=$(curl -s -w "%{http_code}" -o /dev/null -X POST "$url")
if (( http_code == 200 )); then
    echo "Success"
fi
```

---

## Rule #10: Minimal Braces in Bash Variables

**Problem**: Excessive use of `${var}` when `$var` suffices.

**Rule**: Only use braces when required:
- Adjacent to alphanumeric: `${var}text`
- Array access: `${array[i]}`
- String operations: `${var%.*}`, `${#var}`
- Parameter expansion: `${var:-default}`

**Don't use braces for**:
- Simple variables: `$var` not `${var}`
- Variables followed by space, punctuation, or end of string

---

## Rule #11: Binary Data in Filenames

**Problem**: Used raw binary UUIDs as filenames, causing filesystem errors.

**Solution**: Always convert binary IDs to hex strings:
```erlang
% WRONG
FilePath = filename:join(BaseDir, <<MessageId/binary, ".msg">>).

% RIGHT
HexId = binary:encode_hex(MessageId, lowercase),
FilePath = filename:join(BaseDir, <<HexId/binary, ".msg">>).
```

---

## Rule #12: Don't Log Binary Data

**Problem**: Logged binary message IDs and payloads, corrupting logs.

**Solution**:
- Convert binary IDs to hex before logging
- Log payload size, not payload content
- Use `~s` for hex strings, not `~ts` for binaries

```erlang
% WRONG
?LOG_DEBUG("Stored message ~ts", [MessageId]).  % Binary garbage in logs

% RIGHT
HexId = binary:encode_hex(MessageId, lowercase),
?LOG_DEBUG("Stored message ~s (~B bytes)", [HexId, byte_size(Payload)]).
```

---

## Rule #13: Global Registration for Cluster-Wide Processes

**Problem**: Used local registration for process that needs to be called from any node.

**Solution**: Use `global:register_name/2` and `global:whereis_name/1`:
```erlang
% In init/1
case global:register_name(my_process, self()) of
    yes -> {ok, State};
    no -> {stop, name_already_registered}
end.

% When calling from any node
case global:whereis_name(my_process) of
    Pid when is_pid(Pid) ->
        gen_server:call(Pid, Request);
    undefined ->
        {error, not_running}
end.
```

---

## Rule #14: Understand Mirrored Supervisor

**Key facts**:
- Ensures only ONE child process runs cluster-wide
- Child can be on any node
- Automatically migrates on node failure
- Uses `rabbit_db_msup` which supports both Mnesia and Khepri
- When Khepri enabled, uses Khepri for coordination (not Mnesia)

**Usage**:
```erlang
-behaviour(mirrored_supervisor).

start_link() ->
    mirrored_supervisor:start_link(
        {local, ?MODULE},  % Local supervisor name
        ?MODULE,           % Group name
        ?MODULE,           % Callback module
        []                 % Args
    ).
```

---

## Rule #15: Don't Block Boot Process

**Problem**: Long-running operations in init/1 can timeout cluster formation.

**Solution**: 
- Keep init/1 fast
- Defer expensive operations (table creation, network calls)
- Or use try-catch to prevent crashes from blocking boot

---

## Rule #16: Add Try-Catch for Debugging

**Rule**: When debugging initialization issues, wrap init/1 in try-catch:
```erlang
init(Config) ->
    try
        % ... initialization code ...
    catch
        Class:Error:Stack ->
            ?LOG_ERROR("Exception during init: ~p:~p~n~p", 
                      [Class, Error, Stack]),
            {error, {init_exception, Class, Error}}
    end.
```

This reveals the actual error instead of generic timeout messages.

---

## Rule #17: Verify Third-Party Library APIs

**Problem**: Called `aws_client:make_client(#{endpoint => ...})` which doesn't exist.

**Process**:
1. Read the module source code
2. Check function signatures with `grep "^function_name("`
3. Look at examples in tests or documentation
4. Verify parameter types match

**For aws-erlang**:
- `make_client/0` - Uses env vars for credentials and region
- `make_client/1` - Takes region binary
- `make_client/3` - Takes access_key, secret_key, region
- `make_local_client/4` - For DynamoDB Local (access_key, secret_key, port, endpoint)

---

## Rule #18: Check Application Dependencies

**Problem**: Assumed hackney would be started automatically.

**Lesson**: Check application dependency chain:
1. Look at `.app.src` or compiled `.app` file
2. Verify dependencies are in `applications` list
3. Ensure transitive dependencies are correct

**For aws-erlang**:
- Depends on: `hackney`, `jsx`, `aws_beam_core`
- Hackney has `mod` entry, so it's an OTP application
- Must be in applications list to be started

---

## Summary: The Verification Checklist

Before writing ANY code that calls a function:

- [ ] Verify function exists in module
- [ ] Verify function is exported
- [ ] Verify arity (number of parameters) matches
- [ ] Verify parameter types are correct
- [ ] Check return value format
- [ ] Test in erl shell if uncertain
- [ ] Look for usage examples in codebase

**If you can't verify all of these, ASK - don't guess.**

---

## Time Wasted Today Due to Function Invention

1. `rabbit:cluster_name/0` - Should be `rabbit_nodes:cluster_name/0`
2. `mirrored_supervisor:find_child/2` - Should be `rabbit_db_msup:find_mirror/2`
3. `mirrored_supervisor:find_mirror/2` - Not exported, should be `rabbit_db_msup:find_mirror/2`
4. `aws_client:make_client(#{...})` - Doesn't exist, should be `make_local_client/4`

**Total time wasted**: Multiple hours

**Root cause**: Not verifying functions exist before using them

**Solution**: Follow the verification checklist EVERY TIME.

---

## Rule #19: AWS Error Response Format

**Problem**: Assumed aws-erlang would return `{error, {<<"ResourceNotFoundException">>, _}}` but it returns `{error, ErrorMap, {StatusCode, Headers, Client}}`.

**Lesson**: aws-erlang decodes JSON error responses into a map with `<<"__type">>` and `<<"Message">>` fields.

**Process**:
1. Match the 3-tuple error format: `{error, ErrorMap, {StatusCode, _, _}}`
2. Extract `<<"__type">>` from ErrorMap
3. Check for full exception name like `<<"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException">>`

**Example**:
```erlang
case aws_dynamodb:describe_table(Client, Request) of
    {ok, _, _} ->
        ok;
    {error, ErrorMap, {_StatusCode, _Headers, _Client}} ->
        case maps:get(<<"__type">>, ErrorMap, undefined) of
            <<"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException">> ->
                %% Handle missing table
                create_table(...);
            _OtherError ->
                {error, ErrorMap}
        end
end.
```

---

## Rule #20: Application Dependencies vs Boot Steps

**Problem**: Even though hackney was in the dependency chain, it wasn't started before our code ran during boot.

**Lesson**: Application dependencies ensure apps are **loaded**, not **started** at the right time during boot steps.

**Solution**: Explicitly start required applications in your code:
```erlang
init(Config) ->
    {ok, _} = application:ensure_all_started(hackney),
    %% Now safe to make HTTP requests
    ...
```

**Alternative**: Create a boot step that starts the application, but this is less flexible than starting it where needed.

---

## Rule #21: Boot Step Return Values

**Problem**: Boot step MFA called `application:ensure_all_started(hackney)` which returns `{ok, [Apps]}`, but boot steps expect `ok`.

**Solution**: Wrapper function that returns `ok`:
```erlang
ensure_hackney_started() ->
    {ok, _} = application:ensure_all_started(hackney),
    ok.
```

Then use in boot step:
```erlang
-rabbit_boot_step({my_boot_step,
                   [{mfa, {?MODULE, ensure_hackney_started, []}}]}).
```
