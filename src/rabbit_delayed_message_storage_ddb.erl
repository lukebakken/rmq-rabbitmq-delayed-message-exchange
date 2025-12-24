%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_delayed_message_storage_ddb).
-behaviour(rabbit_delayed_message_storage).

-include_lib("kernel/include/logger.hrl").

-export([init/1, store_message/4, fetch_message/3, delete_message/3, terminate/1]).

-record(ddb_storage_state, {
    client :: map(),
    table_name :: binary(),
    broker_id :: binary()
}).

-type state() :: #ddb_storage_state{}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec init(rabbit_delayed_message_storage:config()) ->
    {ok, state()} | {error, term()}.
init(Config) ->
    try
        {ok, _} = application:ensure_all_started(hackney),

        %% Get configuration
        TableName = maps:get(table_name, Config, <<"rabbitmq_delayed_messages">>),
        Endpoint = application:get_env(rabbitmq_delayed_message_exchange, 
                                       dynamodb_endpoint, 
                                       <<"http://localhost:8000">>),
        
        %% Get broker ID from cluster name
        BrokerId = rabbit_nodes:cluster_name(),
        
        %% Create AWS client for DynamoDB Local
        %% Use dummy credentials for local testing
        Client = aws_client:make_local_client(
            <<"fakeMyKeyId">>,
            <<"fakeSecretAccessKey">>,
            <<"8000">>,
            <<"localhost">>
        ),
        
        ?LOG_INFO("Delayed message DynamoDB storage initializing: table=~s, endpoint=~s",
                 [TableName, Endpoint]),
        
        State = #ddb_storage_state{
            client = Client,
            table_name = TableName,
            broker_id = BrokerId
        },
        
        %% Ensure table exists
        case ensure_table_exists(State) of
            ok ->
                ?LOG_INFO("Delayed message DynamoDB storage initialized"),
                {ok, State};
            {error, Reason} ->
                ?LOG_ERROR("Failed to initialize DynamoDB storage: ~tp", [Reason]),
                {error, {table_init_failed, Reason}}
        end
    catch
        Class:Error:Stack ->
            ?LOG_ERROR("Exception during DynamoDB storage init: ~p:~p~n~p", 
                      [Class, Error, Stack]),
            {error, {init_exception, Class, Error}}
    end.

-spec store_message(rabbit_delayed_message_storage:message_id(),
                   rabbit_delayed_message_storage:payload(),
                   rabbit_delayed_message_storage:message_metadata(),
                   state()) ->
    {ok, state()} | {error, term()}.
store_message(MessageId, Payload, Metadata, State = #ddb_storage_state{
    client = Client,
    table_name = TableName,
    broker_id = BrokerId
}) ->
    #{delivery_timestamp := DeliveryTimestamp,
      exchange := Exchange,
      vhost := VHost,
      created_at := CreatedAt} = Metadata,
    
    HexId = binary:encode_hex(MessageId, lowercase),
    
    %% Build partition and sort keys
    Bucket = timestamp_to_bucket(DeliveryTimestamp),
    PartitionKey = build_partition_key(BrokerId, VHost, Exchange, Bucket),
    SortKey = build_sort_key(DeliveryTimestamp, HexId),
    
    %% Build DynamoDB item
    Item = #{
        <<"partition_key">> => #{<<"S">> => PartitionKey},
        <<"sort_key">> => #{<<"S">> => SortKey},
        <<"message_payload">> => #{<<"B">> => base64:encode(Payload)},
        <<"created_at">> => #{<<"N">> => integer_to_binary(CreatedAt)}
    },
    
    Request = #{
        <<"TableName">> => TableName,
        <<"Item">> => Item
    },
    
    case aws_dynamodb:put_item(Client, Request) of
        {ok, _, _} ->
            ?LOG_DEBUG("Stored delayed message payload in DynamoDB: ~s (~B bytes)",
                      [HexId, byte_size(Payload)]),
            {ok, State};
        {error, Reason} ->
            ?LOG_ERROR("Failed to store delayed message payload ~s in DynamoDB: ~tp",
                      [HexId, Reason]),
            {error, {dynamodb_put_failed, MessageId, Reason}}
    end.

-spec fetch_message(rabbit_delayed_message_storage:message_id(),
                   rabbit_delayed_message_storage:message_metadata(),
                   state()) ->
    {ok, rabbit_delayed_message_storage:payload(), state()} | {error, term()}.
fetch_message(MessageId, Metadata, State = #ddb_storage_state{
    client = Client,
    table_name = TableName,
    broker_id = BrokerId
}) ->
    #{delivery_timestamp := DeliveryTimestamp,
      exchange := Exchange,
      vhost := VHost} = Metadata,
    
    HexId = binary:encode_hex(MessageId, lowercase),
    
    %% Build partition and sort keys
    Bucket = timestamp_to_bucket(DeliveryTimestamp),
    PartitionKey = build_partition_key(BrokerId, VHost, Exchange, Bucket),
    SortKey = build_sort_key(DeliveryTimestamp, HexId),
    
    %% Build DynamoDB key
    Key = #{
        <<"partition_key">> => #{<<"S">> => PartitionKey},
        <<"sort_key">> => #{<<"S">> => SortKey}
    },
    
    Request = #{
        <<"TableName">> => TableName,
        <<"Key">> => Key
    },
    
    case aws_dynamodb:get_item(Client, Request) of
        {ok, #{<<"Item">> := Item}, _} ->
            %% Extract and decode payload
            PayloadB64 = maps:get(<<"B">>, maps:get(<<"message_payload">>, Item)),
            Payload = base64:decode(PayloadB64),
            ?LOG_DEBUG("Fetched delayed message payload from DynamoDB: ~s (~B bytes)",
                      [HexId, byte_size(Payload)]),
            {ok, Payload, State};
        {ok, #{}, _} ->
            %% Item not found
            ?LOG_WARNING("Delayed message payload not found in DynamoDB: ~s", [HexId]),
            {error, {not_found, MessageId}};
        {error, Reason} ->
            ?LOG_ERROR("Failed to fetch delayed message payload ~s from DynamoDB: ~tp",
                      [HexId, Reason]),
            {error, {dynamodb_get_failed, MessageId, Reason}}
    end.

-spec delete_message(rabbit_delayed_message_storage:message_id(),
                    rabbit_delayed_message_storage:message_metadata(),
                    state()) ->
    {ok, state()} | {error, term()}.
delete_message(MessageId, Metadata, State = #ddb_storage_state{
    client = Client,
    table_name = TableName,
    broker_id = BrokerId
}) ->
    #{delivery_timestamp := DeliveryTimestamp,
      exchange := Exchange,
      vhost := VHost} = Metadata,
    
    HexId = binary:encode_hex(MessageId, lowercase),
    
    %% Build partition and sort keys
    Bucket = timestamp_to_bucket(DeliveryTimestamp),
    PartitionKey = build_partition_key(BrokerId, VHost, Exchange, Bucket),
    SortKey = build_sort_key(DeliveryTimestamp, HexId),
    
    %% Build DynamoDB key
    Key = #{
        <<"partition_key">> => #{<<"S">> => PartitionKey},
        <<"sort_key">> => #{<<"S">> => SortKey}
    },
    
    Request = #{
        <<"TableName">> => TableName,
        <<"Key">> => Key
    },
    
    case aws_dynamodb:delete_item(Client, Request) of
        {ok, _, _} ->
            ?LOG_DEBUG("Deleted delayed message payload from DynamoDB: ~s", [HexId]),
            {ok, State};
        {error, Reason} ->
            ?LOG_ERROR("Failed to delete delayed message payload ~s from DynamoDB: ~tp",
                      [HexId, Reason]),
            {error, {dynamodb_delete_failed, MessageId, Reason}}
    end.

-spec terminate(state()) -> ok.
terminate(#ddb_storage_state{table_name = TableName}) ->
    ?LOG_INFO("Delayed message DynamoDB storage terminated (table: ~s)", [TableName]),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

timestamp_to_bucket(TimestampMs) ->
    %% Convert milliseconds timestamp to 15-minute bucket string
    %% Format: "YYYY-MM-DD-HH-MM" (e.g., "2025-12-24-07-15")
    {{Year, Month, Day}, {Hour, Minute, _Second}} =
        calendar:system_time_to_universal_time(TimestampMs, millisecond),
    
    %% Round down to nearest 15-minute interval
    BucketMinute = (Minute div 15) * 15,
    
    iolist_to_binary(
        io_lib:format("~4..0B-~2..0B-~2..0B-~2..0B-~2..0B",
                     [Year, Month, Day, Hour, BucketMinute])).

build_partition_key(BrokerId, VHost, Exchange, Bucket) ->
    %% Format: broker_id#vhost#exchange#bucket
    iolist_to_binary([BrokerId, <<"#">>, VHost, <<"#">>, Exchange, <<"#">>, Bucket]).

build_sort_key(DeliveryTimestamp, HexId) ->
    %% Format: timestamp#message_id
    TimestampBin = integer_to_binary(DeliveryTimestamp),
    <<TimestampBin/binary, <<"#">>/binary, HexId/binary>>.

ensure_table_exists(#ddb_storage_state{client = Client, table_name = TableName}) ->
    %% Check if table exists
    DescribeRequest = #{<<"TableName">> => TableName},
    
    %% TODO: Error handling needs improvement - should have helper functions to decode
    %% AWS error types and handle retries, throttling, etc.
    case aws_dynamodb:describe_table(Client, DescribeRequest) of
        {ok, _, _} ->
            %% Table exists
            ?LOG_DEBUG("DynamoDB table ~s already exists", [TableName]),
            ok;
        {error, ErrorMap, {_StatusCode, _Headers, _Client}} ->
            %% Check error type from __type field
            case maps:get(<<"__type">>, ErrorMap, undefined) of
                <<"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException">> ->
                    %% Table doesn't exist, create it
                    ?LOG_INFO("Creating DynamoDB table: ~s", [TableName]),
                    create_table(Client, TableName);
                _OtherError ->
                    ?LOG_ERROR("Failed to describe DynamoDB table ~s: ~tp", [TableName, ErrorMap]),
                    {error, {describe_table_failed, ErrorMap}}
            end;
        {error, Reason} ->
            ?LOG_ERROR("Failed to describe DynamoDB table ~s: ~tp", [TableName, Reason]),
            {error, {describe_table_failed, Reason}}
    end.

create_table(Client, TableName) ->
    Request = #{
        <<"TableName">> => TableName,
        <<"KeySchema">> => [
            #{<<"AttributeName">> => <<"partition_key">>, <<"KeyType">> => <<"HASH">>},
            #{<<"AttributeName">> => <<"sort_key">>, <<"KeyType">> => <<"RANGE">>}
        ],
        <<"AttributeDefinitions">> => [
            #{<<"AttributeName">> => <<"partition_key">>, <<"AttributeType">> => <<"S">>},
            #{<<"AttributeName">> => <<"sort_key">>, <<"AttributeType">> => <<"S">>}
        ],
        <<"BillingMode">> => <<"PAY_PER_REQUEST">>
    },
    
    case aws_dynamodb:create_table(Client, Request) of
        {ok, _, _} ->
            ?LOG_INFO("DynamoDB table ~s created successfully", [TableName]),
            %% Wait for table to become active
            wait_for_table_active(Client, TableName);
        {error, Reason} ->
            ?LOG_ERROR("Failed to create DynamoDB table ~s: ~tp", [TableName, Reason]),
            {error, {create_table_failed, Reason}}
    end.

wait_for_table_active(Client, TableName) ->
    wait_for_table_active(Client, TableName, 30).

wait_for_table_active(_Client, TableName, 0) ->
    ?LOG_ERROR("Timeout waiting for DynamoDB table ~s to become active", [TableName]),
    {error, table_creation_timeout};
wait_for_table_active(Client, TableName, Retries) ->
    DescribeRequest = #{<<"TableName">> => TableName},
    
    case aws_dynamodb:describe_table(Client, DescribeRequest) of
        {ok, #{<<"Table">> := #{<<"TableStatus">> := <<"ACTIVE">>}}, _} ->
            ?LOG_INFO("DynamoDB table ~s is now active", [TableName]),
            ok;
        {ok, #{<<"Table">> := #{<<"TableStatus">> := Status}}, _} ->
            ?LOG_DEBUG("DynamoDB table ~s status: ~s, waiting...", [TableName, Status]),
            timer:sleep(1000),
            wait_for_table_active(Client, TableName, Retries - 1);
        {error, Reason} ->
            ?LOG_ERROR("Failed to check DynamoDB table ~s status: ~tp", [TableName, Reason]),
            {error, {describe_table_failed, Reason}}
    end.
