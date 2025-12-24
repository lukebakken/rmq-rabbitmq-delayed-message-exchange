%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_delayed_message_khepri).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([store_message_metadata/1,
         get_next_message/0,
         delete_message_metadata/1,
         list_all_messages/0]).

%% Khepri path structure for delayed messages:
%% [rabbitmq, delayed_messages, VHost, Exchange, TimestampBucket, MessageId]
%%
%% Example: [rabbitmq, delayed_messages, <<"/">>, <<"my-exchange">>,
%%           <<"2025-12-23-15-00">>, <<"550e8400-e29b-41d4-a716-446655440000">>]

-define(DELAYED_MESSAGES_ROOT, [rabbitmq, delayed_messages]).

-type message_metadata() :: #{
    message_id := binary(),
    delivery_timestamp := integer(),
    routing_key := binary(),
    exchange := binary(),
    vhost := binary(),
    created_at := integer()
}.

-export_type([message_metadata/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec store_message_metadata(message_metadata()) -> ok | {error, term()}.
store_message_metadata(Metadata) ->
    #{message_id := MessageId,
      delivery_timestamp := DeliveryTimestamp,
      vhost := VHost,
      exchange := Exchange} = Metadata,

    Bucket = timestamp_to_bucket(DeliveryTimestamp),
    Path = build_message_path(VHost, Exchange, Bucket, MessageId),

    case khepri:put(Path, Metadata) of
        ok ->
            ?LOG_DEBUG("Stored delayed message metadata in Khepri: ~ts", [MessageId]),
            ok;
        {error, Reason} ->
            ?LOG_ERROR("Failed to store delayed message metadata ~ts in Khepri: ~tp",
                      [MessageId, Reason]),
            %% TODO: Implement retry logic for production
            {error, {khepri_put_failed, MessageId, Reason}}
    end.

-spec get_next_message() -> {ok, message_metadata()} | {error, no_messages}.
get_next_message() ->
    %% Query Khepri for message with earliest delivery timestamp
    %% Strategy: Get current time, query current and past buckets
    %% TODO: Implement efficient bucket scanning for production
    %% For now, use simple approach: list all messages and find earliest
    case list_all_messages() of
        [] ->
            {error, no_messages};
        Messages ->
            %% Find message with earliest delivery_timestamp
            Sorted = lists:sort(
                fun(#{delivery_timestamp := T1}, #{delivery_timestamp := T2}) ->
                    T1 =< T2
                end,
                Messages),
            {ok, hd(Sorted)}
    end.

-spec delete_message_metadata(message_metadata()) -> ok | {error, term()}.
delete_message_metadata(Metadata) ->
    #{message_id := MessageId,
      delivery_timestamp := DeliveryTimestamp,
      vhost := VHost,
      exchange := Exchange} = Metadata,

    Bucket = timestamp_to_bucket(DeliveryTimestamp),
    Path = build_message_path(VHost, Exchange, Bucket, MessageId),

    case khepri:delete(Path) of
        ok ->
            ?LOG_DEBUG("Deleted delayed message metadata from Khepri: ~ts", [MessageId]),
            ok;
        {error, {node_not_found, _}} ->
            %% Already deleted - idempotent operation
            ?LOG_DEBUG("Delayed message metadata already deleted: ~ts", [MessageId]),
            ok;
        {error, Reason} ->
            ?LOG_ERROR("Failed to delete delayed message metadata ~ts from Khepri: ~tp",
                      [MessageId, Reason]),
            {error, {khepri_delete_failed, MessageId, Reason}}
    end.

-spec list_all_messages() -> [message_metadata()].
list_all_messages() ->
    %% List all delayed messages across all vhosts and exchanges
    Pattern = ?DELAYED_MESSAGES_ROOT ++ [?KHEPRI_WILDCARD_STAR_STAR],
    case khepri:get_many(Pattern) of
        {ok, Result} ->
            %% Extract metadata from Khepri result
            lists:filtermap(
                fun({_Path, Metadata}) when is_map(Metadata) ->
                    {true, Metadata};
                   (_) ->
                    false
                end,
                maps:to_list(Result));
        {error, Reason} ->
            ?LOG_WARNING("Failed to list delayed messages from Khepri: ~tp", [Reason]),
            []
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec timestamp_to_bucket(integer()) -> binary().
timestamp_to_bucket(TimestampMs) ->
    %% Convert milliseconds timestamp to 15-minute bucket string
    %% Format: "YYYY-MM-DD-HH-MM" (e.g., "2025-12-23-15-00")
    {{Year, Month, Day}, {Hour, Minute, _Second}} =
        calendar:system_time_to_universal_time(TimestampMs, millisecond),

    %% Round down to nearest 15-minute interval
    BucketMinute = (Minute div 15) * 15,

    iolist_to_binary(
        io_lib:format("~4..0B-~2..0B-~2..0B-~2..0B-~2..0B",
                     [Year, Month, Day, Hour, BucketMinute])).

-spec build_message_path(binary(), binary(), binary(), binary()) -> khepri_path:native_path().
build_message_path(VHost, Exchange, Bucket, MessageId) ->
    ?DELAYED_MESSAGES_ROOT ++ [VHost, Exchange, Bucket, MessageId].
