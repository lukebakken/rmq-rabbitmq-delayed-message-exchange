%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_delayed_message_storage_disk).
-behaviour(rabbit_delayed_message_storage).

-include_lib("kernel/include/logger.hrl").

-export([init/1, store_message/3, fetch_message/2, delete_message/2, terminate/1]).

%% Shared storage directory for all nodes in cluster
%% For local dev clusters: /tmp/rabbitmq-test-instances/delayed_messages
%% For production: Could be NFS mount or node-local (Phase 1 limitation)
-define(DEFAULT_STORAGE_DIR, "/tmp/rabbitmq-test-instances/delayed_messages").

-record(disk_storage_state, {
    base_dir :: file:filename()
}).

-type state() :: #disk_storage_state{}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec init(rabbit_delayed_message_storage:config()) ->
    {ok, state()} | {error, term()}.
init(Config) ->
    BaseDir = maps:get(base_dir, Config, ?DEFAULT_STORAGE_DIR),
    case filelib:ensure_dir(filename:join(BaseDir, "dummy")) of
        ok ->
            ?LOG_INFO("Delayed message disk storage initialized at ~ts", [BaseDir]),
            {ok, #disk_storage_state{base_dir = BaseDir}};
        {error, Reason} ->
            ?LOG_ERROR("Failed to create delayed message storage directory ~ts: ~tp",
                      [BaseDir, Reason]),
            {error, {cannot_create_directory, BaseDir, Reason}}
    end.

-spec store_message(rabbit_delayed_message_storage:message_id(),
                   rabbit_delayed_message_storage:payload(),
                   state()) ->
    {ok, state()} | {error, term()}.
store_message(MessageId, Payload, State = #disk_storage_state{base_dir = BaseDir}) ->
    FilePath = message_file_path(BaseDir, MessageId),
    case file:write_file(FilePath, Payload) of
        ok ->
            ?LOG_DEBUG("Stored delayed message payload: ~ts", [MessageId]),
            {ok, State};
        {error, Reason} ->
            ?LOG_ERROR("Failed to store delayed message payload ~ts: ~tp",
                      [MessageId, Reason]),
            %% TODO: Implement retry logic for production
            {error, {write_failed, MessageId, Reason}}
    end.

-spec fetch_message(rabbit_delayed_message_storage:message_id(),
                   state()) ->
    {ok, rabbit_delayed_message_storage:payload(), state()} | {error, term()}.
fetch_message(MessageId, State = #disk_storage_state{base_dir = BaseDir}) ->
    FilePath = message_file_path(BaseDir, MessageId),
    case file:read_file(FilePath) of
        {ok, Payload} ->
            ?LOG_DEBUG("Fetched delayed message payload: ~ts", [MessageId]),
            {ok, Payload, State};
        {error, enoent} ->
            ?LOG_WARNING("Delayed message payload not found: ~ts", [MessageId]),
            {error, {not_found, MessageId}};
        {error, Reason} ->
            ?LOG_ERROR("Failed to fetch delayed message payload ~ts: ~tp",
                      [MessageId, Reason]),
            %% TODO: Implement retry logic for production
            {error, {read_failed, MessageId, Reason}}
    end.

-spec delete_message(rabbit_delayed_message_storage:message_id(),
                    state()) ->
    {ok, state()} | {error, term()}.
delete_message(MessageId, State = #disk_storage_state{base_dir = BaseDir}) ->
    FilePath = message_file_path(BaseDir, MessageId),
    case file:delete(FilePath) of
        ok ->
            ?LOG_DEBUG("Deleted delayed message payload: ~ts", [MessageId]),
            {ok, State};
        {error, enoent} ->
            %% File already deleted - idempotent operation
            ?LOG_DEBUG("Delayed message payload already deleted: ~ts", [MessageId]),
            {ok, State};
        {error, Reason} ->
            ?LOG_ERROR("Failed to delete delayed message payload ~ts: ~tp",
                      [MessageId, Reason]),
            %% TODO: Decide if delete failures should be fatal
            {error, {delete_failed, MessageId, Reason}}
    end.

-spec terminate(state()) -> ok.
terminate(#disk_storage_state{base_dir = BaseDir}) ->
    ?LOG_INFO("Delayed message disk storage terminated (files persist at ~ts)", [BaseDir]),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Note: TODO optimize with binary construction, perhaps?
message_file_path(BaseDir, MessageId) ->
    filename:join(BaseDir, <<MessageId/binary, ".msg">>).
