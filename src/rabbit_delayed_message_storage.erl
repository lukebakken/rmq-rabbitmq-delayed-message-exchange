%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_delayed_message_storage).

%% Storage backend behavior for delayed message payloads
%% Implementations: disk (Phase 1), DynamoDB (Phase 2)

-type message_id() :: binary().
-type payload() :: binary().
-type config() :: #{atom() => term()}.
-type state() :: term().
-type error_reason() :: term().

-type message_metadata() :: #{
    message_id := binary(),
    delivery_timestamp := integer(),
    routing_key := binary(),
    exchange := binary(),
    vhost := binary(),
    created_at := integer()
}.

-export_type([message_id/0, payload/0, config/0, state/0, message_metadata/0]).

%% Initialize storage backend with configuration
-callback init(Config :: config()) ->
    {ok, State :: state()} |
    {error, Reason :: error_reason()}.

%% Store message payload with metadata
-callback store_message(MessageId :: message_id(),
                       Payload :: payload(),
                       Metadata :: message_metadata(),
                       State :: state()) ->
    {ok, NewState :: state()} |
    {error, Reason :: error_reason()}.

%% Fetch message payload
-callback fetch_message(MessageId :: message_id(),
                       Metadata :: message_metadata(),
                       State :: state()) ->
    {ok, Payload :: payload(), NewState :: state()} |
    {error, Reason :: error_reason()}.

%% Delete message payload
-callback delete_message(MessageId :: message_id(),
                        Metadata :: message_metadata(),
                        State :: state()) ->
    {ok, NewState :: state()} |
    {error, Reason :: error_reason()}.

%% Cleanup on shutdown
-callback terminate(State :: state()) -> ok.
