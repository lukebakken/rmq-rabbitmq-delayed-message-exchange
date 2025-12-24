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

-export_type([message_id/0, payload/0, config/0, state/0]).

%% Initialize storage backend with configuration
-callback init(Config :: config()) ->
    {ok, State :: state()} |
    {error, Reason :: error_reason()}.

%% Store message payload
-callback store_message(MessageId :: message_id(),
                       Payload :: payload(),
                       State :: state()) ->
    {ok, NewState :: state()} |
    {error, Reason :: error_reason()}.

%% Fetch message payload
-callback fetch_message(MessageId :: message_id(),
                       State :: state()) ->
    {ok, Payload :: payload(), NewState :: state()} |
    {error, Reason :: error_reason()}.

%% Delete message payload
-callback delete_message(MessageId :: message_id(),
                        State :: state()) ->
    {ok, NewState :: state()} |
    {error, Reason :: error_reason()}.

%% Cleanup on shutdown
-callback terminate(State :: state()) -> ok.
