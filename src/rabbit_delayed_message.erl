%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_delayed_message).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "delayed message storage setup"},
                    {mfa, {?MODULE, setup_storage, []}},
                    {cleanup, {?MODULE, cleanup_storage, []}},
                    {requires, rabbit_khepri}]}).

-behaviour(gen_server).

-export([start_link/0, delay_message/3, setup_storage/0, cleanup_storage/0, go/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([messages_delayed/1]).

%% For testing, debugging and manual use
-export([refresh_config/0]).

-import(rabbit_delayed_message_utils, [swap_delay_header/1]).

-type t_reference() :: reference().
-type delay() :: non_neg_integer().

-spec delay_message(rabbit_types:exchange(),
                    mc:state(),
                    delay()) ->
                           nodelay | {ok, t_reference()}.

-spec internal_delay_message(t_reference(),
                             rabbit_types:exchange(),
                             mc:state(),
                             delay(),
                             module(),
                             term()) ->
                                    {{ok, t_reference()}, t_reference(), term()}.

-record(state, {
    timer,
    stats_state,
    storage_backend,
    storage_state
}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

go() ->
    gen_server:cast(?MODULE, go).

delay_message(Exchange, Message, Delay) ->
    gen_server:call(?MODULE, {delay_message, Exchange, Message, Delay},
                    infinity).

setup_storage() ->
    %% Storage backend initialization happens in init/1
    %% This function exists for boot step compatibility
    ok.

cleanup_storage() ->
    %% Cleanup happens in terminate/1
    %% This function exists for boot step compatibility
    ok.

messages_delayed(Exchange) ->
    %% Query Khepri for count of delayed messages for this exchange
    ExchangeName = Exchange#exchange.name,
    VHost = (Exchange#exchange.name)#resource.virtual_host,

    %% TODO: Optimize this - currently lists all messages
    AllMessages = rabbit_delayed_message_khepri:list_all_messages(),
    Filtered = lists:filter(
        fun(#{exchange := Ex, vhost := V}) ->
            Ex =:= ExchangeName andalso V =:= VHost
        end,
        AllMessages),
    length(Filtered).

refresh_config() ->
    gen_server:call(?MODULE, refresh_config).

%%--------------------------------------------------------------------

init([]) ->
    %% Initialize storage backend
    StorageBackend = rabbit_delayed_message_storage_disk,
    StorageConfig = #{},

    case StorageBackend:init(StorageConfig) of
        {ok, StorageState} ->
            ?LOG_INFO("Delayed message exchange: storage backend initialized (~tp)",
                     [StorageBackend]),
            _ = recover(),
            {ok, #state{timer = maybe_delay_first(),
                       storage_backend = StorageBackend,
                       storage_state = StorageState}};
        {error, Reason} ->
            ?LOG_ERROR("Delayed message exchange: failed to initialize storage backend: ~tp",
                      [Reason]),
            {stop, {storage_init_failed, Reason}}
    end.

handle_call({delay_message, Exchange, Message, Delay},
            _From, State = #state{timer = CurrTimer,
                                 storage_backend = Backend,
                                 storage_state = StorageState}) ->
    {Reply, NewTimer, NewStorageState} =
        internal_delay_message(CurrTimer, Exchange, Message, Delay, Backend, StorageState),
    State2 = State#state{timer = NewTimer, storage_state = NewStorageState},
    {reply, Reply, State2};

handle_call(refresh_config, _From, State) ->
    {reply, ok, refresh_config(State)};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(go, State) ->
    State2 = refresh_config(State),
    {noreply, State2#state{timer = maybe_delay_first()}};

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({timeout, _TimerRef, {deliver, _DeliveryTimestamp}},
            State = #state{storage_backend = Backend,
                          storage_state = StorageState}) ->
    %% Timer fired - deliver all messages with delivery_timestamp <= now
    Now = erlang:system_time(milli_seconds),

    %% Get all messages ready for delivery
    AllMessages = rabbit_delayed_message_khepri:list_all_messages(),
    ReadyMessages = lists:filter(
        fun(#{delivery_timestamp := TS}) -> TS =< Now end,
        AllMessages),

    %% Deliver each message
    NewStorageState = lists:foldl(
        fun(Metadata, AccStorageState) ->
            deliver_message(Metadata, Backend, AccStorageState, State)
        end,
        StorageState,
        ReadyMessages),

    {noreply, State#state{timer = maybe_delay_first(),
                         storage_state = NewStorageState}};

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, #state{storage_backend = Backend, storage_state = StorageState}) ->
    Backend:terminate(StorageState),
    ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

maybe_delay_first() ->
    case rabbit_delayed_message_khepri:get_next_message() of
        {ok, #{delivery_timestamp := FirstTS}} ->
            %% There are messages that will expire and need to be delivered
            Now = erlang:system_time(milli_seconds),
            start_timer(FirstTS - Now, FirstTS);
        {error, no_messages} ->
            %% Nothing to do
            not_set
    end.

deliver_message(Metadata, Backend, StorageState, State) ->
    #{message_id := MessageId,
      exchange := ExchangeBinName,
      vhost := VHost} = Metadata,

    %% Fetch payload from storage backend
    case Backend:fetch_message(MessageId, StorageState) of
        {ok, PayloadBinary, StorageState2} ->
            %% Reconstruct exchange resource
            ExchangeResource = #resource{virtual_host = VHost,
                                        kind = exchange,
                                        name = ExchangeBinName},

            %% Get exchange record
            case rabbit_db_exchange:get(ExchangeResource) of
                {ok, Exchange} ->
                    %% Deserialize message
                    Message = binary_to_term(PayloadBinary),

                    %% Swap delay header (set to negative)
                    Message2 = swap_delay_header(Message),

                    %% Route and deliver
                    Dests = rabbit_exchange:route(Exchange, Message2),
                    Qs = rabbit_db_queue:get_targets(Dests),
                    _ = rabbit_queue_type:deliver(Qs, Message2, #{}, stateless),

                    %% Bump stats
                    ExName = Exchange#exchange.name,
                    bump_routed_stats(ExName, Qs, State),

                    ?LOG_DEBUG("Delayed message exchange: delivered message ~ts", [MessageId]),

                    %% Delete from Khepri
                    _ = rabbit_delayed_message_khepri:delete_message_metadata(Metadata),

                    %% Delete from storage
                    case Backend:delete_message(MessageId, StorageState2) of
                        {ok, StorageState3} ->
                            StorageState3;
                        {error, Reason} ->
                            ?LOG_WARNING("Failed to delete message payload ~ts: ~tp",
                                        [MessageId, Reason]),
                            StorageState2
                    end;
                {error, not_found} ->
                    ?LOG_WARNING("Exchange not found for delayed message ~ts, cleaning up",
                                [MessageId]),
                    _ = rabbit_delayed_message_khepri:delete_message_metadata(Metadata),
                    _ = Backend:delete_message(MessageId, StorageState2),
                    StorageState2
            end;
        {error, Reason} ->
            ?LOG_ERROR("Failed to fetch message payload ~ts: ~tp",
                      [MessageId, Reason]),
            %% TODO: Handle fetch failures - DLQ? Retry?
            StorageState
    end.

internal_delay_message(CurrTimer, Exchange, Message, Delay, Backend, StorageState) ->
    Now = erlang:system_time(milli_seconds),
    DelayTS = Now + Delay,

    %% Generate unique message ID
    MessageId = rabbit_guid:gen(),

    %% Extract message payload
    %% TODO: Properly serialize mc:state() for storage
    Payload = term_to_binary(Message),

    %% Build metadata
    ExchangeName = Exchange#exchange.name,
    _VHost = ExchangeName#resource.virtual_host,
    _RoutingKey = case mc:routing_keys(Message) of
                     [RK | _] -> RK;
                     [] -> <<>>
                 end,
    _Headers = mc:get_annotation(headers, Message, #{}),

    Metadata = #{
        message_id => MessageId,
        delivery_timestamp => DelayTS,
        routing_key => _RoutingKey,
        headers => _Headers,
        exchange => ExchangeName#resource.name,
        vhost => _VHost,
        created_at => Now
    },

    %% Store metadata in Khepri
    case rabbit_delayed_message_khepri:store_message_metadata(Metadata) of
        ok ->
            %% Store payload in storage backend
            case Backend:store_message(MessageId, Payload, StorageState) of
                {ok, NewStorageState} ->
                    %% Update timer if needed
                    NewTimer = case CurrTimer of
                        not_set ->
                            %% No timer in progress, start one
                            maybe_delay_first();
                        _ ->
                            case erlang:read_timer(CurrTimer) of
                                false ->
                                    %% Timer already expired, handler will fire soon
                                    CurrTimer;
                                CurrMS when Delay < CurrMS ->
                                    %% New message expires sooner, restart timer
                                    _ = erlang:cancel_timer(CurrTimer),
                                    start_timer(Delay, DelayTS);
                                _ ->
                                    %% Current timer expires sooner
                                    CurrTimer
                            end
                    end,
                    {{ok, NewTimer}, NewTimer, NewStorageState};
                {error, Reason} ->
                    ?LOG_ERROR("Failed to store message payload ~ts: ~tp",
                              [MessageId, Reason]),
                    %% TODO: Should we delete from Khepri on storage failure?
                    {{ok, CurrTimer}, CurrTimer, StorageState}
            end;
        {error, Reason} ->
            ?LOG_ERROR("Failed to store message metadata ~ts in Khepri: ~tp",
                      [MessageId, Reason]),
            {{ok, CurrTimer}, CurrTimer, StorageState}
    end.

start_timer(Delay, DeliveryTimestamp) ->
    erlang:start_timer(erlang:max(0, Delay), self(), {deliver, DeliveryTimestamp}).

recover() ->
    %% Topology recovery has already happened
    %% Recover bindings for durable delayed message exchanges
    case list_exchanges() of
        {error, Reason} ->
            ?LOG_ERROR("Delayed message exchange: "
                      "failed to recover durable bindings, reason: ~tp",
                      [Reason]);
        Xs ->
            ?LOG_DEBUG("Delayed message exchange: "
                      "have ~b durable exchanges to recover",
                      [length(Xs)]),
            [recover_exchange_and_bindings(X) || X <- lists:usort(Xs)]
    end.

list_exchanges() ->
    Pattern = #exchange{durable = true, type = 'x-delayed-message', _ = '_'},
    rabbit_db_exchange:match(Pattern).

recover_exchange_and_bindings(#exchange{name = XName} = X) ->
    Bindings = rabbit_binding:list_for_source(XName),
    _ = [rabbit_exchange_type_delayed_message:add_binding(none, X, B)
         || B <- lists:usort(Bindings)],
    ?LOG_DEBUG("Delayed message exchange: recovered bindings for ~ts",
              [rabbit_misc:rs(XName)]).

bump_routed_stats(ExName, Qs, State) ->
    rabbit_global_counters:messages_routed(amqp091, length(Qs)),
    case rabbit_event:stats_level(State, #state.stats_state) of
        fine ->
            [begin
                 QName = amqqueue:get_name(Q),
                 FakeChannelId = self(),
                 Key = {FakeChannelId, {QName, ExName}},
                 rabbit_core_metrics:channel_stats(queue_exchange_stats, publish, Key, 1)
             end
             || Q <- Qs],
            ok;
        _ ->
            ok
    end.

refresh_config(State) ->
    rabbit_event:init_stats_timer(State, #state.stats_state).
