%%%----------------------------------------------------------------------
%%% The MIT License (MIT)
%%%
%%% 	Copyright (c) 2017 Sergey Makeev
%%%
%%% 	Permission is hereby granted, free of charge, to any person obtaining a copy
%%% 	of this software and associated documentation files (the "Software"), to deal
%%% 	in the Software without restriction, including without limitation the rights
%%% 	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% 	copies of the Software, and to permit persons to whom the Software is
%%% 	furnished to do so, subject to the following conditions:
%%%
%%%  The above copyright notice and this permission notice shall be included in
%%% 	all copies or substantial portions of the Software.
%%%
%%% 	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% 	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% 	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% 	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% 	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% 	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%%% 	THE SOFTWARE.
%%%----------------------------------------------------------------------
-module(mod_xmq).

-behaviour(gen_mod).
-behaviour(gen_server).

%% gen_mod API
-export([start/2, stop/1, depends/2, mod_opt_type/1]).


%% gen_server API
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, terminate/2, code_change/3]).

%% timer and hooks
-export([remove_expired_consumers/2, c2s_closed/2, c2s_terminated/2]).

-record(state, {host = <<"">> :: binary(), routes = []}).

-define(XMQ_COMMAND_NS, <<"xmq:command">>).
-define(XMQ_TOPIC_NS, <<"xmq:topic">>).
-define(XMQ_TAG_NAME, <<"xmq">>).

-include("logger.hrl").
-include("xmpp.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%% gen_mod callbacks
%%%----------------------------------------------------------------------
start(Host, Opts) ->
    gen_mod:start_child(?MODULE, Host, Opts).

stop(Host) ->
    gen_mod:stop_child(?MODULE, Host).

%%% handle module options
%%%----------------------------------------------------------------------
mod_opt_type(check_expires_time) ->
    fun (A) when is_integer(A) andalso A >= 0 -> A end;
mod_opt_type(queues) ->
    fun (A) when is_list(A) -> A end;
mod_opt_type(_) -> [queues, check_expires_time].

%%% handle dependencies
%%%----------------------------------------------------------------------
depends(_Host, _Opts) ->
    [].


%%% gen_server callbacks
%%%----------------------------------------------------------------------
init([Host, _Opts]) ->
    ?DEBUG("mod_xmq: initialization ~p", [Host]),
    process_flag(trap_exit, true),
    ejabberd_hooks:add(c2s_closed, Host, ?MODULE, c2s_closed, 50),
    ejabberd_hooks:add(c2s_terminated, Host, ?MODULE, c2s_terminated, 50),
    TableName = gen_mod:get_module_proc(Host, xmq_consumers),
    ets:new(TableName, [set, named_table, public, {read_concurrency, true}]),
    catch ets:new(xmq_counters, [named_table, public]),
    ets:insert(xmq_counters, {round_robin, 0}),
    catch ets:new(xmq_routes, [named_table, public]),
    CfgCheckExpiresTime = gen_mod:get_module_opt(Host, ?MODULE, check_expires_time, fun(X) -> X end, all),
    CheckExpiresTime = if
                           is_integer(CfgCheckExpiresTime) andalso CfgCheckExpiresTime >= 0 -> CfgCheckExpiresTime;
                           true -> 10
                       end,
    ?DEBUG("mod_xmq: check_expires_time = ~p", [CheckExpiresTime]),
    RoutesAll = gen_mod:get_module_opt(Host, ?MODULE, queues, fun(X) -> X end, all),
    HostRoutes = [X || X <- RoutesAll, check_host(X, Host)],
    ?DEBUG("mod_xmq: routes ~p", [HostRoutes]),
    lists:foreach(
        fun([{route, Route}, {type, Type}]) ->
            LRoute = to_lower(Route),
            ?DEBUG("mod_xmq: register route: ~p, type: ~p", [LRoute, Type]),
            ejabberd_router:register_route(LRoute, Host),
            ets:insert(xmq_routes, {LRoute, Type})
    end, HostRoutes),
    %% 
    timer:apply_interval(CheckExpiresTime * 1000, ?MODULE, remove_expired_consumers, [Host, self()]),
    {ok, #state{host = Host, routes = HostRoutes}}.

terminate(_Reason, State) ->
    Host = State#state.host,
    lists:foreach(
        fun([{route, Route}, {type, _Type}]) ->
            LRoute = to_lower(Route),
            ?DEBUG("mod_xmq: unregister route: ~p, type: ~p", [LRoute, Route]),
            ejabberd_router:unregister_route(LRoute, Host)
    end, State#state.routes),
    ejabberd_hooks:delete(c2s_closed, Host, ?MODULE, c2s_closed, 50),
    ejabberd_hooks:delete(c2s_terminated, Host, ?MODULE, c2s_terminated, 50),
    ok.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({route, #iq{to = _To, type = Type} = Packet}, State) ->
    ?DEBUG("mod_xmq: incoming iq ~p", [Packet]),
    case get_iq_namespace( Packet ) of
        ?XMQ_COMMAND_NS ->
            do_xmq_command(Packet, Type);
        _ ->
            do_xmq_reroute(Packet)
    end,
    {noreply, State};
handle_info({route, #message{to = _To, thread = _Thread} = Packet}, State) ->
    ?DEBUG("mod_xmq: incoming messasge ~p", [Packet]),
    do_xmq_reroute(Packet),
    {noreply, State};
handle_info({route, _Packet}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% hook callbacks
%%%----------------------------------------------------------------------
c2s_closed(#{sid := _SID, sockmod := _SockMod, socket := _Socket,
             jid := JID, user := _U, server := S, resource := _R} = State, _Reason) ->
    ?DEBUG("mod_xmq: c2s_closed jid: ~p ~p", [JID, S]),
    remove_consumer(S, JID),
    State.

c2s_terminated(#{sid := _SID, sockmod := _SockMod, socket := _Socket,
                 jid := JID, user := _U, server := S, resource := _R} = State, _Reason) ->
    ?DEBUG("mod_xmq: c2s_terminated jid: ~p ~p", [JID, S]),
    remove_consumer(S, JID),
    State.


%%% execute xmq command
%%%----------------------------------------------------------------------
do_xmq_command(#iq{type = _Type,
                   id = _Id,
                   from = From,
                   to = To,
                   sub_els = [ #xmlel{name = ?XMQ_TAG_NAME, 
                                      attrs = [{<<"xmlns">>, ?XMQ_COMMAND_NS}, 
                                               {<<"command">>, Command},
                                               {<<"topic">>, Topic},
                                               {<<"ttl">>, Ttl},
                                               {<<"load">>, Load}]}]} = Packet, set) ->
    ?DEBUG("mod_xmq: do_xmq_command: ~p", [Packet]),
    Host = From#jid.lserver,
    Route = To#jid.lserver,
    try do_xmq_command_impl(erlang:binary_to_atom(Command, latin1), Topic,
                            erlang:binary_to_integer(Ttl, 10),
                            erlang:binary_to_integer(Load, 10),
                            From, Host, Route) catch
        _:Error -> ?WARNING_MSG("mod_xmq: Unknown error: ~p stack: ~p", [Error, erlang:get_stacktrace()])
    end,
    ok;
do_xmq_command(Packet, Type) ->
    ?WARNING_MSG("mod_xmq: Invalid xmq command: ~p ~p", [Packet, Type]),
    Lang = xmpp:get_lang(Packet),
    Err = xmpp:err_not_authorized(<<"Invalid xmq command">>, Lang),
    ejabberd_router:route_error(Packet, Err),
    ok.

%%% xmq command implementations
%%%----------------------------------------------------------------------
do_xmq_command_impl(del, Topic, _Ttl, _Load, JID, Host, Route) ->
    TableName = gen_mod:get_module_proc(Host, xmq_consumers),
    User = JID#jid.luser,
    Resource = JID#jid.lresource,
    MS = ets:fun2ms( fun({{TUser, TResource, TTopic, TRoute}, _TExpire, _TJID}) when User == TUser, Resource == TResource, Topic == TTopic, Route == TRoute -> true end),
    ets:select_delete(TableName, MS),
    ok;
do_xmq_command_impl(sub, Topic, Ttl, Load, JID, Host, Route) ->
    TableName = gen_mod:get_module_proc(Host, xmq_consumers),
    Expire = erlang:monotonic_time(seconds) + Ttl,
    User = JID#jid.luser,
    Resource = JID#jid.lresource,
    ets:insert(TableName, {{User, Resource, Topic, Route}, Expire, JID, Load}),
    ok;
do_xmq_command_impl(Command, Topic, Ttl, Load, JID, Host, Route) ->
    ?WARNING_MSG("mod_xmq: Unknown xmq command: ~p ~p ~p ~p ~p ~p ~p", [Command, Topic, Ttl, Load, JID, Host, Route]),
    ok.

%%% execute xmq reroute
%%%----------------------------------------------------------------------
do_xmq_reroute(Packet) ->
    From = xmpp:get_from(Packet),
    To = xmpp:get_to(Packet),
    ?DEBUG("mod_xmq: reroute packet from ~p", [From]),
    Route = To#jid.lserver,
    RouteType = case ets:lookup(xmq_routes, Route) of
                  [{_, Type}] -> Type;
                  _ -> none
              end,
    Topic = get_xmq_topic(Packet),
    try do_xmq_reroute_impl(RouteType, From, Route, Topic, From#jid.lserver, Packet) catch
        _:Error -> ?WARNING_MSG("mod_xmq: Unknown error: ~p stack: ~p", [Error, erlang:get_stacktrace()])
    end,
    ok.

%%% xmq reroute implementations
%%%----------------------------------------------------------------------
do_xmq_reroute_impl(_RouteType, _From, _Route, unknown, _Host, Packet) ->
    Lang = xmpp:get_lang(Packet),
    Err = xmpp:err_not_acceptable(<<"XMQ topic not found">>, Lang),
    ejabberd_router:route_error(Packet, Err),
    ok;
do_xmq_reroute_impl(any, From, Route, Topic, Host, Packet) ->
    RoundRobin = get_round_robin(),
    ActiveList = select_active_consumers(Host, Route, Topic),
    ?DEBUG("mod_xmq: reroute to any ~p", [ActiveList]),
    ?DEBUG("mod_xmq: from = ~p", [From]),
    ?DEBUG("mod_xmq: topic = ~p", [Topic]),
    ?DEBUG("mod_xmq: route = ~p", [Route]),
    Len = length(ActiveList),
    if
        Len > 0 ->
            {TargetJID, TargetLoad} = lists:nth((RoundRobin rem Len) + 1, ActiveList),
            ?DEBUG("target jid: ~p, load: ~p", [TargetJID, TargetLoad]),
            Packet2 = xmpp:set_from_to(Packet, From, TargetJID),
            ejabberd_router:route(Packet2);
        true ->
            Lang = xmpp:get_lang(Packet),
            Err = xmpp:err_not_acceptable(<<"No recipients found">>, Lang),
            ejabberd_router:route_error(Packet, Err)
    end,
    ok;
do_xmq_reroute_impl(all, From, Route, Topic, Host, Packet) ->
    ActiveList = select_active_consumers(Host, Route, Topic),
    ?DEBUG("mod_xmq: reroute to all ~p", [ActiveList]),
    ?DEBUG("mod_xmq: from = ~p", [From]),
    ?DEBUG("mod_xmq: topic = ~p", [Topic]),
    ?DEBUG("mod_xmq: route = ~p", [Route]),
    lists:foreach(
        fun({TargetJID, TargetLoad}) ->
            ?DEBUG("target jid: ~p, load: ~p", [TargetJID, TargetLoad]),
            Packet2 = xmpp:set_from_to(Packet, From, TargetJID),
            ejabberd_router:route(Packet2)
    end, ActiveList),
    ok.

%%% 
%%%----------------------------------------------------------------------
select_active_consumers(Host, Route, Topic) ->
    Now = erlang:monotonic_time(seconds),
    TableName = gen_mod:get_module_proc(Host, xmq_consumers),
    MS = ets:fun2ms( fun({{TUser, TResource, TTopic, TRoute}, TExpire, TJID, TLoad}) when Now =< TExpire, TTopic == Topic, TRoute == Route, TLoad =< 90 -> {TJID, TLoad} end),
    ets:select(TableName, MS).

%%% 
%%%----------------------------------------------------------------------
remove_consumer(Host, JID) ->
    TableName = gen_mod:get_module_proc(Host, xmq_consumers),
    User = JID#jid.luser,
    Resource = JID#jid.lresource,
    MS = ets:fun2ms( fun({{TUser, TResource, TTopic, TRoute}, TExpire, TJID, TLoad}) when User == TUser, Resource == TResource -> true end),
    ets:select_delete(TableName, MS),
    ok.

%%% 
%%%----------------------------------------------------------------------
remove_expired_consumers(Host, _PID) ->
    Now = erlang:monotonic_time(seconds),
    TableName = gen_mod:get_module_proc(Host, xmq_consumers),
    MS = ets:fun2ms( fun({TKey, TExpire, TJID, TLoad}) when Now > TExpire -> true end),
    ets:select_delete(TableName, MS),
    ok.

%%% 
%%%----------------------------------------------------------------------
get_round_robin() ->
    ets:update_counter(xmq_counters, round_robin, 1).

%%% 
%%%----------------------------------------------------------------------
get_iq_namespace(#iq{id = _Id, type = _Type, from = _From, to = _To, sub_els = [SubEls]}) ->
    xmpp:get_ns(SubEls);
get_iq_namespace(_) ->
    <<"">>.

to_lower(B) when is_binary(B) ->
    iolist_to_binary(string:to_lower(binary_to_list(B))).

%%% 
%%%----------------------------------------------------------------------
get_xmq_topic(#iq{type = _Type,
                  id = _Id,
                  from = _From,
                  to = _To,
                  sub_els = [ #xmlel{name = ?XMQ_TAG_NAME, 
                                     attrs = [{<<"xmlns">>, ?XMQ_TOPIC_NS}, 
                                              {<<"topic">>, Topic}]}]} = _Packet) ->
     Topic;
get_xmq_topic(#iq{type = _Type,
                  id = _Id,
                  from = _From,
                  to = _To,
                  sub_els = [ #xmlel{}, #xmlel{name = ?XMQ_TAG_NAME, 
                                     attrs = [{<<"xmlns">>, ?XMQ_TOPIC_NS}, 
                                              {<<"topic">>, Topic}]}]} = _Packet) ->
     Topic;
get_xmq_topic(#message{type = _Type,
                       id = _Id,
                       from = _From,
                       to = _To,
                       sub_els = [ #xmlel{name = ?XMQ_TAG_NAME, 
                                          attrs = [{<<"xmlns">>, ?XMQ_TOPIC_NS}, 
                                                   {<<"topic">>, Topic}]}]} = _Packet) ->
     Topic;
get_xmq_topic(_Packet) ->
     unknown.

%%% 
%%%----------------------------------------------------------------------
check_host([{route, Route}, {type, _Type}], Suffix) ->
    S = byte_size(Route),
    L = byte_size(Suffix),
    if
        L > S ->
            false;
        true ->
            RouteSuffix = binary:part(Route, S-L, L),
            case RouteSuffix of
                Suffix -> true;
                _ -> false
            end
    end.

