-module(basho_bench_driver_emdb).

-record(state, {
	  handle
	 }).

-export([new/1,
         run/4]).

-include_lib("basho_bench/include/basho_bench.hrl").


%% ====================================================================
%% API
%% ====================================================================

new(1) ->
    %% Make sure emdb is available
    case code:which(emdb) of
        non_existing ->
            ?FAIL_MSG("~s requires emdb to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,
    %{ok, _} = emdb_sup:start_link(),
    setup(1);
new(Id) ->
    setup(Id).

setup(_Id) ->
    %% Get the target directory
    Dir = basho_bench_config:get(emdb_dir, "/tmp"),
    %Config = basho_bench_config:get(emdb, []),

    %% Start Lightning MDB
    case emdb:open(Dir, 32212254720, 16#10000 bor 16#40000 bor 16#80000) of
	{ok, H} ->
	    {ok, #state{handle=H}};
	{error, Reason} ->
	    ?FAIL_MSG("Failed to establish a Lightning MDB connection, emdb backend unable to start: ~p\n", [Reason]),
	    {error, Reason}
    end.

run(get, KeyGen, _ValueGen, #state{handle=Handle}=State) ->
    case emdb:get(Handle, KeyGen()) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, #state{handle=Handle}=State) ->
    Key = KeyGen(),
    Val = ValueGen(),
    case emdb:upd(Handle, Key, Val) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(delete, KeyGen, _ValueGen, #state{handle=Handle}=State) ->
    case emdb:del(Handle, KeyGen()) of
        ok ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

%% config_value(Key, Config, Default) ->
%%     case proplists:get_value(Key, Config) of
%%         undefined ->
%%             Default;
%%         Value ->
%%             Value
%%     end.
