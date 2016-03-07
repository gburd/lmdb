%%-------------------------------------------------------------------
%% This file is part of LMDB - Erlang Lightning MDB API
%%
%% Copyright (c) 2012 by Aleph Archives. All rights reserved.
%% Copyright (c) 2013 by Basho Technologies, Inc. All rights reserved.
%%
%%-------------------------------------------------------------------
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted only as authorized by the OpenLDAP
%% Public License.
%%
%% A copy of this license is available in the file LICENSE in the
%% top-level directory of the distribution or, alternatively, at
%% <http://www.OpenLDAP.org/license.html>.
%%
%% Permission to use, copy, modify, and distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%-------------------------------------------------------------------

-module(lmdb).

%%====================================================================
%% EXPORTS
%%====================================================================
-export([
         open/1,
         open/2,
         open/3,

         close/1,

         put/3,
         get/2,
         txn_begin/1,
         txn_commit/1,
         txn_abort/1,
         del/2,
	     update/3, upd/3,

         drop/1
        ]).


%% internal export (ex. spawn, apply)
-on_load(init/0).

%% config for testing
-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P), eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

%%====================================================================
%% Includes
%%====================================================================
-include("lmdb.hrl").
-include("async_nif.hrl").

%%====================================================================
%% MACROS
%%====================================================================
-define(LMDB_DRIVER_NAME, "lmdb").
-define(NOT_LOADED, not_loaded(?LINE)).
-define(MDB_MAP_SIZE, 2147483648). %% 2GB in bytes

%%====================================================================
%% PUBLIC API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Create a new MDB database
%% @end
%%--------------------------------------------------------------------
open(DirName) ->
    open(DirName, ?MDB_MAP_SIZE).
open(DirName, MapSize)
  when is_integer(MapSize)
       andalso MapSize > 0 ->
    open(DirName, MapSize, 0).
open(DirName, MapSize, EnvFlags)
  when is_integer(MapSize) andalso MapSize > 0 andalso
       is_integer(EnvFlags) andalso EnvFlags >= 0 ->
    %% ensure directory exists
    ok = filelib:ensure_dir(filename:join([DirName, "x"])),
    ?ASYNC_NIF_CALL(fun open/4, [DirName, MapSize, EnvFlags]).

open(_AsyncRef, _DirName, _MapSize, _EnvFlags) ->
    ?NOT_LOADED.

close(Handle) ->
    ?ASYNC_NIF_CALL(fun close/2, [Handle]).

close(_AsyncRef, _Handle) ->
    ?NOT_LOADED.

put(Handle, Key, Val)
  when is_binary(Key) andalso is_binary(Val) ->
    ?ASYNC_NIF_CALL(fun put/4, [Handle, Key, Val]).

put(_AsyncRef, _Handle, _Key, _Val) ->
    ?NOT_LOADED.

get(Handle, Key)
  when is_binary(Key) ->
    ?ASYNC_NIF_CALL(fun get/3, [Handle, Key]).

get(_AsyncRef, _Handle, _Key) ->
    ?NOT_LOADED.

txn_begin(Handle) ->
    ?ASYNC_NIF_CALL(fun txn_begin/2, [Handle]).

txn_begin(_AsyncRef, _Handle) ->
    ?NOT_LOADED.

txn_commit(Handle) ->
    ?ASYNC_NIF_CALL(fun txn_commit/2, [Handle]).

txn_commit(_AsyncRef, _Handle) ->
    ?NOT_LOADED.

txn_abort(Handle) ->
    ?ASYNC_NIF_CALL(fun txn_abort/2, [Handle]).

txn_abort(_AsyncRef, _Handle) ->
    ?NOT_LOADED.

del(Handle, Key)
  when is_binary(Key) ->
    ?ASYNC_NIF_CALL(fun del/3, [Handle, Key]).

del(_AsyncRef, _Handle, _Key) ->
    ?NOT_LOADED.

upd(Handle, Key, Val) ->
    update(Handle, Key, Val).

update(Handle, Key, Val)
  when is_binary(Key) andalso is_binary(Val) ->
    ?ASYNC_NIF_CALL(fun update/4, [Handle, Key, Val]).

update(_AsyncRef, _Handle, _Key, _Val) ->
    ?NOT_LOADED.

drop(Handle) ->
    ?ASYNC_NIF_CALL(fun drop/2, [Handle]).

drop(_AsyncRef, _Handle) ->
    ?NOT_LOADED.

%%====================================================================
%% PRIVATE API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
init() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end,
    erlang:load_nif(filename:join(PrivDir, ?LMDB_DRIVER_NAME), 0).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

open_test_db() ->
    {ok, CWD} = file:get_cwd(),
    DataDir = filename:join([CWD, "test", "eunit"]),
    ?cmd("rm -rf " ++ DataDir),
    ?assertMatch(ok, filelib:ensure_dir(filename:join([DataDir, "x"]))),
    {ok, Handle} = ?MODULE:open(DataDir, 2147483648),
    [?MODULE:upd(Handle, crypto:hash(sha, <<X>>),
		 crypto:rand_bytes(crypto:rand_uniform(128, 4096))) ||
	X <- lists:seq(1, 10)],
    Handle.

basics_test_() ->
    {setup,
     fun() ->
             open_test_db()
     end,
     fun(Handle) ->
             ok = ?MODULE:close(Handle)
     end,
     fun(Handle) ->
             {inorder,
              [{"open and close a database",
                fun() ->
                        Handle = open_test_db()
                end},
               {"create, then drop an empty database",
                fun() ->
                        Handle = open_test_db(),
                        ?assertMatch(ok, ?MODULE:drop(Handle))
                end},
               {"create, put an item, get it, then drop the database",
                fun() ->
                        Handle = open_test_db(),
                        ?assertMatch(ok, ?MODULE:put(Handle, <<"a">>, <<"apple">>)),
                        ?assertMatch(ok, ?MODULE:put(Handle, <<"b">>, <<"boy">>)),
                        ?assertMatch(ok, ?MODULE:put(Handle, <<"c">>, <<"cat">>)),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:get(Handle, <<"a">>)),
                        ?assertMatch(ok, ?MODULE:update(Handle, <<"a">>, <<"ant">>)),
                        ?assertMatch({ok, <<"ant">>}, ?MODULE:get(Handle, <<"a">>)),
                        ?assertMatch(ok, ?MODULE:del(Handle, <<"a">>)),
                        ?assertMatch(not_found, ?MODULE:get(Handle, <<"a">>)),
                        ?assertMatch(ok, ?MODULE:drop(Handle))
                end}
              ]}
     end}.

-ifdef(EQC).

qc(P) ->
    ?assert(eqc:quickcheck(?QC_OUT(P))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _Handle, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], Handle, Acc0) ->
    ok = ?MODULE:put(Handle, K, V),
    apply_kv_ops(Rest, Handle, orddict:store(K, V, Acc0));
apply_kv_ops([{del, K, _} | Rest], Handle, Acc0) ->
    ok = case ?MODULE:del(Handle, K) of
             ok ->
                 ok;
             not_found ->
                 ok;
             Else ->
                 Else
         end,
    apply_kv_ops(Rest, Handle, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     {ok, CWD} = file:get_cwd(),
		     DataDir = filename:join([CWD, "test", "eqc"]),
                     ?cmd("rm -rf " ++ DataDir),
                     ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
                     {ok, Handle} = ?MODULE:open(DataDir, 2147483648),
                     try
                         Model = apply_kv_ops(Ops, Handle, []),

                         %% Validate that all deleted values return not_found
                         F = fun({K, deleted}) ->
                                     ?assertEqual(not_found, ?MODULE:get(Handle, K));
                                ({K, V}) ->
                                     ?assertEqual({ok, V}, ?MODULE:get(Handle, K))
                             end,
                         lists:map(F, Model),
                         true
                     after
                         ?MODULE:close(Handle)
                     end
                 end)).

prop_put_delete_test_() ->
    {timeout, 3*60, fun() -> qc(prop_put_delete()) end}.

-endif.
-endif.
