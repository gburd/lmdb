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

%% Public API:
-export([env_open/3,
         env_close/1,
         copy/2,
         stat/1,
         sync/2,
         path/1,
         set_maxdbs/2,
         txn_begin/1, txn_begin/2,
         txn_begin/3,
         txn_commit/1,
         txn_abort/1,
         txn_reset/1,
         txn_renew/1,
         dbi_open/1, dbi_open/2, dbi_open/3, dbi_open/4,
         dbi_close/2,
         drop/2, drop/3,
         get/2, get/3,
         put/5,
         del/3, del/4, delete/3, delete/4,
	 cursor_open/2,
	 cursor_close/1,
	 cursor_renew/2,
	 cursor_txn/1,
	 cursor_dbi/1,
	 cursor_get/2, cursor_get/3, cursor_get/4,
	 cursor_put/4,
	 cursor_del/2,
	 cursor_count/1,
	 cmp/4,
	 dcmp/4,
	 lmdb_lib_version/0 ]).

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
-include("async_nif.hrl").

%%====================================================================
%% MACROS
%%====================================================================
-define(LMDB_DRIVER_NAME, "lmdb").
-define(NOT_LOADED, not_loaded(?LINE)).
-define(MDB_DEFAULT_DBI_SIZE, {2, 'GiB'}).

%% Environment Flags
-define(MDB_FIXEDMAP,   16#01).
-define(MDB_NOSUBDIR,   16#4000).
-define(MDB_NOSYNC,     16#10000).
-define(MDB_RDONLY,     16#20000).
-define(MDB_NOMETASYNC, 16#40000).
-define(MDB_WRITEMAP,   16#80000).
-define(MDB_MAPASYNC,   16#100000).
-define(MDB_NOTLS,      16#200000).

%% Database Flags
-define(MDB_REVERSEKEY, 16#02).
-define(MDB_DUPSORT,    16#04).
-define(MDB_INTEGERKEY, 16#08).
-define(MDB_DUPFIXED,   16#10).
-define(MDB_INTEGERDUP, 16#20).
-define(MDB_REVERSEDUP, 16#40).
-define(MDB_CREATE,     16#40000).

%% Write Flags
-define(MDB_NOOVERWRITE, 16#10).
-define(MDB_NODUPDATA,   16#20).
-define(MDB_CURRENT,     16#40).
-define(MDB_RESERVE,     16#10000).
-define(MDB_APPEND,      16#20000).
-define(MDB_APPENDDUP,   16#40000).
-define(MDB_MULTIPLE,    16#80000).

%%====================================================================
%% TYPES
%%====================================================================
-type options() :: [atom()].
-opaque env() :: reference().
-opaque dbi() :: reference().
-opaque txn() :: reference().
-opaque cursor() :: reference().
-type cursor_op() :: first | first_dup | get_both | get_both_range |
		     get_current | get_multiple | last | last_dup |
		     next | next_dup | next_multiple | next_nodup |
		     prev | prev_dup | prev_multiple | prev_nodup |
		     set | set_key | set_range.
-type path() :: string().
-type mode() :: string().
-type byte_size() :: non_neg_integer() | { non_neg_integer(),
                       b|bytes|'GB'|'GiB'|'TB'|'TiB'|'PB'|'PiB' }.

%%====================================================================
%% PUBLIC API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Open an existing or create a new LMDB database environment.
%%
%% If this function fails, env_close() must be called to discard the
%% reference.
%%
%% path        The directory in which the database files reside. This
%%             directory must already exist and be writable.
%%
%% flags       Special options for this environment. This parameter
%%             must be set to zero or more of the values described here.
%%
%%      fixedmap - Use a fixed address for the mmap region.
%%      nosubdir - By default, LMDB creates its environment in a directory whose
%%              pathname is given in \b path, and creates its data and lock files
%%              under that directory. With this option, \b path is used as-is for
%%              the database main data file. The database lock file is the \b path
%%              with "-lock" appended.
%%      rdonly - Open the environment in read-only mode. No write operations will be
%%              allowed. LMDB will still modify the lock file - except on read-only
%%              filesystems, where LMDB does not use locks.
%%      writemap - Use a writeable memory map unless MDB_RDONLY is set. This is faster
%%              and uses fewer mallocs, but loses protection from application bugs
%%              like wild pointer writes and other bad updates into the database.
%%              Incompatible with nested transactions.
%%      nometasync - Flush system buffers to disk only once per transaction, omit the
%%              metadata flush. Defer that until the system flushes files to disk,
%%              or next non-LMDB_RDONLY commit or sync(). This optimization
%%              maintains database integrity, but a system crash may undo the last
%%              one or more committed transaction. I.e. this option preserves the
%%              ACI (atomicity, consistency, isolation) but not D (durability)
%%              database transaction properties.
%%      nosync - Don't flush system buffers to disk when committing a transaction.
%%              This optimization means a system crash can corrupt the database or
%%              lose the last transactions if buffers are not yet flushed to disk.
%%              The risk is governed by how often the system flushes dirty buffers
%%              to disk and how often sync() is called.  However, if the filesystem
%%              preserves write order and the `writemap` flag is not used,
%%              transactions exhibit ACI (atomicity, consistency, isolation)
%%              properties and only lose D (durability).  I.e. database integrity
%%              is maintained, but a system crash may undo the final transactions.
%%              Note that using [nosync, writemap] togher leaves the system with no
%%              hint for when to write transactions to disk, unless sync()
%%              is called. [mapsync, writemap] may be preferable.
%%      mapsync - When using `writemap`, use asynchronous flushes to disk.
%%              As with `nosync`, a system crash can then corrupt the database
%%              or lose the last transactions. Calling sync() ensures on-disk
%%              database integrity until next commit.
%%      notls - Don't use Thread-Local Storage. Tie reader locktable slots to
%%              txn objects instead of to threads. I.e. txn_reset() keeps the
%%              slot reseved for the txn object. A thread may use parallel
%%              read-only transactions. A read-only transaction may span threads if
%%              the user synchronizes its use. Applications that multiplex many
%%              user threads over individual OS threads need this option. Such an
%%              application must also serialize the write transactions in an OS
%%              thread, since MDB's write locking is unaware of the user threads.
%%
%% mode         The UNIX permissions to set on created files. This parameter
%%              is ignored on Windows.
%%
%% Possible errors are:
%%      MDB_VERSION_MISMATCH - the version of the MDB library doesn't match the
%%      version that created the database environment.
%%      MDB_INVALID - the environment file headers are corrupted.
%%      ENOENT - the directory specified by the path parameter doesn't exist.
%%      EACCES - the user didn't have permission to access the environment files.
%%      EAGAIN - the environment was locked by another process.
%% @end
%%--------------------------------------------------------------------

-spec env_open(path(), options(), mode()) -> {ok, env()} | {error, term()}.
env_open(Path, Options, Mode) ->
    ValidOptions = [{ fixedmap, ?MDB_FIXEDMAP, []},
                    { nosubdir, ?MDB_NOSUBDIR, []},
                    { rdonly, ?MDB_RDONLY, [writemap]},
                    { writemap, ?MDB_WRITEMAP, [rdonly]},
                    { nometasync, ?MDB_NOMETASYNC, []},
                    { nosync, ?MDB_NOSYNC, []},
                    { mapsync, ?MDB_MAPASYNC, []},
                    { notls, ?MDB_NOTLS, []} ],
    case check_options(ValidOptions, Options) of
        {ok, Bitmask} ->
            %% Ensure directory exists
            ok = filelib:ensure_dir(filename:join([Path, "x"])),
            ?ASYNC_NIF_CALL(fun env_open_nif/4, [Path, Bitmask, Mode]);
        {error, _Reason}=Error ->
            Error
    end.

env_open_nif(_AsyncRef, _Path, _Options, _Mode) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Copy an MDB environment to the specified path.
%%
%% This function may be used to make a backup of an existing environment.
%% env        An environment handle returned by env_create(). It must
%%            have already been opened successfully.
%% path       The directory in which the copy will reside. This directory
%%            must already exist and be writable but must otherwise be
%%            empty.
%% @end
%%--------------------------------------------------------------------

-spec copy(env(), path()) -> ok | {error, term()}.
copy(Env, Path) ->
    ?ASYNC_NIF_CALL(fun copy_nif/3, [Env, Path]).

copy_nif(_AsyncRef, _Env, _Path) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Return statistics about the MDB environment.
%%
%% env        An environment handle returned by env_create()
%% @end
%%--------------------------------------------------------------------

-spec stat(env()) -> string().
stat(Env) ->
    ?ASYNC_NIF_CALL(fun stat_nif/2, [Env]).

stat_nif(_AsyncRef, _Env) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Flush the data buffers to disk.
%%
%% Data is always written to disk when commit() is called, but the
%% operating system may keep it buffered. MDB always flushes the OS
%% buffers upon commit as well, unless the environment was opened with
%% `nosync` or in part `nometasync`.
%%
%% env        An environment handle returned by env_create()
%% force      If true, force a synchronous flush.  Otherwise if the
%%            environment has the `nosync` flag set the flushes will
%%            be omitted, and with `mapasync` they will be asynchronous.
%%
%% Some possible errors are:
%%      EINVAL - an invalid parameter was specified.
%%      EIO - an error occurred during synchronization.
%% @end
%%--------------------------------------------------------------------

-spec sync(env(), boolean()) -> ok | {error, term()}.
sync(Env, Force) ->
    ?ASYNC_NIF_CALL(fun sync_nif/3, [Env, Force]).

sync_nif(_AsyncRef, _Env, _Force) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Close the environment and release the memory map.
%%
%% Only a single thread may call this function. All transactions, databases,
%% and cursors must already be closed before calling this function. Attempts
%% to use any such handles after calling this function will cause a SIGSEGV.
%% The environment handle will be freed and must not be used again after this
%% call.
%%
%% env        An environment handle returned by env_create()
%% @end
%%--------------------------------------------------------------------

-spec env_close(env()) -> ok | {error, term()}.
env_close(Env) ->
    ?ASYNC_NIF_CALL(fun env_close_nif/2, [Env]).

env_close_nif(_AsyncRef, _Env) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Return the path that was used in env_open().
%%
%% env        An environment handle returned by env_create()
%% @end
%%--------------------------------------------------------------------

-spec path(env()) -> {ok, path()} | {error, term()}.
path(Env) ->
    ?ASYNC_NIF_CALL(fun path_nif/2, [Env]).

path_nif(_AsyncRef, _Env) ->
    ?NOT_LOADED.

%% TODO: set_flags/get_flags
%% TODO: set_mapsize
%% TODO: set_maxreaders/get_maxreaders

%%--------------------------------------------------------------------
%% @doc Set the maximum number of named databases for the environment.
%%
%% This function is only needed if multiple databases will be used in the
%% environment. Simpler applications that use the environment as a single
%% unnamed database can ignore this option.
%% This function may only be called after env_create() and before
%% env_open().
%%
%% env        An environment handle returned by env_create()
%% dbs        The maximum number of databases
%% @end
%%--------------------------------------------------------------------

-spec set_maxdbs(env(), non_neg_integer()) -> ok | {error, term()}.
set_maxdbs(Env, Dbs) ->
    ?ASYNC_NIF_CALL(fun set_maxdbs_nif/3, [Env, Dbs]).

set_maxdbs_nif(_AsyncRef, _Env, _Dbs) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Create a transaction for use with the environment.
%%
%% The transaction handle may be discarded using txn_abort() or
%% txn_commit().
%%
%% Note: A transaction and its cursors must only be used by a single
%% thread, and a thread may only have a single transaction at a time.
%% If `notls` is in use, this does not apply to read-only transactions.
%%
%% Note: Cursors may not span transactions.
%%
%% env        An environment handle returned by env_create()
%% parent     If this parameter is non-NULL, the new transaction will be
%%            a nested transaction, with the transaction indicated by
%%            `parent` as its parent. Transactions may be nested to any
%%            level. A parent transaction may not issue any other
%%            operations besides txn_begin(), txn_abort(), or txn_commit()
%%            while it has active child transactions.
%% flags      Special options for this transaction. This parameter is a list
%%            of zero or more of the flags described here.
%%      rdonly - This transaction will not perform any write operations.
%%
%% Some possible errors are:
%%      MDB_PANIC - a fatal error occurred earlier and the environment
%%                  must be shut down.
%%      MDB_MAP_RESIZED - another process wrote data beyond this MDB_env's
%%                  mapsize and the environment must be shut down.
%%      MDB_READERS_FULL - a read-only transaction was requested and
%%                  the reader lock table is full. See env_set_maxreaders().
%%      ENOMEM - out of memory.
%% @end
%%--------------------------------------------------------------------

-spec txn_begin(env()) -> {ok, txn()} | {error, term()}.
-spec txn_begin(env(), options()) -> {ok, txn()} | {error, term()}.
-spec txn_begin(env(), txn(), options()) -> {ok, txn()} | {error, term()}.
txn_begin(Env) ->
    txn_begin(Env, []).
txn_begin(Env, Options) ->
    txn_begin(Env, undefined, Options).
txn_begin(Env, Parent, Options) ->
    ValidOptions = [{ rdonly, ?MDB_RDONLY, []}],
    case check_options(ValidOptions, Options) of
        {ok, Bitmask} ->
            ?ASYNC_NIF_CALL(fun txn_begin_nif/4, [Env, Parent, Bitmask]);
        {error, _Reason}=Error ->
            Error
    end.

txn_begin_nif(_AsyncRef, _Env, _Parent, _Bitmask) ->
    ?NOT_LOADED.


%%--------------------------------------------------------------------
%% @doc Commit all the operations of a transaction into the database.
%%
%% The transaction handle is freed. It and its cursors must not be used
%% again after this call, except with cursor_renew().
%%
%% Note: Only write-transactions free cursors.
%%
%% txn        A transaction handle returned by txn_begin()
%%
%% Some possible errors are:
%%      EINVAL - an invalid parameter was specified.
%%      ENOSPC - no more disk space.
%%      EIO - a low-level I/O error occurred while writing.
%%      ENOMEM - out of memory.
%% @end
%%--------------------------------------------------------------------

-spec txn_commit(txn()) -> ok | {error, term()}.
txn_commit(Txn) ->
    ?ASYNC_NIF_CALL(fun txn_commit_nif/2, [Txn]).

txn_commit_nif(_AsyncRef, _Txn) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Abandon all the operations of the transaction instead of saving them.
%%
%% The transaction handle is freed. It and its cursors must not be used
%% again after this call, except with cursor_renew().
%%
%% Note: Only write-transactions free cursors.
%%
%% txn        A transaction handle returned by txn_begin()
%% @end
%%--------------------------------------------------------------------

-spec txn_abort(txn()) -> ok | {error, term()}.
txn_abort(Txn) ->
    ?ASYNC_NIF_CALL(fun txn_abort_nif/2, [Txn]).

txn_abort_nif(_AsyncRef, _Txn) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Reset a read-only transaction.
%%
%% Abort the transaction like txn_abort(), but keep the transaction
%% handle. Later, txn_renew() will enable reuse of the handle. This
%% saves allocation overhead if the process will start a new read-only
%% transaction soon, and also locking overhead if `notls` is in use.
%% The reader table lock is released, but the table slot stays tied
%% to its thread or `txn`. Use txn_abort() to discard a reset handle,
%% and to free its lock table slot if `notls` is in use.
%%
%% Note: Cursors opened within the transaction must not be used again
%% after this call, except with cursor_renew().
%%
%% Reader locks generally don't interfere with writers, but they keep old
%% versions of database pages allocated. Thus they prevent the old pages
%% from being reused when writers commit new data, and so under heavy load
%% the database size may grow much more rapidly than otherwise.
%%
%% txn        A transaction handle returned by txn_begin()
%% @end
%%--------------------------------------------------------------------

-spec txn_reset(txn()) -> ok | {error, term()}.
txn_reset(Txn) ->
    ?ASYNC_NIF_CALL(fun txn_reset_nif/2, [Txn]).

txn_reset_nif(_AsyncRef, _Txn) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Renew a read-only transaction.
%%
%% This acquires a new reader lock for a transaction handle that had been
%% released by txn_reset(). It must be called before a reset transaction
%% may be used again.
%%
%% txn        A transaction handle returned by txn_begin()
%%
%% Some possible errors are:
%%      MDB_PANIC - a fatal error occurred earlier and the environment
%%                  must be shut down.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec txn_renew(txn()) -> ok | {error, term()}.
txn_renew(Txn) ->
    ?ASYNC_NIF_CALL(fun txn_renew_nif/2, [Txn]).

txn_renew_nif(_AsyncRef, _Txn) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Open a database in the environment.
%%
%% A database handle denotes the name and parameters of a database,
%% independently of whether such a database exists.
%% The database handle may be discarded by calling dbi_close().
%% The database handle will be private to the current transaction until
%% the transaction is successfully committed. If the transaction is
%% aborted the handle will be closed automatically.
%% After a successful commit the handle will reside in the shared
%% environment, and may be used by other transactions.
%%
%% Note: To use named databases (with name != NULL), env_set_maxdbs()
%% must be called before opening the environment.
%%
%% txn        A transaction handle returned by txn_begin()
%% name       The name of the database to open. If only a single
%%            database is needed in the environment, this value may
%%            be NULL.
%% size       The maximum size of the database in bytes or a 2-tuple
%%            {non_neg_integer(), b|bytes|MB|MiB|GB|GiB|TB|TiB|PB|PiB}.
%% flags      Special options for this database. This parameter is a
%%            list of zero or more of the values described here.
%%      reversekey - Keys are strings to be compared in reverse
%%            order, from the end of the strings to the beginning. By
%%            default, Keys are treated as strings and compared from
%%            beginning to end.
%%      dupsort - Duplicate keys may be used in the database.
%%            (Or, from another perspective, keys may have multiple data
%%            items, stored in sorted order.) By default keys must be unique
%%            and may have only a single data item.
%%      integerkey - Keys are binary integers in native byte order. Setting
%%            this option requires all keys to be the same size, typically
%%            sizeof(int) or sizeof(size_t).
%%      dupfixed - This flag may only be used in combination with `dupsort`.
%%            This option tells the library that the data items for this
%%            database are all the same size, which allows further
%%            optimizations in storage and retrieval. When all data items
%%            are the same size, the `get_multiple` and `next_multiple`
%%            cursor operations may be used to retrieve multiple items at once.
%%      integerdup - This option specifies that duplicate data items are also
%%            integers, and should be sorted as such.
%%      reversedup - This option specifies that duplicate data items should be
%%            compared as strings in reverse order.
%%      create - Create the named database if it doesn't exist. This option is
%%            not allowed in a read-only transaction or a read-only environment.
%%
%% Some possible errors are:
%%      MDB_NOTFOUND - the specified database doesn't exist in the environment
%%                    and `create` was not specified.
%%      MDB_DBS_FULL - too many databases have been opened. See env_set_maxdbs().
%% @end
%%--------------------------------------------------------------------

-spec dbi_open(txn()) -> {ok, dbi()} | {error, term()}.
-spec dbi_open(txn(), string()) -> {ok, dbi()} | {error, term()}.
-spec dbi_open(txn(), string(), byte_size()) -> {ok, dbi()} | {error, term()}.
-spec dbi_open(txn(), string(), byte_size(), options()) -> {ok, dbi()} | {error, term()}.
dbi_open(Txn) ->
    dbi_open(Txn, file:get_cwd()).
dbi_open(Txn, Path) ->
    dbi_open(Txn, Path, ?MDB_DEFAULT_DBI_SIZE).
dbi_open(Txn, Path, Size) ->
    dbi_open(Txn, Path, Size, [create]).
dbi_open(Txn, Path, Size, Options) ->
    ValidOptions = [{reversekey, ?MDB_REVERSEKEY, []},
                    {dupsort, ?MDB_DUPSORT, []},
                    {integerkey, ?MDB_INTEGERKEY, [reversekey]},
                    {dupfixed, ?MDB_DUPFIXED, []},
                    {integerdup, ?MDB_INTEGERDUP, []},
                    {reversedup, ?MDB_REVERSEDUP, []},
                    {create, ?MDB_CREATE, []}],
    case check_options(ValidOptions, Options) of
        {ok, Bitmask} ->
            Bytes = in_bytes(Size),
            ?ASYNC_NIF_CALL(fun dbi_open_nif/5, [Txn, Path, Bytes, Bitmask]);
        {error, _Reason}=Error ->
            Error
    end.

dbi_open_nif(_AsyncRef, _Txn, _Path, _Size, _Bitmask) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Close a database handle.
%%
%% env        An environment handle returned by env_create()
%% dbi        A database handle returned by dbi_open()
%% @end
%%--------------------------------------------------------------------

-spec dbi_close(env(), dbi()) -> ok | {error, term()}.
dbi_close(Env, Dbi) ->
    ?ASYNC_NIF_CALL(fun dbi_close/3, [Env, Dbi]).

dbi_close(_AsyncRef, _Env, _Dbi) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Delete a database and/or free all its pages.
%%
%% If the `del` parameter is true, the DB handle will be closed and the
%% DB will be deleted.  Otherwise this simply empties the database.
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%% del        `true` will delete the DB files from the environment,
%%            `false` will just free its pages (e.g. empty it).
%% @end
%%--------------------------------------------------------------------

-spec drop(txn(), dbi()) -> ok | {error, term()}.
-spec drop(txn(), dbi(), boolean()) -> ok | {error, term()}.
drop(Txn, Dbi) ->
    drop(Txn, Dbi, false).
drop(Txn, Dbi, Delete) ->
    ?ASYNC_NIF_CALL(fun drop/4, [Txn, Dbi, Delete]).

drop(_AsyncRef, _Txn, _Dbi, _Delete) ->
    ?NOT_LOADED.

%% TODO: mdb_set_compare
%% TODO: mdb_set_dupsort

%%--------------------------------------------------------------------
%% @doc Get items from a database.
%%
%% This function retrieves key/data pairs from the database. If the
%% database supports duplicate keys (`dupsort`) then the first data item
%% for the key will be returned. Retrieval of other items requires the use
%% of cursor_get().
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%% key        The key to search for in the database
%%
%% Some possible errors are:
%%      MDB_NOTFOUND - the key was not in the database.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec get(dbi(), binary()) -> {ok, binary()} | {error, term()}.
-spec get(txn(), dbi(), binary()) -> {ok, binary()} | {error, term()}.
get(Dbi, Key) ->
    get(undefined, Dbi, Key).
get(Txn, Dbi, Key)
  when is_binary(Key) ->
    ?ASYNC_NIF_CALL(fun get/4, [Txn, Dbi, Key]).

get(_AsyncRef, _Txn, _Dbi, _Key) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Store items into a database.
%%
%% This function stores key/data pairs in the database. The default behavior
%% is to enter the new key/data pair, replacing any previously existing key
%% if duplicates are disallowed, or adding a duplicate data item if
%% duplicates are allowed (`dupsort`).
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%% key        The key to store in the database
%% data       The data to store
%% flags      Special options for this operation. This parameter must be set
%%            to a list of zero or more of the values described here.
%%      nodupdata - enter the new key/data pair only if it does not
%%            already appear in the database. This flag may only be specified
%%            if the database was opened with `dupsort`. The function will
%%            return `exists` if the key/data pair already appears in the
%%            database.
%%      nooverwrite - enter the new key/data pair only if the key does not
%%            already appear in the database. The function will return
%%            `exists` if the key already appears in the database, even if
%%            the database supports duplicates (`dupsort`). The `data`
%%            parameter will be set to point to the existing item.
%%       append - append the given key/data pair to the end of the database.
%%            No key comparisons are performed. This option allows fast bulk
%%            loading when keys are already known to be in the correct order.
%%            Loading unsorted keys with this flag will cause data corruption.
%%       appenddup - as above, but for sorted dup data.
%%
%% Some possible errors are:
%%      MDB_MAP_FULL - the database is full, see env_set_mapsize().
%%      MDB_TXN_FULL - the transaction has too many dirty pages.
%%      EACCES - an attempt was made to write in a read-only transaction.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec put(txn(), dbi(), binary(), binary(), options()) -> ok | {error, term()}.
put(Txn, Dbi, Key, Value, Options)
  when is_binary(Key) andalso is_binary(Value) ->
    ValidOptions = [{nodupdata, ?MDB_NODUPDATA, []},
		    {nooverwrite, ?MDB_NOOVERWRITE, []},
		    {append, ?MDB_APPEND, []},
		    {appenddup, ?MDB_APPENDDUP, []}],
    case check_options(ValidOptions, Options) of
        {ok, Bitmask} ->
            ?ASYNC_NIF_CALL(fun put_nif/6, [Txn, Dbi, Key, Value, Bitmask]);
        {error, _Reason}=Error ->
            Error
    end.

put_nif(_AsyncRef, _Txn, _Dbi, _Key, _Value, _Bitmask) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Delete items from a database.
%%
%% This function removes key/data pairs from the database. If the database
%% does not support sorted duplicate data items (`dupsort`) the data
%% parameter is ignored. However, if the database supports sorted duplicates
%% and the data parameter is NULL, all of the duplicate data items for the
%% key will be deleted. Otherwise, if the data parameter is non-NULL only
%% the matching data item will be deleted.
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%% key        The key to delete from the database
%% data       The data to delete, or `<<>>` or `undefined`
%%
%% Some possible errors are:
%%      EACCES - an attempt was made to write in a read-only transaction.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec delete(txn(), dbi(), binary()) -> ok | {error, term()}.
-spec delete(txn(), dbi(), binary(), binary() | undefined) -> ok | {error, term()}.
-spec del(txn(), dbi(), binary()) -> ok | {error, term()}.
-spec del(txn(), dbi(), binary(), binary() | undefined) -> ok | {error, term()}.
delete(Txn, Dbi, Key) ->
    del(Txn, Dbi, Key).
delete(Txn, Dbi, Key, DupValue) ->
    del(Txn, Dbi, Key, DupValue).
del(Txn, Dbi, Key)
  when is_binary(Key) ->
    del(Txn, Dbi, Key, <<>>).
del(Txn, Dbi, Key, DupValue)
  when is_binary(Key) ->
    ?ASYNC_NIF_CALL(fun del/5, [Txn, Dbi, Key, DupValue]).

del(_AsyncRef, _Txn, _Dbi, _Key, _DupValue) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Create a cursor handle.
%%
%% A cursor is associated with a specific transaction and database.  A cursor
%% cannot be used when its database handle is closed.  Nor when its transaction
%% has ended, except with cursor_renew().  It can be discarded with
%% cursor_close().  A cursor in a write-transaction can be closed before its
%% transaction ends, and will otherwise be closed when its transaction ends.  A
%% cursor in a read-only transaction must be closed explicitly, before or after
%% its transaction ends. It can be reused withcursor_renew() before finally
%% closing it.
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%%
%% Some possible errors are:
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec cursor_open(txn(), dbi()) -> {ok, cursor()} | {error, term()}.
cursor_open(Txn, Dbi) ->
    ?ASYNC_NIF_CALL(fun cursor_open/3, [Txn, Dbi]).

cursor_open(_AsyncRef, _Txn, _Dbi) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Close a cursor handle.
%%
%% The cursor handle will be freed and must not be used again after this call.
%% Its transaction must still be live if it is a write-transaction.
%%
%% cursor     A cursor handle returned by cursor_open()
%% @end
%%--------------------------------------------------------------------

-spec cursor_close(cursor()) -> ok | {error, term()}.
cursor_close(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_close_nif/2, [Cursor]).

cursor_close_nif(_AsyncRef, _Cursor) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Renew a cursor handle.
%%
%% A cursor is associated with a specific transaction and database.
%% Cursors that are only used in read-only transactions may be re-used,
%% to avoid unnecessary malloc/free overhead.
%% The cursor may be associated with a new read-only transaction, and
%% referencing the same database handle as it was created with.
%% This may be done whether the previous transaction is live or dead.
%%
%% txn        A transaction handle returned by txn_begin()
%% cursor     A cursor handle returned by cursor_open()
%%
%% Some possible errors are:
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec cursor_renew(txn(), cursor()) -> ok | {error, term()}.
cursor_renew(Txn, Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_renew_nif/3, [Txn, Cursor]).

cursor_renew_nif(_AsyncRef, _Txn, _Cursor) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Return the cursor's transaction handle.
%%
%% cursor     A cursor handle returned by cursor_open()
%% @end
%%--------------------------------------------------------------------

-spec cursor_txn(cursor()) -> {ok, txn()} | {error, term()}.
cursor_txn(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_txn_nif/2, [Cursor]).

cursor_txn_nif(_AsyncRef, _Cursor) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Return the cursor's database handle.
%%
%% cursor     A cursor handle returned by cursor_open()
%% @end
%%--------------------------------------------------------------------

-spec cursor_dbi(cursor()) -> {ok, dbi()} | {error, term()}.
cursor_dbi(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_dbi_nif/2, [Cursor]).

cursor_dbi_nif(_AsyncRef, _Cursor) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Position and retrieve by cursor.
%%
%% This function retrieves key/data pairs from the database. Both the
%% key and the data are returned (except when using the `set` option,
%% in which case the key object will indicate where the cursor should
%% be positioned in the sorted keyspace of the database).
%%
%% cursor     A cursor handle returned by cursor_open()
%% key        The key used to position the cursor
%% data       The data used when `dupsort` to further refine the
%%            position within the duplicate values for this key.
%% op         One of the following cursor operations:
%%      first - position at first key/data item
%%      first_dup - position at first data item of current key, only
%%            valid when configured for `dupsort`
%%      get_both -  position at duplicate data for key, only valid when
%%            configured for `dupsort`
%%      get_both_range - position at duplicate nearest to data for key,
%%            only valid when configured for `dupsort`
%%      get_current - return key and data at current cursor position
%%      get_multiple - return all the duplicate data items at the current
%%            cursor position, only valid when configured for `dupfixed`
%%      last - position at last key/data item in the database
%%      last_dup - position at last data item of current key, only valid
%%            when configured for `dupsort`
%%      next - position at next data item
%%      next_dup - position at next data item of current key, only valid
%%            when configured for `dupsort`
%%      next_multiple - return all duplicate data items at the next cursor
%%           position, only valid when configured for `dupfixed`
%%      next_nodup - position at first data item of next key (skipping over
%%           any duplicates for the current key), only valid when
%%           configured for `dupsort`
%%      prev - position at previous data item
%%      prev_dup - position at previous data item of current key, only valid
%%            when configured for `dupsort`
%%      prev_multiple - return all duplicate data items at the previous
%%           cursor position, only valid when configured for `dupfixed`
%%      prev_nodup - position at first data item of prev key (skipping over
%%           any duplicates for the current key), only valid when
%%           configured for `dupsort`
%%      set - position at specified key
%%      set_key - position at specified key and return the key/data
%%      set_range - position at first key greater than or equal to
%%           specified key
%%
%% Some possible errors are:
%%      MDB_NOTFOUND - no matching key found.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec cursor_get(cursor(), cursor_op()) ->
			ok | {ok, binary()} |
			{ok, binary(), binary()} |
			{ok, [binary()]} |
			{error, term()}.
-spec cursor_get(cursor(), binary() | undefined, cursor_op()) ->
			ok | {ok, binary()} |
			{ok, binary(), binary()} |
			{ok, [binary()]} |
			{error, term()}.
-spec cursor_get(cursor(), binary() | undefined, binary() | undefined, cursor_op()) ->
			ok | {ok, binary()} |
			{ok, binary(), binary()} |
			{ok, [binary()]} |
			{error, term()}.
cursor_get(Cursor, CursorOp) ->
    cursor_get(Cursor, <<>>, <<>>, CursorOp).
cursor_get(Cursor, Key, CursorOp)
  when is_binary(Key) ->
    cursor_get(Cursor, Key, <<>>, CursorOp).
cursor_get(Cursor, undefined, undefined, CursorOp) ->
    cursor_get(Cursor, <<>>, <<>>, CursorOp);
cursor_get(Cursor, undefined, DupValue, CursorOp)
  when is_binary(DupValue) ->
    cursor_get(Cursor, <<>>, DupValue, CursorOp);
cursor_get(Cursor, Key, DupValue, CursorOp)
  when is_binary(Key) andalso is_binary(DupValue) ->
    ValidCursorOps = [first, first_dup, get_both, get_both_range,
		      get_current, get_multiple, last, last_dup, next,
		      next_dup, next_multiple, next_nodup, prev,
		      prev_dup, prev_multiple, prev_nodup, set, set_key,
		      set_range],
    case lists:member(CursorOp, ValidCursorOps) of
	true ->
	    ?ASYNC_NIF_CALL(fun cursor_get_nif/5, [Cursor, Key, DupValue, CursorOp]);
	false ->
	    {error, badarg}
    end.

cursor_get_nif(_AsyncRef, _Cursor, _Key, _DupValue, _CursorOp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Store data using a cursor.
%%
%% This function stores key/data pairs into the database. If the function
%% fails for any reason, the state of the cursor will be unchanged. If the
%% function succeeds and an item is inserted into the database, the cursor
%% is always positioned to refer to the newly inserted item.
%%
%% cursor     A cursor handle returned by cursor_open()
%% key        The key operated on.
%% data       The data operated on.
%% flags      Special options for this operation. This parameter must be set
%%            to a list of zero or more of the values described here.
%%      current - overwrite the data of the key/data pair to which the cursor
%%            refers with the specified data item. The key parameter is ignored.
%%      nodupdata - enter the new key/data pair only if it does not already
%%            appear in the database. This flag may only be specified if the
%%            database was opened with `dupsort`. The function will return
%%            `exists` if the key/data pair already appears in the database.
%%      nooverwrite - enter the new key/data pair only if the key does not
%%            already appear in the database. The function will return `exists`
%%            if the key already appears in the database, even if the database
%%            supports duplicates (`dupsort`).
%%      append - append the given key/data pair to the end of the database. No
%%            key comparisons are performed. This option allows fast bulk loading
%%            when keys are already known to be in the correct order. Loading
%%            unsorted keys with this flag will cause data corruption.
%%      appenddup - as above, but for sorted dup data.
%%
%% Some possible errors are:
%%      MDB_MAP_FULL - the database is full, see env_set_mapsize().
%%      MDB_TXN_FULL - the transaction has too many dirty pages.
%%      EACCES - an attempt was made to modify a read-only database.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec cursor_put(cursor(), binary(), binary(), options()) -> ok | {error, term()}.
cursor_put(Cursor, Key, Value, Options)
  when is_binary(Key) andalso is_binary(Value) ->
    ValidOptions = [{current, ?MDB_CURRENT, []},
		    {nodupdata, ?MDB_NODUPDATA, []},
		    {nooverwrite, ?MDB_NOOVERWRITE, []},
		    {append, ?MDB_APPEND, []},
		    {appenddup, ?MDB_APPENDDUP, []}],
    case check_options(ValidOptions, Options) of
        {ok, Bitmask} ->
            ?ASYNC_NIF_CALL(fun cursor_put_nif/5, [Cursor, Key, Value, Bitmask]);
        {error, _Reason}=Error ->
            Error
    end.

cursor_put_nif(_AsyncRef, _Cursor, _Key, _Value, _Options) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Delete current key/data pair
%%
%% This function deletes the key/data pair to which the cursor refers.
%%
%% cursor     A cursor handle returned by cursor_open()
%% flags      Special options for this operation. This parameter must be set
%%            to a list of zero or more of the values described here.
%%      nodupdata - delete all of the data items for the current key. This
%%            flag may only be specified if the database was opened with
%%            `dupsort`.
%%
%% Some possible errors are:
%%      EACCES - an attempt was made to modify a read-only database.
%%      EINVAL - an invalid parameter was specified.
%% @end
%%--------------------------------------------------------------------

-spec cursor_del(cursor(), options()) -> ok | {error, term()}.
cursor_del(Cursor, Options) ->
    ValidOptions = [{nodupdata, ?MDB_NODUPDATA, []}],
    case check_options(ValidOptions, Options) of
        {ok, Bitmask} ->
            ?ASYNC_NIF_CALL(fun cursor_del_nif/3, [Cursor, Bitmask]);
        {error, _Reason}=Error ->
            Error
    end.

cursor_del_nif(_AsyncRef, _Cursor, _Options) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Return count of duplicates for current key.
%%
%% This call is only valid on databases that support sorted duplicate
%% data items `dupsort`.
%%
%% cursor     A cursor handle returned by cursor_open()
%%
%% Some possible errors are:
%%      EINVAL - cursor is not initialized, or an invalid parameter
%%            was specified.
%% @end
%%--------------------------------------------------------------------

-spec cursor_count(cursor()) -> ok | {error, term()}.
cursor_count(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_count_nif/2, [Cursor]).

cursor_count_nif(_AsyncRef, _Cursor) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Compare two data items according to a particular database.
%%
%% This returns a comparison as if the two data items were keys in the
%% specified database.
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%% a          The first item to compare
%% b          The second item to compare
%% @end
%%--------------------------------------------------------------------

-spec cmp(txn(), dbi(), binary(), binary()) -> lt | gt | eq | {error, term()}.
cmp(Txn, Dbi, A, B)
    when is_binary(A) andalso is_binary(B) ->
    ?ASYNC_NIF_CALL(fun cmp_nif/5, [Txn, Dbi, A, B]).

cmp_nif(_AsyncRef, _Txn, _Dbi, _A, _B) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Compare two data items according to a particular database.
%%
%% This returns a comparison as if the two data items were keys in the
%% specified database.
%%
%% txn        A transaction handle returned by txn_begin()
%% dbi        A database handle returned by dbi_open()
%% a          The first item to compare
%% b          The second item to compare
%% @end
%%--------------------------------------------------------------------

-spec dcmp(txn(), dbi(), binary(), binary()) -> lt | gt | eq | {error, term()}.
dcmp(Txn, Dbi, A, B)
    when is_binary(A) andalso is_binary(B) ->
    ?ASYNC_NIF_CALL(fun dcmp_nif/5, [Txn, Dbi, A, B]).

dcmp_nif(_AsyncRef, _Txn, _Dbi, _A, _B) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% @doc Return the version information for LMDB
%%
%% Which has:
%%    1. Version String
%%    2. Major, Minor, Patch
%%    3. git describe --always --long --tags
%%       for the repo: git://gitorious.org/mdb/mdb.git
%% Example:
%% { "MDB 0.9.6: (January 10, 2013)", 0, 9, 6, "LMDB_0_9_6-60-g0cdd9df"}
%% @end
%%--------------------------------------------------------------------

-spec lmdb_lib_version() -> { string(), non_neg_integer(),
			      non_neg_integer(), non_neg_integer(),
			      string() }.
lmdb_lib_version() ->
    ?ASYNC_NIF_CALL(fun lmdb_lib_version_nif/1, []).

lmdb_lib_version_nif(_AsyncRef) ->
    ?NOT_LOADED.

%%====================================================================
%% PRIVATE API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

pow(Base, Exponent)
  when Exponent < 0 ->
  pow(1 / Base, -Exponent);

pow(Base, Exponent)
  when is_integer(Exponent) ->
  pow(Exponent, 1, Base).

pow(0, Product, _Modifier) ->
  Product;

pow(Exponent, Product, Modifier)
  when Exponent rem 2 =:= 1 ->
  pow(Exponent div 2, Product * Modifier, Modifier * Modifier);

pow(Exponent, Product, Modifier) ->
  pow(Exponent div 2, Product, Modifier * Modifier).

-spec in_bytes(byte_size()) -> non_neg_integer().
in_bytes(Size)
  when is_integer(Size) ->
    Size;
in_bytes({Amount, b}) ->
    Amount;
in_bytes({Amount, bytes}) ->
    Amount;
in_bytes({Amount, 'KB'}) ->
    Amount * 1000;
in_bytes({Amount, 'KiB'}) ->
    Amount * 1024;
in_bytes({Amount, 'MB'}) ->
    Amount * pow(1000, 2);
in_bytes({Amount, 'MiB'}) ->
    Amount * pow(1024, 2);
in_bytes({Amount, 'GB'}) ->
    Amount * pow(1000, 3);
in_bytes({Amount, 'GiB'}) ->
    Amount * pow(1024, 3);
in_bytes({Amount, 'TB'}) ->
    Amount * pow(1000, 4);
in_bytes({Amount, 'TiB'}) ->
    Amount * pow(1024, 4);
in_bytes({Amount, 'PB'}) ->
    Amount * pow(1000, 5);
in_bytes({Amount, 'PiB'}) ->
    Amount * pow(1024, 5);
in_bytes({Amount, kb}) ->
    Amount * 1000;
in_bytes({Amount, kib}) ->
    Amount * 1024;
in_bytes({Amount, mb}) ->
    Amount * pow(1000, 2);
in_bytes({Amount, mib}) ->
    Amount * pow(1024, 2);
in_bytes({Amount, gb}) ->
    Amount * pow(1000, 3);
in_bytes({Amount, gib}) ->
    Amount * pow(1024, 3);
in_bytes({Amount, tb}) ->
    Amount * pow(1000, 4);
in_bytes({Amount, tib}) ->
    Amount * pow(1024, 4);
in_bytes({Amount, pb}) ->
    Amount * pow(1000, 5);
in_bytes({Amount, pib}) ->
    Amount * pow(1024, 5).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

-spec check_options([{atom(), non_neg_integer(), [atom()]}], options()) ->
                           {ok, non_neg_integer()} | {error, any()}.
check_options([], []) ->
    {ok, 0};
check_options(_ValidOptions, []) ->
    {ok, 0};
check_options([], _Options) ->
    {error, badarg};
check_options(ValidOptions, Options) ->
    case check_options(ValidOptions, Options, 0, []) of
        {error, _Reason}=Error ->
            Error;
        {Mask, _Excl} ->
            {ok, Mask}
    end.

check_options(_ValidOptions, [], Mask, Excl) ->
    {Mask, Excl};
check_options(ValidOptions, [Option | Rest], Mask, Excl) ->
    case lists:member(Option, Excl) of
        true ->
            {error, badarg};
        false ->
            {Mask, Excl} =
                check_option(lists:keyfind(Option, 1, ValidOptions), Mask, Excl),
            check_options(ValidOptions, Rest, Mask, Excl)
    end.

check_option(false, Mask, Excl) ->
    {Mask, Excl};
check_option({_Option, Bitmask, Excluding}, Mask, Excl) ->
    {Mask bor Bitmask, [Excluding | Excl]}.

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
    [?MODULE:upd(Handle, crypto:sha(<<X>>),
                 crypto:rand_bytes(crypto:rand_uniform(128, 4096))) ||
        X <- lists:seq(1, 100)],
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
