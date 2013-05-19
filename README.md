EMDB
====

EMDB is a NIF library for the [Memory-Mapped Database](http://highlandsun.com/hyc/mdb/) database, aka. MDB.

The main purpose of this package is to provide an Erlang API for this excellent BTREE implementation.  Secondly to build an alternative Riak/KV [backend](http://wiki.basho.com/Storage-Backends.html) and Riak's AAE feature based on this.  Finally it'd be nice to build an ETS-compatible API (ala "lets" for LevelDB) to ease adoption in other places where DETS is just not up to the task.

Requirements
------------
* Erlang R14B04+
* GCC 4.2+ or MS VisualStudio 2010+

Build
-----

$ make


API
---

The following functions were implemented:

* `open/1`: equivalent to `emdb:open(DirName, 10485760)`.
* `open/2`: equivalent to `emdb:open(DirName, 10485760, 0)`.
* `open/3`: creates a new MDB database. This call also re-open an already existing one. Arguments are:
	* DirName: database directory name
	* MapSize: database map size (see [map.hrl](http://gitorious.org/mdb/mdb/blobs/master/libraries/libmdb/mdb.h))
	* EnvFlags: database environment flags (see [map.hrl](http://gitorious.org/mdb/mdb/blobs/master/libraries/libmdb/mdb.h)). The possible values are defined in **emdb.hrl**.
* `close/2`: closes the database
* `put/2`: inserts Key with value Val into the database. Assumes that the key is not present, 'key_exit' is returned otherwise.
* `get/1`: retrieves the value stored with Key in the database.
* `del/1`: Removes the key-value with key Key from database.
* `update/2` or `upd/2`: inserts Key with value Val into the database if the key is not present, otherwise updates Key to value Val.
* `drop/1`: deletes all key-value pairs in the database.


Usage
-----

```
$ make
$ ./start.sh
	%% create a new database
	1> {ok, Handle} = emdb:open("/tmp/emdb1").

	%% insert the key <<"a">> with value <<"1">>
	2> ok = emdb:put(Handle, <<"a">>, <<"1">>).

	%% try to re-insert the same key <<"a">>
	3> key_exist = emdb:put(Handle, <<"a">>, <<"2">>).

	%% add a new key-value pair
	4> ok = emdb:put(Handle, <<"b">>, <<"2">>).

	%% search a non-existing key <<"c">>
	5> none = emdb:get(Handle, <<"c">>).

	%% retrieve the value for key <<"b">>
	6> {ok, <<"2">>} = emdb:get(Handle, <<"b">>).

	%% retrieve the value for key <<"a">>
	7> {ok, <<"1">>} = emdb:get(Handle, <<"a">>).

	%% delete key <<"b">>
	8> ok = emdb:del(Handle, <<"b">>).

	%% search a non-existing key <<"b">>
	9> none = emdb:get(Handle, <<"b">>).

	%% delete a non-existing key <<"z">>
	10> none = emdb:del(Handle, <<"z">>).

	%% ensure key <<"a">>'s value is still <<"1">>
	11> {ok, <<"1">>} = emdb:get(Handle, <<"a">>).

	%% update the value for key <<"a">>
	12> ok = emdb:update(Handle, <<"a">>, <<"7">>).

	%% check the new value for key <<"a">>
	13> {ok, <<"7">>} = emdb:get(Handle, <<"a">>).

	%% delete all key-value pairs in the database
	14> ok = emdb:drop(Handle).

	%% try to retrieve key <<"a">> value
	15> none = emdb:get(Handle, <<"a">>).

	%% close the database
	16> ok = emdb:close(Handle).

	...

	17> q().
```

#### Note:
The code below creates a new database with **80GB** MapSize, **avoids fsync** after each commit (for an "ACI" but not "D" database we trade durability for speed) and uses the experimental **MDB_FIXEDMAP**.

```
	{ok, Handle} = emdb:open("/tmp/emdb2", 85899345920, ?MDB_NOSYNC bor ?MDB_FIXEDMAP).
```

Performance
-----------

See the [microbench](http://highlandsun.com/hyc/mdb/microbench/) against:
* Google's LevelDB (which is slower and can stall unlike Basho's fork of LevelDB)
* SQLite3
* Kyoto TreeDB
* BerkeleyDB 5.x

MDB performs mmap's the database, so unless your dataset is < 2^32 bytes you'll
need to run on a 64-bit arch system.


Supported Operating Systems
--------------

Should work on:

* Linux
* OSX
* FreeBSD
* Windows

TODO
----

* Fold over keys and/or values
* Unit tests
* PropEr testing
* Bulk "writing"
* basho_bench driver
* EQC, PULSE testing
* Key expirey
* renaming
  * emdb -> lmdb
  * emdb.c -> lmdb_nif.c
* improve stats
* txn API
* cursor API
* config
* use async_nif affinity
* riak_kv backend
  * use dups
  * 2i
* aae alternative

Status
------

Work in progress, not production quality and not supported by Basho Technologies.

LICENSE
-------

EMDB is Copyright (C) 2012-2013 by Aleph Archives and Basho Technologies, Inc., and released under the [OpenLDAP](http://www.OpenLDAP.org/license.html) License.
