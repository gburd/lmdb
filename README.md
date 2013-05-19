An Erlang NIF for LMDB
======================

This is an Erlang NIF for OpenLDAP's Lightning Memory-Mapped Database (LMDB) database library. LMDB is an ultra-fast, ultra-compact key-value data store developed by Symas for the OpenLDAP Project. It uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases, and is only limited to the size of the virtual address space, (it is not limited to the size of physical RAM). LMDB was originally called MDB, but was renamed to avoid confusion with other software associated with the name MDB

Quick Overview
--------------

LMDB is a tiny database with some excellent properties:
 * Ordered-map interface (keys are always sorted, supports range lookups)
 * Reader/writer transactions: readers don't block writers and writers don't block readers. Writers are fully serialized, so writes are always deadlock-free.
 * Read transactions are extremely cheap, and can be performed using no mallocs or any other blocking calls.
 * Supports multi-thread and multi-process concurrency, environments may be opened by multiple processes on the same host.
 * Multiple sub-databases may be created with transactions covering all sub-databases.
 * Memory-mapped, allowing for zero-copy lookup and iteration.
 * Maintenance-free, no external process or background cleanup/compaction required.
 * No application-level caching. LMDB fully exploits the operating system's buffer cache.
 * 32KB of object code and 6KLOC of C.

The main purpose of this integration is to provide Erlang programmers access to this excellent, and open source friendly, BTREE implementation.

Requirements
------------
* Erlang R14B04+
* Clang, GCC 4.2+ or MS VisualStudio 2010+

Build
-----

$ make


API
---

* `open/1`: equivalent to `lmdb:open(DirName, 10485760)`.
* `open/2`: equivalent to `lmdb:open(DirName, 10485760, 0)`.
* `open/3`: creates a new MDB database. This call also re-open an already existing one. Arguments are:
	* DirName: database directory name
	* MapSize: database map size (see [map.hrl](http://gitorious.org/mdb/mdb/blobs/master/libraries/libmdb/mdb.h))
	* EnvFlags: database environment flags (see [map.hrl](http://gitorious.org/mdb/mdb/blobs/master/libraries/libmdb/mdb.h)). The possible values are defined in **lmdb.hrl**.
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
1> {ok, Handle} = lmdb:open("/tmp/lmdb1").

%% insert the key <<"a">> with value <<"1">>
2> ok = lmdb:put(Handle, <<"a">>, <<"1">>).

%% try to re-insert the same key <<"a">>
3> key_exist = lmdb:put(Handle, <<"a">>, <<"2">>).

%% add a new key-value pair
4> ok = lmdb:put(Handle, <<"b">>, <<"2">>).

%% search a non-existing key <<"c">>
5> none = lmdb:get(Handle, <<"c">>).

%% retrieve the value for key <<"b">>
6> {ok, <<"2">>} = lmdb:get(Handle, <<"b">>).

%% retrieve the value for key <<"a">>
7> {ok, <<"1">>} = lmdb:get(Handle, <<"a">>).

%% delete key <<"b">>
8> ok = lmdb:del(Handle, <<"b">>).

%% search a non-existing key <<"b">>
9> none = lmdb:get(Handle, <<"b">>).

%% delete a non-existing key <<"z">>
10> none = lmdb:del(Handle, <<"z">>).

%% ensure key <<"a">>'s value is still <<"1">>
11> {ok, <<"1">>} = lmdb:get(Handle, <<"a">>).

%% update the value for key <<"a">>
12> ok = lmdb:update(Handle, <<"a">>, <<"7">>).

%% check the new value for key <<"a">>
13> {ok, <<"7">>} = lmdb:get(Handle, <<"a">>).

%% delete all key-value pairs in the database
14> ok = lmdb:drop(Handle).

%% try to retrieve key <<"a">> value
15> none = lmdb:get(Handle, <<"a">>).

%% close the database
16> ok = lmdb:close(Handle).
```

#### Note:
The code below creates a new database with **80GB** MapSize, **avoids fsync** after each commit (for an "ACI" but not "D" database we trade durability for speed) and uses the experimental **MDB_FIXEDMAP**.

```
{ok, Handle} = lmdb:open("/tmp/lmdb2", 85899345920, ?MDB_NOSYNC bor ?MDB_FIXEDMAP).
```

Performance
-----------

See the [microbench](http://highlandsun.com/hyc/mdb/microbench/) against:
* Google's LevelDB (which is slower and can stall unlike Basho's fork of LevelDB)
* SQLite3
* Kyoto TreeDB
* BerkeleyDB 5.x
* btree2n.c (?)
* WiredTiger (?)

MDB will mmap the entire database into memory which means that if your dataset is larger than 2^32 bytes you'll need to run on a 64-bit arch system.


Supported Operating Systems
--------------

Should work on:

* Linux
* Mac OS/X
* *BSD
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
* improve stats
* txn API
* cursor API
* config
* use async_nif affinity

Other Ideas
-----------

* Create a Riak/KV [backend](http://wiki.basho.com/Storage-Backends.html)
  * use dups for siblings
  * use txn for 2i
* Create a Riak/KV AAE (riak_kv/src/hashtree.erl) alternative using LMDB

Status
------

Work in progress, not production quality and not supported by Basho Technologies.  This is an experiment at this time, nothing more.  You are encouraged to contribute code, tests, etc. as you see fit.

LICENSE
-------

LMDB is Copyright (C) 2012 by Aleph Archives and (C) 2013 by Basho Technologies, Inc., and released under the [OpenLDAP](http://www.OpenLDAP.org/license.html) License.
