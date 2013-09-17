/* -------------------------------------------------------------------------
 * This file is part of LMDB - Erlang Lightning MDB API
 *
 * Copyright (c) 2012 by Aleph Archives. All rights reserved.
%% Copyright (c) 2013 by Basho Technologies, Inc. All rights reserved.
 *
 * -------------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * -------------------------------------------------------------------------*/

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/param.h>
#include <erl_nif.h>
#include <erl_driver.h>

#include "common.h"
#include "async_nif.h"
#include "queue.h"
#include "lmdb.h"


static ErlNifResourceType *lmdb_RESOURCE;
struct lmdb {
    MDB_env *env;
    MDB_dbi dbi;
    STAILQ_ENTRY(lmdb) entries;
};

static ErlNifResourceType *lmdb_txn_RESOURCE;
struct lmdb_txn {
    MDB_txn *txn;
};

static ErlNifResourceType *lmdb_cursor_RESOURCE;
struct lmdb_cursor {
    MDB_cursor *cursor;
};

struct lmdb_priv_data {
    void *async_nif_priv; // Note: must be first element in struct
    STAILQ_HEAD(envs, lmdb) envs;
    ErlNifMutex *envs_mutex;
};

/* Global init for async_nif. */
ASYNC_NIF_INIT(lmdb);

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_BAD_RSLOT;
static ERL_NIF_TERM ATOM_BRANCH_PAGES;
static ERL_NIF_TERM ATOM_CORRUPTED;
static ERL_NIF_TERM ATOM_CURSOR_FULL;
static ERL_NIF_TERM ATOM_DBS_FULL;
static ERL_NIF_TERM ATOM_DEPTH;
static ERL_NIF_TERM ATOM_ENTRIES;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_EXISTS;
static ERL_NIF_TERM ATOM_INCOMPATIBLE;
static ERL_NIF_TERM ATOM_KEYEXIST;
static ERL_NIF_TERM ATOM_KEYEXIST;
static ERL_NIF_TERM ATOM_LEAF_PAGES;
static ERL_NIF_TERM ATOM_MAP_FULL;
static ERL_NIF_TERM ATOM_MAP_RESIZED;
static ERL_NIF_TERM ATOM_NOTFOUND;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_OVERFLOW_PAGES;
static ERL_NIF_TERM ATOM_PAGE_FULL;
static ERL_NIF_TERM ATOM_PAGE_NOTFOUND;
static ERL_NIF_TERM ATOM_PANIC;
static ERL_NIF_TERM ATOM_PSIZE;
static ERL_NIF_TERM ATOM_READERS_FULL;
static ERL_NIF_TERM ATOM_TLS_FULL;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_TXN_FULL;
static ERL_NIF_TERM ATOM_VERSION_MISMATCH;

#define CHECK(expr, label)						\
    if (MDB_SUCCESS != (ret = (expr))) {				\
	DPRINTF("CHECK(\"%s\") failed \"%s\" at %s:%d in %s()\n",	\
	        #expr, mdb_strerror(ret), __FILE__, __LINE__, __func__);\
	err = __strerror_term(env, ret);				\
	goto label;							\
    }

#define FAIL_ERR(e, label)				\
    do {						\
	err = __strerror_term(env, (e));		\
	goto label;					\
    } while(0)

/**
 * Convenience function to generate {error, {errno, Reason}}
 *
 * env    NIF environment
 * err    number of last error
 */
static ERL_NIF_TERM
__strerror_term(ErlNifEnv* env, int err)
{
    ERL_NIF_TERM term;

    if (err < MDB_LAST_ERRCODE && err > MDB_KEYEXIST) {
	switch (err) {
	case MDB_KEYEXIST: /** key/data pair already exists */
	    term = ATOM_KEYEXIST;
	    break;
	case MDB_NOTFOUND: /** key/data pair not found (EOF) */
	    term = ATOM_NOTFOUND;
	    break;
	case MDB_PAGE_NOTFOUND: /** Requested page not found - this usually indicates corruption */
	    term = ATOM_PAGE_NOTFOUND;
	    break;
	case MDB_CORRUPTED: /** Located page was wrong type */
	    term = ATOM_CORRUPTED;
	    break;
	case MDB_PANIC	: /** Update of meta page failed, probably I/O error */
	    term = ATOM_PANIC;
	    break;
	case MDB_VERSION_MISMATCH: /** Environment version mismatch */
	    term = ATOM_VERSION_MISMATCH;
	    break;
	case MDB_INVALID: /** File is not a valid MDB file */
	    term = ATOM_KEYEXIST;
	    break;
	case MDB_MAP_FULL: /** Environment mapsize reached */
	    term = ATOM_MAP_FULL;
	    break;
	case MDB_DBS_FULL: /** Environment maxdbs reached */
	    term = ATOM_DBS_FULL;
	    break;
	case MDB_READERS_FULL: /** Environment maxreaders reached */
	    term = ATOM_READERS_FULL;
	    break;
	case MDB_TLS_FULL: /** Too many TLS keys in use - Windows only */
	    term = ATOM_TLS_FULL;
	    break;
	case MDB_TXN_FULL: /** Txn has too many dirty pages */
	    term = ATOM_TXN_FULL;
	    break;
	case MDB_CURSOR_FULL: /** Cursor stack too deep - internal error */
	    term = ATOM_CURSOR_FULL;
	    break;
	case MDB_PAGE_FULL: /** Page has not enough space - internal error */
	    term = ATOM_PAGE_FULL;
	    break;
	case MDB_MAP_RESIZED: /** Database contents grew beyond environment mapsize */
	    term = ATOM_MAP_RESIZED;
	    break;
	case MDB_INCOMPATIBLE: /** Database flags changed or would change */
	    term = ATOM_INCOMPATIBLE;
	    break;
	case MDB_BAD_RSLOT: /** Invalid reuse of reader locktable slot */
	    term = ATOM_BAD_RSLOT;
	    break;
	}
    } else {
	term = enif_make_atom(env, erl_errno_id(err));
    }

    /* We return the errno value as well as the message here because the error
       message provided by strerror() for differ across platforms and/or may be
       localized to any given language (i18n).  Use the errno atom rather than
       the message when matching in Erlang.  You've been warned. */
    return enif_make_tuple(env, 2, ATOM_ERROR,
		enif_make_tuple(env, 2, term,
		     enif_make_string(env, mdb_strerror(err), ERL_NIF_LATIN1)));
}

/**
 * Opens an MDB environment
 *
 * argv[0]    path to directory for the database files
 * argv[1]    flags
 * argv[2]    file mode
 * argv[3]    mapsize
 * argv[4]    maxreaders
 * argv[5]    maxdbs
 */
ASYNC_NIF_DECL(
  lmdb_env_open,
  { // struct

      char dirname[MAXPATHLEN];
      unsigned int flags;
      mdb_mode_t mode;
      size_t mapsize;
      unsigned int maxreaders;
      MDB_dbi maxdbs;
      struct lmdb_priv_data *priv;
  },
  { // pre

      if (!(argc == 6 &&
	    enif_is_list(env, argv[0]) &&
	    enif_is_number(env, argv[1]) &&
	    enif_is_number(env, argv[2]) &&
	    enif_is_number(env, argv[3]) &&
	    enif_is_number(env, argv[4]) &&
	    enif_is_number(env, argv[5]))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (enif_get_string(env, argv[0], args->dirname, MAXPATHLEN, ERL_NIF_LATIN1) <= 0)
	  ASYNC_NIF_RETURN_BADARG();
      enif_get_uint(env, argv[1], &(args->flags));
      enif_get_uint(env, argv[2], &(args->mode));
#if (__SIZEOF_SIZE_T__ == 8)
      enif_get_uint64(env, argv[3], &(args->mapsize));
#elif (__SIZEOF_SIZE_T__ == 4)
      enif_get_int(env, argv[3], &(args->mapsize));
#endif
      enif_get_uint(env, argv[4], &(args->maxreaders));
      enif_get_uint(env, argv[5], &(args->maxdbs));
      args->priv = (struct lmdb_priv_data *)enif_priv_data(env);
  },
  { // work

      int ret;
      ERL_NIF_TERM err;
      struct lmdb *handle;

      if ((handle = enif_alloc_resource(lmdb_RESOURCE, sizeof(struct lmdb))) == NULL)
	  FAIL_ERR(ENOMEM, err2);

      CHECK(mdb_env_create(&(handle->env)), err1);

      if (mdb_env_set_mapsize(handle->env, args->mapsize)) {
	  err = enif_make_badarg(env);
          goto err1;
      }

      if (mdb_env_set_maxreaders(handle->env, args->maxreaders)) {
	  err = enif_make_badarg(env);
          goto err1;
      }

      if (mdb_env_set_maxdbs(handle->env, args->maxdbs)) {
	  err = enif_make_badarg(env);
          goto err1;
      }

      enif_mutex_lock(args->priv->envs_mutex);
      STAILQ_INSERT_TAIL(&args->priv->envs, handle, entries);
      enif_mutex_unlock(args->priv->envs_mutex);

      ERL_NIF_TERM term = enif_make_resource(env, handle);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_OK, term));
      enif_release_resource(handle);
      return;

  err1:
      mdb_env_close(handle->env);
  err2:
      enif_release_resource(handle);
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

  });

/**
 * Copy an MDB environment to the specified path.
 *
 * argv[0]    an environment handle
 * argv[1]    destination path
 */
ASYNC_NIF_DECL(
  lmdb_copy,
  { // struct

      struct lmdb *handle;
      char dirname[MAXPATHLEN];
  },
  { // pre

      if (!(argc == 2 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_list(env, argv[1]))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (enif_get_string(env, argv[1], args->dirname, MAXPATHLEN,
			  ERL_NIF_LATIN1) <= 0)
	  ASYNC_NIF_RETURN_BADARG();
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      int ret;

      CHECK(mdb_env_copy(args->handle->env, args->dirname), err);
      ASYNC_NIF_REPLY(ATOM_OK);
      return;

  err:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Return statistics about the MDB environment.
 *
 * argv[0]    an environment handle
 */
ASYNC_NIF_DECL(
  lmdb_stat,
  { // struct

      struct lmdb *handle;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      ERL_NIF_TERM term;
      MDB_stat stats;
      int ret;

      CHECK(mdb_env_stat(args->handle->env, &stats), err1);

      term = enif_make_tuple(env, 6,
			     enif_make_tuple(env, 2, ATOM_PSIZE, enif_make_uint(env, stats.ms_psize)),
			     enif_make_tuple(env, 2, ATOM_DEPTH, enif_make_uint(env, stats.ms_depth)),
			     enif_make_tuple(env, 2, ATOM_BRANCH_PAGES, enif_make_uint(env, stats.ms_branch_pages)),
			     enif_make_tuple(env, 2, ATOM_LEAF_PAGES, enif_make_uint(env, stats.ms_leaf_pages)),
			     enif_make_tuple(env, 2, ATOM_OVERFLOW_PAGES, enif_make_uint(env, stats.ms_overflow_pages)),
			     enif_make_tuple(env, 2, ATOM_ENTRIES, enif_make_uint(env, stats.ms_entries)));

      ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_OK, term));
      return;

  err1:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Flush the data buffers to disk.
 *
 * argv[0]    an environment handle
 * argv[1]    if true, force a synchronous flush.  Otherwise if the environment
 *            has the ?MDB_NOSYNC flag set the flushes will be omitted, and
 *            with ?MDB_MAPASYNC they will be asynchronous.
 */
ASYNC_NIF_DECL(
  lmdb_sync,
  { // struct

      struct lmdb *handle;
      int force;
  },
  { // pre

      if (!(argc == 2 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_atom(env, argv[1]))) {
	  ASYNC_NIF_RETURN_BADARG();
      }

      args->force = enif_is_identical(ATOM_TRUE, argv[1]) ? 1 : 0;
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      int ret;

      CHECK(mdb_env_sync(args->handle->env, args->force), err1);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 1, ATOM_OK));
      return;

  err1:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Close the environment and release the memory map.
 *
 * argv[0]    an environment handle
 */
ASYNC_NIF_DECL(
  lmdb_env_close,
  { // struct

      struct lmdb *handle;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

   /* Only a single thread may call mdb_env_close() function. All transactions,
      databases, and cursors must already be closed before calling this
      function. Attempts to use any such handles after calling this function
      will cause a SIGSEGV.  The environment handle will be freed and must not
      be used again after this call. */

      /* TODO(gburd): close eveything first */
      mdb_env_close(args->handle->env);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 1, ATOM_OK));
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Return the path that was used in lmdb:env_open().
 *
 * argv[0]    an environment handle
 */
ASYNC_NIF_DECL(
  lmdb_path,
  { // struct

      struct lmdb *handle;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      int ret;
      const char *path;

      CHECK(mdb_env_get_path(args->handle->env, &path), err1);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_OK,
				      enif_make_string(env, path, ERL_NIF_LATIN1)));
      return;

  err1:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Set the maximum number of named databases for the environment.
 *
 * This function is only needed if multiple databases will be used in the
 * environment. Simpler applications that use the environment as a single
 * unnamed database can ignore this option.  This function may only be called
 * after lmdb:env_create() and before lmdb:env_open().
 *
 * argv[0]    an environment handle
 * argv[1]    the maximum number of databases
 */
ASYNC_NIF_DECL(
  lmdb_set_maxdbs,
  { // struct

      struct lmdb *handle;
      unsigned int dbs;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_number(env, argv[1]))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_get_uint(env, argv[1], &(args->dbs));
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      int ret;

      CHECK(mdb_env_set_maxdbs(args->handle->env, args->dbs), err1);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 1, ATOM_OK));
      return;

  err1:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Create a transaction for use with the environment.
 *
 * The transaction handle may be discarded using lmdb:txn_abort() or
 * lmdb:txn_commit().
 *

 * NOTE: A transaction and its cursors must only be used by a single
 * thread, and a thread may only have a single transaction at a time.
	 * If #MDB_NOTLS is in use, this does not apply to read-only transactions.
	 * @note Cursors may not span transactions.
	 * @param[in] env An environment handle returned by #mdb_env_create()
	 * @param[in] parent If this parameter is non-NULL, the new transaction
	 * will be a nested transaction, with the transaction indicated by \b parent
	 * as its parent. Transactions may be nested to any level. A parent
	 * transaction may not issue any other operations besides mdb_txn_begin,
	 * mdb_txn_abort, or mdb_txn_commit while it has active child transactions.
	 * @param[in] flags Special options for this transaction. This parameter
	 * must be set to 0 or by bitwise OR'ing together one or more of the
	 * values described here.
	 * <ul>
	 *	<li>#MDB_RDONLY
	 *		This transaction will not perform any write operations.
	 * </ul>
	 * @param[out] txn Address where the new #MDB_txn handle will be stored
	 * @return A non-zero error value on failure and 0 on success. Some possible
	 * errors are:
	 * <ul>
	 *	<li>#MDB_PANIC - a fatal error occurred earlier and the environment
	 *		must be shut down.
	 *	<li>#MDB_MAP_RESIZED - another process wrote data beyond this MDB_env's
	 *		mapsize and the environment must be shut down.
	 *	<li>#MDB_READERS_FULL - a read-only transaction was requested and
	 *		the reader lock table is full. See #mdb_env_set_maxreaders().
	 *	<li>ENOMEM - out of memory.
	 * </ul>
 *
 * argv[0]    ??
 */
ASYNC_NIF_DECL(
  lmdb_??,
  { // struct

      struct lmdb *handle;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      int ret;

      CHECK(??, err1);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_OK, term));
      return;

  err1:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

      enif_release_resource((void*)args->handle);
  });

/**
 * Opens a MDB database.
 *
 * Note: mdb_dbi_open() must not be called from multiple concurrent
 * transactions. A transaction that uses this function must finish (either
 * commit or abort) before any other transaction may use this function.
 *
 * argv[0]    path to directory for the database files
 * argv[1]    size of database
 * argv[2]    flags
 */
ASYNC_NIF_DECL(
  lmdb_open,
  { // struct

      char dirname[MAXPATHLEN];
      ErlNifUInt64 mapsize;
      ErlNifUInt64 envflags;
  },
  { // pre
      if (!(argc == 3 &&
	    enif_is_list(env, argv[0]) &&
	    enif_is_number(env, argv[1]) &&
	    enif_is_number(env, argv[2]))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (enif_get_string(env, argv[0], args->dirname,
			  MAXPATHLEN, ERL_NIF_LATIN1) <= 0)
	  ASYNC_NIF_RETURN_BADARG();
      enif_get_uint64(env, argv[1], &(args->mapsize));
      enif_get_uint64(env, argv[2], &(args->envflags));
  },
  { // work

      ERL_NIF_TERM err;
      MDB_txn *txn;
      struct lmdb *handle;
      int ret;

      if ((handle = enif_alloc_resource(lmdb_RESOURCE, sizeof(struct lmdb))) == NULL)
	  FAIL_ERR(ENOMEM, err3);
      enif_release_resource(handle);

      CHECK(mdb_env_create(&(handle->env)), err2);

      if (mdb_env_set_mapsize(handle->env, args->mapsize)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }

      CHECK(mdb_env_open(handle->env, args->dirname, args->envflags, 0664), err2);
      CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err2);
      CHECK(mdb_open(txn, NULL, 0, &(handle->dbi)), err1);
      CHECK(mdb_txn_commit(txn), err1);

      ERL_NIF_TERM term = enif_make_resource(env, handle);
      ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_OK, term));
      return;

  err1:
      mdb_txn_abort(txn);
  err2:
      mdb_env_close(handle->env);
  err3:
      enif_release_resource(handle);
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

  });


/**
 * Closes a MDB database.
 *
 * The old database handle is returned if the database was already open.
 * The handle must only be closed once.
 * This call is not mutex protected. Handles should only be closed by
 * a single thread, and only if no other threads are going to reference
 * the database handle or one of its cursors any further. Do not close
 * a handle if an existing transaction has modified its database.
 *
 * argv[0]    reference to the MDB handle resource
 */
ASYNC_NIF_DECL(
  lmdb_close,
  { // struct

      struct lmdb *handle;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      mdb_env_close(args->handle->env);
      args->handle->env = NULL;
      ASYNC_NIF_REPLY(ATOM_OK);
      return;
  },
  { // post

    enif_release_resource((void*)args->handle);
  });


/**
 * Store a value indexed by key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    value as an Erlang binary
 */
ASYNC_NIF_DECL(
  lmdb_put,
  { // struct

      struct lmdb *handle;
      ERL_NIF_TERM key;
      ERL_NIF_TERM val;
  },
  { // pre

      if (!(argc == 3 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_binary(env, argv[1]) &&
	    enif_is_binary(env, argv[2]) )) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
      args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
      args->val = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
  },
  { // work

      ERL_NIF_TERM err;
      ErlNifBinary key;
      ErlNifBinary val;
      MDB_val mkey;
      MDB_val mdata;
      MDB_txn * txn;
      int ret;

      if (!enif_inspect_iolist_as_binary(env, args->key, &key)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }
      if (!enif_inspect_iolist_as_binary(env, args->val, &val)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }

      mkey.mv_size  = key.size;
      mkey.mv_data  = key.data;
      mdata.mv_size = val.size;
      mdata.mv_data = val.data;
      CHECK(mdb_txn_begin(args->handle->env, NULL, 0, & txn), err2);

      ret = mdb_put(txn, args->handle->dbi, &mkey, &mdata, MDB_NOOVERWRITE);
      if (MDB_KEYEXIST == ret) {
	  ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_ERROR, ATOM_EXISTS));
	  return;
      }
      if (ret != 0)
	  FAIL_ERR(ret, err1);

      CHECK(mdb_txn_commit(txn), err1);
      ASYNC_NIF_REPLY(ATOM_OK);
      return;

  err1:
      mdb_txn_abort(txn);
  err2:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

    enif_release_resource((void*)args->handle);
  });


/**
 * Update and existin value indexed by key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    value as an Erlang binary
 */
ASYNC_NIF_DECL(
  lmdb_update,
  { // struct

      struct lmdb *handle;
      ERL_NIF_TERM key;
      ERL_NIF_TERM val;
  },
  { // pre

      if (!(argc == 3 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_binary(env, argv[1]) &&
	    enif_is_binary(env, argv[2]) )) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
      args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
      args->val = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
  },
  { // work

      ERL_NIF_TERM err;
      ErlNifBinary key;
      ErlNifBinary val;
      MDB_val mkey;
      MDB_val mdata;
      MDB_txn * txn;
      int ret;

      if (!enif_inspect_iolist_as_binary(env, args->key, &key)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }
      if (!enif_inspect_iolist_as_binary(env, args->val, &val)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }

      mkey.mv_size  = key.size;
      mkey.mv_data  = key.data;
      mdata.mv_size = val.size;
      mdata.mv_data = val.data;

      CHECK(mdb_txn_begin(args->handle->env, NULL, 0, & txn), err2);
      CHECK(mdb_put(txn, args->handle->dbi, &mkey, &mdata, 0), err1);
      CHECK(mdb_txn_commit(txn), err1);
      ASYNC_NIF_REPLY(ATOM_OK);
      return;

  err1:
      mdb_txn_abort(txn);
  err2:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

    enif_release_resource((void*)args->handle);
  });


/**
 * Retrieve the value associated with the key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 */
ASYNC_NIF_DECL(
  lmdb_get,
  { // struct

      struct lmdb *handle;
      ERL_NIF_TERM key;
  },
  { // pre

      if (!(argc == 2 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_binary(env, argv[1]) )) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
      args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
  },
  { // work

      ERL_NIF_TERM err;
      ErlNifBinary key;
      ERL_NIF_TERM val;
      unsigned char *bin;
      MDB_val mkey;
      MDB_val mdata;
      MDB_txn * txn;
      int ret;

      if (!enif_inspect_iolist_as_binary(env, args->key, &key)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }

      mkey.mv_size  = key.size;
      mkey.mv_data  = key.data;

      CHECK(mdb_txn_begin(args->handle->env, NULL, 0, &txn), err);

      ret = mdb_get(txn, args->handle->dbi, &mkey, &mdata);
      mdb_txn_abort(txn);
      if (MDB_NOTFOUND == ret) {
	  ASYNC_NIF_REPLY(ATOM_NOT_FOUND);
	  return;
      }

      if (ret != 0)
	  FAIL_ERR(ret, err);

      bin = enif_make_new_binary(env, mdata.mv_size, &val);
      if (!bin)
	  FAIL_ERR(ENOMEM, err);
      memcpy(bin, mdata.mv_data, mdata.mv_size);

      ASYNC_NIF_REPLY(enif_make_tuple(env, 2, ATOM_OK, val));
      return;

  err:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

    enif_release_resource((void*)args->handle);
  });


/**
 * Delete the value associated with the key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 */
ASYNC_NIF_DECL(
  lmdb_del,
  { // struct

      struct lmdb *handle;
      ERL_NIF_TERM key;
  },
  { // pre

      if (!(argc == 2 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle) &&
	    enif_is_binary(env, argv[1]) )) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
      args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
  },
  { // work

      ERL_NIF_TERM err;
      ErlNifBinary key;
      MDB_val mkey;
      MDB_txn * txn;
      int ret;

      if (!enif_inspect_iolist_as_binary(env, args->key, &key)) {
	  ASYNC_NIF_REPLY(enif_make_badarg(env));
	  return;
      }

      mkey.mv_size  = key.size;
      mkey.mv_data  = key.data;

      CHECK(mdb_txn_begin(args->handle->env, NULL, 0, & txn), err);
      ret = mdb_del(txn, args->handle->dbi, &mkey, NULL);

      if(MDB_NOTFOUND == ret) {
	  mdb_txn_abort(txn);
	  ASYNC_NIF_REPLY(ATOM_NOT_FOUND);
	  return;
      }

      CHECK(mdb_txn_commit(txn), err);
      ASYNC_NIF_REPLY(ATOM_OK);
      return;

  err:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

    enif_release_resource((void*)args->handle);
  });


/**
 * Drop a MDB database.
 *
 * argv[0]    reference to the MDB handle resource
 */
ASYNC_NIF_DECL(
  lmdb_drop,
  { // struct

      struct lmdb *handle;
  },
  { // pre

      if (!(argc == 1 &&
	    enif_get_resource(env, argv[0], lmdb_RESOURCE, (void**)&args->handle))) {
	  ASYNC_NIF_RETURN_BADARG();
      }
      if (!args->handle->env)
	  ASYNC_NIF_RETURN_BADARG();
      enif_keep_resource((void*)args->handle);
  },
  { // work

      ERL_NIF_TERM err;
      MDB_txn * txn;
      int ret;

      CHECK(mdb_txn_begin(args->handle->env, NULL, 0, & txn), err2);
      CHECK(mdb_drop(txn, args->handle->dbi, 0), err1);
      CHECK(mdb_txn_commit(txn), err1);
      ASYNC_NIF_REPLY(ATOM_OK);
      return;

  err1:
      mdb_txn_abort(txn);

  err2:
      ASYNC_NIF_REPLY(err);
      return;
  },
  { // post

    enif_release_resource((void*)args->handle);
  });



/**
 * Called as this driver is loaded by the Erlang BEAM runtime triggered by the
 * module's on_load directive.
 *
 * env        the NIF environment
 * priv_data  used to hold the state for this NIF rather than global variables
 * load_info  an Erlang term passed in with this call
 */
static int
lmdb_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    UNUSED(load_info);
    int err;
    ErlNifResourceFlags flags;
    struct lmdb_priv_data *priv;

    flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    priv = enif_alloc(sizeof(struct lmdb_priv_data));
    if (!priv)
	FAIL_ERR(ENOMEM, err1);
    memset(priv, 0, sizeof(struct lmdb_priv_data));
    priv->envs_mutex = enif_mutex_create(NULL);
    STAILQ_INIT(&priv->envs);

    /* Note: !!! the first element of our priv_data struct *must* be the
       pointer to the async_nif's private data which we set here. */
    ASYNC_NIF_LOAD(lmdb, env, priv->async_nif_priv);
    if (!priv)
	FAIL_ERR(ENOMEM, err2);
    *priv_data = priv;

    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
    ATOM_EXISTS = enif_make_atom(env, "exists");
    ATOM_TRUE = enif_make_atom(env, "true");

    ATOM_KEYEXIST = enif_make_atom(env, "key_exist");
    ATOM_NOTFOUND = enif_make_atom(env, "notfound");
    ATOM_CORRUPTED = enif_make_atom(env, "corrupted");
    ATOM_PANIC = enif_make_atom(env, "panic");
    ATOM_VERSION_MISMATCH = enif_make_atom(env, "version_mismatch");
    ATOM_MAP_FULL = enif_make_atom(env, "map_full");
    ATOM_DBS_FULL = enif_make_atom(env, "dbs_full");
    ATOM_READERS_FULL = enif_make_atom(env, "readers_full");
    ATOM_TLS_FULL = enif_make_atom(env, "tls_full");
    ATOM_TXN_FULL = enif_make_atom(env, "txn_full");
    ATOM_CURSOR_FULL = enif_make_atom(env, "cursor_full");
    ATOM_PAGE_FULL = enif_make_atom(env, "page_full");
    ATOM_MAP_RESIZED = enif_make_atom(env, "map_resized");
    ATOM_INCOMPATIBLE = enif_make_atom(env, "incompatible");
    ATOM_BAD_RSLOT = enif_make_atom(env, "bad_rslot");

    ATOM_PSIZE = enif_make_atom(env, "psize");
    ATOM_DEPTH = enif_make_atom(env, "depth");
    ATOM_BRANCH_PAGES = enif_make_atom(env, "branch_pages");
    ATOM_LEAF_PAGES = enif_make_atom(env, "leaf_pages");
    ATOM_OVERFLOW_PAGES = enif_make_atom(env, "overflow_pages");
    ATOM_ENTRIES = enif_make_atom(env, "entries");


    lmdb_RESOURCE = enif_open_resource_type(env, NULL, "lmdb_resource",
					    NULL, flags, NULL);
    fprintf(stderr, "NIF on_load complete (lmdb version: %s)", MDB_VERSION_STRING);
    fflush(stderr);
    return (0);

err2:
    enif_mutex_destroy(priv->envs_mutex);
    enif_free(priv);
err1:
    return (err);
}

/**
 * TODO:
 */
static int
lmdb_reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM info)
{
    UNUSED(env);
    UNUSED(priv_data);
    UNUSED(info);
    return (0); // TODO:
}


/**
 * TODO:
 */
static int
lmdb_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv, ERL_NIF_TERM load_info)
{
    UNUSED(env);
    UNUSED(priv_data);
    UNUSED(old_priv);
    UNUSED(load_info);
    ASYNC_NIF_UPGRADE(lmdb, env);
    return (0); // TODO:
}


/**
 * TODO:
 */
static void
lmdb_unload(ErlNifEnv* env, void* priv_data)
{
    struct lmdb_priv_data *priv = (struct lmdb_priv_data *)priv_data;
    struct lmdb *handle;

    enif_mutex_lock(priv->envs_mutex);
    STAILQ_FOREACH(handle, &priv->envs, entries) {
	mdb_env_close(handle->env);
	enif_free(handle);
    }
    ASYNC_NIF_UNLOAD(lmdb, env, priv->async_nif_priv);
    enif_mutex_unlock(priv->envs_mutex);
    enif_mutex_destroy(priv->envs_mutex);
    enif_free(priv);
    return;
}

static ErlNifFunc nif_funcs [] = {
    {"env_open_nif",      7, lmdb_env_open},     // [(Ref), Path, Bitmask, Mode]
    {"copy_nif",          3, lmdb_copy},         // [(Ref), Env, Path]
    {"stat_nif",          2, lmdb_stat},         // [(Ref), Env]
    {"sync_nif",          3, lmdb_sync},         // [(Ref), Env, Force]
    {"env_close_nif",     2, lmdb_env_close},    // [(Ref), Env]
    {"path_nif",          2, lmdb_path},         // [(Ref), Env]
    {"set_maxdbs_nif",    3, lmdb_set_maxdbs},   // [(Ref), Env, Dbs]
    {"txn_begin_nif",     4, lmdb_txn_begin},    // [(Ref), Env, Parent, Bitmask]);
    {"txn_commit_nif",    2, lmdb_txn_commit},   // [(Ref), Txn]
    {"txn_abort_nif",     2, lmdb_txn_abort},    // [(Ref), Txn]
    {"txn_reset_nif",     2, lmdb_txn_reset},    // [(Ref), Txn]
    {"txn_renew_nif",     2, lmdb_txn_renew},    // [(Ref), Txn]
    {"dbi_open_nif",      5, lmdb_dbi_open},     // [(Ref), Txn, Path, Size, Bitmask]
    {"dbi_close_nif",     2, lmdb_dbi_close},    // [(Ref), Env, Dbi]
    {"drop_nif",          4, lmdb_drop},         // [(Ref), Txn, Dbi, Delete]
    {"get_nif",           3, lmdb_get},          // [(Ref), Txn, Dbi, Key]
    {"put_nif",           4, lmdb_put},          // [(Ref), Txn, Dbi, Key, Value, Bitmask]
    {"del_nif",           5, lmdb_del},          // [(Ref), Txn, Dbi, Key, Value]
    {"cursor_open_nif",   3, lmdb_cursor_open},  // [(Ref), Txn, Dbi]
    {"cursor_close_nif",  2, lmdb_cursor_close}, // [(Ref), Cursor]
    {"cursor_renew_nif",  3, lmdb_cursor_renew}, // [(Ref), Txn, Cursor]
    {"cursor_txn_nif",    2, lmdb_cursor_txn},   // [(Ref), Cursor]
    {"cursor_dbi_nif",    2, lmdb_cursor_dbi},   // [(Ref), Cursor]
    {"cursor_get_nif",    5, lmdb_cursor_get},   // [(Ref), Cursor, Key, DupValue, CursorOp]
    {"cursor_put_nif",    5, lmdb_cursor_put},   // [(Ref), Cursor, Key, Value, Options]
    {"cursor_del_nif",    3, lmdb_cursor_del},   // [(Ref), Cursor, Bitmask]
    {"cursor_count_nif",  2, lmdb_cursor_count}, // [(Ref), Cursor]
    {"cmp_nif",           5, lmdb_cmp},          // [(Ref), Txn, Dbi, A, B]
    {"dup_cmp_nif",       5, lmdb_dup_cmp},      // [(Ref), Txn, Dbi, A, B]
    {"version_nif",       1, lmdb_version}       // [(Ref)]
};

/* driver entry point */
ERL_NIF_INIT(lmdb,
             nif_funcs,
             & lmdb_load,
             & lmdb_reload,
             & lmdb_upgrade,
             & lmdb_unload)
