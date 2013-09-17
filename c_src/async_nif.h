/*
 * async_nif: An async thread-pool layer for Erlang's NIF API
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 * Author: Gregory Burd <greg@basho.com> <greg@burd.me>
 *
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#ifndef __ASYNC_NIF_H__
#define __ASYNC_NIF_H__

#if defined(__cplusplus)
extern "C" {
#endif

#include <assert.h>

#include "queue.h"

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#define ASYNC_NIF_MAX_WORKERS 1024
#define ASYNC_NIF_MIN_WORKERS 2
#define ASYNC_NIF_WORKER_QUEUE_SIZE 8192
#define ASYNC_NIF_MAX_QUEUED_REQS ASYNC_NIF_WORKER_QUEUE_SIZE * ASYNC_NIF_MAX_WORKERS

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_EAGAIN;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_ENQUEUED;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_SHUTDOWN;


struct async_nif_req_entry {
  ERL_NIF_TERM ref;
  ErlNifEnv *env;
  ErlNifPid pid;
  void *args;
  void (*fn_work)(ErlNifEnv*, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *);
  void (*fn_post)(void *);
  STAILQ_ENTRY(async_nif_req_entry) entries;
};


struct async_nif_work_queue {
  unsigned int num_workers;
  unsigned int depth;
  ErlNifMutex *reqs_mutex;
  ErlNifCond *reqs_cnd;
  struct async_nif_work_queue *next;
  STAILQ_HEAD(reqs, async_nif_req_entry) reqs;
};

struct async_nif_worker_entry {
  ErlNifTid tid;
  unsigned int worker_id;
  struct async_nif_state *async_nif;
  struct async_nif_work_queue *q;
  SLIST_ENTRY(async_nif_worker_entry) entries;
};

struct async_nif_state {
  unsigned int shutdown;
  ErlNifMutex *we_mutex;
  unsigned int we_active;
  SLIST_HEAD(joining, async_nif_worker_entry) we_joining;
  unsigned int num_queues;
  unsigned int next_q;
  STAILQ_HEAD(recycled_reqs, async_nif_req_entry) recycled_reqs;
  unsigned int num_reqs;
  ErlNifMutex *recycled_req_mutex;
  struct async_nif_work_queue queues[];
};

#define ASYNC_NIF_DECL(decl, frame, pre_block, work_block, post_block)  \
  struct decl ## _args frame;                                           \
  static void fn_work_ ## decl (ErlNifEnv *env, ERL_NIF_TERM ref, ErlNifPid *pid, unsigned int worker_id, struct decl ## _args *args) { \
  UNUSED(worker_id);                                                    \
  DPRINTF("async_nif: calling \"%s\"", __func__);                       \
  do work_block while(0);                                               \
  DPRINTF("async_nif: returned from \"%s\"", __func__);                 \
  }                                                                     \
  static void fn_post_ ## decl (struct decl ## _args *args) {           \
    UNUSED(args);                                                       \
    DPRINTF("async_nif: calling \"fn_post_%s\"", #decl);                \
    do post_block while(0);                                             \
    DPRINTF("async_nif: returned from \"fn_post_%s\"", #decl);          \
  }                                                                     \
  static ERL_NIF_TERM decl(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv_in[]) { \
    struct decl ## _args on_stack_args;                                 \
    struct decl ## _args *args = &on_stack_args;                        \
    struct decl ## _args *copy_of_args;                                 \
    struct async_nif_req_entry *req = NULL;                             \
    unsigned int affinity = 0;                                          \
    ErlNifEnv *new_env = NULL;                                          \
    /* argv[0] is a ref used for selective recv */                      \
    const ERL_NIF_TERM *argv = argv_in + 1;                             \
    argc -= 1;                                                          \
    /* Note: !!! this assumes that the first element of priv_data is ours */ \
    struct async_nif_state *async_nif = *(struct async_nif_state**)enif_priv_data(env); \
    if (async_nif->shutdown)						\
	return enif_make_tuple2(env, ATOM_ERROR, ATOM_SHUTDOWN);	\
    req = async_nif_reuse_req(async_nif);                               \
    if (!req)								\
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_ENOMEM);		\
    new_env = req->env;                                                 \
    DPRINTF("async_nif: calling \"%s\"", __func__);                     \
    do pre_block while(0);                                              \
    DPRINTF("async_nif: returned from \"%s\"", __func__);               \
    copy_of_args = (struct decl ## _args *)malloc(sizeof(struct decl ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_ ## decl (args);                                          \
      async_nif_recycle_req(req, async_nif);                            \
      return enif_make_tuple2(env, ATOM_ERROR, ATOM_ENOMEM);		\
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct decl ## _args));           \
    req->ref = enif_make_copy(new_env, argv_in[0]);                     \
    enif_self(env, &req->pid);                                          \
    req->args = (void*)copy_of_args;                                    \
    req->fn_work = (void (*)(ErlNifEnv *, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *))fn_work_ ## decl ; \
    req->fn_post = (void (*)(void *))fn_post_ ## decl;                 \
    int h = -1;                                                        \
    if (affinity)                                                      \
        h = ((unsigned int)affinity) % async_nif->num_queues;          \
    ERL_NIF_TERM reply = async_nif_enqueue_req(async_nif, req, h);     \
    if (!reply) {                                                      \
      fn_post_ ## decl (args);                                         \
      async_nif_recycle_req(req, async_nif);                           \
      free(copy_of_args);					       \
      return enif_make_tuple2(env, ATOM_ERROR, ATOM_EAGAIN);	       \
    }                                                                  \
    return reply;                                                      \
  }

#define ASYNC_NIF_INIT(name)                                            \
        static ErlNifMutex *name##_async_nif_coord = NULL;

#define ASYNC_NIF_LOAD(name, env, priv) do {				\
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create("nif_coord load"); \
        enif_mutex_lock(name##_async_nif_coord);                        \
        priv = async_nif_load(env);					\
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);
#define ASYNC_NIF_UNLOAD(name, env, priv) do {                          \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create("nif_coord unload"); \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_unload(env, priv);                                    \
        enif_mutex_unlock(name##_async_nif_coord);                      \
        enif_mutex_destroy(name##_async_nif_coord);                     \
        name##_async_nif_coord = NULL;                                  \
    } while(0);
#define ASYNC_NIF_UPGRADE(name, env) do {                               \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create("nif_coord upgrade"); \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_upgrade(env);                                         \
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);

#define ASYNC_NIF_RETURN_BADARG() do {                                  \
        async_nif_recycle_req(req, async_nif);                          \
        return enif_make_badarg(env);                                   \
    } while(0);
#define ASYNC_NIF_WORK_ENV new_env

#define ASYNC_NIF_REPLY(msg) enif_send(NULL, pid, env, enif_make_tuple2(env, ref, msg))

/**
 * Return a request structure from the recycled req queue if one exists,
 * otherwise create one.
 */
struct async_nif_req_entry *
async_nif_reuse_req(struct async_nif_state *async_nif)
{
    struct async_nif_req_entry *req = NULL;
    ErlNifEnv *env = NULL;

    enif_mutex_lock(async_nif->recycled_req_mutex);
    if (STAILQ_EMPTY(&async_nif->recycled_reqs)) {
        if (async_nif->num_reqs < ASYNC_NIF_MAX_QUEUED_REQS) {
            req = malloc(sizeof(struct async_nif_req_entry));
            if (req) {
                memset(req, 0, sizeof(struct async_nif_req_entry));
                env = enif_alloc_env();
                if (env) {
                    req->env = env;
                    __sync_fetch_and_add(&async_nif->num_reqs, 1);
                } else {
                    free(req);
                    req = NULL;
                }
            }
        }
    } else {
        req = STAILQ_FIRST(&async_nif->recycled_reqs);
        STAILQ_REMOVE(&async_nif->recycled_reqs, req, async_nif_req_entry, entries);
    }
    enif_mutex_unlock(async_nif->recycled_req_mutex);
    return req;
}

/**
 * Store the request for future re-use.
 *
 * req         a request entry with an ErlNifEnv* which will be cleared
 *             before reuse, but not until then.
 * async_nif   a handle to our state so that we can find and use the mutex
 */
void
async_nif_recycle_req(struct async_nif_req_entry *req, struct async_nif_state *async_nif)
{
    ErlNifEnv *env = NULL;
    enif_mutex_lock(async_nif->recycled_req_mutex);
    enif_clear_env(req->env);
    env = req->env;
    memset(req, 0, sizeof(struct async_nif_req_entry));
    req->env = env;
    STAILQ_INSERT_TAIL(&async_nif->recycled_reqs, req, entries);
    enif_mutex_unlock(async_nif->recycled_req_mutex);
}

static void *async_nif_worker_fn(void *);

/**
 * Start up a worker thread.
 */
static int
async_nif_start_worker(struct async_nif_state *async_nif, struct async_nif_work_queue *q)
{
  struct async_nif_worker_entry *we;

  if (0 == q)
      return EINVAL;

  enif_mutex_lock(async_nif->we_mutex);

  we = SLIST_FIRST(&async_nif->we_joining);
  while(we != NULL) {
    struct async_nif_worker_entry *n = SLIST_NEXT(we, entries);
    SLIST_REMOVE(&async_nif->we_joining, we, async_nif_worker_entry, entries);
    void *exit_value = 0; /* We ignore the thread_join's exit value. */
    enif_thread_join(we->tid, &exit_value);
    free(we);
    async_nif->we_active--;
    we = n;
  }

  if (async_nif->we_active == ASYNC_NIF_MAX_WORKERS) {
      enif_mutex_unlock(async_nif->we_mutex);
      return EAGAIN;
  }

  we = malloc(sizeof(struct async_nif_worker_entry));
  if (!we) {
      enif_mutex_unlock(async_nif->we_mutex);
      return ENOMEM;
  }
  memset(we, 0, sizeof(struct async_nif_worker_entry));
  we->worker_id = async_nif->we_active++;
  we->async_nif = async_nif;
  we->q = q;

  enif_mutex_unlock(async_nif->we_mutex);
  return enif_thread_create(NULL,&we->tid, &async_nif_worker_fn, (void*)we, 0);
}

/**
 * Enqueue a request for processing by a worker thread.
 *
 * Places the request into a work queue determined either by the
 * provided affinity or by iterating through the available queues.
 */
static ERL_NIF_TERM
async_nif_enqueue_req(struct async_nif_state* async_nif, struct async_nif_req_entry *req, int hint)
{
  /* Identify the most appropriate worker for this request. */
  unsigned int i, last_qid, qid = 0;
  struct async_nif_work_queue *q = NULL;
  double avg_depth = 0.0;

  /* Either we're choosing a queue based on some affinity/hinted value or we
     need to select the next queue in the rotation and atomically update that
     global value (next_q is shared across worker threads) . */
  if (hint >= 0) {
      qid = (unsigned int)hint;
  } else {
      do {
          last_qid = __sync_fetch_and_add(&async_nif->next_q, 0);
          qid = (last_qid + 1) % async_nif->num_queues;
      } while (!__sync_bool_compare_and_swap(&async_nif->next_q, last_qid, qid));
  }

  /* Now we inspect and interate across the set of queues trying to select one
     that isn't too full or too slow. */
  for (i = 0; i < async_nif->num_queues; i++) {
      /* Compute the average queue depth not counting queues which are empty or
         the queue we're considering right now. */
      unsigned int j, n = 0;
      for (j = 0; j < async_nif->num_queues; j++) {
          if (j != qid && async_nif->queues[j].depth != 0) {
              n++;
              avg_depth += async_nif->queues[j].depth;
          }
      }
      if (avg_depth) avg_depth /= n;

      /* Lock this queue under consideration, then check for shutdown.  While
         we hold this lock either a) we're shutting down so exit now or b) this
         queue will be valid until we release the lock. */
      q = &async_nif->queues[qid];
      enif_mutex_lock(q->reqs_mutex);

      /* Try not to enqueue a request into a queue that isn't keeping up with
         the request volume. */
      if (q->depth <= avg_depth) break;
      else {
          enif_mutex_unlock(q->reqs_mutex);
          qid = (qid + 1) % async_nif->num_queues;
      }
  }

  /* If the for loop finished then we didn't find a suitable queue for this
     request, meaning we're backed up so trigger eagain.  Note that if we left
     the loop in this way we hold no lock. */
  if (i == async_nif->num_queues) return 0;

  /* Add the request to the queue. */
  STAILQ_INSERT_TAIL(&q->reqs, req, entries);
  __sync_fetch_and_add(&q->depth, 1);

  /* We've selected a queue for this new request now check to make sure there are
     enough workers actively processing requests on this queue. */
  while (q->depth > q->num_workers) {
      switch(async_nif_start_worker(async_nif, q)) {
      case EINVAL: case ENOMEM: default: return 0;
      case EAGAIN: continue;
      case 0:      __sync_fetch_and_add(&q->num_workers, 1); goto done;
      }
  }done:;

  /* Build the term before releasing the lock so as not to race on the use of
     the req pointer (which will soon become invalid in another thread
     performing the request). */
  double pct_full = (double)avg_depth / (double)ASYNC_NIF_WORKER_QUEUE_SIZE;
  ERL_NIF_TERM reply = enif_make_tuple2(req->env, ATOM_OK,
					enif_make_tuple2(req->env, ATOM_ENQUEUED,
							 enif_make_double(req->env, pct_full)));
  enif_cond_signal(q->reqs_cnd);
  enif_mutex_unlock(q->reqs_mutex);
  return reply;
}

/**
 * Worker threads execute this function.  Here each worker pulls requests of
 * their respective queues, executes that work and continues doing that until
 * they see the shutdown flag is set at which point they exit.
 */
static void *
async_nif_worker_fn(void *arg)
{
  struct async_nif_worker_entry *we = (struct async_nif_worker_entry *)arg;
  unsigned int worker_id = we->worker_id;
  struct async_nif_state *async_nif = we->async_nif;
  struct async_nif_work_queue *q = we->q;
  struct async_nif_req_entry *req = NULL;
  unsigned int tries = async_nif->num_queues;

  for(;;) {
    /* Examine the request queue, are there things to be done? */
    enif_mutex_lock(q->reqs_mutex);
    check_again_for_work:
    if (async_nif->shutdown) {
        enif_mutex_unlock(q->reqs_mutex);
        break;
    }
    if (STAILQ_EMPTY(&q->reqs)) {
      /* Queue is empty so we wait for more work to arrive. */
	enif_mutex_unlock(q->reqs_mutex);
	if (tries == 0 && q == we->q) {
	    if (q->num_workers > ASYNC_NIF_MIN_WORKERS) {
		/* At this point we've tried to find/execute work on all queues
		 * and there are at least MIN_WORKERS on this queue so we
		 * leaving this loop (break) which leads to a thread exit/join. */
		break;
	    } else {
		enif_mutex_lock(q->reqs_mutex);
		enif_cond_wait(q->reqs_cnd, q->reqs_mutex);
		goto check_again_for_work;
	    }
	} else {
	    tries--;
	    __sync_fetch_and_add(&q->num_workers, -1);
	    q = q->next;
	    __sync_fetch_and_add(&q->num_workers, 1);
	    continue; // try next queue
	}
    } else {
      /* At this point the next req is ours to process and we hold the
         reqs_mutex lock.  Take the request off the queue. */
      req = STAILQ_FIRST(&q->reqs);
      STAILQ_REMOVE(&q->reqs, req, async_nif_req_entry, entries);
      __sync_fetch_and_add(&q->depth, -1);

      /* Wake up other worker thread watching this queue to help process work. */
      enif_cond_signal(q->reqs_cnd);
      enif_mutex_unlock(q->reqs_mutex);

      /* Perform the work. */
      req->fn_work(req->env, req->ref, &req->pid, worker_id, req->args);

      /* Now call the post-work cleanup function. */
      req->fn_post(req->args);

      /* Clean up req for reuse. */
      req->ref = 0;
      req->fn_work = 0;
      req->fn_post = 0;
      free(req->args);
      req->args = NULL;
      async_nif_recycle_req(req, async_nif);
      req = NULL;
    }
  }
  enif_mutex_lock(async_nif->we_mutex);
  SLIST_INSERT_HEAD(&async_nif->we_joining, we, entries);
  enif_mutex_unlock(async_nif->we_mutex);
  __sync_fetch_and_add(&q->num_workers, -1);
  enif_thread_exit(0);
  return 0;
}

static void
async_nif_unload(ErlNifEnv *env, struct async_nif_state *async_nif)
{
  unsigned int i;
  unsigned int num_queues = async_nif->num_queues;
  struct async_nif_work_queue *q = NULL;
  struct async_nif_req_entry *req = NULL;
  struct async_nif_worker_entry *we = NULL;
  UNUSED(env);

  /* Signal the worker threads, stop what you're doing and exit.  To ensure
     that we don't race with the enqueue() process we first lock all the worker
     queues, then set shutdown to true, then unlock.  The enqueue function will
     take the queue mutex, then test for shutdown condition, then enqueue only
     if not shutting down. */
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];
      enif_mutex_lock(q->reqs_mutex);
  }
  /* Set the shutdown flag so that worker threads will no continue
     executing requests. */
  async_nif->shutdown = 1;
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];
      enif_mutex_unlock(q->reqs_mutex);
  }

  /* Join for the now exiting worker threads. */
  while(async_nif->we_active > 0) {
      for (i = 0; i < num_queues; i++)
          enif_cond_broadcast(async_nif->queues[i].reqs_cnd);
      enif_mutex_lock(async_nif->we_mutex);
      we = SLIST_FIRST(&async_nif->we_joining);
      while(we != NULL) {
          struct async_nif_worker_entry *n = SLIST_NEXT(we, entries);
          SLIST_REMOVE(&async_nif->we_joining, we, async_nif_worker_entry, entries);
          void *exit_value = 0; /* We ignore the thread_join's exit value. */
          enif_thread_join(we->tid, &exit_value);
          free(we);
          async_nif->we_active--;
          we = n;
      }
      enif_mutex_unlock(async_nif->we_mutex);
  }
  enif_mutex_destroy(async_nif->we_mutex);

  /* Cleanup in-flight requests, mutexes and conditions in each work queue. */
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];

      /* Worker threads are stopped, now toss anything left in the queue. */
      req = NULL;
      req = STAILQ_FIRST(&q->reqs);
      while(req != NULL) {
          struct async_nif_req_entry *n = STAILQ_NEXT(req, entries);
          enif_clear_env(req->env);
          enif_send(NULL, &req->pid, req->env,
		    enif_make_tuple2(req->env, ATOM_ERROR, ATOM_SHUTDOWN));
          req->fn_post(req->args);
          enif_free_env(req->env);
          free(req->args);
          free(req);
          req = n;
      }
      enif_mutex_destroy(q->reqs_mutex);
      enif_cond_destroy(q->reqs_cnd);
  }

  /* Free any req structures sitting unused on the recycle queue. */
  enif_mutex_lock(async_nif->recycled_req_mutex);
  req = NULL;
  req = STAILQ_FIRST(&async_nif->recycled_reqs);
  while(req != NULL) {
      struct async_nif_req_entry *n = STAILQ_NEXT(req, entries);
      enif_free_env(req->env);
      free(req);
      req = n;
  }

  enif_mutex_unlock(async_nif->recycled_req_mutex);
  enif_mutex_destroy(async_nif->recycled_req_mutex);
  memset(async_nif, 0, sizeof(struct async_nif_state) + (sizeof(struct async_nif_work_queue) * async_nif->num_queues));
  free(async_nif);
}

static void *
async_nif_load(ErlNifEnv *env)
{
  static int has_init = 0;
  unsigned int i, num_queues;
  ErlNifSysInfo info;
  struct async_nif_state *async_nif;

  /* Don't init more than once. */
  if (has_init) return 0;
  else has_init = 1;

  /* Init some static references to commonly used atoms. */
  ATOM_EAGAIN = enif_make_atom(env, "eagain");
  ATOM_ENOMEM = enif_make_atom(env, "enomem");
  ATOM_ENQUEUED = enif_make_atom(env, "enqueued");
  ATOM_ERROR = enif_make_atom(env, "error");
  ATOM_OK = enif_make_atom(env, "ok");
  ATOM_SHUTDOWN = enif_make_atom(env, "shutdown");

  /* Find out how many schedulers there are. */
  enif_system_info(&info, sizeof(ErlNifSysInfo));

  /* Size the number of work queues according to schedulers. */
  if (info.scheduler_threads > ASYNC_NIF_MAX_WORKERS / 2) {
      num_queues = ASYNC_NIF_MAX_WORKERS / 2;
  } else {
      int remainder = ASYNC_NIF_MAX_WORKERS % info.scheduler_threads;
      if (remainder != 0)
          num_queues = info.scheduler_threads - remainder;
      else
          num_queues = info.scheduler_threads;
      if (num_queues < 2)
          num_queues = 2;
  }

  /* Init our portion of priv_data's module-specific state. */
  async_nif = malloc(sizeof(struct async_nif_state) +
		     sizeof(struct async_nif_work_queue) * num_queues);
  if (!async_nif)
      return NULL;
  memset(async_nif, 0, sizeof(struct async_nif_state) +
                       sizeof(struct async_nif_work_queue) * num_queues);

  async_nif->num_queues = num_queues;
  async_nif->we_active = 0;
  async_nif->next_q = 0;
  async_nif->shutdown = 0;
  STAILQ_INIT(&async_nif->recycled_reqs);
  async_nif->recycled_req_mutex = enif_mutex_create("recycled_req");
  async_nif->we_mutex = enif_mutex_create("we");
  SLIST_INIT(&async_nif->we_joining);

  for (i = 0; i < async_nif->num_queues; i++) {
      struct async_nif_work_queue *q = &async_nif->queues[i];
      STAILQ_INIT(&q->reqs);
      q->reqs_mutex = enif_mutex_create("reqs");
      q->reqs_cnd = enif_cond_create("reqs");
      q->next = &async_nif->queues[(i + 1) % num_queues];
  }
  return async_nif;
}

static void
async_nif_upgrade(ErlNifEnv *env)
{
     UNUSED(env);
    // TODO:
}


#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
