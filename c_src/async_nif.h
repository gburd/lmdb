/*
 * async_nif: An async thread-pool layer for Erlang's NIF API
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 * Author: Gregory Burd <greg@basho.com> <greg@burd.me>
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef __ASYNC_NIF_H__
#define __ASYNC_NIF_H__

#if defined(__cplusplus)
extern "C" {
#endif

#include <assert.h>
#include "fifo_q.h"
#include "stats.h"

#ifndef __UNUSED
#define __UNUSED(v) ((void)(v))
#endif

#define ASYNC_NIF_MAX_WORKERS 128
#define ASYNC_NIF_WORKER_QUEUE_SIZE 500
#define ASYNC_NIF_MAX_QUEUED_REQS 1000 * ASYNC_NIF_MAX_WORKERS

STAT_DECL(qwait, 1000);

struct async_nif_req_entry {
  ERL_NIF_TERM ref;
  ErlNifEnv *env;
  ErlNifPid pid;
  void *args;
  void (*fn_work)(ErlNifEnv*, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *);
  void (*fn_post)(void *);
};
DECL_FIFO_QUEUE(reqs, struct async_nif_req_entry);

struct async_nif_work_queue {
  STAT_DEF(qwait);
  ErlNifMutex *reqs_mutex;
  ErlNifCond *reqs_cnd;
  FIFO_QUEUE_TYPE(reqs) reqs;
};

struct async_nif_worker_entry {
  ErlNifTid tid;
  unsigned int worker_id;
  struct async_nif_state *async_nif;
  struct async_nif_work_queue *q;
};

struct async_nif_state {
  STAT_DEF(qwait);
  unsigned int shutdown;
  unsigned int num_workers;
  struct async_nif_worker_entry worker_entries[ASYNC_NIF_MAX_WORKERS];
  unsigned int num_queues;
  unsigned int next_q;
  FIFO_QUEUE_TYPE(reqs) recycled_reqs;
  unsigned int num_reqs;
  ErlNifMutex *recycled_req_mutex;
  struct async_nif_work_queue queues[];
};

#define ASYNC_NIF_DECL(decl, frame, pre_block, work_block, post_block)  \
  struct decl ## _args frame;                                           \
  static void fn_work_ ## decl (ErlNifEnv *env, ERL_NIF_TERM ref, ErlNifPid *pid, unsigned int worker_id, struct decl ## _args *args) { \
  __UNUSED(worker_id);                                                  \
  do work_block while(0);                                               \
  }                                                                     \
  static void fn_post_ ## decl (struct decl ## _args *args) {           \
    __UNUSED(args);                                                     \
    do post_block while(0);                                             \
  }                                                                     \
  static ERL_NIF_TERM decl(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv_in[]) { \
    struct decl ## _args on_stack_args;                                 \
    struct decl ## _args *args = &on_stack_args;                        \
    struct decl ## _args *copy_of_args;                                 \
    struct async_nif_req_entry *req = NULL;                             \
    const char *affinity = NULL;                                        \
    ErlNifEnv *new_env = NULL;                                          \
    /* argv[0] is a ref used for selective recv */                      \
    const ERL_NIF_TERM *argv = argv_in + 1;                             \
    argc -= 1;                                                          \
    /* Note: !!! this assumes that the first element of priv_data is ours */ \
    struct async_nif_state *async_nif = *(struct async_nif_state**)enif_priv_data(env); \
    if (async_nif->shutdown)                                            \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "shutdown"));         \
    req = async_nif_reuse_req(async_nif);                               \
    new_env = req->env;                                                 \
    if (!req)                                                           \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "eagain"));           \
    do pre_block while(0);                                              \
    copy_of_args = (struct decl ## _args *)enif_alloc(sizeof(struct decl ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_ ## decl (args);                                          \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct decl ## _args));           \
    req->ref = enif_make_copy(new_env, argv_in[0]);                     \
    enif_self(env, &req->pid);                                          \
    req->args = (void*)copy_of_args;                                    \
    req->fn_work = (void (*)(ErlNifEnv *, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *))fn_work_ ## decl ; \
    req->fn_post = (void (*)(void *))fn_post_ ## decl;                 \
    int h = -1;                                                        \
    if (affinity)                                                      \
        h = async_nif_str_hash_func(affinity) % async_nif->num_queues; \
    ERL_NIF_TERM reply = async_nif_enqueue_req(async_nif, req, h);     \
    if (!reply) {                                                      \
      fn_post_ ## decl (args);                                         \
      enif_free(copy_of_args);                                         \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),       \
                              enif_make_atom(env, "shutdown"));        \
    }                                                                  \
    return reply;                                                      \
  }

#define ASYNC_NIF_INIT(name)                                            \
        static ErlNifMutex *name##_async_nif_coord = NULL;

#define ASYNC_NIF_LOAD(name, priv) do {                                 \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create(NULL);           \
        enif_mutex_lock(name##_async_nif_coord);                        \
        priv = async_nif_load();                                        \
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);
#define ASYNC_NIF_UNLOAD(name, env, priv) do {                          \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create(NULL);           \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_unload(env, priv);                                    \
        enif_mutex_unlock(name##_async_nif_coord);                      \
        enif_mutex_destroy(name##_async_nif_coord);                     \
        name##_async_nif_coord = NULL;                                  \
    } while(0);
#define ASYNC_NIF_UPGRADE(name, env) do {                               \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create(NULL);           \
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
    if (fifo_q_empty(reqs, async_nif->recycled_reqs)) {
        if (async_nif->num_reqs < ASYNC_NIF_MAX_QUEUED_REQS) {
            req = enif_alloc(sizeof(struct async_nif_req_entry));
            if (req) {
                memset(req, 0, sizeof(struct async_nif_req_entry));
                env = enif_alloc_env();
                if (!env) {
                    enif_free(req);
                    req = NULL;
                } else {
                    req->env = env;
                    async_nif->num_reqs++;
                }
            }
        }
    } else {
        req = fifo_q_get(reqs, async_nif->recycled_reqs);
    }
    enif_mutex_unlock(async_nif->recycled_req_mutex);
    STAT_TICK(async_nif, qwait);
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
    STAT_TOCK(async_nif, qwait);
    enif_mutex_lock(async_nif->recycled_req_mutex);
    fifo_q_put(reqs, async_nif->recycled_reqs, req);
    enif_mutex_unlock(async_nif->recycled_req_mutex);
}

/**
 * A string hash function.
 *
 * A basic hash function for strings of characters used during the
 * affinity association.
 *
 * s    a NULL terminated set of bytes to be hashed
 * ->   an integer hash encoding of the bytes
 */
static inline unsigned int
async_nif_str_hash_func(const char *s)
{
  unsigned int h = (unsigned int)*s;
  if (h) for (++s ; *s; ++s) h = (h << 5) - h + (unsigned int)*s;
  return h;
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
  unsigned int qid = 0;
  struct async_nif_work_queue *q = NULL;
  unsigned int n = async_nif->num_queues;

  /* Either we're choosing a queue based on some affinity/hinted value or we
     need to select the next queue in the rotation and atomically update that
     global value (next_q is shared across worker threads) . */
  if (hint >= 0) {
      qid = (unsigned int)hint;
  } else {
      qid = async_nif->next_q;
      qid = (qid + 1) % async_nif->num_queues;
      async_nif->next_q = qid;
  }

  /* Now we inspect and interate across the set of queues trying to select one
     that isn't too full or too slow. */
  do {
      q = &async_nif->queues[qid];
      enif_mutex_lock(q->reqs_mutex);

      /* Now that we hold the lock, check for shutdown.  As long as we hold
         this lock either a) we're shutting down so exit now or b) this queue
         will be valid until we release the lock. */
      if (async_nif->shutdown) {
          enif_mutex_unlock(q->reqs_mutex);
          return 0;
      }
      double await = STAT_MEAN_LOG2_SAMPLE(async_nif, qwait);
      double await_inthisq = STAT_MEAN_LOG2_SAMPLE(q, qwait);
      if (fifo_q_full(reqs, q->reqs) || await_inthisq > await) {
          enif_mutex_unlock(q->reqs_mutex);
          qid = (qid + 1) % async_nif->num_queues;
          q = &async_nif->queues[qid];
      } else {
          break;
      }
      // TODO: at some point add in work sheading/stealing
  } while(n-- > 0);

  /* We hold the queue's lock, and we've seletect a reasonable queue for this
     new request so add the request. */
  STAT_TICK(q, qwait);
  fifo_q_put(reqs, q->reqs, req);

  /* Build the term before releasing the lock so as not to race on the use of
     the req pointer (which will soon become invalid in another thread
     performing the request). */
  ERL_NIF_TERM reply = enif_make_tuple2(req->env, enif_make_atom(req->env, "ok"),
                                        enif_make_atom(req->env, "enqueued"));
  enif_mutex_unlock(q->reqs_mutex);
  enif_cond_signal(q->reqs_cnd);
  return reply;
}

/**
 * TODO:
 */
static void *
async_nif_worker_fn(void *arg)
{
  struct async_nif_worker_entry *we = (struct async_nif_worker_entry *)arg;
  unsigned int worker_id = we->worker_id;
  struct async_nif_state *async_nif = we->async_nif;
  struct async_nif_work_queue *q = we->q;
  struct async_nif_req_entry *req = NULL;

  for(;;) {
    /* Examine the request queue, are there things to be done? */
    enif_mutex_lock(q->reqs_mutex);
    check_again_for_work:
    if (async_nif->shutdown) {
        enif_mutex_unlock(q->reqs_mutex);
        break;
    }
    if (fifo_q_empty(reqs, q->reqs)) {
      /* Queue is empty so we wait for more work to arrive. */
      STAT_RESET(q, qwait);
      enif_cond_wait(q->reqs_cnd, q->reqs_mutex);
      goto check_again_for_work;
    } else {
      assert(fifo_q_size(reqs, q->reqs) > 0);
      assert(fifo_q_size(reqs, q->reqs) < fifo_q_capacity(reqs, q->reqs));
      /* At this point the next req is ours to process and we hold the
         reqs_mutex lock.  Take the request off the queue. */
      req = fifo_q_get(reqs, q->reqs);
      enif_mutex_unlock(q->reqs_mutex);

      /* Ensure that there is at least one other worker thread watching this
         queue. */
      enif_cond_signal(q->reqs_cnd);

      /* Perform the work. */
      req->fn_work(req->env, req->ref, &req->pid, worker_id, req->args);
      STAT_TOCK(q, qwait);

      /* Now call the post-work cleanup function. */
      req->fn_post(req->args);

      /* Clean up req for reuse. */
      req->ref = 0;
      req->fn_work = 0;
      req->fn_post = 0;
      enif_free(req->args);
      req->args = NULL;
      enif_clear_env(req->env);
      async_nif_recycle_req(req, async_nif);
      req = NULL;
    }
  }
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
  __UNUSED(env);

  STAT_PRINT(async_nif, qwait, "wterl");

  /* Signal the worker threads, stop what you're doing and exit.  To
     ensure that we don't race with the enqueue() process we first
     lock all the worker queues, then set shutdown to true, then
     unlock.  The enqueue function will take the queue mutex, then
     test for shutdown condition, then enqueue only if not shutting
     down. */
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];
      enif_mutex_lock(q->reqs_mutex);
  }
  async_nif->shutdown = 1;
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];
      enif_cond_broadcast(q->reqs_cnd);
      enif_mutex_unlock(q->reqs_mutex);
  }

  /* Join for the now exiting worker threads. */
  for (i = 0; i < async_nif->num_workers; ++i) {
    void *exit_value = 0; /* We ignore the thread_join's exit value. */
    enif_thread_join(async_nif->worker_entries[i].tid, &exit_value);
  }

  /* Free req structres sitting on the recycle queue. */
  enif_mutex_lock(async_nif->recycled_req_mutex);
  req = NULL;
  fifo_q_foreach(reqs, async_nif->recycled_reqs, req, {
      enif_free_env(req->env);
      enif_free(req);
  });
  fifo_q_free(reqs, async_nif->recycled_reqs);

  /* Cleanup in-flight requests, mutexes and conditions in each work queue. */
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];

      /* Worker threads are stopped, now toss anything left in the queue. */
      req = NULL;
      fifo_q_foreach(reqs, q->reqs, req, {
          enif_clear_env(req->env);
          enif_send(NULL, &req->pid, req->env,
                    enif_make_tuple2(req->env, enif_make_atom(req->env, "error"),
                                     enif_make_atom(req->env, "shutdown")));
          req->fn_post(req->args);
          enif_free_env(req->env);
          enif_free(req->args);
          enif_free(req);
          });
      fifo_q_free(reqs, q->reqs);
      enif_mutex_destroy(q->reqs_mutex);
      enif_cond_destroy(q->reqs_cnd);
  }

  enif_mutex_unlock(async_nif->recycled_req_mutex);
  enif_mutex_destroy(async_nif->recycled_req_mutex);
  memset(async_nif, 0, sizeof(struct async_nif_state) + (sizeof(struct async_nif_work_queue) * async_nif->num_queues));
  enif_free(async_nif);
}

static void *
async_nif_load()
{
  static int has_init = 0;
  unsigned int i, j, num_queues;
  ErlNifSysInfo info;
  struct async_nif_state *async_nif;

  /* Don't init more than once. */
  if (has_init) return 0;
  else has_init = 1;

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
  async_nif = enif_alloc(sizeof(struct async_nif_state) +
                         sizeof(struct async_nif_work_queue) * num_queues);
  if (!async_nif)
      return NULL;
  memset(async_nif, 0, sizeof(struct async_nif_state) +
         sizeof(struct async_nif_work_queue) * num_queues);

  async_nif->num_queues = num_queues;
  async_nif->num_workers = 2 * num_queues;
  async_nif->next_q = 0;
  async_nif->shutdown = 0;
  async_nif->recycled_reqs = fifo_q_new(reqs, ASYNC_NIF_MAX_QUEUED_REQS);
  async_nif->recycled_req_mutex = enif_mutex_create(NULL);
  STAT_INIT(async_nif, qwait);

  for (i = 0; i < async_nif->num_queues; i++) {
      struct async_nif_work_queue *q = &async_nif->queues[i];
      q->reqs = fifo_q_new(reqs, ASYNC_NIF_WORKER_QUEUE_SIZE);
      q->reqs_mutex = enif_mutex_create(NULL);
      q->reqs_cnd = enif_cond_create(NULL);
      STAT_INIT(q, qwait);
  }

  /* Setup the thread pool management. */
  memset(async_nif->worker_entries, 0, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);

  /* Start the worker threads. */
  for (i = 0; i < async_nif->num_workers; i++) {
    struct async_nif_worker_entry *we = &async_nif->worker_entries[i];
    we->async_nif = async_nif;
    we->worker_id = i;
    we->q = &async_nif->queues[i % async_nif->num_queues];
    if (enif_thread_create(NULL, &async_nif->worker_entries[i].tid,
                            &async_nif_worker_fn, (void*)we, NULL) != 0) {
      async_nif->shutdown = 1;

      for (j = 0; j < async_nif->num_queues; j++) {
          struct async_nif_work_queue *q = &async_nif->queues[j];
          enif_cond_broadcast(q->reqs_cnd);
      }

      while(i-- > 0) {
        void *exit_value = 0; /* Ignore this. */
        enif_thread_join(async_nif->worker_entries[i].tid, &exit_value);
      }

      for (j = 0; j < async_nif->num_queues; j++) {
          struct async_nif_work_queue *q = &async_nif->queues[j];
          enif_mutex_destroy(q->reqs_mutex);
          enif_cond_destroy(q->reqs_cnd);
      }

      memset(async_nif->worker_entries, 0, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);
      enif_free(async_nif);
      return NULL;
    }
  }
  return async_nif;
}

static void
async_nif_upgrade(ErlNifEnv *env)
{
     __UNUSED(env);
    // TODO:
}


#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
