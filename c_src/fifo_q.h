/*
 * fifo_q: a macro-based implementation of a FIFO Queue
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

#ifndef __FIFO_Q_H__
#define __FIFO_Q_H__

#if defined(__cplusplus)
extern "C" {
#endif

#define FIFO_QUEUE_TYPE(name)             \
  struct fifo_q__ ## name *
#define DECL_FIFO_QUEUE(name, type)       \
  struct fifo_q__ ## name {               \
    unsigned int h, t, s;                 \
    type *items[];                        \
  };                                      \
  static struct fifo_q__ ## name *fifo_q_ ## name ## _new(unsigned int n) { \
    int sz = sizeof(struct fifo_q__ ## name) + ((n+1) * sizeof(type *));\
    struct fifo_q__ ## name *q = enif_alloc(sz);                        \
    if (!q)                                                             \
        return 0;                                                       \
    memset(q, 0, sz);                                                   \
    q->s = n + 1;                                                       \
    return q;                                                           \
  }                                                                     \
  static inline void fifo_q_ ## name ## _free(struct fifo_q__ ## name *q) {    \
    memset(q, 0, sizeof(struct fifo_q__ ## name) + (q->s * sizeof(type *))); \
    enif_free(q);                                                       \
  }                                                                     \
  static inline type *fifo_q_ ## name ## _put(struct fifo_q__ ## name *q, type *n) { \
    q->items[q->h] = n;                                                 \
    q->h = (q->h + 1) % q->s;                                           \
    return n;                                                           \
  }                                                                     \
  static inline type *fifo_q_ ## name ## _get(struct fifo_q__ ## name *q) {    \
    type *n = q->items[q->t];                                           \
    q->items[q->t] = 0;                                                 \
    q->t = (q->t + 1) % q->s;                                           \
    return n;                                                           \
  }                                                                     \
  static inline unsigned int fifo_q_ ## name ## _size(struct fifo_q__ ## name *q) { \
    return (q->h - q->t + q->s) % q->s;                                 \
  }                                                                     \
  static inline unsigned int fifo_q_ ## name ## _capacity(struct fifo_q__ ## name *q) { \
    return q->s - 1;                                                    \
  }                                                                     \
  static inline int fifo_q_ ## name ## _empty(struct fifo_q__ ## name *q) {    \
    return (q->t == q->h);                                              \
  }                                                                     \
  static inline int fifo_q_ ## name ## _full(struct fifo_q__ ## name *q) {     \
    return ((q->h + 1) % q->s) == q->t;                                 \
  }

#define fifo_q_new(name, size) fifo_q_ ## name ## _new(size)
#define fifo_q_free(name, queue) fifo_q_ ## name ## _free(queue)
#define fifo_q_get(name, queue) fifo_q_ ## name ## _get(queue)
#define fifo_q_put(name, queue, item) fifo_q_ ## name ## _put(queue, item)
#define fifo_q_size(name, queue) fifo_q_ ## name ## _size(queue)
#define fifo_q_capacity(name, queue) fifo_q_ ## name ## _capacity(queue)
#define fifo_q_empty(name, queue) fifo_q_ ## name ## _empty(queue)
#define fifo_q_full(name, queue) fifo_q_ ## name ## _full(queue)
#define fifo_q_foreach(name, queue, item, task) do {                    \
    while(!fifo_q_ ## name ## _empty(queue)) {                          \
      item = fifo_q_ ## name ## _get(queue);                            \
      do task while(0);                                                 \
    }                                                                   \
  } while(0);


#if defined(__cplusplus)
}
#endif

#endif // __FIFO_Q_H__
