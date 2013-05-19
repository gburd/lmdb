/*
 * stats: measure all the things
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

#ifndef __STATS_H__
#define __STATS_H__

#if defined(__cplusplus)
extern "C" {
#endif

#include "duration.h"

/**
 * Calculate the log2 of 64bit unsigned integers.
 */
#ifdef __GCC__
#define LOG2(X) ((unsigned) ((8 * (sizeof(uint64_t) - 1))  - __builtin_clzll((X))))
#else
static unsigned int __log2_64(uint64_t x) {
     static const int tab64[64] = {
          63,  0, 58,  1, 59, 47, 53,  2,
          60, 39, 48, 27, 54, 33, 42,  3,
          61, 51, 37, 40, 49, 18, 28, 20,
          55, 30, 34, 11, 43, 14, 22,  4,
          62, 57, 46, 52, 38, 26, 32, 41,
          50, 36, 17, 19, 29, 10, 13, 21,
          56, 45, 25, 31, 35, 16,  9, 12,
          44, 24, 15,  8, 23,  7,  6,  5};
     if (x == 0) return 0;
     uint64_t v = x;
     v |= v >> 1;
     v |= v >> 2;
     v |= v >> 4;
     v |= v >> 8;
     v |= v >> 16;
     v |= v >> 32;
     return tab64[((uint64_t)((v - (v >> 1)) * 0x07EDD5E59A4E28C2)) >> 58];
}
#define LOG2(X) __log2_64(X)
#endif

#define STAT_DEF(name) struct name ## _stat name ## _stat;

#define STAT_DECL(name, nsamples)                                       \
     struct name ## _stat {                                             \
         duration_t d;                                                  \
         uint64_t histogram[64];                                        \
         uint32_t h, n;                                                 \
         uint64_t samples[nsamples];                                    \
         uint64_t min, max;                                             \
         double mean;                                                   \
     };                                                                 \
     static inline double name ## _stat_mean(struct name ## _stat *s) { \
         uint32_t t = s->h;                                             \
         uint32_t h = (s->h + 1) % nsamples;                            \
         double mean = 0;                                               \
         while (h != t) {                                               \
             mean += s->samples[h];                                     \
             h = (h + 1) % nsamples;                                    \
         }                                                              \
         if (mean > 0)                                                  \
             mean /= (double)(s->n < nsamples ? s->n : nsamples);       \
         return mean;                                                   \
     }                                                                  \
     static inline double name ## _stat_mean_lg2(struct name ## _stat *s) { \
         uint32_t i;                                                    \
         double mean = 0;                                               \
         for (i = 0; i < 64; i++)                                       \
             mean += (s->histogram[i] * i);                             \
         if (mean > 0)                                                  \
             mean /= (double)s->n;                                      \
         return mean;                                                   \
     }                                                                  \
     static inline uint64_t name ## _stat_tick(struct name ## _stat *s) \
     {                                                                  \
         uint64_t t = ts(s->d.unit);                                    \
         s->d.then = t;                                                 \
         return t;                                                      \
     }                                                                  \
     static inline void name ## _stat_reset(struct name ## _stat *s)    \
     {                                                                  \
         s->min = ~0;                                                   \
         s->max = 0;                                                    \
         s->h = 0;                                                      \
         memset(&s->histogram, 0, sizeof(uint64_t) * 64);               \
         memset(&s->samples, 0, sizeof(uint64_t) * nsamples);           \
     }                                                                  \
     static inline uint64_t name ## _stat_tock(struct name ## _stat *s) \
     {                                                                  \
         uint64_t now = ts(s->d.unit);                                  \
         uint64_t elapsed = now - s->d.then;                            \
         uint32_t i = s->h;                                             \
         if (s->n == nsamples) {                                        \
             s->mean = (s->mean + name ## _stat_mean(s)) / 2.0;         \
             if (s->n >= 4294967295)                                    \
                 name ## _stat_reset(s);                                \
         }                                                              \
         s->h = (s->h + 1) % nsamples;                                  \
         s->samples[i] = elapsed;                                       \
         if (elapsed < s->min)                                          \
             s->min = elapsed;                                          \
         if (elapsed > s->max)                                          \
             s->max = elapsed;                                          \
         s->histogram[LOG2(elapsed)]++;                                 \
         s->n++;                                                        \
         s->d.then = ts(s->d.unit);                                     \
         return elapsed;                                                \
     }                                                                  \
     static void name ## _stat_print_histogram(struct name ## _stat *s, const char *mod) \
     {                                                                  \
         uint8_t logs[64];                                              \
         uint8_t i, j, max_log = 0;                                     \
         double m = (s->mean + name ## _stat_mean(s) / 2.0);            \
                                                                        \
         fprintf(stderr, "%s:async_nif request latency histogram:\n", mod); \
         for (i = 0; i < 64; i++) {                                     \
             logs[i] = LOG2(s->histogram[i]);                           \
             if (logs[i] > max_log)                                     \
                 max_log = logs[i];                                     \
         }                                                              \
         for (i = max_log; i > 0; i--) {                                \
             if (!(i % 10))                                             \
                 fprintf(stderr, "2^%2d ", i);                          \
             else                                                       \
                 fprintf(stderr, "     ");                              \
             for(j = 0; j < 64; j++)                                    \
                 fprintf(stderr, logs[j] >= i ?  "•" : " ");            \
             fprintf(stderr, "\n");                                     \
         }                                                              \
         if (max_log == 0) {                                            \
             fprintf(stderr, "[empty]\n");                              \
         } else {                                                       \
             fprintf(stderr, "     ns        μs        ms        s         ks\n"); \
             fprintf(stderr, "min: ");                                  \
             if (s->min < 1000)                                         \
                 fprintf(stderr, "%lu (ns)", s->min);                   \
             else if (s->min < 1000000)                                 \
                 fprintf(stderr, "%.2f (μs)", s->min / 1000.0);         \
             else if (s->min < 1000000000)                              \
                 fprintf(stderr, "%.2f (ms)", s->min / 1000000.0);      \
             else if (s->min < 1000000000000)                           \
                 fprintf(stderr, "%.2f (s)", s->min / 1000000000.0);    \
             fprintf(stderr, "  max: ");                                \
             if (s->max < 1000)                                         \
                 fprintf(stderr, "%lu (ns)", s->max);                   \
             else if (s->max < 1000000)                                 \
                 fprintf(stderr, "%.2f (μs)", s->max / 1000.0);         \
             else if (s->max < 1000000000)                              \
                 fprintf(stderr, "%.2f (ms)", s->max / 1000000.0);      \
             else if (s->max < 1000000000000)                           \
                 fprintf(stderr, "%.2f (s)", s->max / 1000000000.0);    \
             fprintf(stderr, "  mean: ");                               \
             if (m < 1000)                                              \
                 fprintf(stderr, "%.2f (ns)", m);                       \
             else if (m < 1000000)                                      \
                 fprintf(stderr, "%.2f (μs)", m / 1000.0);              \
             else if (m < 1000000000)                                   \
                 fprintf(stderr, "%.2f (ms)", m / 1000000.0);           \
             else if (m < 1000000000000)                                \
                 fprintf(stderr, "%.2f (s)", m / 1000000000.0);         \
             fprintf(stderr, "\n");                                     \
         }                                                              \
         fflush(stderr);                                                \
     }


#define STAT_INIT(var, name)                                            \
     var->name ## _stat.min = ~0;                                       \
     var->name ## _stat.max = 0;                                        \
     var->name ## _stat.mean = 0.0;                                     \
     var->name ## _stat.h = 0;                                          \
     var->name ## _stat.d.then = 0;                                     \
     var->name ## _stat.d.unit = ns;

#define STAT_TICK(var, name) name ## _stat_tick(&var->name ## _stat)

#define STAT_TOCK(var, name) name ## _stat_tock(&var->name ## _stat)

#define STAT_RESET(var, name) name ## _stat_reset(&var->name ## _stat)

#define STAT_MEAN_LOG2_SAMPLE(var, name)                                \
    name ## _stat_mean_lg2(&var->name ## _stat)

#define STAT_MEAN_SAMPLE(var, name)                                     \
    name ## _stat_mean(&var->name ## _stat)

#define STAT_PRINT(var, name, mod)                                      \
    name ## _stat_print_histogram(&var->name ## _stat, mod)


#if defined(__cplusplus)
}
#endif

#endif // __STATS_H__
