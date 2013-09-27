/*
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

#ifndef __COMMON_H__
#define __COMMON_H__

#if defined(__cplusplus)
extern "C" {
#endif

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif
#ifndef COMPQUIET
#define COMPQUIET(n, v) do {                                            \
        (n) = (v);                                                      \
        (n) = (n);                                                      \
} while (0)
#endif

#ifdef __APPLE__
#define PRIuint64(x) (x)
#else
#define PRIuint64(x) (unsigned long long)(x)
#endif


#if defined(__cplusplus)
}
#endif

#endif // __COMMON_H__
