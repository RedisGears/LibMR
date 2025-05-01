
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef SRC_MR_MEMORY_H_
#define SRC_MR_MEMORY_H_

#include <stdlib.h>
#include <string.h>

#ifdef VALGRIND
#define MR_ALLOC malloc
#define MR_CALLOC calloc
#define MR_REALLOC realloc
#define MR_FREE free
#define MR_STRDUP strdup
#else
#include "redismodule.h"
#define MR_ALLOC RedisModule_Alloc
#define MR_CALLOC RedisModule_Calloc
#define MR_REALLOC RedisModule_Realloc
#define MR_FREE RedisModule_Free
#define MR_STRDUP RedisModule_Strdup
#endif

#endif /* SRC_MR_MEMORY_H_ */
