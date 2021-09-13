/*
 * mr_memory.h
 *
 *  Created on: Aug 25, 2021
 *      Author: root
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
