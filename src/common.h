/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_

#define xstr(s) str(s)
#define str(s) #s

#ifndef MODULE_NAME
#error "MODULE_NAME is not defined"
#endif

typedef struct MR_RedisVersion
{
    int redisMajorVersion;
    int redisMinorVersion;
    int redisPatchVersion;
} MR_RedisVersion;

extern MR_RedisVersion MR_currVersion;

extern int MR_RlecMajorVersion;
extern int MR_RlecMinorVersion;
extern int MR_RlecPatchVersion;
extern int MR_RlecBuild;
extern int MR_IsEnterprise;

static inline int MR_IsEnterpriseBuild() {
    return MR_IsEnterprise;
}

#endif /* SRC_COMMON_H_ */
