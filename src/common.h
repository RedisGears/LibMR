/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_

#define xstr(s) str(s)
#define str(s) #s

#ifndef MODULE_NAME
#error "MODULE_NAME is not defined"
#endif

/** The name of the ACL category for the commands created by LibMR for
 * its own operations.
 *
 * The user may redefine the category name by defining the macro
 * LIBMR_ACL_COMMAND_CATEGORY_NAME before including this header.
 */
#ifndef LIBMR_ACL_COMMAND_CATEGORY_NAME
#define LIBMR_ACL_COMMAND_CATEGORY_NAME                                        \
  "_" xstr(MODULE_NAME) "_libmr_internal"
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
