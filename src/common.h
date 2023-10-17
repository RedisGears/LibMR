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
