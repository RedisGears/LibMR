/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/* Exercise the production topology code with deterministic Redis API fixtures.
 * No Redis processes or network connections are created by this test. */
#include "../../src/cluster.c"
#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>

RedisModuleCtx *mr_staticCtx;
static int locked, unlocked_api_calls, rebuilds, aborted;
static EventLoopTaskCallback queued;
static void *queued_ctx;
static const char local_id[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static const char remote_id[] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
static int current_port = 7000;
static int test_failures;
static pthread_mutex_t pause_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t pause_cond = PTHREAD_COND_INITIALIZER;
static int pause_mode, writer_paused, release_writer, worker_started, worker_done, worker_result;

static void pause_writer(int mode) {
    pthread_mutex_lock(&pause_lock);
    if (pause_mode == mode && !writer_paused) {
        writer_paused = 1;
        pthread_cond_broadcast(&pause_cond);
        while (!release_writer)
            pthread_cond_wait(&pause_cond, &pause_lock);
    }
    pthread_mutex_unlock(&pause_lock);
}
static void *test_alloc(size_t size) {
    if (!clusterCtx.CurrCluster)
        pause_writer(1);
    if (clusterCtx.CurrCluster && !clusterCtx.CurrCluster->slots[8192])
        pause_writer(2);
    return malloc(size);
}

static void *test_calloc(size_t count, size_t size) {
    if (!clusterCtx.CurrCluster) pause_writer(1);
    return calloc(count, size);
}

#define CHECK(expr)                                                                                \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #expr);                                \
            test_failures++;                                                                       \
        }                                                                                          \
    } while (0)
static void require_lock(void) {
    if (!locked)
        unlocked_api_calls++;
}
static void lock_ctx(RedisModuleCtx *ctx) {
    (void)ctx;
    assert(!locked);
    locked = 1;
}
static void unlock_ctx(RedisModuleCtx *ctx) {
    (void)ctx;
    assert(locked);
    locked = 0;
}
static int context_flags(RedisModuleCtx *ctx) {
    (void)ctx;
    require_lock();
    if (!clusterCtx.CurrCluster)
        pause_writer(1);
    return REDISMODULE_CTX_FLAGS_CLUSTER;
}
static const char *my_id(void) {
    require_lock();
    rebuilds++;
    return local_id;
}
static void random_hex(char *buf, size_t len) { memset(buf, '0', len); }
static void log_msg(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {
    (void)ctx;
    (void)level;
    (void)fmt;
}
static void assertion(const char *expr, const char *file, int line) {
    fprintf(stderr, "%s:%d %s\n", file, line, expr);
    abort();
}
bool MR_IsMainThread(void) { return 0; }
void MR_AbortRunningExecutions(void) { aborted++; }
void MR_EventLoopAddTask(EventLoopTaskCallback cb, void *ctx) {
    assert(!queued);
    queued = cb;
    queued_ctx = ctx;
}
void MR_EventLoopDelayTaskCancel(MR_LoopTaskCtx *ctx) {
    (void)ctx;
    abort();
}

static int accessible_slot(int slot) { return slot == 42; }
static void legacy_range(int *first, int *last) {
    *first = 50;
    *last = 60;
}
static void *ownership_thread(void *unused) {
    (void)unused;
    pthread_mutex_lock(&pause_lock);
    worker_started = 1;
    pthread_cond_broadcast(&pause_cond);
    pthread_mutex_unlock(&pause_lock);
    int result = MR_ClusterIsMySlot(8192);
    pthread_mutex_lock(&pause_lock);
    worker_result = result;
    worker_done = 1;
    pthread_cond_broadcast(&pause_cond);
    pthread_mutex_unlock(&pause_lock);
    return NULL;
}
static const char *string_ptr(const RedisModuleString *str, size_t *len) {
    const char *value = (const char *)str;
    if (len) *len = strlen(value);
    return value;
}
static int string_integer(const RedisModuleString *str, long long *value) {
    char *end;
    *value = strtoll((const char *)str, &end, 10);
    return *end ? REDISMODULE_ERR : REDISMODULE_OK;
}
static char endpoint[64] = "password@127.0.0.1:7000";
static const char *command[] = {
    "MRTESTS.CLUSTERSET", "unused", "unused", "unused", "unused", "unused",
    local_id, "RANGES", "1", "SHARD", local_id, "SLOTRANGE", "0", "16383",
    "ADDR", endpoint, "MASTER"
};
static int apply_command(void) {
    lock_ctx(NULL);
    pthread_rwlock_wrlock(&topologyLock);
    int result = MR_SetClusterData((RedisModuleString **)command,
                                  sizeof(command) / sizeof(command[0]));
    pthread_rwlock_unlock(&topologyLock);
    unlock_ctx(NULL);
    return result;
}
static void *refresh_thread(void *unused) {
    (void)unused;
    CHECK(apply_command() == REDISMODULE_OK);
    return NULL;
}
/* Pause a real writer in the temporary unconfigured/empty-map window. The
 * worker must wait for publication and report local ownership, never the gap. */
static void test_worker_ownership(int mode) {
    pthread_t writer, worker;
    pause_mode = mode;
    writer_paused = release_writer = worker_started = worker_done = 0;
    if (mode == 1) {
        current_port++;
        snprintf(endpoint, sizeof(endpoint), "password@127.0.0.1:%d", current_port);
    } else {
        command[1] = "changed-unused-argument";
    }
    CHECK(pthread_create(&writer, NULL, refresh_thread, NULL) == 0);
    pthread_mutex_lock(&pause_lock);
    while (!writer_paused)
        pthread_cond_wait(&pause_cond, &pause_lock);
    pthread_mutex_unlock(&pause_lock);
    CHECK(pthread_create(&worker, NULL, ownership_thread, NULL) == 0);
    pthread_mutex_lock(&pause_lock);
    while (!worker_started)
        pthread_cond_wait(&pause_cond, &pause_lock);
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec++;
    int wait_result = 0;
    while (!worker_done && wait_result != ETIMEDOUT)
        wait_result = pthread_cond_timedwait(&pause_cond, &pause_lock, &deadline);
    CHECK(!worker_done);
    release_writer = 1;
    pthread_cond_broadcast(&pause_cond);
    pthread_mutex_unlock(&pause_lock);
    pthread_join(writer, NULL);
    pthread_join(worker, NULL);
    CHECK(worker_result);
    pause_mode = 0;
}

int main(void) {
    RedisModule_Alloc = test_alloc;
    RedisModule_Calloc = test_calloc;
    RedisModule_Realloc = realloc;
    RedisModule_Free = free;
    RedisModule_Strdup = strdup;
    RedisModule__Assert = assertion;
    RedisModule_Log = log_msg;
    RedisModule_ThreadSafeContextLock = lock_ctx;
    RedisModule_ThreadSafeContextUnlock = unlock_ctx;
    RedisModule_GetContextFlags = context_flags;
    RedisModule_GetMyClusterID = my_id;
    RedisModule_GetRandomHexChars = random_hex;
    RedisModule_StringPtrLen = string_ptr;
    RedisModule_StringToLongLong = string_integer;
    clusterCtx.nodesMsgIds = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);
    CHECK(apply_command() == REDISMODULE_OK);
    Node *retained = MR_GetNode(clusterCtx.CurrCluster, local_id);
    CHECK(apply_command() == REDISMODULE_OK);
    CHECK(MR_GetNode(clusterCtx.CurrCluster, local_id) == retained);
    test_worker_ownership(1);
    retained = MR_GetNode(clusterCtx.CurrCluster, local_id);
    test_worker_ownership(2);
    CHECK(MR_GetNode(clusterCtx.CurrCluster, local_id) == retained);
    CHECK(!strcmp(clusterCtx.CurrCluster->clusterSetCommand[1], command[1]));

    /* Identical announcement arguments except for MYID must not take the no-op. */
    int before_abort = aborted;
    command[6] = remote_id;
    CHECK(apply_command() == REDISMODULE_OK);
    CHECK(aborted == before_abort + 1);
    CHECK(!strcmp(clusterCtx.CurrCluster->myId, remote_id));
    command[6] = local_id;
    CHECK(apply_command() == REDISMODULE_OK);

    /* A local shard may have disjoint ranges or no slots after a slot move. */
    MR_CreateNode(clusterCtx.CurrCluster, remote_id, "127.0.0.2", 7002, NULL, NULL, 4000, 7999);
    TopologyViewEntry view[3] = {{.port = current_port, .minSlot = 0, .maxSlot = 3999},
                               {.port = 7002, .minSlot = 4000, .maxSlot = 7999},
                               {.port = current_port, .minSlot = 8000, .maxSlot = 16383}};
    strcpy(view[0].id, local_id); strcpy(view[0].ip, "127.0.0.1");
    strcpy(view[1].id, remote_id); strcpy(view[1].ip, "127.0.0.2");
    strcpy(view[2].id, local_id); strcpy(view[2].ip, "127.0.0.1");
    strcpy(view[0].password, "password"); strcpy(view[2].password, "password");
    CHECK(MR_TopologyViewIsValid(view, 3));
    CHECK(MR_TryApplyTopologyInPlace(view, 3));
    CHECK(MR_ClusterIsMySlot(0)); CHECK(MR_ClusterIsMySlot(3999));
    CHECK(!MR_ClusterIsMySlot(4000)); CHECK(!MR_ClusterIsMySlot(7999));
    CHECK(MR_ClusterIsMySlot(8000)); CHECK(MR_ClusterIsMySlot(16383));
    CHECK(!MR_ClusterIsMySlot(16384));
    view[0].maxSlot = -1; view[1].minSlot = 0; view[1].maxSlot = 16383;
    CHECK(MR_TopologyViewIsValid(view, 2));
    CHECK(MR_TryApplyTopologyInPlace(view, 2));
    CHECK(!MR_ClusterIsMySlot(0)); CHECK(!MR_ClusterIsMySlot(16383));
    view[0].maxSlot = 1;
    CHECK(!MR_TopologyViewIsValid(view, 2)); /* Overlap. */
    view[0].maxSlot = -1; view[1].minSlot = 1;
    CHECK(!MR_TopologyViewIsValid(view, 2)); /* Missing slot. */
    RedisModule_ClusterCanAccessKeysInSlot = accessible_slot;
    CHECK(MR_ClusterIsMySlot(42)); CHECK(!MR_ClusterIsMySlot(43));
    RedisModule_ClusterCanAccessKeysInSlot = NULL;
    RedisModule_ShardingGetSlotRange = legacy_range;
    CHECK(MR_ClusterIsMySlot(50)); CHECK(!MR_ClusterIsMySlot(49));
    CHECK(unlocked_api_calls == 0);
    FreeCluster(clusterCtx.CurrCluster);
    mr_dictRelease(clusterCtx.nodesMsgIds);
    printf("long-form topology regression checks: %d failures\n", test_failures);
    return test_failures ? 1 : 0;
}
