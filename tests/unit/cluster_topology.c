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
static int current_port = 7000, master_count = 1;
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
    if (clusterCtx.CurrCluster && !clusterCtx.CurrCluster->slots[8192])
        pause_writer(2);
    return malloc(size);
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

/* A CLUSTER SLOTS reply for one master covering the whole slot space. */
struct RedisModuleCallReply {
    int type;
    long long integer;
    const char *str;
    size_t len;
    struct RedisModuleCallReply *items[3];
};
static struct RedisModuleCallReply lo = {.type = REDISMODULE_REPLY_INTEGER, .integer = 0};
static struct RedisModuleCallReply hi = {.type = REDISMODULE_REPLY_INTEGER, .integer = 16383};
static struct RedisModuleCallReply ip = {
    .type = REDISMODULE_REPLY_STRING, .str = "127.0.0.1", .len = 9};
static struct RedisModuleCallReply port_reply = {.type = REDISMODULE_REPLY_INTEGER,
                                                 .integer = 7000};
static struct RedisModuleCallReply id_reply = {
    .type = REDISMODULE_REPLY_STRING, .str = local_id, .len = 40};
static struct RedisModuleCallReply node_reply = {
    .type = REDISMODULE_REPLY_ARRAY, .len = 3, .items = {&ip, &port_reply, &id_reply}};
static struct RedisModuleCallReply range_reply = {
    .type = REDISMODULE_REPLY_ARRAY, .len = 3, .items = {&lo, &hi, &node_reply}};
static struct RedisModuleCallReply remote_lo = {.type = REDISMODULE_REPLY_INTEGER, .integer = 8192};
static struct RedisModuleCallReply remote_hi = {.type = REDISMODULE_REPLY_INTEGER,
                                                .integer = 16383};
static struct RedisModuleCallReply remote_ip = {
    .type = REDISMODULE_REPLY_STRING, .str = "127.0.0.2", .len = 9};
static struct RedisModuleCallReply remote_id_reply = {
    .type = REDISMODULE_REPLY_STRING, .str = remote_id, .len = 40};
static struct RedisModuleCallReply remote_node = {
    .type = REDISMODULE_REPLY_ARRAY,
    .len = 3,
    .items = {&remote_ip, &port_reply, &remote_id_reply}};
static struct RedisModuleCallReply remote_range = {
    .type = REDISMODULE_REPLY_ARRAY, .len = 3, .items = {&remote_lo, &remote_hi, &remote_node}};
static struct RedisModuleCallReply slots_reply = {
    .type = REDISMODULE_REPLY_ARRAY, .len = 1, .items = {&range_reply}};
static RedisModuleCallReply *call(RedisModuleCtx *ctx, const char *cmd, const char *fmt, ...) {
    (void)ctx;
    (void)fmt;
    require_lock();
    assert(!strcmp(cmd, "cluster"));
    hi.integer = master_count == 1 ? 16383 : 8191;
    slots_reply.len = master_count;
    slots_reply.items[1] = &remote_range;
    return &slots_reply;
}
static int reply_type(RedisModuleCallReply *r) { return r->type; }
static size_t reply_len(RedisModuleCallReply *r) { return r->len; }
static RedisModuleCallReply *reply_element(RedisModuleCallReply *r, size_t i) {
    assert(i < r->len);
    return r->items[i];
}
static long long reply_integer(RedisModuleCallReply *r) { return r->integer; }
static const char *reply_string(RedisModuleCallReply *r, size_t *len) {
    *len = r->len;
    return r->str;
}
static void free_reply(RedisModuleCallReply *r) { (void)r; }
static int node_info(RedisModuleCtx *ctx, const char *id, char *ip_out, char *master, int *port,
                     int *flags) {
    (void)ctx;
    (void)id;
    (void)master;
    require_lock();
    if (ip_out)
        strcpy(ip_out, !strcmp(id, local_id) ? "127.0.0.1" : "127.0.0.2");
    if (port)
        *port = !strcmp(id, local_id) ? current_port : 7002;
    if (flags)
        *flags = REDISMODULE_NODE_MASTER;
    return REDISMODULE_OK;
}
static char **node_list(RedisModuleCtx *ctx, size_t *len) {
    (void)ctx;
    require_lock();
    static char *ids[] = {(char *)local_id, (char *)remote_id};
    *len = master_count;
    return ids;
}
static void free_node_list(char **list) { (void)list; }
static RedisModuleSlotRangeArray *node_ranges(RedisModuleCtx *ctx, const char *id) {
    (void)ctx;
    (void)id;
    require_lock();
    RedisModuleSlotRangeArray *r = malloc(sizeof(*r) + sizeof(r->ranges[0]));
    r->num_ranges = 1;
    r->ranges[0].start = !strcmp(id, local_id) ? 0 : 8192;
    r->ranges[0].end = master_count == 2 && !strcmp(id, local_id) ? 8191 : 16383;
    return r;
}
static void free_ranges(RedisModuleCtx *ctx, RedisModuleSlotRangeArray *r) {
    (void)ctx;
    free(r);
}
static int accessible_slot(int slot) { return slot == 42; }
static void legacy_range(int *first, int *last) {
    *first = 50;
    *last = 60;
}
static void run_event(void) {
    MR_ClusterRefreshTopology(0);
    CHECK(queued != NULL);
    if (queued) {
        EventLoopTaskCallback cb = queued;
        queued = NULL;
        cb(queued_ctx);
    }
    CHECK(!locked);
}

static void *refresh_thread(void *unused) {
    (void)unused;
    MR_TopoRefreshTask(NULL);
    return NULL;
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
/* Pause a real writer in the temporary unconfigured/empty-map window. The
 * worker must wait for publication and report local ownership, never the gap. */
static void test_worker_ownership(int mode) {
    pthread_t writer, worker;
    pause_mode = mode;
    writer_paused = release_writer = worker_started = worker_done = 0;
    if (mode == 1)
        current_port++;
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
static void install_old_command(void) {
    Cluster *c = clusterCtx.CurrCluster;
    CHECK(c->clusterSetCommand == NULL);
    c->clusterSetCommandSize = 10;
    c->clusterSetCommand = calloc(10, sizeof(char *));
    for (int i = 0; i < 10; i++)
        c->clusterSetCommand[i] = strdup("stale-long-form");
}

int main(void) {
    RedisModule_Alloc = test_alloc;
    RedisModule_Calloc = calloc;
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
    RedisModule_Call = call;
    RedisModule_CallReplyType = reply_type;
    RedisModule_CallReplyLength = reply_len;
    RedisModule_CallReplyArrayElement = reply_element;
    RedisModule_CallReplyInteger = reply_integer;
    RedisModule_CallReplyStringPtr = reply_string;
    RedisModule_FreeCallReply = free_reply;
    RedisModule_GetClusterNodeInfo = node_info;
    RedisModule_GetClusterNodesList = node_list;
    RedisModule_FreeClusterNodesList = free_node_list;
    RedisModule_ClusterFreeSlotRanges = free_ranges;
    cachedClusterType = ClusterType_OSS;
    clusterCtx.topologyEvents = true;
    clusterCtx.nodesMsgIds = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);

    /* Older hosts: no native ranges API. Event rebuild still acquires the lock
     * and records authority, including the second event after authority is set. */
    run_event();
    CHECK(unlocked_api_calls == 0);
    CHECK(MR_TopologyEventSeen);
    CHECK(rebuilds == 1);
    MR_TopologyEventSeen = true; /* Also exercise the native callback's flag state. */
    run_event();
    CHECK(rebuilds == 2);

    /* Newer hosts: unchanged topology reuses nodes; an address change needs a
     * full rebuild even though an earlier event established authority. */
    RedisModule_GetClusterNodeSlotRanges = node_ranges;
    run_event();
    CHECK(rebuilds == 2);
    current_port = 7001;
    run_event();
    CHECK(rebuilds == 3);
    CHECK(MR_GetNode(clusterCtx.CurrCluster, local_id)->port == 7001);
    master_count = 2;
    run_event();
    CHECK(rebuilds == 4);
    CHECK(mr_dictSize(clusterCtx.CurrCluster->nodes) == 2);
    CHECK(!MR_ClusterIsMySlot(8192));
    master_count = 1;
    run_event();
    CHECK(rebuilds == 5);
    CHECK(mr_dictSize(clusterCtx.CurrCluster->nodes) == 1);
    CHECK(MR_ClusterIsMySlot(8192));
    int before = rebuilds;
    lock_ctx(NULL);
    MR_RefreshClusterData();
    unlock_ctx(NULL);
    CHECK(MR_SetClusterData(NULL, 1) == REDISMODULE_OK);
    CHECK(rebuilds == before); /* Legacy refresh cannot overwrite event state. */

    /* Same masters, disjoint local ranges with a remote interval between them. */
    current_port = MR_GetNode(clusterCtx.CurrCluster, local_id)->port;
    const char *remote = remote_id;
    MR_CreateNode(clusterCtx.CurrCluster, remote, "127.0.0.2", 7002, NULL, NULL, 4000, 7999);
    TopologyViewEntry view[3] = {{.port = current_port, .minSlot = 0, .maxSlot = 3999},
                                 {.port = 7002, .minSlot = 4000, .maxSlot = 7999},
                                 {.port = current_port, .minSlot = 8000, .maxSlot = 16383}};
    strcpy(view[0].id, local_id);
    strcpy(view[0].ip, "127.0.0.1");
    strcpy(view[1].id, remote);
    strcpy(view[1].ip, "127.0.0.2");
    strcpy(view[2].id, local_id);
    strcpy(view[2].ip, "127.0.0.1");
    CHECK(MR_TopologyViewIsValid(view, 3));
    CHECK(MR_TryApplyTopologyInPlace(view, 3));
    CHECK(MR_ClusterIsMySlot(0));
    CHECK(MR_ClusterIsMySlot(3999));
    CHECK(!MR_ClusterIsMySlot(4000));
    CHECK(!MR_ClusterIsMySlot(7999));
    CHECK(MR_ClusterIsMySlot(8000));
    CHECK(MR_ClusterIsMySlot(16383));
    CHECK(!MR_ClusterIsMySlot(16384));
    view[0].maxSlot = -1;
    view[1].minSlot = 0;
    view[1].maxSlot = 16383;
    CHECK(MR_TopologyViewIsValid(view, 2));
    CHECK(MR_TryApplyTopologyInPlace(view, 2));
    CHECK(!MR_ClusterIsMySlot(0));
    CHECK(!MR_ClusterIsMySlot(16383));
    RedisModule_ClusterCanAccessKeysInSlot = accessible_slot;
    CHECK(MR_ClusterIsMySlot(42));
    CHECK(!MR_ClusterIsMySlot(43));
    RedisModule_ClusterCanAccessKeysInSlot = NULL;
    RedisModule_ShardingGetSlotRange = legacy_range;
    CHECK(MR_ClusterIsMySlot(50));
    CHECK(MR_ClusterIsMySlot(60));
    CHECK(!MR_ClusterIsMySlot(49));
    CHECK(!MR_ClusterIsMySlot(61));
    RedisModule_ShardingGetSlotRange = NULL;
    run_event(); /* Restore one local master before the concurrent checks. */
    CHECK(MR_ClusterIsMySlot(8192));
    test_worker_ownership(1);
    test_worker_ownership(2);

    /* A same-topology snapshot must replace an old broadcast command too. */
    install_old_command();
    Node *retained = MR_GetNode(clusterCtx.CurrCluster, local_id);
    lock_ctx(NULL);
    Cluster *snapshot = MR_BuildCluster(NULL, 1, NULL);
    unlock_ctx(NULL);
    MR_UpdateClusterTopologyIfNeededUnderLock(snapshot);
    CHECK(MR_GetNode(clusterCtx.CurrCluster, local_id) == retained);
    CHECK(clusterCtx.CurrCluster->clusterSetCommandSize == 1);
    CHECK(!strcmp(clusterCtx.CurrCluster->clusterSetCommand[0], CLUSTER_SET_FROM_SHARD_COMMAND));

    /* Changed ranges with the same masters take the in-place snapshot path. */
    master_count = 2;
    MR_CreateNode(clusterCtx.CurrCluster, remote_id, "127.0.0.2", 7002, NULL, NULL, 4000, 7999);
    view[0].port = view[2].port = current_port;
    view[0].maxSlot = 3999;
    view[1].minSlot = 4000;
    view[1].maxSlot = 7999;
    CHECK(MR_TryApplyTopologyInPlace(view, 3));
    lock_ctx(NULL);
    snapshot = MR_BuildCluster(NULL, 1, NULL);
    unlock_ctx(NULL);
    char **fresh_command = snapshot->clusterSetCommand;
    MR_UpdateClusterTopologyIfNeededUnderLock(snapshot);
    CHECK(MR_GetNode(clusterCtx.CurrCluster, local_id) == retained);
    CHECK(clusterCtx.CurrCluster->clusterSetCommand == fresh_command);
    CHECK(MR_ClusterIsMySlot(8191));
    CHECK(!MR_ClusterIsMySlot(8192));
    CHECK(unlocked_api_calls == 0);
    FreeCluster(clusterCtx.CurrCluster);
    mr_dictRelease(clusterCtx.nodesMsgIds);
    printf("topology regression checks: %d failures\n", test_failures);
    return test_failures ? 1 : 0;
}
