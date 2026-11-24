/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "../../src/cluster.c"
#include <assert.h>
RedisModuleCtx *mr_staticCtx;
static const char *string_ptr(const RedisModuleString *s, size_t *len) {
    if (len) *len = strlen((const char *)s);
    return (const char *)s;
}
static int failures;
#define CHECK(e) do { if (!(e)) { fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #e); failures++; } } while (0)
int main(void) {
    RedisModule_StringPtrLen = string_ptr;
    const char *plain[] = {"MRTESTS.CLUSTERSET", "unused", "unused", "unused", "unused", "unused",
        "1", "RANGES", "1", "SHARD", "1", "SLOTRANGE", "0", "16383",
        "ADDR", "password@127.0.0.1:6379", "MASTER"};
    const char *socket[] = {"MRTESTS.CLUSTERSET", "unused", "unused", "unused", "unused", "unused",
        "1", "RANGES", "1", "SHARD", "1", "SLOTRANGE", "0", "16383",
        "ADDR", "password@127.0.0.1:6379", "UNIXADDR", "/tmp/proxy.sock", "MASTER"};
    Cluster current = {0};
    char id[REDISMODULE_NODE_ID_LEN + 1];
    memset(id, '0', REDISMODULE_NODE_ID_LEN); id[REDISMODULE_NODE_ID_LEN - 1] = '1';
    id[REDISMODULE_NODE_ID_LEN] = 0;
    current.myId = id;
    clusterCtx.CurrCluster = &current;
    for (int cached_socket = 0; cached_socket < 2; cached_socket++) {
        char *cached[19];
        memcpy(cached, cached_socket ? socket : plain, (cached_socket ? 19 : 17) * sizeof(char *));
        cached[CLUSTERSET_MYID_LONG_FORM_INDEX] = NULL;
        current.clusterSetCommand = cached;
        current.clusterSetCommandSize = cached_socket ? 19 : 17;
        CHECK(IsSameLongFormClusterSet((RedisModuleString **)plain, 17));
        CHECK(IsSameLongFormClusterSet((RedisModuleString **)socket, 19));
        const char *modified[19]; memcpy(modified, socket, sizeof(modified));
        modified[17] = "/tmp/another-proxy.sock";
        CHECK(IsSameLongFormClusterSet((RedisModuleString **)modified, 19));
        const int positions[] = {6, 10, 12, 13, 15, 18};
        const char *changes[] = {"2", "2", "1", "10000", "password2@127.0.0.1:6379", "REPLICA"};
        for (int j = 0; j < 6; j++) {
            memcpy(modified, socket, sizeof(modified)); modified[positions[j]] = changes[j];
            CHECK(!IsSameLongFormClusterSet((RedisModuleString **)modified, 19));
        }
        memcpy(modified, socket, sizeof(modified)); modified[15] = "password@127.0.0.2:6380";
        CHECK(!IsSameLongFormClusterSet((RedisModuleString **)modified, 19));
        CHECK(!IsSameLongFormClusterSet((RedisModuleString **)socket, 18));
        CHECK(!IsSameLongFormClusterSet((RedisModuleString **)plain, 16));
    }
    printf("long-form no-op checks: %d failures\n", failures);
    return failures ? 1 : 0;
}
