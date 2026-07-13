/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef SRC_CLUSTER_H_
#define SRC_CLUSTER_H_

#include <stdbool.h>

#define CLUSTER_ERROR "ERRCLUSTER"

/* Send a message to a shard by shard id,
 * NULL id means to send the message to all the shards.
 * Take ownership on the given message */
void MR_ClusterSendMsg(const char* nodeId, functionId function, char* msg, size_t len);

void MR_ClusterCopyAndSendMsg(const char* nodeId, functionId function, char* msg, size_t len);

void MR_ClusterSendMsgBySlot(size_t slot, functionId function, char* msg, size_t len);

void MR_ClusterCopyAndSendMsgBySlot(size_t slot, functionId function, char* msg, size_t len);

functionId MR_ClusterRegisterMsgReceiver(MR_ClusterMessageReceiver receiver);

int MR_ClusterIsClusterMode();

const char* MR_ClusterGetMyId();

int MR_IsClusterInitialize();

size_t MR_ClusterGetSize();

int MR_ClusterInit(RedisModuleCtx* rctx, char *password);

/* Schedule an OSS cluster topology refresh on the event loop. 'change_flags'
 * is a bitmask of REDISMODULE_CLUSTER_TOPOLOGY_CHANGE_FLAG_* reasons and is
 * advisory only: the refresh reconciles against CLUSTER SLOTS and rebuilds the
 * connections only when the set of slot-serving primaries actually changed,
 * otherwise it updates just the slot->node routing and preserves the existing
 * connections (so in-flight cross-shard queries are not disrupted by an in-place
 * reshard). No-op outside of OSS cluster mode. Safe to call from a Redis
 * server-event callback. */
void MR_ClusterRefreshTopology(int change_flags);

size_t MR_ClusterGetSlotByKey(const char* key, size_t len);

int MR_ClusterIsMySlot(size_t slot);

#endif /* SRC_CLUSTER_H_ */
