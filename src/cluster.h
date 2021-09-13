#ifndef SRC_CLUSTER_H_
#define SRC_CLUSTER_H_

#include "redismodule.h"
#include <stdbool.h>

#define CLUSTER_ERROR "ERRCLUSTER"

typedef size_t functionId;

typedef void (*MR_ClusterMessageReceiver)(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);

/* Send a message to a shard by shard id,
 * NULL id means to send the message to all the shards.
 * Take ownership on the given message */
void MR_ClusterSendMsg(const char* nodeId, functionId function, char* msg, size_t len);

void MR_ClusterCopyAndSendMsg(const char* nodeId, functionId function, char* msg, size_t len);

functionId MR_ClusterRegisterMsgReceiver(MR_ClusterMessageReceiver receiver);

bool MR_ClusterIsClusterMode();

char* MR_ClusterGetMyId();

size_t MR_ClusterGetSize();

int MR_ClusterInit(RedisModuleCtx* rctx);

bool MR_ClusterIsInitialized();

const char* MR_ClusterGetNodeIdByKey(const char* key);

#endif /* SRC_CLUSTER_H_ */
