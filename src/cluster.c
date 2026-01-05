/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "cluster.h"
#include "common.h"
#include "mr.h"
#include "event_loop.h"
#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "utils/adlist.h"

#include <hiredis.h>
#include <hiredis_ssl.h>
#include <async.h>
#include <libevent.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <stdlib.h>


#define RETRY_INTERVAL 1000 // 1 second
#define MSG_MAX_RETRIES 3
#define CLUSTER_SET_MY_ID_INDEX 6
#define MAX_SLOT 16384
#define RUN_ID_SIZE 40

#define CLUSTER_INNER_COMMUNICATION_COMMAND xstr(MODULE_NAME)".INNERCOMMUNICATION"
#define CLUSTER_INNER_COMMUNICATION_BATCH_COMMAND xstr(MODULE_NAME)".INNERCOMMUNICATIONBATCH"
#define CLUSTER_HELLO_COMMAND               xstr(MODULE_NAME)".HELLO"
#define CLUSTER_REFRESH_COMMAND             xstr(MODULE_NAME)".REFRESHCLUSTER"
#define CLUSTER_SET_COMMAND                 xstr(MODULE_NAME)".CLUSTERSET"
#define CLUSTER_SET_FROM_SHARD_COMMAND      xstr(MODULE_NAME)".CLUSTERSETFROMSHARD"
#define CLUSTER_INFO_COMMAND                xstr(MODULE_NAME)".INFOCLUSTER"
#define NETWORK_TEST_COMMAND                xstr(MODULE_NAME)".NETWORKTEST"
#define FORCE_SHARDS_CONNECTION             xstr(MODULE_NAME)".FORCESHARDSCONNECTION"

/** @brief  Register a new Redis command with the required ACLs.
 *  @see    RedisModule_CreateCommand
 *  @return true if the command was registered successfully, false
 * otherwise.
 */
static inline __attribute__((always_inline)) bool
RegisterRedisCommand(RedisModuleCtx *ctx, const char *name,
                     RedisModuleCmdFunc cmdfunc, const char *strflags,
                     int firstkey, int lastkey, int keystep) {
  const int ret = RedisModule_CreateCommand(ctx, name, cmdfunc, strflags,
                                            firstkey, lastkey, keystep);

  if (ret != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Couldn't register the command %s", name);

    return false;
  }

  return true;
}

typedef enum NodeStatus{
    NodeStatus_Connected, NodeStatus_Disconnected, NodeStatus_HelloSent, NodeStatus_Free, NodeStatus_Uninitialized
}NodeStatus;

typedef enum SendMsgType{
    SendMsgType_BySlot, SendMsgType_ById, SendMsgType_ToAll
}SendMsgType;

typedef struct SendMsg{
    size_t refCount; // ref count does not need to be thread safe as its only touched on the event loop
    union {
        char idToSend[REDISMODULE_NODE_ID_LEN + 1];
        size_t slotToSend;
    };
    SendMsgType sendMsgType;
    functionId function;
    char* msg;
    size_t msgLen;
}SendMsg;

typedef struct NodeSendMsg{
    SendMsg* msg;
    size_t msgId;
    size_t retries;
}NodeSendMsg;

typedef struct SlotRange{
    uint16_t minSlot;
    uint16_t maxSlot;
}SlotRange;
typedef struct Node{
    char* id;
    char* ip;
    unsigned short port;
    char* password;
    char* unixSocket;
    redisAsyncContext *c;
    char* runId;
    unsigned long long msgId;
    mr_list* pendingMessages;
    /* Messages that are queued to be sent (not yet in-flight). */
    mr_list* outboxMessages;
    bool flushScheduled;
    mr_list* slotRanges;
    bool isMe;
    NodeStatus status;
    MR_LoopTaskCtx* reconnectEvent;
    MR_LoopTaskCtx* resendHelloEvent;
    bool sendClusterTopologyOnNextConnect;
}Node;

typedef struct Cluster {
    char* myId;
    mr_dict* nodes;
    Node* slots[MAX_SLOT];
    size_t clusterSetCommandSize;
    char** clusterSetCommand;
    char runId[RUN_ID_SIZE + 1];
}Cluster;

struct ClusterCtx {
    ARR(MR_ClusterMessageReceiver) callbacks;
    Cluster* CurrCluster;
    mr_dict* nodesMsgIds;
    // Note that the slot range in ClusterCtx are legacy code and only used as a fallback.
    // The general case (i.e., of possible multiple ranges per shard) are handled in `Node`.
    size_t minSlot;
    size_t maxSlot;
    size_t clusterSize;
    char myId[REDISMODULE_NODE_ID_LEN + 1];
    int isOss;
    functionId networkTestMsgReciever;
    char *password;
}clusterCtx;

typedef struct ClusterSetCtx {
    RedisModuleBlockedClient* bc;
    RedisModuleString **argv;
    int argc;
    bool force;
}ClusterSetCtx;

typedef struct InboundMsgCtx {
    size_t senderIdLen;
    char senderIdStr[REDISMODULE_NODE_ID_LEN + 1];
    size_t senderRunIdLen;
    char senderRunIdStr[RUN_ID_SIZE + 1];
    long long msgId;
    long long functionId;
    size_t msgLen;
    char* msgBuf;
}InboundMsgCtx;

static void MR_OnResponseArrived(struct redisAsyncContext* c, void* a, void* b);
static int MR_ClusterInnerCommunicationMsgBatch(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static void MR_ConnectToShard(Node* n);
static void MR_HelloResponseArrived(struct redisAsyncContext* c, void* a, void* b);
static Node* MR_GetNode(const char* id);

static SlotRange* NewSlotRange(uint16_t minSlot, uint16_t maxSlot) {
    SlotRange* result = MR_ALLOC(sizeof(*result));
    result->minSlot = minSlot;
    result->maxSlot = maxSlot;
    return result;
}

static void FreeSlotRange(void *ptr) {
    MR_FREE(ptr);
}

static void MR_ClusterFreeMsg(void* ptr){
    SendMsg* msg = ptr;
    if (--msg->refCount > 0) {
        return;
    }
    MR_FREE(msg->msg);
    MR_FREE(msg);
}

static void MR_ClusterFreeNodeMsg(void* ptr){
    NodeSendMsg* nodeMsg = ptr;
    MR_ClusterFreeMsg(nodeMsg->msg);
    MR_FREE(nodeMsg);
}

static void MR_ClusterFreeNodeMsgNoRef(void* ptr){
    NodeSendMsg* nodeMsg = ptr;
    MR_FREE(nodeMsg);
}

static void MR_ClusterFreeNodeMsgList(mr_list* list){
    if (!list) {
        return;
    }
    mr_listIter* iter = mr_listGetIterator(list, AL_START_HEAD);
    mr_listNode* node = NULL;
    while((node = mr_listNext(iter)) != NULL){
        NodeSendMsg* m = mr_listNodeValue(node);
        MR_ClusterFreeNodeMsg(m);
    }
    mr_listReleaseIterator(iter);
    /* list->free might be NULL; ensure we don't double-free */
    list->free = NULL;
    mr_listRelease(list);
}

static void MR_ClusterSendMsgToNodeInternal(Node* node, NodeSendMsg* nodeMsg){
    // CLUSTER_INNER_COMMUNICATION_COMMAND <myid> <runid> <functionid> <msg> <msgId>
    redisAsyncCommand(node->c, MR_OnResponseArrived, node, CLUSTER_INNER_COMMUNICATION_COMMAND" %s %s %llu %b %llu",
            clusterCtx.CurrCluster->myId,
            clusterCtx.CurrCluster->runId,
            nodeMsg->msg->function,
            nodeMsg->msg->msg, nodeMsg->msg->msgLen,
            nodeMsg->msgId);
}

typedef struct BatchReplyCtx {
    Node* n;
    size_t count;
} BatchReplyCtx;

static size_t innercomm_batch_max = 32;

static void MR_InitInnerCommBatchMax() {
    const char* env = getenv("RG_INNERCOMM_BATCH_MAX");
    if (!env || !env[0]) {
        return;
    }
    char* end = NULL;
    unsigned long v = strtoul(env, &end, 10);
    if (!end || end == env || *end != '\0') {
        return;
    }
    if (v < 1) v = 1;
    if (v > 256) v = 256;
    innercomm_batch_max = (size_t)v;
}

static void MR_OnBatchResponseArrived(struct redisAsyncContext* c, void* a, void* b){
    redisReply* reply = (redisReply*)a;
    BatchReplyCtx* brc = (BatchReplyCtx*)b;
    if(!brc){
        return;
    }
    Node* n = brc->n;
    const size_t count = brc->count;
    MR_FREE(brc);

    if(!reply || !c->data || !n){
        return;
    }
    if(reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, CLUSTER_ERROR, strlen(CLUSTER_ERROR)) == 0){
        n->sendClusterTopologyOnNextConnect = true;
        RedisModule_Log(mr_staticCtx, "warning", "Received ERRCLUSTER reply from shard %s (%s:%d), will send cluster topology to the shard on next connect", n->id, n->ip, n->port);
        redisAsyncDisconnect(c);
        return;
    }
    if(reply->type != REDIS_REPLY_STATUS){
        RedisModule_Log(mr_staticCtx, "warning", "Received an invalid status reply from shard %s (%s:%d), will disconnect and try to reconnect. This is usually because the Redis server's 'proto-max-bulk-len' configuration setting is too low.", n->id, n->ip, n->port);
        redisAsyncDisconnect(c);
        return;
    }
    for (size_t i = 0; i < count; i++) {
        mr_listNode* node = mr_listFirst(n->pendingMessages);
        if (!node) {
            break;
        }
        mr_listDelNode(n->pendingMessages, node);
    }
}

static void MR_ClusterFlushOutbox(void* ctx){
    Node* n = ctx;
    n->flushScheduled = false;
    if (n->status != NodeStatus_Connected || !n->c || !clusterCtx.CurrCluster) {
        return;
    }

    while (mr_listLength(n->outboxMessages) > 0) {
        /* Build a batch of messages */
        NodeSendMsg* msgs[256];
        size_t batchCount = 0;

        while (batchCount < innercomm_batch_max && mr_listLength(n->outboxMessages) > 0) {
            mr_listNode* ln = mr_listFirst(n->outboxMessages);
            NodeSendMsg* m = mr_listNodeValue(ln);
            mr_listDelNode(n->outboxMessages, ln); /* outbox has no free() */
            msgs[batchCount++] = m;
            /* Move to pending (in-flight) */
            mr_listAddNodeTail(n->pendingMessages, m);
        }

        char numBuf[32];
        snprintf(numBuf, sizeof(numBuf), "%zu", batchCount);

        const int argc = (int)(4 + (batchCount * 3));
        const char** argv = MR_ALLOC(sizeof(char*) * argc);
        size_t* argvlen = MR_ALLOC(sizeof(size_t) * argc);
        char** tmpNums = MR_ALLOC(sizeof(char*) * (batchCount * 2)); /* functionId + msgId */

        argv[0] = CLUSTER_INNER_COMMUNICATION_BATCH_COMMAND;
        argvlen[0] = strlen(argv[0]);
        argv[1] = clusterCtx.CurrCluster->myId;
        argvlen[1] = strlen(argv[1]);
        argv[2] = clusterCtx.CurrCluster->runId;
        argvlen[2] = strlen(argv[2]);
        argv[3] = numBuf;
        argvlen[3] = strlen(argv[3]);

        for (size_t i = 0; i < batchCount; i++) {
            char* fbuf = MR_ALLOC(32);
            char* ibuf = MR_ALLOC(32);
            snprintf(fbuf, 32, "%llu", (unsigned long long)msgs[i]->msg->function);
            snprintf(ibuf, 32, "%llu", (unsigned long long)msgs[i]->msgId);
            tmpNums[i*2] = fbuf;
            tmpNums[i*2 + 1] = ibuf;

            const int base = 4 + (int)(i*3);
            argv[base] = fbuf;
            argvlen[base] = strlen(fbuf);
            argv[base + 1] = msgs[i]->msg->msg;
            argvlen[base + 1] = msgs[i]->msg->msgLen;
            argv[base + 2] = ibuf;
            argvlen[base + 2] = strlen(ibuf);
        }

        BatchReplyCtx* brc = MR_ALLOC(sizeof(*brc));
        brc->n = n;
        brc->count = batchCount;

        redisAsyncCommandArgv(n->c, MR_OnBatchResponseArrived, brc, argc, argv, argvlen);

        for (size_t i = 0; i < batchCount * 2; i++) {
            MR_FREE(tmpNums[i]);
        }
        MR_FREE(tmpNums);
        MR_FREE(argv);
        MR_FREE(argvlen);
    }
}

static void MR_ClusterSendMsgToNode(Node* node, SendMsg* msg){
    msg->refCount+=1;
    NodeSendMsg* nodeMsg = MR_ALLOC(sizeof(*nodeMsg));
    nodeMsg->msg = msg;
    nodeMsg->retries = 0;
    nodeMsg->msgId = node->msgId++;
    mr_listAddNodeTail(node->outboxMessages, nodeMsg);
    if (node->status == NodeStatus_Uninitialized) {
        MR_ConnectToShard(node);
        node->status = NodeStatus_Disconnected;
    }
    if (node->status == NodeStatus_Connected && !node->flushScheduled) {
        node->flushScheduled = true;
        MR_EventLoopAddTask(MR_ClusterFlushOutbox, node);
    }
}

/* Runs on the event loop */
static void MR_ClusterSendMsgTask(void* ctx) {
    SendMsg* sendMsg = ctx;
    if (!clusterCtx.CurrCluster) {
        RedisModule_Log(mr_staticCtx, "warning", "try to send a message on an uninitialize cluster, message will not be sent.");
        MR_ClusterFreeMsg(sendMsg);
        return;
    }
    if (sendMsg->sendMsgType == SendMsgType_ById) {
        Node* n = MR_GetNode(sendMsg->idToSend);
        if(!n){
            RedisModule_Log(mr_staticCtx, "warning", "Could not find node to send message to");
        } else {
            MR_ClusterSendMsgToNode(n, sendMsg);
        }
    } else if (sendMsg->sendMsgType == SendMsgType_ToAll) {
        mr_dictIterator *iter = mr_dictGetIterator(clusterCtx.CurrCluster->nodes);
        mr_dictEntry *entry = NULL;
        while((entry = mr_dictNext(iter))){
            Node* n = mr_dictGetVal(entry);
            if(!n->isMe){
                MR_ClusterSendMsgToNode(n, sendMsg);
            }
        }
        mr_dictReleaseIterator(iter);
    } else if (sendMsg->sendMsgType == SendMsgType_BySlot) {
        Node* n = clusterCtx.CurrCluster->slots[sendMsg->slotToSend];
        if(!n){
            RedisModule_Log(mr_staticCtx, "warning", "Could not find node to send message to");
            return;
        }
        MR_ClusterSendMsgToNode(n, sendMsg);
    } else {
        RedisModule_Assert(false);
    }
    MR_ClusterFreeMsg(sendMsg);
}

void MR_ClusterSendMsg(const char* nodeId, functionId function, char* msg, size_t len) {
    SendMsg* msgStruct = MR_ALLOC(sizeof(*msgStruct));
    if(nodeId){
        memcpy(msgStruct->idToSend, nodeId, REDISMODULE_NODE_ID_LEN);
        msgStruct->idToSend[REDISMODULE_NODE_ID_LEN] = '\0';
        msgStruct->sendMsgType = SendMsgType_ById;
    }else{
        msgStruct->sendMsgType = SendMsgType_ToAll;
    }
    msgStruct->function = function;
    msgStruct->msg = msg;
    msgStruct->msgLen = len;
    msgStruct->refCount = 1;
    MR_EventLoopAddTask(MR_ClusterSendMsgTask, msgStruct);
}

void MR_ClusterCopyAndSendMsg(const char* nodeId, functionId function, char* msg, size_t len) {
    char* cMsg = MR_ALLOC(len);
    memcpy(cMsg, msg, len);
    MR_ClusterSendMsg(nodeId, function, cMsg, len);
}

void MR_ClusterSendMsgBySlot(size_t slot, functionId function, char* msg, size_t len) {
    SendMsg* msgStruct = MR_ALLOC(sizeof(*msgStruct));
    msgStruct->slotToSend = slot;
    msgStruct->sendMsgType = SendMsgType_BySlot;
    msgStruct->function = function;
    msgStruct->msg = msg;
    msgStruct->msgLen = len;
    msgStruct->refCount = 1;
    MR_EventLoopAddTask(MR_ClusterSendMsgTask, msgStruct);
}

void MR_ClusterCopyAndSendMsgBySlot(size_t slot, functionId function, char* msg, size_t len) {
    char* cMsg = MR_ALLOC(len);
    memcpy(cMsg, msg, len);
    MR_ClusterSendMsgBySlot(slot, function, cMsg, len);
}

functionId MR_ClusterRegisterMsgReceiver(MR_ClusterMessageReceiver receiver) {
    clusterCtx.callbacks = array_append(clusterCtx.callbacks, receiver);
    return array_len(clusterCtx.callbacks) - 1;
}

static void MR_OnResponseArrived(struct redisAsyncContext* c, void* a, void* b){
    redisReply* reply = (redisReply*)a;
    if(!reply){
        return;
    }
    if(!c->data){
        return;
    }
    Node* n = (Node*)b;
    if(reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, CLUSTER_ERROR, strlen(CLUSTER_ERROR)) == 0){
        n->sendClusterTopologyOnNextConnect = true;
        RedisModule_Log(mr_staticCtx, "warning", "Received ERRCLUSTER reply from shard %s (%s:%d), will send cluster topology to the shard on next connect", n->id, n->ip, n->port);
        redisAsyncDisconnect(c);
        return;
    }
    if(reply->type != REDIS_REPLY_STATUS){
        RedisModule_Log(mr_staticCtx, "warning", "Received an invalid status reply from shard %s (%s:%d), will disconnect and try to reconnect. This is usually because the Redis server's 'proto-max-bulk-len' configuration setting is too low.", n->id, n->ip, n->port);
        redisAsyncDisconnect(c);
        return;
    }
    mr_listNode* node = mr_listFirst(n->pendingMessages);
    if(!node){
        return;
    }
    mr_listDelNode(n->pendingMessages, node);
}

static void MR_ClusterResendHelloMessage(void* ctx){
    Node* n = ctx;
    n->resendHelloEvent = NULL;
    if(n->status == NodeStatus_Disconnected){
        // we will resent the hello request when reconnect
        return;
    }
    if(n->sendClusterTopologyOnNextConnect && clusterCtx.CurrCluster->clusterSetCommand){
        RedisModule_Log(mr_staticCtx, "notice", "Sending cluster topology to %s (%s:%d) on rg.hello retry", n->id, n->ip, n->port);
        clusterCtx.CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = MR_STRDUP(n->id);
        redisAsyncCommandArgv(n->c, NULL, NULL, clusterCtx.CurrCluster->clusterSetCommandSize, (const char**)clusterCtx.CurrCluster->clusterSetCommand, NULL);
        MR_FREE(clusterCtx.CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX]);
        clusterCtx.CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = NULL;
        n->sendClusterTopologyOnNextConnect = false;
    }

    RedisModule_Log(mr_staticCtx, "notice", "Resending hello request to %s (%s:%d)", n->id, n->ip, n->port);
    redisAsyncCommand((redisAsyncContext*)n->c, MR_HelloResponseArrived, n, CLUSTER_HELLO_COMMAND);
}

static void MR_HelloResponseArrived(struct redisAsyncContext* c, void* a, void* b){
    redisReply* reply = (redisReply*)a;
    if(!reply){
        return;
    }
    Node* n = (Node*)b;
    if(!c->data){
        return;
    }

    if(reply->type != REDIS_REPLY_STRING){
        // we did not got a string reply
        // the shard is probably not yet up.
        // we will try again in one second.
        if(reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, CLUSTER_ERROR, strlen(CLUSTER_ERROR)) == 0){
            RedisModule_Log(mr_staticCtx, "warning", "Got uninitialize cluster error on hello response from %s (%s:%d), will resend cluster topology in next try in 1 second.", n->id, n->ip, n->port);
            n->sendClusterTopologyOnNextConnect = true;
        }else{
            RedisModule_Log(mr_staticCtx, "warning", "Got bad hello response from %s (%s:%d), will try again in 1 second, %s.", n->id, n->ip, n->port, reply->str);
        }
        n->resendHelloEvent = MR_EventLoopAddTaskWithDelay(MR_ClusterResendHelloMessage, n, RETRY_INTERVAL);
        return;
    }

    bool resendPendingMessages = true;;

    if(n->runId){
        if(strcmp(n->runId, reply->str) != 0){
            /* here we know that the shard has crashed
             * There is no need to send pending messages
             */
            resendPendingMessages = false;
            n->msgId = 0;
            mr_listEmpty(n->pendingMessages);
        }
        MR_FREE(n->runId);
    }

    if(resendPendingMessages){
        // we need to send pending messages to the shard
        mr_listIter* iter = mr_listGetIterator(n->pendingMessages, AL_START_HEAD);
        mr_listNode *node = NULL;
        while((node = mr_listNext(iter)) != NULL){
            NodeSendMsg* sentMsg = mr_listNodeValue(node);
            ++sentMsg->retries;
            if(MSG_MAX_RETRIES == 0 || sentMsg->retries < MSG_MAX_RETRIES){
                MR_ClusterSendMsgToNodeInternal(n, sentMsg);
            }else{
                RedisModule_Log(mr_staticCtx, "warning", "Gave up of message because failed to send it for more than %d time", MSG_MAX_RETRIES);
                mr_listDelNode(n->pendingMessages, node);
            }
        }
        mr_listReleaseIterator(iter);
    }
    n->runId = MR_STRDUP(reply->str);
    n->status = NodeStatus_Connected;

    /* Flush any queued (not yet in-flight) messages. */
    if (mr_listLength(n->outboxMessages) > 0 && !n->flushScheduled) {
        n->flushScheduled = true;
        MR_EventLoopAddTask(MR_ClusterFlushOutbox, n);
    }
}

static void MR_ClusterReconnect(void* ctx){
    Node* n = ctx;
    n->reconnectEvent = NULL;
    MR_ConnectToShard(n);
}

static void MR_ClusterAsyncDisconnect(void* ctx){
    Node* n = ctx;
    redisAsyncFree(n->c);
}

static void MR_ClusterOnDisconnectCallback(const struct redisAsyncContext* c, int status){
    RedisModule_Log(mr_staticCtx, "warning", "disconnected : %s:%d, status : %d, %s.", c->c.tcp.host, c->c.tcp.port, status,
            c->data ? "will try to reconnect later" : "no context data");
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    n->status = NodeStatus_Disconnected;
    n->c = NULL;
    n->reconnectEvent = MR_EventLoopAddTaskWithDelay(MR_ClusterReconnect, n, RETRY_INTERVAL);
}

char* getConfigValue(RedisModuleCtx *ctx, const char* confName){
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "config", "cc", "get",
            confName);
    RedisModule_Assert(
            RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY);
    if (RedisModule_CallReplyLength(rep) == 0) {
        RedisModule_FreeCallReply(rep);
        return NULL;
    }
    RedisModule_Assert(RedisModule_CallReplyLength(rep) == 2);
    RedisModuleCallReply *valueRep = RedisModule_CallReplyArrayElement(rep, 1);
    RedisModule_Assert(
            RedisModule_CallReplyType(valueRep) == REDISMODULE_REPLY_STRING);
    size_t len;
    const char* valueRepCStr = RedisModule_CallReplyStringPtr(valueRep, &len);

    char* res = MR_CALLOC(1, len + 1);
    memcpy(res, valueRepCStr, len);

    RedisModule_FreeCallReply(rep);

    return res;
}

static int checkTLS(char** client_key, char** client_cert, char** ca_cert, char** key_pass){
    int ret = 1;
    RedisModule_ThreadSafeContextLock(mr_staticCtx);
    char* clusterTls = NULL;
    char* tlsPort = NULL;

    clusterTls = getConfigValue(mr_staticCtx, "tls-cluster");
    if (!clusterTls || strcmp(clusterTls, "yes")) {
        tlsPort = getConfigValue(mr_staticCtx, "tls-port");
        if (!tlsPort || !strcmp(tlsPort, "0")) {
            ret = 0;
            goto done;
        }
    }

    *client_key = getConfigValue(mr_staticCtx, "tls-key-file");
    *client_cert = getConfigValue(mr_staticCtx, "tls-cert-file");
    *ca_cert = getConfigValue(mr_staticCtx, "tls-ca-cert-file");
    *key_pass = getConfigValue(mr_staticCtx, "tls-key-file-pass");

    if (!*client_key || !*client_cert || !*ca_cert) {
        ret = 0;
        if (*client_key) {
            MR_FREE(*client_key);
        }
        if (*client_cert) {
            MR_FREE(*client_cert);
        }
        if (*ca_cert) {
            MR_FREE(*client_cert);
        }
    }

done:
    if (clusterTls) {
        MR_FREE(clusterTls);
    }
    if (tlsPort) {
        MR_FREE(tlsPort);
    }
    RedisModule_ThreadSafeContextUnlock(mr_staticCtx);
    return ret;
}

/* Callback for passing a keyfile password stored as an sds to OpenSSL */
static int MR_TlsPasswordCallback(char *buf, int size, int rwflag, void *u) {
    const char *pass = u;
    size_t pass_len;

    if (!pass) return -1;
    pass_len = strlen(pass);
    if (pass_len > (size_t) size) return -1;
    memcpy(buf, pass, pass_len);

    return (int) pass_len;
}

SSL_CTX* MR_CreateSSLContext(const char *cacert_filename,
							 const char *cert_filename,
							 const char *private_key_filename,
							 const char *private_key_pass,
							 redisSSLContextError *error)
{
    SSL_CTX *ssl_ctx = SSL_CTX_new(SSLv23_client_method());
    if (!ssl_ctx) {
        if (error) *error = REDIS_SSL_CTX_CREATE_FAILED;
        goto error;
    }

    SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);

    /* always set the callback, otherwise if key is encrypted and password
     * was not given, we will be waiting on stdin. */
    SSL_CTX_set_default_passwd_cb(ssl_ctx, MR_TlsPasswordCallback);
    SSL_CTX_set_default_passwd_cb_userdata(ssl_ctx, (void *) private_key_pass);

    if ((cert_filename != NULL && private_key_filename == NULL) ||
            (private_key_filename != NULL && cert_filename == NULL)) {
        if (error) *error = REDIS_SSL_CTX_CERT_KEY_REQUIRED;
        goto error;
    }

    if (cacert_filename) {
        if (!SSL_CTX_load_verify_locations(ssl_ctx, cacert_filename, NULL)) {
            if (error) *error = REDIS_SSL_CTX_CA_CERT_LOAD_FAILED;
            goto error;
        }
    }

    if (cert_filename) {
        if (!SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_filename)) {
            if (error) *error = REDIS_SSL_CTX_CLIENT_CERT_LOAD_FAILED;
            goto error;
        }
        if (!SSL_CTX_use_PrivateKey_file(ssl_ctx, private_key_filename, SSL_FILETYPE_PEM)) {
            if (error) *error = REDIS_SSL_CTX_PRIVATE_KEY_LOAD_FAILED;
            goto error;
        }
    }

    return ssl_ctx;

error:
    if (ssl_ctx) SSL_CTX_free(ssl_ctx);
    return NULL;
}

static void MR_OnConnectCallback(const struct redisAsyncContext* c, int status){
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    if(status == -1){
        // connection failed lets try again
        n->c = NULL;
        n->reconnectEvent = MR_EventLoopAddTaskWithDelay(MR_ClusterReconnect, n, RETRY_INTERVAL);
    }else{
        char* client_cert = NULL;
        char* client_key = NULL;
        char* ca_cert = NULL;
        char* key_file_pass = NULL;
        if(checkTLS(&client_key, &client_cert, &ca_cert, &key_file_pass)){
            redisSSLContextError ssl_error = 0;
            SSL_CTX *ssl_context = MR_CreateSSLContext(ca_cert, client_cert, client_key, key_file_pass, &ssl_error);
            MR_FREE(client_key);
            MR_FREE(client_cert);
            MR_FREE(ca_cert);
            if (key_file_pass) {
            	MR_FREE(key_file_pass);
            }
            if(ssl_context == NULL || ssl_error != 0) {
                RedisModule_Log(mr_staticCtx, "warning", "SSL context generation to %s:%d failed, will initiate retry.", c->c.tcp.host, c->c.tcp.port);
                // disconnect async, its not possible to free redisAsyncContext here
                MR_EventLoopAddTask(MR_ClusterAsyncDisconnect, n);
                return;
            }
            SSL *ssl = SSL_new(ssl_context);
            SSL_CTX_free(ssl_context);
            const redisContextFuncs *old_callbacks = c->c.funcs;
            if (redisInitiateSSL((redisContext *)(&c->c), ssl) != REDIS_OK) {
                const char *err = "Unknown error";
                if (c->c.err != 0) {
                    err = c->c.errstr;
                }
                // This is a temporary fix to the bug describe on https://github.com/redis/hiredis/issues/1233.
                // In case of SSL initialization failure. We need to reset the callbacks value, as the `redisInitiateSSL`
                // function will not do it for us.
                ((struct redisAsyncContext*)c)->c.funcs = old_callbacks;
                RedisModule_Log(mr_staticCtx, "warning", "SSL auth to %s:%d failed, will initiate retry. %s.", c->c.tcp.host, c->c.tcp.port, err);
                // disconnect async, its not possible to free redisAsyncContext here
                MR_EventLoopAddTask(MR_ClusterAsyncDisconnect, n);
                return;
            }
        }

        RedisModule_Log(mr_staticCtx, "notice", "connected : %s:%d, status = %d\r\n", c->c.tcp.host, c->c.tcp.port, status);

        if (n->password){
            /* If password is provided to us we will use it (it means it was given to us with clusterset) */
            redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s", n->password);
        } else if (RedisModule_GetInternalSecret && !MR_IsEnterpriseBuild()) {
            /* OSS deployment that support internal secret, lets use it. */
            RedisModule_ThreadSafeContextLock(mr_staticCtx);
            size_t len;
            const char *secret = RedisModule_GetInternalSecret(mr_staticCtx, &len);
            RedisModule_Assert(secret);
            redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s %b", "internal connection", secret, len);
            RedisModule_ThreadSafeContextUnlock(mr_staticCtx);
        }

        if(n->sendClusterTopologyOnNextConnect && clusterCtx.CurrCluster->clusterSetCommand){
            RedisModule_Log(mr_staticCtx, "notice", "Sending cluster topology to %s (%s:%d) after reconnect", n->id, n->ip, n->port);
            clusterCtx.CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = MR_STRDUP(n->id);
            redisAsyncCommandArgv((redisAsyncContext*)c, NULL, NULL, clusterCtx.CurrCluster->clusterSetCommandSize, (const char**)clusterCtx.CurrCluster->clusterSetCommand, NULL);
            MR_FREE(clusterCtx.CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX]);
            clusterCtx.CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = NULL;
            n->sendClusterTopologyOnNextConnect = false;
        }
        redisAsyncCommand((redisAsyncContext*)c, MR_HelloResponseArrived, n, CLUSTER_HELLO_COMMAND);
        n->status = NodeStatus_HelloSent;
    }
}

static void MR_ConnectToShard(Node* n){
    redisAsyncContext* c = redisAsyncConnect(n->ip, n->port);
    if (!c) {
        RedisModule_Log(mr_staticCtx, "warning", "Got NULL async connection");
        return;
    }
    if (c->err) {
        /* Let *c leak for now... */
        RedisModule_Log(mr_staticCtx, "warning", "Error: %s\n", c->errstr);
        return;
    }
    c->data = n;
    n->c = c;
    redisLibeventAttach(c, MR_EventLoopGet());
    redisAsyncSetConnectCallback(c, MR_OnConnectCallback);
    redisAsyncSetDisconnectCallback(c, MR_ClusterOnDisconnectCallback);
}

static void MR_ClusterConnectToShards(){
    mr_dictIterator *iter = mr_dictGetIterator(clusterCtx.CurrCluster->nodes);
    mr_dictEntry *entry = NULL;
    while((entry = mr_dictNext(iter))){
        Node* n = mr_dictGetVal(entry);
        if(n->isMe){
            continue;
        }
        if (n->status == NodeStatus_Uninitialized) {
            MR_ConnectToShard(n);
            n->status = NodeStatus_Disconnected;
        }
    }
    mr_dictReleaseIterator(iter);
}

static void MR_NodeFreeInternals(Node* n){
    if (n->reconnectEvent) {
        MR_EventLoopDelayTaskCancel(n->reconnectEvent);
        n->reconnectEvent = NULL;
    }
    if (n->resendHelloEvent) {
        MR_EventLoopDelayTaskCancel(n->resendHelloEvent);
        n->resendHelloEvent = NULL;
    }
    MR_FREE(n->id);
    MR_FREE(n->ip);
    if(n->unixSocket){
        MR_FREE(n->unixSocket);
    }
    if(n->password){
        MR_FREE(n->password);
    }
    if(n->runId){
        MR_FREE(n->runId);
    }
    if(n->c){
        redisAsyncFree(n->c);
    }
    mr_listRelease(n->pendingMessages);
    MR_ClusterFreeNodeMsgList(n->outboxMessages);
    mr_listRelease(n->slotRanges);
    MR_FREE(n);
}

static void MR_NodeFree(Node* n){
    if(n->c){
        n->c->data = NULL;
    }
    n->status = NodeStatus_Free;
    MR_NodeFreeInternals(n);
}

static void MR_ClusterFree(){
    if(clusterCtx.CurrCluster->myId){
        MR_FREE(clusterCtx.CurrCluster->myId);
    }
    if(clusterCtx.CurrCluster->nodes){
        mr_dictIterator *iter = mr_dictGetIterator(clusterCtx.CurrCluster->nodes);
        mr_dictEntry *entry = NULL;
        while((entry = mr_dictNext(iter))){
            Node* n = mr_dictGetVal(entry);
            MR_NodeFree(n);
        }
        mr_dictReleaseIterator(iter);
        mr_dictRelease(clusterCtx.CurrCluster->nodes);
    }

    if(clusterCtx.CurrCluster->clusterSetCommand){
        for(int i = 0 ; i < clusterCtx.CurrCluster->clusterSetCommandSize ; ++i){
            if(clusterCtx.CurrCluster->clusterSetCommand[i]){
                MR_FREE(clusterCtx.CurrCluster->clusterSetCommand[i]);
            }
        }
        MR_FREE(clusterCtx.CurrCluster->clusterSetCommand);
    }

    MR_FREE(clusterCtx.CurrCluster);
    clusterCtx.CurrCluster = NULL;
    clusterCtx.minSlot = 0;
    clusterCtx.maxSlot = 0;
    clusterCtx.clusterSize = 1;
    memset(clusterCtx.myId, '0', REDISMODULE_NODE_ID_LEN);
}

static Node* MR_GetNode(const char* id){
    mr_dictEntry *entry = mr_dictFind(clusterCtx.CurrCluster->nodes, id);
    Node* n = NULL;
    if(entry){
        n = mr_dictGetVal(entry);
    }
    return n;
}

static Node* MR_CreateNode(const char* id, const char* ip, unsigned short port, const char* password, const char* unixSocket, long long minSlot, long long maxSlot){
    RedisModule_Assert(!MR_GetNode(id));

    mr_list* slotRanges = mr_listCreate();
    mr_listSetFreeMethod(slotRanges, FreeSlotRange);
    if (minSlot <= maxSlot) {
        mr_listAddNodeTail(slotRanges, NewSlotRange(minSlot, maxSlot));
    }

    mr_list* pendingMessages = mr_listCreate();
    mr_listSetFreeMethod(pendingMessages, MR_ClusterFreeNodeMsg);

    mr_list* outboxMessages = mr_listCreate();
    /* outboxMessages holds NodeSendMsg that are not yet in-flight; we manage their lifetime */
    mr_listSetFreeMethod(outboxMessages, NULL);

    Node* n = MR_ALLOC(sizeof(*n));
    *n = (Node){
            .id = MR_STRDUP(id),
            .ip = MR_STRDUP(ip),
            .port = port,
            .password = password ? MR_STRDUP(password) : NULL,
            .unixSocket = unixSocket ? MR_STRDUP(unixSocket) : NULL,
            .c = NULL,
            .msgId = 0,
            .pendingMessages = pendingMessages,
            .outboxMessages = outboxMessages,
            .flushScheduled = false,
            .slotRanges = slotRanges,
            .isMe = false,
            .status = NodeStatus_Uninitialized,
            .sendClusterTopologyOnNextConnect = false,
            .runId = NULL,
            .reconnectEvent = NULL,
            .resendHelloEvent = NULL,
    };
    mr_dictAdd(clusterCtx.CurrCluster->nodes, n->id, n);
    n->isMe = strcmp(id, clusterCtx.CurrCluster->myId) == 0;

    return n;
}

static void MR_RefreshClusterData(){
    if(clusterCtx.CurrCluster){
        MR_ClusterFree();
    }

    RedisModule_Log(mr_staticCtx, "notice", "Got cluster refresh command");

    if(!(RedisModule_GetContextFlags(mr_staticCtx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        return;
    }

    clusterCtx.CurrCluster = MR_CALLOC(1, sizeof(*clusterCtx.CurrCluster));

    // generate runID
    RedisModule_GetRandomHexChars(clusterCtx.CurrCluster->runId, RUN_ID_SIZE);
    clusterCtx.CurrCluster->runId[RUN_ID_SIZE] = '\0';

    clusterCtx.CurrCluster->clusterSetCommand = NULL;
    clusterCtx.CurrCluster->clusterSetCommandSize = 0;

    clusterCtx.CurrCluster->myId = MR_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    memcpy(clusterCtx.CurrCluster->myId, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN);
    clusterCtx.CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
    memcpy(clusterCtx.myId, clusterCtx.CurrCluster->myId, REDISMODULE_NODE_ID_LEN + 1);
    clusterCtx.CurrCluster->nodes = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);

    RedisModule_ThreadSafeContextLock(mr_staticCtx);
    RedisModuleCallReply *allSlotsReply = RedisModule_Call(mr_staticCtx, "cluster", "c", "slots");
    RedisModule_ThreadSafeContextUnlock(mr_staticCtx);

    RedisModule_Assert(RedisModule_CallReplyType(allSlotsReply) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(allSlotsReply) ; ++i){
        RedisModuleCallReply *slotRangeReply = RedisModule_CallReplyArrayElement(allSlotsReply, i);

        RedisModuleCallReply *minSlotReply = RedisModule_CallReplyArrayElement(slotRangeReply, 0);
        RedisModule_Assert(RedisModule_CallReplyType(minSlotReply) == REDISMODULE_REPLY_INTEGER);
        long long minSlot = RedisModule_CallReplyInteger(minSlotReply);

        RedisModuleCallReply *maxSlotReply = RedisModule_CallReplyArrayElement(slotRangeReply, 1);
        RedisModule_Assert(RedisModule_CallReplyType(maxSlotReply) == REDISMODULE_REPLY_INTEGER);
        long long maxSlot = RedisModule_CallReplyInteger(maxSlotReply);

        RedisModuleCallReply *nodeDetailsReply = RedisModule_CallReplyArrayElement(slotRangeReply, 2);
        RedisModule_Assert(RedisModule_CallReplyType(nodeDetailsReply) == REDISMODULE_REPLY_ARRAY);
        RedisModule_Assert(RedisModule_CallReplyLength(nodeDetailsReply) >= 3);
        RedisModuleCallReply *nodeipReply = RedisModule_CallReplyArrayElement(nodeDetailsReply, 0);
        RedisModuleCallReply *nodeidReply = RedisModule_CallReplyArrayElement(nodeDetailsReply, 2);
        size_t idLen;
        size_t ipLen;
        const char* id = RedisModule_CallReplyStringPtr(nodeidReply,&idLen);
        const char* ip = RedisModule_CallReplyStringPtr(nodeipReply,&ipLen);

        char nodeId[REDISMODULE_NODE_ID_LEN + 1];
        memcpy(nodeId, id, REDISMODULE_NODE_ID_LEN);
        nodeId[REDISMODULE_NODE_ID_LEN] = '\0';

        char nodeIp[ipLen + 1];
        memcpy(nodeIp, ip, ipLen);
        nodeIp[ipLen] = '\0';

        // We need to get the port using the `RedisModule_GetClusterNodeInfo` API because on 7.2
        // invoking `cluster slot` from RM_Call will always return the none tls port.
        // For for information refer to: https://github.com/redis/redis/pull/12233
        int port = 0;
        RedisModule_ThreadSafeContextLock(mr_staticCtx);
        RedisModule_GetClusterNodeInfo(mr_staticCtx, nodeId, NULL, NULL, &port, NULL);
        RedisModule_ThreadSafeContextUnlock(mr_staticCtx);


        Node* n = MR_GetNode(nodeId);
        if(!n){
            /* If we have internal secret we will ignore the clusterCtx.password, we do not need it. */
            n = MR_CreateNode(nodeId, nodeIp, (unsigned short)port, RedisModule_GetInternalSecret ? NULL : clusterCtx.password, NULL, minSlot, maxSlot);
        }

        if (n->isMe) {
            clusterCtx.minSlot = minSlot;
            clusterCtx.maxSlot = maxSlot;
        }

        for(int k = minSlot ; k <= maxSlot ; ++k){
            clusterCtx.CurrCluster->slots[k] = n;
        }
    }
    RedisModule_FreeCallReply(allSlotsReply);
    clusterCtx.clusterSize = mr_dictSize(clusterCtx.CurrCluster->nodes);
    mr_dictEmpty(clusterCtx.nodesMsgIds, NULL);
}

static void MR_SetClusterData(RedisModuleString** argv, int argc){
    if(clusterCtx.CurrCluster){
        MR_ClusterFree();
    }

    RedisModule_Log(mr_staticCtx, "notice", "Got cluster set command");

    if(argc < 10){
        RedisModule_Log(mr_staticCtx, "warning", "Could not parse cluster set arguments");
        return;
    }

    clusterCtx.CurrCluster = MR_CALLOC(1, sizeof(*clusterCtx.CurrCluster));

    // generate runID
    RedisModule_GetRandomHexChars(clusterCtx.CurrCluster->runId, RUN_ID_SIZE);
    clusterCtx.CurrCluster->runId[RUN_ID_SIZE] = '\0';

    clusterCtx.CurrCluster->clusterSetCommand = MR_ALLOC(sizeof(char*) * argc);
    clusterCtx.CurrCluster->clusterSetCommandSize = argc;

    clusterCtx.CurrCluster->clusterSetCommand[0] = MR_STRDUP(CLUSTER_SET_FROM_SHARD_COMMAND);

    for(int i = 1 ; i < argc ; ++i){
        if(i == CLUSTER_SET_MY_ID_INDEX){
            clusterCtx.CurrCluster->clusterSetCommand[i] = NULL;
            continue;
        }
        const char* arg = RedisModule_StringPtrLen(argv[i], NULL);
        clusterCtx.CurrCluster->clusterSetCommand[i] = MR_STRDUP(arg);
    }

    size_t myIdLen;
    RedisModule_Assert(CLUSTER_SET_MY_ID_INDEX < argc);
    const char* myId = RedisModule_StringPtrLen(argv[CLUSTER_SET_MY_ID_INDEX], &myIdLen);
    clusterCtx.CurrCluster->myId = MR_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    size_t zerosPadding = REDISMODULE_NODE_ID_LEN - myIdLen;
    memset(clusterCtx.CurrCluster->myId, '0', zerosPadding);
    memcpy(clusterCtx.CurrCluster->myId + zerosPadding, myId, myIdLen);
    clusterCtx.CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
    memcpy(clusterCtx.myId, clusterCtx.CurrCluster->myId, REDISMODULE_NODE_ID_LEN + 1);

    clusterCtx.CurrCluster->nodes = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);

    size_t index = 7;
    const char *token = RedisModule_StringPtrLen(argv[index], NULL);
    bool hasReplication = strcasecmp(token, "HASREPLICATION") == 0;
    if (hasReplication) {
        index++;
        RedisModule_Assert(index < argc);
        token = RedisModule_StringPtrLen(argv[index], NULL);
    }
    RedisModule_Assert(strcasecmp(token, "RANGES") == 0);
    index++;
    RedisModule_Assert(index < argc);
    long long numOfRanges;
    RedisModule_Assert(RedisModule_StringToLongLong(argv[index], &numOfRanges) == REDISMODULE_OK);
    if (hasReplication) {
        RedisModule_Assert(numOfRanges % 2 == 0);
    }
    index++;

    for (size_t j = 0 ; j < numOfRanges ; ++j){
        RedisModule_Assert(index < argc);
        token = RedisModule_StringPtrLen(argv[index], NULL);
        RedisModule_Assert(strcasecmp(token, "SHARD") == 0);

        index++;
        RedisModule_Assert(index < argc);
        size_t shardIdLen;
        const char* shardId = RedisModule_StringPtrLen(argv[index], &shardIdLen);
        char realId[REDISMODULE_NODE_ID_LEN + 1];
        size_t zerosPadding = REDISMODULE_NODE_ID_LEN - shardIdLen;
        memset(realId, '0', zerosPadding);
        memcpy(realId + zerosPadding, shardId, shardIdLen);
        realId[REDISMODULE_NODE_ID_LEN] = '\0';
        index++;

        long long minSlot = 0, maxSlot = -1;  // min > max indicates a no-hslots range (i.e., used for slotless shards)
        RedisModule_Assert(index < argc);
        token = RedisModule_StringPtrLen(argv[index], NULL);
        if (strcasecmp(token, "SLOTRANGE") == 0) {
            index++;
            RedisModule_Assert(index < argc);
            RedisModule_Assert(RedisModule_StringToLongLong(argv[index++], &minSlot) == REDISMODULE_OK);
            RedisModule_Assert(index < argc);
            RedisModule_Assert(RedisModule_StringToLongLong(argv[index++], &maxSlot) == REDISMODULE_OK);
        }

        RedisModule_Assert(index < argc);
        token = RedisModule_StringPtrLen(argv[index], NULL);
        RedisModule_Assert(strcasecmp(token, "ADDR") == 0);
        index++;

        RedisModule_Assert(index < argc);
        const char* addr = RedisModule_StringPtrLen(argv[index++], NULL);
        char* passEnd = strstr(addr, "@");
        RedisModule_Assert(passEnd != NULL);
        size_t passSize = passEnd - addr;
        char password[passSize + 1];
        memcpy(password, addr, passSize);
        password[passSize] = '\0';

        addr = passEnd + 1;

        if (addr[0] == '[') {
            addr += 1; /* skip ipv6 opener `[` */
        }

        /* Find last `:` */
        const char *ipEnd = strrchr(addr, ':');
        RedisModule_Assert(ipEnd != NULL);

        size_t ipSize = ipEnd - addr;
        if (addr[ipSize - 1] == ']') {
            --ipSize; /* Skip ipv6 closer `]` */
        }

        char ip[ipSize + 1];
        memcpy(ip, addr, ipSize);
        ip[ipSize] = '\0';
        unsigned short port = (unsigned short)atoi(ipEnd + 1);

        if (index >= argc)
            break;
        token = RedisModule_StringPtrLen(argv[index], NULL);
        if (strcasecmp(token, "UNIXADDR") == 0)
            index += 2; // Ignore it and its value

        if (index >= argc)
            break;
        token = RedisModule_StringPtrLen(argv[index], NULL);
        if (strcasecmp(token, "MASTER") != 0)
            continue; // Ignore non-master nodes

        // All info is parsed, create a new node or update an existing one
        Node* aMasterNode = MR_GetNode(realId);
        if(!aMasterNode){
            aMasterNode = MR_CreateNode(realId, ip, port, password, NULL, minSlot, maxSlot);
        } else {
            RedisModule_Assert(minSlot <= maxSlot);  // slotless nodes are only created (above)
            mr_listAddNodeTail(aMasterNode->slotRanges, NewSlotRange(minSlot, maxSlot));
        }

        for(int k = minSlot ; k <= maxSlot ; ++k){
            clusterCtx.CurrCluster->slots[k] = aMasterNode;
        }

        if (aMasterNode->isMe) {
            clusterCtx.minSlot = minSlot;
            clusterCtx.maxSlot = maxSlot;
        }

        index++;
    }
    clusterCtx.clusterSize = mr_dictSize(clusterCtx.CurrCluster->nodes);
    mr_dictEmpty(clusterCtx.nodesMsgIds, NULL);
}

/* runs in the event loop so its safe to update cluster
 * topology here */
static void MR_ClusterRefreshFromCommand(void* ctx){
    RedisModuleBlockedClient* bc = ctx;
    MR_RefreshClusterData();
    RedisModuleCtx* rCtx = RedisModule_GetThreadSafeContext(bc);
    RedisModule_ReplyWithSimpleString(rCtx, "OK");
    RedisModule_FreeThreadSafeContext(rCtx);
    RedisModule_UnblockClient(bc, NULL);
}

/* runs in the event loop so its safe to update cluster
 * topology here */
static void MR_ClusterSetFromCommand(void* ctx){
    ClusterSetCtx* csCtx = ctx;
    if (!clusterCtx.CurrCluster || csCtx->force) {
        MR_SetClusterData(csCtx->argv, csCtx->argc);
    }
    RedisModule_UnblockClient(csCtx->bc, csCtx);
}

static int MR_ClusterSetUnblock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    ClusterSetCtx* csCtx = RedisModule_GetBlockedClientPrivateData(ctx);
    for(size_t i = 0 ; i < csCtx->argc ; ++i){
        RedisModule_FreeString(NULL, csCtx->argv[i]);
    }
    MR_FREE(csCtx->argv);
    MR_FREE(csCtx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static int MR_ClusterSetInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool force){
    ClusterSetCtx* csCtx = MR_ALLOC(sizeof(*csCtx));
    csCtx->bc = RedisModule_BlockClient(ctx, MR_ClusterSetUnblock, NULL, NULL, 0);
    csCtx->argv = argv;
    csCtx->argc = argc;
    csCtx->force = force;
    MR_EventLoopAddTask(MR_ClusterSetFromCommand, csCtx);
    return REDISMODULE_OK;
}

static int MR_ClusterRefresh(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    MR_EventLoopAddTask(MR_ClusterRefreshFromCommand, bc);
    return REDISMODULE_OK;
}

static int MR_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 10){
        RedisModule_ReplyWithError(ctx, "Could not parse cluster set arguments");
        return REDISMODULE_OK;
    }
    // we must copy argv because if the client will disconnect the redis will free it
    RedisModuleString **argvNew = MR_ALLOC(sizeof(RedisModuleString *) * argc);
    for(size_t i = 0 ; i < argc ; ++i){
        argvNew[i] = RedisModule_HoldString(NULL, argv[i]);
    }
    MR_ClusterSetInternal(ctx, argvNew, argc, true);
    return REDISMODULE_OK;
}

static int MR_ClusterSetFromShard(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 10){
        RedisModule_ReplyWithError(ctx, "Could not parse cluster set arguments");
        return REDISMODULE_OK;
    }
    // we must copy argv because if the client will disconnect the redis will free it
    RedisModuleString **argvNew = MR_ALLOC(sizeof(RedisModuleString *) * argc);
    for(size_t i = 0 ; i < argc ; ++i){
        argvNew[i] = RedisModule_HoldString(NULL, argv[i]);
    }
    MR_ClusterSetInternal(ctx, argvNew, argc, false);
    return REDISMODULE_OK;
}

int MR_ClusterHello(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(!clusterCtx.CurrCluster){
        RedisModule_Log(mr_staticCtx, "warning", "Got hello msg while cluster is NULL");
        return RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" NULL cluster state on hello msg");
    }
    RedisModule_ReplyWithStringBuffer(ctx, clusterCtx.CurrCluster->runId, strlen(clusterCtx.CurrCluster->runId));
    return REDISMODULE_OK;
}

/* run on the event loop */
static void MR_ClusterInnerCommunicationMsgRun(void* ctx) {
    InboundMsgCtx* msgCtx = ctx;
    if(!clusterCtx.CurrCluster){
        RedisModule_Log(mr_staticCtx, "warning", "Got msg from another shard while cluster is NULL");
        MR_FREE(msgCtx->msgBuf);
        MR_FREE(msgCtx);
        return;
    }

    if(!MR_IsClusterInitialize()){
        RedisModule_Log(mr_staticCtx, "warning", "Got msg from another shard while cluster is not initialized");
        MR_FREE(msgCtx->msgBuf);
        MR_FREE(msgCtx);
        return;
    }

    if (msgCtx->functionId < 0 || msgCtx->functionId >= array_len(clusterCtx.callbacks)) {
        RedisModule_Log(mr_staticCtx, "warning", "bad function id given");
        MR_FREE(msgCtx->msgBuf);
        MR_FREE(msgCtx);
        return;
    }

    char combinedId[msgCtx->senderIdLen + msgCtx->senderRunIdLen + 1]; // +1 is for '\0'
    memcpy(combinedId, msgCtx->senderIdStr, msgCtx->senderIdLen);
    memcpy(combinedId + msgCtx->senderIdLen, msgCtx->senderRunIdStr, msgCtx->senderRunIdLen);
    combinedId[msgCtx->senderIdLen + msgCtx->senderRunIdLen] = '\0';

    mr_dictEntry* entity = mr_dictFind(clusterCtx.nodesMsgIds, combinedId);
    long long currId = -1;
    if(entity){
        currId = mr_dictGetSignedIntegerVal(entity);
    }else{
        entity = mr_dictAddRaw(clusterCtx.nodesMsgIds, (char*)combinedId, NULL);
    }
    if(msgCtx->msgId <= currId){
        /* Duplicate message ignored (most commonly due to retries). */
        MR_FREE(msgCtx->msgBuf);
        MR_FREE(msgCtx);
        return;
    }
    mr_dictSetSignedIntegerVal(entity, msgCtx->msgId);

    RedisModuleString* payload = RedisModule_CreateString(mr_staticCtx, msgCtx->msgBuf, msgCtx->msgLen);
    clusterCtx.callbacks[msgCtx->functionId](mr_staticCtx, msgCtx->senderIdStr, 0, payload);
    RedisModule_FreeString(mr_staticCtx, payload);

    MR_FREE(msgCtx->msgBuf);
    MR_FREE(msgCtx);
    return;
}

int MR_NetworkTestCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    MR_ClusterCopyAndSendMsg(NULL, clusterCtx.networkTestMsgReciever, "test msg", strlen("test msg"));
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static void MR_ClusterInfo(void* pd) {
#define NO_CLUSTER_MODE_REPLY "no cluster mode"
    RedisModuleBlockedClient* bc = pd;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(bc);
    if(!clusterCtx.CurrCluster){
        RedisModule_ReplyWithStringBuffer(ctx, NO_CLUSTER_MODE_REPLY, strlen(NO_CLUSTER_MODE_REPLY));
        RedisModule_UnblockClient(bc, NULL);
        return;
    }
    RedisModule_ReplyWithArray(ctx, 5);
    RedisModule_ReplyWithStringBuffer(ctx, "MyId", strlen("MyId"));
    RedisModule_ReplyWithStringBuffer(ctx, clusterCtx.CurrCluster->myId, strlen(clusterCtx.CurrCluster->myId));
    RedisModule_ReplyWithStringBuffer(ctx, "MyRunId", strlen("MyRunId"));
    RedisModule_ReplyWithStringBuffer(ctx, clusterCtx.CurrCluster->runId, strlen(clusterCtx.CurrCluster->runId));
    RedisModule_ReplyWithArray(ctx, mr_dictSize(clusterCtx.CurrCluster->nodes));
    mr_dictIterator *iter = mr_dictGetIterator(clusterCtx.CurrCluster->nodes);
    mr_dictEntry *entry = NULL;
    while((entry = mr_dictNext(iter))){
        Node* n = mr_dictGetVal(entry);
        RedisModule_ReplyWithArray(ctx, 18);
        RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
        RedisModule_ReplyWithStringBuffer(ctx, n->id, strlen(n->id));
        RedisModule_ReplyWithStringBuffer(ctx, "ip", strlen("ip"));
        RedisModule_ReplyWithStringBuffer(ctx, n->ip, strlen(n->ip));
        RedisModule_ReplyWithStringBuffer(ctx, "port", strlen("port"));
        RedisModule_ReplyWithLongLong(ctx, n->port);
        RedisModule_ReplyWithStringBuffer(ctx, "unixSocket", strlen("unixSocket"));
        if(n->unixSocket){
            RedisModule_ReplyWithStringBuffer(ctx, n->unixSocket, strlen(n->unixSocket));
        }else{
            RedisModule_ReplyWithStringBuffer(ctx, "None", strlen("None"));
        }
        RedisModule_ReplyWithStringBuffer(ctx, "runid", strlen("runid"));
        if(n->runId){
            RedisModule_ReplyWithStringBuffer(ctx, n->runId, strlen(n->runId));
        }else{
            if(n->isMe){
                const char* runId = clusterCtx.CurrCluster->runId;
                RedisModule_ReplyWithStringBuffer(ctx, runId, strlen(runId));
            }else{
                RedisModule_ReplyWithNull(ctx);
            }
        }

        RedisModule_Assert(n->slotRanges->len == 1);
        SlotRange* slotRange = mr_listFirst(n->slotRanges)->value;
        RedisModule_ReplyWithStringBuffer(ctx, "minHslot", strlen("minHslot"));
        RedisModule_ReplyWithLongLong(ctx, slotRange->minSlot);
        RedisModule_ReplyWithStringBuffer(ctx, "maxHslot", strlen("maxHslot"));
        RedisModule_ReplyWithLongLong(ctx, slotRange->maxSlot);
        RedisModule_ReplyWithStringBuffer(ctx, "pendingMessages", strlen("pendingMessages"));
        RedisModule_ReplyWithLongLong(ctx, mr_listLength(n->pendingMessages));

        RedisModule_ReplyWithStringBuffer(ctx, "status", strlen("status"));
        if (n->isMe) {
            RedisModule_ReplyWithStringBuffer(ctx, "connected", strlen("connected"));
        } else if (n->status == NodeStatus_Connected) {
            RedisModule_ReplyWithStringBuffer(ctx, "connected", strlen("connected"));
        } else if (n->status == NodeStatus_Disconnected) {
            RedisModule_ReplyWithStringBuffer(ctx, "disconnected", strlen("disconnected"));
        } else if (n->status == NodeStatus_HelloSent) {
            RedisModule_ReplyWithStringBuffer(ctx, "hello_sent", strlen("hello_sent"));
        } else if (n->status == NodeStatus_Free) {
            RedisModule_ReplyWithStringBuffer(ctx, "free", strlen("free"));
        } else if (n->status == NodeStatus_Uninitialized) {
            RedisModule_ReplyWithStringBuffer(ctx, "uninitialized", strlen("uninitialized"));
        }
    }
    mr_dictReleaseIterator(iter);
    RedisModule_FreeThreadSafeContext(ctx);
    RedisModule_UnblockClient(bc, NULL);
}

static int MR_ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    MR_EventLoopAddTask(MR_ClusterInfo, bc);
    return REDISMODULE_OK;
}

/* runs in the event loop so its safe to update cluster
 * topology here */
static void MR_ForceShardsConnection(void* ctx){
    RedisModuleBlockedClient* bc = ctx;
    MR_ClusterConnectToShards();
    RedisModuleCtx* rCtx = RedisModule_GetThreadSafeContext(bc);
    RedisModule_ReplyWithSimpleString(rCtx, "OK");
    RedisModule_FreeThreadSafeContext(rCtx);
    RedisModule_UnblockClient(bc, NULL);
}


int MR_ForceShardsConnectionCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    MR_EventLoopAddTask(MR_ForceShardsConnection, bc);
    return REDISMODULE_OK;
}

int MR_ClusterInnerCommunicationMsg(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 6){
        return RedisModule_WrongArity(ctx);
    }

    if(!clusterCtx.CurrCluster){
        return RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" NULL cluster state");
    }
    if(!MR_IsClusterInitialize()){
        return RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" Uninitialized cluster state");
    }

    long long msgId;
    if(RedisModule_StringToLongLong(argv[5], &msgId) != REDISMODULE_OK){
        return RedisModule_ReplyWithError(ctx, "Err bad message id");
    }
    long long functionId;
    if(RedisModule_StringToLongLong(argv[3], &functionId) != REDISMODULE_OK){
        return RedisModule_ReplyWithError(ctx, "Err bad function id");
    }
    if (functionId < 0 || functionId >= array_len(clusterCtx.callbacks)) {
        return RedisModule_ReplyWithError(ctx, "Err bad function id");
    }

    InboundMsgCtx* msgCtx = MR_ALLOC(sizeof(*msgCtx));
    size_t senderIdLen;
    const char* senderIdStr = RedisModule_StringPtrLen(argv[1], &senderIdLen);
    size_t senderRunIdLen;
    const char* senderRunIdStr = RedisModule_StringPtrLen(argv[2], &senderRunIdLen);
    size_t msgLen;
    const char* msgBuf = RedisModule_StringPtrLen(argv[4], &msgLen);

    msgCtx->senderIdLen = senderIdLen > REDISMODULE_NODE_ID_LEN ? REDISMODULE_NODE_ID_LEN : senderIdLen;
    memcpy(msgCtx->senderIdStr, senderIdStr, msgCtx->senderIdLen);
    msgCtx->senderIdStr[msgCtx->senderIdLen] = '\0';

    msgCtx->senderRunIdLen = senderRunIdLen > RUN_ID_SIZE ? RUN_ID_SIZE : senderRunIdLen;
    memcpy(msgCtx->senderRunIdStr, senderRunIdStr, msgCtx->senderRunIdLen);
    msgCtx->senderRunIdStr[msgCtx->senderRunIdLen] = '\0';

    msgCtx->msgId = msgId;
    msgCtx->functionId = functionId;
    msgCtx->msgLen = msgLen;
    msgCtx->msgBuf = MR_ALLOC(msgLen);
    memcpy(msgCtx->msgBuf, msgBuf, msgLen);

    /* Reply immediately to avoid per-message BlockClient/UnblockClient overhead under pipelining.
     * The actual handling (including duplicate suppression) is done asynchronously on the event loop. */
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    MR_EventLoopAddTask(MR_ClusterInnerCommunicationMsgRun, msgCtx);
    return REDISMODULE_OK;
}

int MR_ClusterInnerCommunicationMsgBatch(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    /* Format:
     *  <cmd> <senderId> <senderRunId> <num> (<functionId> <msg> <msgId>)... */
    if (argc < 5) {
        return RedisModule_WrongArity(ctx);
    }

    if(!clusterCtx.CurrCluster){
        return RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" NULL cluster state");
    }
    if(!MR_IsClusterInitialize()){
        return RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" Uninitialized cluster state");
    }

    long long numMsgs = 0;
    if (RedisModule_StringToLongLong(argv[3], &numMsgs) != REDISMODULE_OK || numMsgs < 0) {
        return RedisModule_ReplyWithError(ctx, "Err bad num messages");
    }
    if (argc != (4 + (int)numMsgs * 3)) {
        return RedisModule_WrongArity(ctx);
    }

    size_t senderIdLen;
    const char* senderIdStr = RedisModule_StringPtrLen(argv[1], &senderIdLen);
    size_t senderRunIdLen;
    const char* senderRunIdStr = RedisModule_StringPtrLen(argv[2], &senderRunIdLen);

    for (long long i = 0; i < numMsgs; i++) {
        const int base = 4 + (int)(i * 3);
        long long functionId;
        if(RedisModule_StringToLongLong(argv[base], &functionId) != REDISMODULE_OK){
            continue;
        }
        long long msgId;
        if(RedisModule_StringToLongLong(argv[base + 2], &msgId) != REDISMODULE_OK){
            continue;
        }
        size_t msgLen;
        const char* msgBuf = RedisModule_StringPtrLen(argv[base + 1], &msgLen);

        InboundMsgCtx* msgCtx = MR_ALLOC(sizeof(*msgCtx));

        msgCtx->senderIdLen = senderIdLen > REDISMODULE_NODE_ID_LEN ? REDISMODULE_NODE_ID_LEN : senderIdLen;
        memcpy(msgCtx->senderIdStr, senderIdStr, msgCtx->senderIdLen);
        msgCtx->senderIdStr[msgCtx->senderIdLen] = '\0';

        msgCtx->senderRunIdLen = senderRunIdLen > RUN_ID_SIZE ? RUN_ID_SIZE : senderRunIdLen;
        memcpy(msgCtx->senderRunIdStr, senderRunIdStr, msgCtx->senderRunIdLen);
        msgCtx->senderRunIdStr[msgCtx->senderRunIdLen] = '\0';

        msgCtx->msgId = msgId;
        msgCtx->functionId = functionId;
        msgCtx->msgLen = msgLen;
        msgCtx->msgBuf = MR_ALLOC(msgLen);
        memcpy(msgCtx->msgBuf, msgBuf, msgLen);

        MR_EventLoopAddTask(MR_ClusterInnerCommunicationMsgRun, msgCtx);
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int MR_ClusterIsMySlot(size_t slot) {
    if (RedisModule_ClusterCanAccessKeysInSlot != NULL)
        return RedisModule_ClusterCanAccessKeysInSlot(slot);
    if (RedisModule_ShardingGetSlotRange != NULL) {
        int first_slot, last_slot;
        RedisModule_ShardingGetSlotRange(&first_slot, &last_slot);
        return first_slot <= slot && last_slot >= slot;
    }
    // Fallback.
    return clusterCtx.minSlot <= slot && clusterCtx.maxSlot >= slot;
}

uint16_t MR_Crc16(const char *buf, int len);

static unsigned int keyHashSlot(const char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return MR_Crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return MR_Crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return MR_Crc16(key+s+1,e-s-1) & 0x3FFF;
}

size_t MR_ClusterGetSlotdByKey(const char* key, size_t len) {
    return keyHashSlot(key, len);
}

static void MR_NetworkTest(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    RedisModule_Log(ctx, "notice", "got a nextwork test msg");
}

int MR_ClusterInit(RedisModuleCtx* rctx, char *password) {
    MR_InitInnerCommBatchMax();
    clusterCtx.CurrCluster = NULL;
    clusterCtx.callbacks = array_new(MR_ClusterMessageReceiver, 10);
    clusterCtx.nodesMsgIds = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);
    clusterCtx.minSlot = 0;
    clusterCtx.maxSlot = 0;
    clusterCtx.clusterSize = 1;
    clusterCtx.isOss = true;
    clusterCtx.password = password ? MR_STRDUP(password) : NULL;
    memset(clusterCtx.myId, '0', REDISMODULE_NODE_ID_LEN);

    RedisModuleServerInfoData *info = RedisModule_GetServerInfo(rctx, "Server");
    const char *rlecVersion = RedisModule_ServerInfoGetFieldC(info, "rlec_version");
    if (rlecVersion) {
        clusterCtx.isOss = false;
    }
    RedisModule_FreeServerInfo(rctx, info);

    RedisModule_Log(rctx, "notice", "Detected redis %s", clusterCtx.isOss? "oss" : "enterprise");

    const char *command_flags = "readonly deny-script";
    if (MR_IsEnterpriseBuild()) {
        command_flags = "readonly deny-script _proxy-filtered";
    } else {
        if (RedisModule_GetInternalSecret) {
            /* We run at a version that supports internal commands, let use it. */
            command_flags = "readonly deny-script internal";
        }
    }

    if (!MR_IsEnterpriseBuild()) {
        /* Refresh cluster is only relevant for oss, also notice that refresh cluster
         * is not considered internal and should be performed by the user. */
        if (!RegisterRedisCommand(rctx, CLUSTER_REFRESH_COMMAND, MR_ClusterRefresh,
                                  "readonly deny-script", 0, 0, 0)) {
            return REDISMODULE_ERR;
        }
    }

    /* The CLUSTERSET should be visible in COMMAND LIST, otherwise the RAMP packer
     * will miss it and a module will not be notified in an enterprise cluster */
    if (!RegisterRedisCommand(rctx, CLUSTER_SET_COMMAND, MR_ClusterSet,
                            MR_IsEnterpriseBuild() ? "readonly deny-script _proxy-filtered" : "readonly deny-script",
                            0, 0, -1)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, CLUSTER_SET_FROM_SHARD_COMMAND,
                            MR_ClusterSetFromShard, command_flags, 0, 0,
                            -1)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, CLUSTER_HELLO_COMMAND, MR_ClusterHello,
                            command_flags, 0, 0, 0)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, CLUSTER_INNER_COMMUNICATION_COMMAND,
                            MR_ClusterInnerCommunicationMsg, command_flags, 0, 0,
                            0)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, CLUSTER_INNER_COMMUNICATION_BATCH_COMMAND,
                            MR_ClusterInnerCommunicationMsgBatch, command_flags, 0, 0,
                            0)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, NETWORK_TEST_COMMAND, MR_NetworkTestCommand,
                            command_flags, 0, 0, 0)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, CLUSTER_INFO_COMMAND, MR_ClusterInfoCommand,
                            command_flags, 0, 0, 0)) {
        return REDISMODULE_ERR;
    }

    if (!RegisterRedisCommand(rctx, FORCE_SHARDS_CONNECTION,
                            MR_ForceShardsConnectionCommand, command_flags, 0, 0,
                            0)) {
        return REDISMODULE_ERR;
    }

    clusterCtx.networkTestMsgReciever = MR_ClusterRegisterMsgReceiver(MR_NetworkTest);

    return REDISMODULE_OK;
}

int MR_IsClusterInitialize() {
    return clusterCtx.isOss || clusterCtx.CurrCluster != NULL;
}

size_t MR_ClusterGetSize(){
    return clusterCtx.clusterSize;
}

int MR_ClusterIsClusterMode(){
    return MR_ClusterGetSize() > 1;
}

int MR_ClusterIsInClusterMode() {
    return MR_ClusterIsClusterMode() && MR_IsClusterInitialize();
}

const char* MR_ClusterGetMyId(){
    return clusterCtx.myId;
}

