/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "redismodule.h"

#include "common.h"
#include "mr.h"
#include "cluster.h"
#include "event_loop.h"
#include "utils/arr_rm_alloc.h"
#include "utils/buffer.h"
#include "utils/dict.h"
#include "utils/adlist.h"

#include <ctype.h>

#include <hiredis.h>
#include <hiredis_ssl.h>
#include <async.h>
#include <libevent.h>

#include <openssl/ssl.h>
#include <openssl/err.h>


#define RETRY_INTERVAL 1000 // 1 second
#define MSG_MAX_RETRIES 3
#define NUMBER_OF_SLOTS 16384
#define RUN_ID_SIZE 40

/*
 * The CLUSTERSET command can come in a long form (legacy, very explicit):
 *
 * <module-name>.CLUSTERSET
 * HASHFUNC {hashing_function}
 * NUMSLOTS {number_of_slots}
 * MYID {current_shard_id}
 * [HASREPLICATION]
 * RANGES {num_of_ranges}
 *   { SHARD {shard_id}
 *     [SLOTRANGE {start_slot} {end_slot}]
 *     ADDR {auth@ip:port} [UNIXADDR {unixsock}]
 *     [MASTER]
 *   }
 *
 *   or short form (new, more compact; the module will get the topology from the server directly):
 *
 * <module-name>.CLUSTERSET [AUTH {pwd}]  // the assumption is that all nodes use the same (or no) password
 */

#define CLUSTERSET_MYID_LONG_FORM_INDEX  6

static bool IsLongFormClusterSet(int argc) {
    return argc >= 10;
}

static bool IsShortFormClusterSet(int argc) {
    return argc == 1 || argc == 3;
}

#define CLUSTER_INNER_COMMUNICATION_COMMAND xstr(MODULE_NAME)".INNERCOMMUNICATION"
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
    mr_list* slotRanges;
    bool isMe;
    unsigned short index;  // A small int unique node identifier for internal usage
    NodeStatus status;
    MR_LoopTaskCtx* reconnectEvent;
    MR_LoopTaskCtx* resendHelloEvent;
    bool sendClusterTopologyOnNextConnect;
}Node;

typedef struct Cluster {
    char* myId;
    mr_dict* nodes;  // Note: we only keep master nodes here (including slotless master nodes, if any)
    Node* slots[NUMBER_OF_SLOTS];
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
    functionId networkTestMsgReceiver;
    char *password;
}clusterCtx;

typedef struct ClusterSetCtx {
    RedisModuleBlockedClient* bc;
    RedisModuleString **argv;
    int argc;
    bool force;
    const char* errReply;  // NULL => reply "OK"; otherwise reply this error to the client
}ClusterSetCtx;

typedef enum MessageReply {
    MessageReply_Undefined,
    MessageReply_OK,
    MessageReply_ClusterUninitialized,
    MessageReply_ClusterNull,
    MessageReply_BadMsgId,
    MessageReply_BadFunctionId,
    MessageReply_DuplicateMsg,
}MessageReply;

typedef struct MessageCtx {
    RedisModuleBlockedClient* bc;
    RedisModuleString **argv;
    int argc;
    MessageReply reply;
}MessageCtx;

static void MR_OnStatusResponseArrived(struct redisAsyncContext* c, void* a, void* b);
static void MR_OnDataResponseArrived(struct redisAsyncContext* c, void* a, void* b);  // A response to an internal-commands command
static void MR_ConnectToShard(Node* n);
static void MR_HelloResponseArrived(struct redisAsyncContext* c, void* a, void* b);
static Node* MR_GetNode(Cluster* cluster, const char* id);

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

static void MR_ClusterSendMsgToNodeInternal(Node* node, NodeSendMsg* nodeMsg){
    // CLUSTER_INNER_COMMUNICATION_COMMAND <myid> <runid> <functionid> <msg> <msgId>
    void (*onResponse)(struct redisAsyncContext*, void*, void*) =
        (nodeMsg->msg->function & FUNCTION_ID_INTERNAL) ? MR_OnDataResponseArrived : MR_OnStatusResponseArrived;
    redisAsyncCommand(node->c, onResponse, node, CLUSTER_INNER_COMMUNICATION_COMMAND" %s %s %llu %b %llu",
            clusterCtx.CurrCluster->myId,
            clusterCtx.CurrCluster->runId,
            nodeMsg->msg->function,
            nodeMsg->msg->msg, nodeMsg->msg->msgLen,
            nodeMsg->msgId);
}

static void MR_ClusterSendMsgToNode(Node* node, SendMsg* msg){
    msg->refCount+=1;
    NodeSendMsg* nodeMsg = MR_ALLOC(sizeof(*nodeMsg));
    nodeMsg->msg = msg;
    nodeMsg->retries = 0;
    nodeMsg->msgId = node->msgId++;
    if (node->status == NodeStatus_Connected) {
        MR_ClusterSendMsgToNodeInternal(node, nodeMsg);
    } else {
        if (node->status == NodeStatus_Uninitialized) {
            MR_ConnectToShard(node);
            node->status = NodeStatus_Disconnected;
        }
        RedisModule_Log(mr_staticCtx, "warning", "message was not sent because status is not connected");
    }
    mr_listAddNodeTail(node->pendingMessages, nodeMsg);
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
        Node* n = MR_GetNode(clusterCtx.CurrCluster, sendMsg->idToSend);
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
            bool isInternalCommand = (sendMsg->function & FUNCTION_ID_INTERNAL) != 0;
            bool shouldSendToNode = !n->isMe || isInternalCommand;
            if (shouldSendToNode)
                MR_ClusterSendMsgToNode(n, sendMsg);
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
    for (functionId fid = 0; fid < array_len(clusterCtx.callbacks); fid++)
        if (clusterCtx.callbacks[fid] == receiver)
            return fid;
    clusterCtx.callbacks = array_append(clusterCtx.callbacks, receiver);
    return array_len(clusterCtx.callbacks) - 1;
}

static void MR_OnDataResponseArrived(struct redisAsyncContext* c, void* r, void* n) {
    redisReply* reply = r;
    if (!reply || !c->data) return;
    Node* node = n;
    if (reply->type != REDIS_REPLY_ARRAY) {
        RedisModule_Log(mr_staticCtx, "warning",
            "Received an invalid status reply from shard %s (%s:%d), will disconnect and try to reconnect.",
            node->id, node->ip, node->port);
        redisAsyncDisconnect(c);
        return;
    }

    mr_listNode* pendingMessage = mr_listFirst(node->pendingMessages);
    NodeSendMsg *message = pendingMessage->value;
    Execution *e = MR_GetExecution(message->msg->msg, message->msg->msgLen);
    mr_listDelNode(node->pendingMessages, pendingMessage);

    MR_SetInternalCommandResults(node->index, reply, e);
}

static void MR_OnStatusResponseArrived(struct redisAsyncContext* c, void* r, void* n){
    redisReply* reply = r;
    if (!reply || !c->data) return;
    Node* node = n;

    if(reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, CLUSTER_ERROR, strlen(CLUSTER_ERROR)) == 0){
        node->sendClusterTopologyOnNextConnect = true;
        RedisModule_Log(mr_staticCtx, "warning",
            "Received ERRCLUSTER reply from shard %s (%s:%d), will send cluster topology to the shard on next connect",
            node->id, node->ip, node->port);
        redisAsyncDisconnect(c);
        return;
    }
    if(reply->type != REDIS_REPLY_STATUS){
        RedisModule_Log(mr_staticCtx, "warning",
            "Received an invalid status reply from shard %s (%s:%d), will disconnect and try to reconnect. "
            "This is usually because the Redis server's 'proto-max-bulk-len' configuration setting is too low.",
            node->id, node->ip, node->port);
        redisAsyncDisconnect(c);
        return;
    }
    mr_listNode* pendingMessage = mr_listFirst(node->pendingMessages);
    mr_listDelNode(node->pendingMessages, pendingMessage);
}

static void MR_ClusterResendHelloMessage(void* ctx){
    Node* n = ctx;
    n->resendHelloEvent = NULL;
    if(n->status == NodeStatus_Disconnected){
        // we will resent the hello request when reconnect
        return;
    }
    if(n->sendClusterTopologyOnNextConnect && clusterCtx.CurrCluster->clusterSetCommand){
        bool isLongForm = IsLongFormClusterSet(clusterCtx.CurrCluster->clusterSetCommandSize);
        RedisModule_Log(mr_staticCtx, "notice", "Sending cluster (%s form) topology to %s (%s:%d) on rg.hello retry",
                isLongForm ? "long" : "short", n->id, n->ip, n->port);
        if (isLongForm)
            clusterCtx.CurrCluster->clusterSetCommand[CLUSTERSET_MYID_LONG_FORM_INDEX] = MR_STRDUP(n->id);
        redisAsyncCommandArgv(n->c, NULL, NULL, clusterCtx.CurrCluster->clusterSetCommandSize, (const char**)clusterCtx.CurrCluster->clusterSetCommand, NULL);
        if (isLongForm) {
            MR_FREE(clusterCtx.CurrCluster->clusterSetCommand[CLUSTERSET_MYID_LONG_FORM_INDEX]);
            clusterCtx.CurrCluster->clusterSetCommand[CLUSTERSET_MYID_LONG_FORM_INDEX] = NULL;
        }
        n->sendClusterTopologyOnNextConnect = false;
    }

    RedisModule_Log(mr_staticCtx, "notice", "Resending hello request to %s (%s:%d)", n->id, n->ip, n->port);
    redisAsyncCommand((redisAsyncContext*)n->c, MR_HelloResponseArrived, n, CLUSTER_HELLO_COMMAND);
}

static void SendAuthCommandIfNeeded(const struct redisAsyncContext* c, const Node *n) {
    if (n->password){
        /* If password is provided to us we will use it (it means it was given to us with clusterset) */
        redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s", n->password);
        return;
    }
    if (RedisModule_GetInternalSecret && clusterCtx.isOss) {
        /* OSS deployment that support internal secret, lets use it. */
        RedisModule_ThreadSafeContextLock(mr_staticCtx);
        size_t len;
        const char *secret = RedisModule_GetInternalSecret(mr_staticCtx, &len);
        RedisModule_Assert(secret);
        redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s %b", "internal connection", secret, len);
        RedisModule_ThreadSafeContextUnlock(mr_staticCtx);
    }
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
            // This might happen if the AUTH has failed because we sent it too early when `n`
            // accepted connections but did not set its internal secret to the cluster's yet.
            // In such cases we will have a regular (i.e., non-internal) connection and the
            // hidden commands (including `<module>.HELLO`) will not be visible to us.
            if (clusterCtx.isOss && strstr(reply->str, "unknown command") != NULL)
                SendAuthCommandIfNeeded(c, n);
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
}

static void MR_ClusterReconnect(void* ctx){
    Node* n = ctx;
    n->reconnectEvent = NULL;
    MR_ConnectToShard(n);
}

static void MR_ClusterAsyncDisconnect(void* ctx){
    Node* n = ctx;
    if (n->c) {
        redisAsyncFree(n->c);
        n->c = NULL;
    }
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
    bool clusterTlsEnabled = clusterTls && !strcmp(clusterTls, "yes");
    if (!clusterTlsEnabled) {
        ret = 0;
        goto done;
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
            MR_FREE(*ca_cert);
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
        return;
    }

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
        if (redisInitiateSSL((redisContext *)(&c->c), ssl) != REDIS_OK) {
            const char *err = "Unknown error";
            if (c->c.err != 0) {
                err = c->c.errstr;
            }
            RedisModule_Log(mr_staticCtx, "warning", "SSL auth to %s:%d failed, will initiate retry. %s.", c->c.tcp.host, c->c.tcp.port, err);
            // disconnect async, its not possible to free redisAsyncContext here
            MR_EventLoopAddTask(MR_ClusterAsyncDisconnect, n);
            return;
        }
    }

    RedisModule_Log(mr_staticCtx, "notice", "connected : %s:%d, status = %d", c->c.tcp.host, c->c.tcp.port, status);

    SendAuthCommandIfNeeded(c, n);

    if(n->sendClusterTopologyOnNextConnect && clusterCtx.CurrCluster->clusterSetCommand){
        bool isLongForm = IsLongFormClusterSet(clusterCtx.CurrCluster->clusterSetCommandSize);
        RedisModule_Log(mr_staticCtx, "notice", "Sending cluster (%s form) topology to %s (%s:%d) after reconnect",
                isLongForm ? "long" : "short", n->id, n->ip, n->port);
        if (isLongForm)
            clusterCtx.CurrCluster->clusterSetCommand[CLUSTERSET_MYID_LONG_FORM_INDEX] = MR_STRDUP(n->id);
        redisAsyncCommandArgv((redisAsyncContext*)c, NULL, NULL, clusterCtx.CurrCluster->clusterSetCommandSize, (const char**)clusterCtx.CurrCluster->clusterSetCommand, NULL);
        if (isLongForm) {
            MR_FREE(clusterCtx.CurrCluster->clusterSetCommand[CLUSTERSET_MYID_LONG_FORM_INDEX]);
            clusterCtx.CurrCluster->clusterSetCommand[CLUSTERSET_MYID_LONG_FORM_INDEX] = NULL;
        }
        n->sendClusterTopologyOnNextConnect = false;
    }
    redisAsyncCommand((redisAsyncContext*)c, MR_HelloResponseArrived, n, CLUSTER_HELLO_COMMAND);
    n->status = NodeStatus_HelloSent;
}

static void MR_ConnectToShard(Node* n){
    redisAsyncContext* c = redisAsyncConnect(n->ip, n->port);
    if (!c) {
        RedisModule_Log(mr_staticCtx, "warning", "Got NULL async connection");
        return;
    }
    if (c->err) {
        RedisModule_Log(mr_staticCtx, "warning", "Error: %s\n", c->errstr);
        redisAsyncFree(c);
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

static void FreeCluster(Cluster* cluster){
    if(!cluster)
        return;

    if(cluster->myId)
        MR_FREE(cluster->myId);
    if(cluster->nodes){
        mr_dictIterator *iter = mr_dictGetIterator(cluster->nodes);
        mr_dictEntry *entry = NULL;
        while((entry = mr_dictNext(iter))){
            Node* n = mr_dictGetVal(entry);
            MR_NodeFree(n);
        }
        mr_dictReleaseIterator(iter);
        mr_dictRelease(cluster->nodes);
    }

    if(cluster->clusterSetCommand){
        for(int i = 0 ; i < cluster->clusterSetCommandSize ; ++i){
            if(cluster->clusterSetCommand[i]){
                MR_FREE(cluster->clusterSetCommand[i]);
            }
        }
        MR_FREE(cluster->clusterSetCommand);
    }

    MR_FREE(cluster);
}

static void MR_ClusterFree(){
    MR_AbortRunningExecutions();

    FreeCluster(clusterCtx.CurrCluster);
    clusterCtx.CurrCluster = NULL;
    clusterCtx.minSlot = 0;
    clusterCtx.maxSlot = 0;
    clusterCtx.clusterSize = 1;
    memset(clusterCtx.myId, '0', REDISMODULE_NODE_ID_LEN);
}

static Node* MR_GetNode(Cluster* cluster, const char* id){
    mr_dictEntry *entry = mr_dictFind(cluster->nodes, id);
    Node* n = NULL;
    if(entry){
        n = mr_dictGetVal(entry);
    }
    return n;
}

static Node* MR_CreateNode(Cluster* cluster, const char* id, const char* ip, unsigned short port, const char* password, const char* unixSocket, long long minSlot, long long maxSlot){
    RedisModule_Assert(!MR_GetNode(cluster, id));

    mr_list* slotRanges = mr_listCreate();
    mr_listSetFreeMethod(slotRanges, FreeSlotRange);
    if (minSlot <= maxSlot) {
        mr_listAddNodeTail(slotRanges, NewSlotRange(minSlot, maxSlot));
    }

    mr_list* pendingMessages = mr_listCreate();
    mr_listSetFreeMethod(pendingMessages, MR_ClusterFreeNodeMsg);

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
            .slotRanges = slotRanges,
            .isMe = false,
            .index = 0,
            .status = NodeStatus_Uninitialized,
            .sendClusterTopologyOnNextConnect = false,
            .runId = NULL,
            .reconnectEvent = NULL,
            .resendHelloEvent = NULL,
    };
    n->index = mr_dictSize(cluster->nodes);
    n->isMe = strcmp(id, cluster->myId) == 0;
    mr_dictAdd(cluster->nodes, n->id, n);

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


        Node* n = MR_GetNode(clusterCtx.CurrCluster, nodeId);
        if(!n){
            /* If we have internal secret we will ignore the clusterCtx.password, we do not need it. */
            n = MR_CreateNode(clusterCtx.CurrCluster, nodeId, nodeIp, (unsigned short)port, MR_ClusterGetPassword(), NULL, minSlot, maxSlot);
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

static void GenerateRunId(Cluster* cluster){
    RedisModule_GetRandomHexChars(cluster->runId, RUN_ID_SIZE);
    cluster->runId[RUN_ID_SIZE] = '\0';
}

static void CopyClusterSetArgs(Cluster* cluster, RedisModuleString** argv, int argc){
    for(int i = 1 ; i < argc ; ++i){
        if (IsLongFormClusterSet(argc) && i == CLUSTERSET_MYID_LONG_FORM_INDEX) {
            cluster->clusterSetCommand[i] = NULL;
            continue;
        }
        const char* arg = RedisModule_StringPtrLen(argv[i], NULL);
        cluster->clusterSetCommand[i] = MR_STRDUP(arg);
    }
}

static void SetMyId(Cluster* cluster, RedisModuleString** argv, int argc){
    const char* myId = RedisModule_GetMyClusterID();
    size_t myIdLen = REDISMODULE_NODE_ID_LEN;
    if (IsLongFormClusterSet(argc)) {
        RedisModule_Assert(CLUSTERSET_MYID_LONG_FORM_INDEX < argc);
        myId = RedisModule_StringPtrLen(argv[CLUSTERSET_MYID_LONG_FORM_INDEX], &myIdLen);
    }

    cluster->myId = MR_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    size_t zerosPadding = REDISMODULE_NODE_ID_LEN - myIdLen;
    memset(cluster->myId, '0', zerosPadding);
    memcpy(cluster->myId + zerosPadding, myId, myIdLen);
    cluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
}

static void InitClusterData(Cluster* cluster, RedisModuleString** argv, int argc){
    cluster->clusterSetCommand = MR_ALLOC(sizeof(char*) * argc);
    cluster->clusterSetCommandSize = argc;
    cluster->clusterSetCommand[0] = MR_STRDUP(CLUSTER_SET_FROM_SHARD_COMMAND);

    GenerateRunId(cluster);
    CopyClusterSetArgs(cluster, argv, argc);
    SetMyId(cluster, argv, argc);

    cluster->nodes = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);
}

#define INTERNAL_PASSWORD_MAX_SIZE 100

// Parse a SHARD entry into the output arguments and return the index of the last parsed token
static int ParseShardEntry(RedisModuleString** argv, int argc, int index,
                           char realId[],
                           char ip[], unsigned short* port, char password[],
                           long long* minSlot, long long* maxSlot,
                           bool* shouldSkip){
    *shouldSkip = false;
    const char* token;

    RedisModule_Assert(index < argc);
    token = RedisModule_StringPtrLen(argv[index], NULL);
    RedisModule_Assert(strcasecmp(token, "SHARD") == 0);

    index++;
    RedisModule_Assert(index < argc);
    size_t shardIdLen;
    const char* shardId = RedisModule_StringPtrLen(argv[index], &shardIdLen);
    size_t zerosPadding = REDISMODULE_NODE_ID_LEN - shardIdLen;
    memset(realId, '0', zerosPadding);
    memcpy(realId + zerosPadding, shardId, shardIdLen);
    realId[REDISMODULE_NODE_ID_LEN] = '\0';
    index++;

    *minSlot = 0;
    *maxSlot = -1;  // min > max indicates a no-hslots range (i.e., used for slotless shards)
    RedisModule_Assert(index < argc);
    token = RedisModule_StringPtrLen(argv[index], NULL);
    if (strcasecmp(token, "SLOTRANGE") == 0) {
        index++;
        RedisModule_Assert(index < argc);
        RedisModule_Assert(RedisModule_StringToLongLong(argv[index++], minSlot) == REDISMODULE_OK);
        RedisModule_Assert(index < argc);
        RedisModule_Assert(RedisModule_StringToLongLong(argv[index++], maxSlot) == REDISMODULE_OK);
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
    RedisModule_Assert(passSize < INTERNAL_PASSWORD_MAX_SIZE);
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
    RedisModule_Assert(ipSize < INET6_ADDRSTRLEN);

    memcpy(ip, addr, ipSize);
    ip[ipSize] = '\0';
    *port = (unsigned short)atoi(ipEnd + 1);

    if (index >= argc) {
        *shouldSkip = true;
        return index;
    }
    token = RedisModule_StringPtrLen(argv[index], NULL);
    if (strcasecmp(token, "UNIXADDR") == 0)
        index += 2; // Ignore it and its value

    if (index >= argc) {
        *shouldSkip = true;
        return index;
    }
    token = RedisModule_StringPtrLen(argv[index], NULL);
    if (strcasecmp(token, "MASTER") != 0) {
        *shouldSkip = true; // Ignore non-master nodes
        return index;
    }

    return index;
}

Cluster* BuildCluster(RedisModuleString** argv, int argc, const char* password) {
    size_t numNodes;
    char **nodeList = RedisModule_GetClusterNodesList(mr_staticCtx, &numNodes);
    if (!nodeList) {
        RedisModule_Log(mr_staticCtx, "warning", "Failed to get cluster nodes list");
        return NULL;
    }

    Cluster* cluster = MR_CALLOC(1, sizeof(*cluster));
    InitClusterData(cluster, argv, argc);
    size_t coveredSlots = 0;

    for (size_t i = 0; i < numNodes; i++) {
        char nodeId[REDISMODULE_NODE_ID_LEN + 1];  // nodeList[i] is not null-terminated
        memcpy(nodeId, nodeList[i], REDISMODULE_NODE_ID_LEN);
        nodeId[REDISMODULE_NODE_ID_LEN] = '\0';

        char ip[INET6_ADDRSTRLEN];  // INET6_ADDRSTRLEN includes the closing '\0'
        int port, flags;

        if (RedisModule_GetClusterNodeInfo(mr_staticCtx, nodeId, ip, NULL, &port, &flags) != REDISMODULE_OK) {
            RedisModule_Log(mr_staticCtx, "warning", "Failed to get info for node %s", nodeId);
            continue;
        }

        if (!(flags & REDISMODULE_NODE_MASTER)) continue;  // Skip replica nodes

        RedisModuleSlotRangeArray *slots = RedisModule_GetClusterNodeSlotRanges(mr_staticCtx, nodeId);
        RedisModule_Assert(slots != NULL);
        long long minSlot = 0, maxSlot = -1;  // min > max indicates a no-hslots range (i.e., used for slotless shards)
        if (slots->num_ranges > 0) {
            minSlot = slots->ranges[0].start;
            maxSlot = slots->ranges[0].end;
        }

        Node* aMasterNode = MR_CreateNode(cluster, nodeId, ip, port, password, NULL, minSlot, maxSlot);
        // Note that MR_CreateNode has already set the isMe bool, but since here we have the flags
        // which are the "formal" way to know if this node is me, we override it here.
        // Basically they should have the same outcome, but given the various ways we name a node
        // (e.g., in RE they are the shard uids, left-padded with 0s), it's safer not to assert
        // that they are the same and use the flags as the actual true value.
        aMasterNode->isMe = (flags & REDISMODULE_NODE_MYSELF) != 0;

        for (size_t j = 0; j < slots->num_ranges; j++) {
            minSlot = slots->ranges[j].start;
            maxSlot = slots->ranges[j].end;
            coveredSlots += (maxSlot - minSlot + 1);
            if (j > 0)  // The 0 case is handled by the MR_CreateNode() above
                mr_listAddNodeTail(aMasterNode->slotRanges, NewSlotRange(minSlot, maxSlot));
            for (int k = minSlot ; k <= maxSlot ; k++) {
                if (cluster->slots[k] != NULL) {
                    RedisModule_Log(mr_staticCtx, "warning",
                        "Slot %d is claimed by two master nodes: %s and %s; rejecting topology",
                        k, cluster->slots[k]->id, aMasterNode->id);
                    FreeCluster(cluster);
                    cluster = NULL;
                    break;
                }
                cluster->slots[k] = aMasterNode;
            }
            if (cluster == NULL)
                break;
        }

        RedisModule_ClusterFreeSlotRanges(mr_staticCtx, slots);
        if (cluster == NULL)
            break;
    }
    RedisModule_FreeClusterNodesList(nodeList);

    if (cluster != NULL && coveredSlots != NUMBER_OF_SLOTS) {
        RedisModule_Log(mr_staticCtx, "warning",
            "Cluster topology covers %zu of %d slots; rejecting topology",
            coveredSlots, NUMBER_OF_SLOTS);
        FreeCluster(cluster);
        cluster = NULL;
    }

    return cluster;
}

static bool SameNode(Node* a, Node* b) {
    return strcmp(a->id, b->id) == 0 && strcmp(a->ip, b->ip) == 0 && a->port == b->port;
}

static bool SameSlotRanges(Node* a, Node* b) {
    if (mr_listLength(a->slotRanges) != mr_listLength(b->slotRanges))
        return false;
    mr_listNode* ia = mr_listFirst(a->slotRanges);
    mr_listNode* ib = mr_listFirst(b->slotRanges);
    while (ia != NULL) {
        SlotRange* ra = mr_listNodeValue(ia);
        SlotRange* rb = mr_listNodeValue(ib);
        if (ra->minSlot != rb->minSlot || ra->maxSlot != rb->maxSlot)
            return false;
        ia = mr_listNextNode(ia);
        ib = mr_listNextNode(ib);
    }
    return true;
}

static bool SameCluster(Cluster* a, Cluster* b) {
    if (a == b)
        return true;
    if (a == NULL || b == NULL)
        return false;
    if (mr_dictSize(a->nodes) != mr_dictSize(b->nodes))
        return false;
    mr_dictIterator* iter = mr_dictGetIterator(a->nodes);
    mr_dictEntry* entry = NULL;
    while ((entry = mr_dictNext(iter)) != NULL) {
        Node* na = mr_dictGetVal(entry);
        Node* nb = MR_GetNode(b, na->id);
        if (nb == NULL || !SameNode(na, nb) || !SameSlotRanges(na, nb))
            break;
    }
    mr_dictReleaseIterator(iter);
    return entry == NULL;
}

void MR_UpdateClusterTopologyIfNeeded(void* ctx){
    Cluster* cluster = ctx;
    RedisModule_Assert(cluster != NULL);

    if (SameCluster(cluster, clusterCtx.CurrCluster)) {
        FreeCluster(cluster);
        return;
    }

    if (clusterCtx.CurrCluster)
        MR_ClusterFree();
    clusterCtx.CurrCluster = cluster;
    memcpy(clusterCtx.myId, cluster->myId, REDISMODULE_NODE_ID_LEN + 1);

    // Calculate the min/max slots for clusterCtx (legacy, fallback single-range; see the comment at the struct declaration)
    long long minSlot = 0, maxSlot = -1;  // min > max indicates a no-hslots range (i.e., used for slotless shards)
    mr_dictIterator *iter = mr_dictGetIterator(cluster->nodes);
    mr_dictEntry *entry = NULL;
    while ((entry = mr_dictNext(iter)) != NULL) {
        Node* n = mr_dictGetVal(entry);
        if (!n->isMe)
            continue;
        if (mr_listLength(n->slotRanges) > 0) {
            SlotRange* r = mr_listNodeValue(mr_listFirst(n->slotRanges));
            minSlot = r->minSlot;
            maxSlot = r->maxSlot;
        }
        break;
    }
    mr_dictReleaseIterator(iter);
    clusterCtx.minSlot = minSlot;
    clusterCtx.maxSlot = maxSlot;

    clusterCtx.clusterSize = mr_dictSize(cluster->nodes);
    mr_dictEmpty(clusterCtx.nodesMsgIds, NULL);
}

static int SetClusterDataShortForm(RedisModuleString** argv, int argc){
    RedisModule_Log(mr_staticCtx, "notice", "Got cluster set command (short form)");

    // RedisModule_GetClusterNodeSlotRanges may be NULL when the host Redis
    // build does not export it (e.g. OSS Redis without the backport). Reject
    // the command with an error instead of crashing or silently no-op'ing, so
    // the caller (e.g. DMC) can fall back to the long-form CLUSTERSET. The
    // cluster stays unconfigured until a long-form CLUSTERSET or REFRESHCLUSTER
    // arrives.
    if (RedisModule_GetClusterNodeSlotRanges == NULL) {
        RedisModule_Log(mr_staticCtx, "warning",
            "Short-form CLUSTERSET received, but RedisModule_GetClusterNodeSlotRanges "
            "is not available in this Redis build. Use long-form CLUSTERSET or "
            "REFRESHCLUSTER to configure the cluster.");
        return REDISMODULE_ERR;
    }

    const char *password = NULL;
    switch (argc) {
    case 1:
        password = MR_ClusterGetPassword();
        break;
    case 3: {
        const char *token = RedisModule_StringPtrLen(argv[1], NULL);
        RedisModule_Assert(strcasecmp(token, "AUTH") == 0);
        password = RedisModule_StringPtrLen(argv[2], NULL);
        break;
    }
    default:
        RedisModule_Assert(0);
    }

    Cluster* cluster = BuildCluster(argv, argc, password);
    if (!cluster)
        return REDISMODULE_ERR;

    MR_UpdateClusterTopologyIfNeeded(cluster);
    return REDISMODULE_OK;
}

static void SetClusterDataLongForm(RedisModuleString** argv, int argc){
    RedisModule_Log(mr_staticCtx, "notice", "Got cluster set command (long form)");

    clusterCtx.CurrCluster = MR_CALLOC(1, sizeof(*clusterCtx.CurrCluster));
    InitClusterData(clusterCtx.CurrCluster, argv, argc);
    memcpy(clusterCtx.myId, clusterCtx.CurrCluster->myId, REDISMODULE_NODE_ID_LEN + 1);

    size_t index = CLUSTERSET_MYID_LONG_FORM_INDEX + 1;
    const char *token = RedisModule_StringPtrLen(argv[index], NULL);
    bool hasReplication = strcasecmp(token, "HASREPLICATION") == 0;
    if (hasReplication) {  // skip this token; we ignore it
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
        char realId[REDISMODULE_NODE_ID_LEN + 1];
        char ip[INET6_ADDRSTRLEN];  // INET6_ADDRSTRLEN includes the closing '\0'
        unsigned short port;
        char password[INTERNAL_PASSWORD_MAX_SIZE + 1];
        long long minSlot, maxSlot;
        bool shouldSkip;

        index = ParseShardEntry(argv, argc, index, realId, ip, &port, password, &minSlot, &maxSlot, &shouldSkip);
        if (index >= argc)
            break;
        if (shouldSkip)
            continue;

        // Create a new node or update an existing one
        Node* aMasterNode = MR_GetNode(clusterCtx.CurrCluster, realId);
        if(!aMasterNode){
            aMasterNode = MR_CreateNode(clusterCtx.CurrCluster, realId, ip, port, password, NULL, minSlot, maxSlot);
        } else {
            RedisModule_Assert(minSlot <= maxSlot);  // slotless nodes are only created (above)
            mr_listAddNodeTail(aMasterNode->slotRanges, NewSlotRange(minSlot, maxSlot));
        }

        for(int k = minSlot ; k <= maxSlot ; ++k){
            clusterCtx.CurrCluster->slots[k] = aMasterNode;
        }

        if (aMasterNode->isMe) {
            // fill the fallback single-range; see the comment at the declaration of minSlot and maxSlot
            clusterCtx.minSlot = minSlot;
            clusterCtx.maxSlot = maxSlot;
        }

        index++;
    }
    clusterCtx.clusterSize = mr_dictSize(clusterCtx.CurrCluster->nodes);
    mr_dictEmpty(clusterCtx.nodesMsgIds, NULL);
}

static int MR_SetClusterData(RedisModuleString** argv, int argc){
    if(clusterCtx.CurrCluster)
        MR_ClusterFree();

    if (IsLongFormClusterSet(argc)) {
        SetClusterDataLongForm(argv, argc);
        return REDISMODULE_OK;
    } else if (IsShortFormClusterSet(argc)) {
        return SetClusterDataShortForm(argv, argc);
    } else {
        RedisModule_Log(mr_staticCtx, "warning", "Could not parse cluster set arguments");
        return REDISMODULE_ERR;
    }
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
        if (MR_SetClusterData(csCtx->argv, csCtx->argc) != REDISMODULE_OK) {
            csCtx->errReply = CLUSTER_ERROR" Failed to set cluster topology";
        }
    }
    RedisModule_UnblockClient(csCtx->bc, csCtx);
}

static int MR_ClusterSetUnblock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    ClusterSetCtx* csCtx = RedisModule_GetBlockedClientPrivateData(ctx);
    const char* errReply = csCtx->errReply;
    for(size_t i = 0 ; i < csCtx->argc ; ++i){
        RedisModule_FreeString(NULL, csCtx->argv[i]);
    }
    MR_FREE(csCtx->argv);
    MR_FREE(csCtx);
    if (errReply) {
        RedisModule_ReplyWithError(ctx, errReply);
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    return REDISMODULE_OK;
}

static int MR_ClusterSetInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool force){
    ClusterSetCtx* csCtx = MR_ALLOC(sizeof(*csCtx));
    csCtx->bc = RedisModule_BlockClient(ctx, MR_ClusterSetUnblock, NULL, NULL, 0);
    csCtx->argv = argv;
    csCtx->argc = argc;
    csCtx->force = force;
    csCtx->errReply = NULL;
    MR_EventLoopAddTask(MR_ClusterSetFromCommand, csCtx);
    return REDISMODULE_OK;
}

static int MR_ClusterRefresh(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    MR_EventLoopAddTask(MR_ClusterRefreshFromCommand, bc);
    return REDISMODULE_OK;
}

static int MR_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if (!(IsShortFormClusterSet(argc) || IsLongFormClusterSet(argc))) {
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
    if (!(IsShortFormClusterSet(argc) || IsLongFormClusterSet(argc))) {
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
    MessageCtx* msgCtx = ctx;
    if(!clusterCtx.CurrCluster){
        RedisModule_Log(mr_staticCtx, "warning", "Got msg from another shard while cluster is NULL");
        msgCtx->reply = MessageReply_ClusterNull;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }

    if(!MR_IsClusterInitialize()){
        RedisModule_Log(mr_staticCtx, "warning", "Got msg from another shard while cluster is not initialized");
        msgCtx->reply = MessageReply_ClusterUninitialized;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }

    RedisModuleString** argv = msgCtx->argv;
    size_t argc = msgCtx->argc;

    RedisModuleString* senderId = argv[1];
    RedisModuleString* senderRunId = argv[2];
    RedisModuleString* functionToCall = argv[3];
    RedisModuleString* msg = argv[4];
    RedisModuleString* msgIdStr = argv[5];

    long long msgId;
    if(RedisModule_StringToLongLong(msgIdStr, &msgId) != REDISMODULE_OK){
        RedisModule_Log(mr_staticCtx, "warning", "bad msg id given");
        msgCtx->reply = MessageReply_BadMsgId;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }

    long long functionId;
    if(RedisModule_StringToLongLong(functionToCall, &functionId) != REDISMODULE_OK){
        RedisModule_Log(mr_staticCtx, "warning", "bad function id given");
        msgCtx->reply = MessageReply_BadFunctionId;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }

    if (functionId < 0 || functionId >= array_len(clusterCtx.callbacks)) {
        RedisModule_Log(mr_staticCtx, "warning", "bad function id given");
        msgCtx->reply = MessageReply_BadFunctionId;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }

    size_t senderIdLen;
    const char* senderIdStr = RedisModule_StringPtrLen(senderId, &senderIdLen);
    size_t senderRunIdLen;
    const char* senderRunIdStr = RedisModule_StringPtrLen(senderRunId, &senderRunIdLen);

    char combinedId[senderIdLen + senderRunIdLen + 1]; // +1 is for '\0'
    memcpy(combinedId, senderIdStr, senderIdLen);
    memcpy(combinedId + senderIdLen, senderRunIdStr, senderRunIdLen);
    combinedId[senderIdLen + senderRunIdLen] = '\0';

    mr_dictEntry* entity = mr_dictFind(clusterCtx.nodesMsgIds, combinedId);
    long long currId = -1;
    if(entity){
        currId = mr_dictGetSignedIntegerVal(entity);
    }else{
        entity = mr_dictAddRaw(clusterCtx.nodesMsgIds, (char*)combinedId, NULL);
    }
    if(msgId <= currId){
        RedisModule_Log(mr_staticCtx, "warning", "duplicate message ignored, msgId: %lld, currId: %lld", msgId, currId);
        msgCtx->reply = MessageReply_DuplicateMsg;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }
    mr_dictSetSignedIntegerVal(entity, msgId);
    clusterCtx.callbacks[functionId](mr_staticCtx, senderIdStr, 0, msg);

    msgCtx->reply = MessageReply_OK;
    RedisModule_UnblockClient(msgCtx->bc, msgCtx);
    return;
}

static void MR_ClusterInnerCommunicationMsgFreePD(RedisModuleCtx* ctx, void* pd){
    MessageCtx* msgCtx = pd;
    for(size_t i = 0 ; i < msgCtx->argc ; ++i){
        RedisModule_FreeString(NULL, msgCtx->argv[i]);
    }
    MR_FREE(msgCtx->argv);
    MR_FREE(msgCtx);
}

static int MR_ClusterInnerCommunicationMsgUnblock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    MessageCtx* msgCtx = RedisModule_GetBlockedClientPrivateData(ctx);
    switch(msgCtx->reply) {
    case MessageReply_OK:
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        break;
    case MessageReply_ClusterUninitialized:
        RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" Uninitialized cluster state");
        break;
    case MessageReply_ClusterNull:
        RedisModule_ReplyWithError(ctx, CLUSTER_ERROR" NULL cluster state");
        break;
    case MessageReply_BadMsgId:
        RedisModule_ReplyWithError(ctx, "Err bad message id");
        break;
    case MessageReply_BadFunctionId:
        RedisModule_ReplyWithError(ctx, "Err bad function id");
        break;
    case MessageReply_DuplicateMsg:
        RedisModule_ReplyWithSimpleString(ctx, "duplicate message ignored");
        break;
    default:
        RedisModule_Assert(0);
    }

    return REDISMODULE_OK;
}

int MR_NetworkTestCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    MR_ClusterCopyAndSendMsg(NULL, clusterCtx.networkTestMsgReceiver, "test msg", strlen("test msg"));
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

int MR_ClusterExecuteInternalCommands(RedisModuleCtx *ctx, RedisModuleString *msg);

int MR_ClusterInnerCommunicationMsg(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 6){
        return RedisModule_WrongArity(ctx);
    }
    functionId fid = (functionId)strtoull(RedisModule_StringPtrLen(argv[3], NULL), NULL, 10);
    if (fid & FUNCTION_ID_INTERNAL) {
        RedisModuleString *msg = argv[4];
        return MR_ClusterExecuteInternalCommands(ctx, msg);
    }

    /* We must copy argv because this command defers processing to the LibMR
     * event-loop thread. Using RedisModule_HoldString() here is unsafe: strings
     * originating from client command arguments may share the client's query
     * buffer, which Redis can trim/realloc after the command returns. */
    RedisModuleString **argvNew = MR_ALLOC(sizeof(RedisModuleString *) * argc);
    for(size_t i = 0 ; i < argc ; ++i){
        argvNew[i] = RedisModule_CreateStringFromString(NULL, argv[i]);
    }

    MessageCtx* msgCtx = MR_ALLOC(sizeof(*msgCtx));
    msgCtx->bc = RedisModule_BlockClient(ctx, MR_ClusterInnerCommunicationMsgUnblock, NULL, MR_ClusterInnerCommunicationMsgFreePD, 0);
    msgCtx->argv = argvNew;
    msgCtx->argc = argc;
    msgCtx->reply = MessageReply_Undefined;
    MR_EventLoopAddTask(MR_ClusterInnerCommunicationMsgRun, msgCtx);
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

size_t MR_ClusterGetSlotByKey(const char* key, size_t len) {
    return keyHashSlot(key, len);
}

static void MR_NetworkTest(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    RedisModule_Log(ctx, "notice", "got a nextwork test msg");
}

int MR_ClusterInit(RedisModuleCtx* rctx, char *password) {
    clusterCtx.CurrCluster = NULL;
    clusterCtx.callbacks = array_new(MR_ClusterMessageReceiver, 10);
    clusterCtx.nodesMsgIds = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);
    clusterCtx.minSlot = 0;
    clusterCtx.maxSlot = 0;
    clusterCtx.clusterSize = 1;
    clusterCtx.isOss = true;
    clusterCtx.password = password ? MR_STRDUP(password) : NULL;
    memset(clusterCtx.myId, '0', REDISMODULE_NODE_ID_LEN);

    /* Note: RedisModule_GetContextFlags() does NOT report
     * REDISMODULE_CTX_FLAGS_CLUSTER yet at module-load time, so read the
     * cluster-enabled config directly to learn the runtime mode. */
    bool ossClusterRuntime = false;
    bool ceResolved = false;
    RedisModuleCallReply *ceReply = RedisModule_Call(rctx, "config", "cc", "get", "cluster-enabled");
    if (ceReply) {
        /* CONFIG GET replies as a flat array in RESP2 and as a map in RESP3.
         * The module ctx defaults to RESP2, but handle both shapes so a future
         * RESP3 default can't silently break detection and misclassify an
         * OSS-cluster shard as enterprise. */
        int ceType = RedisModule_CallReplyType(ceReply);
        RedisModuleCallReply *val = NULL;
        if (ceType == REDISMODULE_REPLY_ARRAY && RedisModule_CallReplyLength(ceReply) >= 2) {
            val = RedisModule_CallReplyArrayElement(ceReply, 1);
        } else if (ceType == REDISMODULE_REPLY_MAP && RedisModule_CallReplyMapElement &&
                   RedisModule_CallReplyLength(ceReply) >= 1) {
            RedisModule_CallReplyMapElement(ceReply, 0, NULL, &val);
        }
        if (val && RedisModule_CallReplyType(val) == REDISMODULE_REPLY_STRING) {
            size_t vlen = 0;
            const char *vstr = RedisModule_CallReplyStringPtr(val, &vlen);
            if (vstr) {
                ceResolved = true;
                /* CONFIG GET returns the canonical lowercase "yes"/"no" (Redis
                 * spec), so a case-sensitive compare of exactly 3 bytes is safe. */
                if (vlen == 3 && memcmp(vstr, "yes", 3) == 0) {
                    ossClusterRuntime = true;
                }
            }
        }
        RedisModule_FreeCallReply(ceReply);
    }
    if (!ceResolved) {
        /* CONFIG GET cluster-enabled should always succeed; an error, an
         * unexpected reply shape, or an empty value leaves the runtime cluster
         * mode unknown. Keep the conservative default (ossClusterRuntime stays
         * false, i.e. an rlec_version shard is treated as enterprise as before)
         * but log it so a resulting misclassification is diagnosable rather than
         * silent. */
        RedisModule_Log(rctx, "warning",
                        "Could not read cluster-enabled config; assuming not in OSS cluster mode");
    }
    /* rlec_version is only present on enterprise binaries. Its presence was
     * already detected by MR_GetRedisVersion() (run before MR_ClusterInit) and
     * recorded in MR_RlecVersionPresent, so reuse it here instead of fetching
     * Server info a second time. Use the presence flag, not MR_RlecMajorVersion
     * >= 0: the latter only becomes true once sscanf parses the version, so a
     * present-but-unparseable rlec_version would otherwise read as OSS.
     *
     * Treat the shard as enterprise only when rlec_version is present AND we
     * are not in OSS cluster mode. This is the load-bearing decision of the
     * PR: it unblocks an enterprise binary running as an OSS cluster (e.g. the
     * env0 testing setup), which must take the OSS path (internal-secret AUTH,
     * no _proxy-filtered command flags) rather than the enterprise path. */
    if (MR_RlecVersionPresent && !ossClusterRuntime) {
        clusterCtx.isOss = false;
    }

    RedisModule_Log(rctx, "notice", "Detected redis %s (cluster-enabled=%s)",
                    clusterCtx.isOss ? "oss" : "enterprise",
                    ossClusterRuntime ? "yes" : "no");

    const char *command_flags = "readonly deny-script";
    if (!clusterCtx.isOss) {
        command_flags = "readonly deny-script _proxy-filtered";
    } else {
        if (RedisModule_GetInternalSecret) {
            /* We run at a version that supports internal commands, let use it. */
            command_flags = "readonly deny-script internal";
        }
    }

    if (clusterCtx.isOss) {
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
                            !clusterCtx.isOss ? "readonly deny-script _proxy-filtered" : "readonly deny-script",
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

    clusterCtx.networkTestMsgReceiver = MR_ClusterRegisterMsgReceiver(MR_NetworkTest);

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

const char *MR_ClusterGetPassword(){
    return RedisModule_GetInternalSecret ? NULL : clusterCtx.password;
}
