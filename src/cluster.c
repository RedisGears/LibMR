#include "cluster.h"
#include "mr.h"
#include "event_loop.h"
#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "utils/adlist.h"

#include <hiredis.h>
#include <async.h>
#include <libevent.h>


#define RETRY_INTERVAL 1000 // 1 second
#define MSG_MAX_RETRIES 3
#define CLUSTER_SET_MY_ID_INDEX 6
#define MAX_SLOT 16384
#define RUN_ID_SIZE 40

#ifndef MODULE_NAME
#error "MODULE_NAME is not defined"
#endif

#define xstr(s) str(s)
#define str(s) #s

#define CLUSTER_INNER_COMMUNICATION_COMMAND xstr(MODULE_NAME)".INNERCOMMUNICATION"
#define CLUSTER_HELLO_COMMAND               xstr(MODULE_NAME)".HELLO"
#define CLUSTER_REFRESH_COMMAND             xstr(MODULE_NAME)".REFRESHCLUSTER"
#define CLUSTER_SET_COMMAND                 xstr(MODULE_NAME)".CLUSTERSET"
#define CLUSTER_SET_FROM_SHARD_COMMAND      xstr(MODULE_NAME)".CLUSTERSETFROMSHARD"

typedef enum NodeStatus{
    NodeStatus_Connected, NodeStatus_Disconnected, NodeStatus_HelloSent, NodeStatus_Free
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
    size_t retries;
    size_t msgId;
}SendMsg;

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
    size_t minSlot;
    size_t maxSlot;
    bool isMe;
    NodeStatus status;
    MR_LoopTaskCtx* reconnectEvent;
    MR_LoopTaskCtx* resendHelloEvent;
    bool sendClusterTopologyOnNextConnect;
}Node;

typedef struct Cluster {
    char* myId;
    bool isClusterMode;
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
    size_t minSlot;
    size_t maxSlot;
    size_t clusterSize;
    char myId[REDISMODULE_NODE_ID_LEN + 1];
}clusterCtx;

typedef struct ClusterSetCtx {
    RedisModuleBlockedClient* bc;
    RedisModuleString **argv;
    int argc;
    bool force;
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

static void MR_OnResponseArrived(struct redisAsyncContext* c, void* a, void* b);
static void MR_ConnectToShard(Node* n);
static void MR_HelloResponseArrived(struct redisAsyncContext* c, void* a, void* b);
static Node* MR_GetNode(const char* id);

static void MR_ClusterFreeMsg(void* ptr){
    SendMsg* msg = ptr;
    if (--msg->refCount > 0) {
        return;
    }
    MR_FREE(msg->msg);
    MR_FREE(msg);
}

static void MR_ClusterSendMsgToNodeInternal(Node* node, SendMsg* msg){
    // CLUSTER_INNER_COMMUNICATION_COMMAND <myid> <runid> <functionid> <msg> <msgId>
    redisAsyncCommand(node->c, MR_OnResponseArrived, node, CLUSTER_INNER_COMMUNICATION_COMMAND" %s %s %llu %b %llu",
            clusterCtx.CurrCluster->myId,
            clusterCtx.CurrCluster->runId,
            msg->function,
            msg->msg, msg->msgLen,
            msg->msgId);
}

static void MR_ClusterSendMsgToNode(Node* node, SendMsg* msg){
    msg->msgId = node->msgId++;
    if(node->status == NodeStatus_Connected){
        MR_ClusterSendMsgToNodeInternal(node, msg);
    }else{
        RedisModule_Log(mr_staticCtx, "warning", "message was not sent because status is not connected");
    }
    msg->refCount+=1;
    mr_listAddNodeTail(node->pendingMessages, msg);
}

/* Runs on the event loop */
static void MR_ClusterSendMsgTask(void* ctx) {
    SendMsg* sendMsg = ctx;
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
    msgStruct->retries = 0;
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
    msgStruct->retries = 0;
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
            RedisModule_Log(mr_staticCtx, "warning", "Got bad hello response from %s (%s:%d), will try again in 1 second", n->id, n->ip, n->port);
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
            SendMsg* sentMsg = mr_listNodeValue(node);
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

static void MR_ClusterOnDisconnectCallback(const struct redisAsyncContext* c, int status){
    RedisModule_Log(mr_staticCtx, "warning", "disconnected : %s:%d, status : %d, will try to reconnect.\r\n", c->c.tcp.host, c->c.tcp.port, status);
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    n->status = NodeStatus_Disconnected;
    n->c = NULL;
    n->reconnectEvent = MR_EventLoopAddTaskWithDelay(MR_ClusterReconnect, n, RETRY_INTERVAL);
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
        RedisModule_Log(mr_staticCtx, "notice", "connected : %s:%d, status = %d\r\n", c->c.tcp.host, c->c.tcp.port, status);
        if(n->password){
            redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s", n->password);
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
        MR_ConnectToShard(n);
    }
    mr_dictReleaseIterator(iter);
    mr_dictEmpty(clusterCtx.nodesMsgIds, NULL);
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
}

static Node* MR_GetNode(const char* id){
    mr_dictEntry *entry = mr_dictFind(clusterCtx.CurrCluster->nodes, id);
    Node* n = NULL;
    if(entry){
        n = mr_dictGetVal(entry);
    }
    return n;
}

static Node* MR_CreateNode(const char* id, const char* ip, unsigned short port, const char* password, const char* unixSocket, size_t minSlot, size_t maxSlot){
    RedisModule_Assert(!MR_GetNode(id));
    Node* n = MR_ALLOC(sizeof(*n));
    *n = (Node){
            .id = MR_STRDUP(id),
            .ip = MR_STRDUP(ip),
            .port = port,
            .password = password ? MR_STRDUP(password) : NULL,
            .unixSocket = unixSocket ? MR_STRDUP(unixSocket) : NULL,
            .c = NULL,
            .msgId = 0,
            .pendingMessages = mr_listCreate(),
            .minSlot = minSlot,
            .maxSlot = maxSlot,
            .isMe = false,
            .status = NodeStatus_Disconnected,
            .sendClusterTopologyOnNextConnect = false,
            .runId = NULL,
            .reconnectEvent = NULL,
            .resendHelloEvent = NULL,
    };
    mr_listSetFreeMethod(n->pendingMessages, MR_ClusterFreeMsg);
    mr_dictAdd(clusterCtx.CurrCluster->nodes, n->id, n);
    if(strcmp(id, clusterCtx.CurrCluster->myId) == 0){
        n->isMe = true;
    }
    return n;
}

static void MR_RefreshClusterData(){
    if(clusterCtx.CurrCluster){
        MR_ClusterFree();
    }

    RedisModule_Log(mr_staticCtx, "notice", "Got cluster refresh command");

    clusterCtx.CurrCluster = MR_CALLOC(1, sizeof(*clusterCtx.CurrCluster));

    // generate runID
    RedisModule_GetRandomHexChars(clusterCtx.CurrCluster->runId, RUN_ID_SIZE);
    clusterCtx.CurrCluster->runId[RUN_ID_SIZE] = '\0';

    clusterCtx.CurrCluster->clusterSetCommand = NULL;
    clusterCtx.CurrCluster->clusterSetCommandSize = 0;

    if(!(RedisModule_GetContextFlags(mr_staticCtx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        clusterCtx.CurrCluster->isClusterMode = false;
        return;
    }

    clusterCtx.CurrCluster->isClusterMode = true;

    clusterCtx.CurrCluster->myId = MR_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    memcpy(clusterCtx.CurrCluster->myId, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN);
    clusterCtx.CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
    memcpy(clusterCtx.myId, clusterCtx.CurrCluster->myId, REDISMODULE_NODE_ID_LEN + 1);
    clusterCtx.CurrCluster->nodes = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);

    RedisModule_ThreadSafeContextLock(mr_staticCtx);
    RedisModuleCallReply *allSlotsRelpy = RedisModule_Call(mr_staticCtx, "cluster", "c", "slots");
    RedisModule_ThreadSafeContextUnlock(mr_staticCtx);

    RedisModule_Assert(RedisModule_CallReplyType(allSlotsRelpy) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(allSlotsRelpy) ; ++i){
        RedisModuleCallReply *slotRangeRelpy = RedisModule_CallReplyArrayElement(allSlotsRelpy, i);

        RedisModuleCallReply *minslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 0);
        RedisModule_Assert(RedisModule_CallReplyType(minslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long minslot = RedisModule_CallReplyInteger(minslotRelpy);

        RedisModuleCallReply *maxslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 1);
        RedisModule_Assert(RedisModule_CallReplyType(maxslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long maxslot = RedisModule_CallReplyInteger(maxslotRelpy);

        RedisModuleCallReply *nodeDetailsRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 2);
        RedisModule_Assert(RedisModule_CallReplyType(nodeDetailsRelpy) == REDISMODULE_REPLY_ARRAY);
        RedisModule_Assert(RedisModule_CallReplyLength(nodeDetailsRelpy) == 3);
        RedisModuleCallReply *nodeipReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 0);
        RedisModuleCallReply *nodeportReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 1);
        RedisModuleCallReply *nodeidReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 2);
        size_t idLen;
        size_t ipLen;
        const char* id = RedisModule_CallReplyStringPtr(nodeidReply,&idLen);
        const char* ip = RedisModule_CallReplyStringPtr(nodeipReply,&ipLen);
        long long port = RedisModule_CallReplyInteger(nodeportReply);

        char nodeId[REDISMODULE_NODE_ID_LEN + 1];
        memcpy(nodeId, id, REDISMODULE_NODE_ID_LEN);
        nodeId[REDISMODULE_NODE_ID_LEN] = '\0';

        char nodeIp[ipLen + 1];
        memcpy(nodeIp, ip, ipLen);
        nodeIp[ipLen] = '\0';

        Node* n = MR_GetNode(nodeId);
        if(!n){
            n = MR_CreateNode(nodeId, nodeIp, (unsigned short)port, NULL, NULL, minslot, maxslot);
        }

        if (n->isMe) {
            clusterCtx.minSlot = minslot;
            clusterCtx.maxSlot = maxslot;
        }

        for(int i = minslot ; i <= maxslot ; ++i){
            clusterCtx.CurrCluster->slots[i] = n;
        }
    }
    RedisModule_FreeCallReply(allSlotsRelpy);
    clusterCtx.clusterSize = mr_dictSize(clusterCtx.CurrCluster->nodes);
    MR_ClusterConnectToShards();
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
    const char* myId = RedisModule_StringPtrLen(argv[CLUSTER_SET_MY_ID_INDEX], &myIdLen);
    clusterCtx.CurrCluster->myId = MR_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    size_t zerosPadding = REDISMODULE_NODE_ID_LEN - myIdLen;
    memset(clusterCtx.CurrCluster->myId, '0', zerosPadding);
    memcpy(clusterCtx.CurrCluster->myId + zerosPadding, myId, myIdLen);
    clusterCtx.CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
    memcpy(clusterCtx.myId, clusterCtx.CurrCluster->myId, REDISMODULE_NODE_ID_LEN + 1);

    clusterCtx.CurrCluster->nodes = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);

    long long numOfRanges;
    RedisModule_Assert(RedisModule_StringToLongLong(argv[8], &numOfRanges) == REDISMODULE_OK);

    clusterCtx.CurrCluster->isClusterMode = numOfRanges > 1;

    for(size_t i = 9, j = 0 ; j < numOfRanges ; i += 8, ++j){
        size_t shardIdLen;
        const char* shardId = RedisModule_StringPtrLen(argv[i + 1], &shardIdLen);
        char realId[REDISMODULE_NODE_ID_LEN + 1];
        size_t zerosPadding = REDISMODULE_NODE_ID_LEN - shardIdLen;
        memset(realId, '0', zerosPadding);
        memcpy(realId + zerosPadding, shardId, shardIdLen);
        realId[REDISMODULE_NODE_ID_LEN] = '\0';

        long long minslot;
        RedisModule_Assert(RedisModule_StringToLongLong(argv[i + 3], &minslot) == REDISMODULE_OK);
        long long maxslot;
        RedisModule_Assert(RedisModule_StringToLongLong(argv[i + 4], &maxslot) == REDISMODULE_OK);

        const char* addr = RedisModule_StringPtrLen(argv[i + 6], NULL);
        char* passEnd = strstr(addr, "@");
        size_t passSize = passEnd - addr;
        char password[passSize + 1];
        memcpy(password, addr, passSize);
        password[passSize] = '\0';

        addr = passEnd + 1;

        char* ipEnd = strstr(addr, ":");
        size_t ipSize = ipEnd - addr;
        char ip[ipSize + 1];
        memcpy(ip, addr, ipSize);
        ip[ipSize] = '\0';

        addr = ipEnd + 1;

        unsigned short port = (unsigned short)atoi(addr);

        Node* n = MR_GetNode(realId);
        if(!n){
            n = MR_CreateNode(realId, ip, port, password, NULL, minslot, maxslot);
        }
        for(int i = minslot ; i <= maxslot ; ++i){
            clusterCtx.CurrCluster->slots[i] = n;
        }

        if (n->isMe) {
            clusterCtx.minSlot = minslot;
            clusterCtx.maxSlot = maxslot;
        }

        if(j < numOfRanges - 1){
            // we are not at the last range
            const char* unixAdd = RedisModule_StringPtrLen(argv[i + 7], NULL);
            if(strcmp(unixAdd, "UNIXADDR") == 0){
                i += 2;
            }
        }
    }
    clusterCtx.clusterSize = mr_dictSize(clusterCtx.CurrCluster->nodes);
    MR_ClusterConnectToShards();
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
    MR_SetClusterData(csCtx->argv, csCtx->argc);
    RedisModule_UnblockClient(csCtx->bc, csCtx);
}

static int MR_ClusterSetUnblock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    ClusterSetCtx* csCtx = RedisModule_GetBlockedClientPrivateData(ctx);
    for(size_t i = 0 ; i < csCtx->argc ; ++i){
        RedisModule_FreeString(NULL, csCtx->argv[i]);
    }
    MR_FREE(csCtx->argv);
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
    MessageCtx* msgCtx = ctx;
    if(!clusterCtx.CurrCluster){
        RedisModule_Log(mr_staticCtx, "warning", "Got msg from another shard while cluster is NULL");
        msgCtx->reply = MessageReply_ClusterNull;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }

//    if(!MR_ClusterIsInitialized()){
//        RedisModule_Log(mr_staticCtx, "warning", "Got msg from another shard while cluster is not initialized");
//        msgCtx->reply = MessageReply_ClusterUninitialized;
//        return;
//    }

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
        msgCtx->reply = MessageReply_BadFunctionId;
        RedisModule_UnblockClient(msgCtx->bc, msgCtx);
        return;
    }
    mr_dictSetSignedIntegerVal(entity, msgId);
    clusterCtx.callbacks[functionId](mr_staticCtx, senderIdStr, 0, msg);

    msgCtx->reply = MessageReply_OK;
    RedisModule_UnblockClient(msgCtx->bc, msgCtx);
    return;
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
        RedisModule_ReplyWithSimpleString(ctx, "Duplicate message");
        break;
    default:
        RedisModule_Assert(0);
    }
    for(size_t i = 0 ; i < msgCtx->argc ; ++i){
        RedisModule_FreeString(NULL, msgCtx->argv[i]);
    }
    MR_FREE(msgCtx->argv);
    MR_FREE(msgCtx);

    return REDISMODULE_OK;
}

int MR_ClusterInnerCommunicationMsg(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 6){
        return RedisModule_WrongArity(ctx);
    }

    // we must copy argv because if the client will disconnect the redis will free it
    RedisModuleString **argvNew = MR_ALLOC(sizeof(RedisModuleString *) * argc);
    for(size_t i = 0 ; i < argc ; ++i){
        argvNew[i] = RedisModule_HoldString(NULL, argv[i]);
    }

    MessageCtx* msgCtx = MR_ALLOC(sizeof(*msgCtx));
    msgCtx->bc = RedisModule_BlockClient(ctx, MR_ClusterInnerCommunicationMsgUnblock, NULL, NULL, 0);
    msgCtx->argv = argvNew;
    msgCtx->argc = argc;
    msgCtx->reply = MessageReply_Undefined;
    MR_EventLoopAddTask(MR_ClusterInnerCommunicationMsgRun, msgCtx);
    return REDISMODULE_OK;
}

int MR_ClusterIsMySlot(size_t slot) {
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

int MR_ClusterInit(RedisModuleCtx* rctx) {
    clusterCtx.CurrCluster = NULL;
    clusterCtx.callbacks = array_new(MR_ClusterMessageReceiver, 10);
    clusterCtx.nodesMsgIds = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);
    clusterCtx.minSlot = 0;
    clusterCtx.maxSlot = 0;
    clusterCtx.clusterSize = 1;
    memset(clusterCtx.myId, '0', REDISMODULE_NODE_ID_LEN);

    if (RedisModule_CreateCommand(rctx, CLUSTER_REFRESH_COMMAND, MR_ClusterRefresh, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(rctx, "warning", "could not register command " CLUSTER_REFRESH_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(rctx, CLUSTER_SET_COMMAND, MR_ClusterSet, "readonly", 0, 0, -1) != REDISMODULE_OK) {
        RedisModule_Log(rctx, "warning", "could not register command " CLUSTER_SET_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(rctx, CLUSTER_SET_FROM_SHARD_COMMAND, MR_ClusterSetFromShard, "readonly", 0, 0, -1) != REDISMODULE_OK) {
        RedisModule_Log(rctx, "warning", "could not register command "CLUSTER_SET_FROM_SHARD_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(rctx, CLUSTER_HELLO_COMMAND, MR_ClusterHello, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(rctx, "warning", "could not register command "CLUSTER_HELLO_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(rctx, CLUSTER_INNER_COMMUNICATION_COMMAND, MR_ClusterInnerCommunicationMsg, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(rctx, "warning", "could not register command "CLUSTER_INNER_COMMUNICATION_COMMAND);
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

size_t MR_ClusterGetSize(){
    return clusterCtx.clusterSize;
}

bool MR_ClusterIsClusterMode(){
    return MR_ClusterGetSize() > 1;
}

const char* MR_ClusterGetMyId(){
    return clusterCtx.myId;
}

