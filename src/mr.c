#include "mr.h"
#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "mr_memory.h"
#include "event_loop.h"
#include "cluster.h"
#include "record.h"
#include "utils/thpool.h"
#include "utils/adlist.h"
#include "utils/buffer.h"

#include <pthread.h>

#define EXECUTION_DEFAULT_MAX_IDLE_MS 5000

#define ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(size_t)
#define STR_ID_LEN  REDISMODULE_NODE_ID_LEN + 13

RedisModuleCtx* mr_staticCtx;

/* Remote functions ids */
functionId NEW_EXECUTION_RECIEVED_FUNCTION_ID = 0;
functionId ACK_EXECUTION_FUNCTION_ID = 0;
functionId INVOKE_EXECUTION_FUNCTION_ID = 0;
functionId PASS_RECORD_FUNCTION_ID = 0;
functionId NOTIFY_STEP_DONE_FUNCTION_ID = 0;
functionId NOTIFY_DONE_FUNCTION_ID = 0;
functionId DROP_EXECUTION_FUNCTION_ID = 0;

typedef struct RemoteFunctionDef {
    functionId* funcIdPointer;
    MR_ClusterMessageReceiver functionPointer;
}RemoteFunctionDef;

typedef void (*ExecutionTaskCallback)(Execution* e, void* pd);

/* functions declarations */
static void MR_ExecutionAddTask(Execution* e, ExecutionTaskCallback callback, void* pd);
static void MR_RunExecution(Execution* e, void* pd);
void MR_FreeExecution(Execution* e);

/* Remote functions declaration */
static void MR_NewExecutionRecieved(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);
static void MR_AckExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);
static void MR_InvokeExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);
static void MR_PassRecord(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);
static void MR_NotifyDone(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);
static void MR_NotifyStepDone(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);
static void MR_DropExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload);

/* Remote functions array */
RemoteFunctionDef remoteFunctions[] = {
        {
                .funcIdPointer = &NEW_EXECUTION_RECIEVED_FUNCTION_ID,
                .functionPointer = MR_NewExecutionRecieved,
        },
        {
                .funcIdPointer = &ACK_EXECUTION_FUNCTION_ID,
                .functionPointer = MR_AckExecution,
        },
        {
                .funcIdPointer = &INVOKE_EXECUTION_FUNCTION_ID,
                .functionPointer = MR_InvokeExecution,
        },
        {
                .funcIdPointer = &PASS_RECORD_FUNCTION_ID,
                .functionPointer = MR_PassRecord,
        },
        {
                .funcIdPointer = &NOTIFY_DONE_FUNCTION_ID,
                .functionPointer = MR_NotifyDone,
        },
        {
                .funcIdPointer = &NOTIFY_STEP_DONE_FUNCTION_ID,
                .functionPointer = MR_NotifyStepDone,
        },
        {
                .funcIdPointer = &DROP_EXECUTION_FUNCTION_ID,
                .functionPointer = MR_DropExecution,
        },
};

typedef struct MRStats {
    size_t nMissedExecutions;
    size_t nMaxIdleReached;
}MRStats;

struct MRCtx {
    size_t lastExecutionId;

    /* protected by the event loop */
    mr_dict* executionsDict;

    /* should be initialized at start and then read only */
    ARR(MRObjectType*) objectTypesDict;

    /* Steps dictionaries */
    mr_dict* readerDict;
    mr_dict* mappersDict;

    mr_threadpool executionsThreadPool;

    MRStats stats;
}mrCtx;

static uint64_t idHashFunction(const void *key){
    return mr_dictGenHashFunction(key, ID_LEN);
}

static int idKeyCompare(void *privdata, const void *key1, const void *key2){
    return memcmp(key1, key2, ID_LEN) == 0;
}

static void idKeyDestructor(void *privdata, void *key){
    MR_FREE(key);
}

static void* idKeyDup(void *privdata, const void *key){
    char* ret = MR_ALLOC(ID_LEN);
    memcpy(ret, key , ID_LEN);
    return ret;
}

mr_dictType dictTypeHeapIds = {
        .hashFunction = idHashFunction,
        .keyDup = idKeyDup,
        .valDup = NULL,
        .keyCompare = idKeyCompare,
        .keyDestructor = idKeyDestructor,
        .valDestructor = NULL,
};

typedef enum StepType {
    StepType_Reader,
    StepType_Mapper,
    StepType_Collect,
}StepType;

typedef enum StepRunResult {
    StepRunResult_Continue, StepRunResult_More, StepRunResult_Hold,
}StepRunResult;

typedef struct StepDefinition {
    char* name;
    MRObjectType* type;
    void* callback;
}StepDefinition;

typedef struct ReadStep {
    ExecutionReader readCallback;
}ReadStep;

typedef struct MapStep {
    ExecutionMapper mapCallback;
}MapStep;

typedef struct CollectStep {
    ARR(Record*) collectedRecords;
    size_t nDone;
}CollectStep;

typedef struct ExecutionBuilderStep {
    void* args;
    MRObjectType* argsType;
    char* name;
    StepType type;
}ExecutionBuilderStep;

#define StepFlag_Done 1<<0

typedef struct Step {
    int flags;
    ExecutionBuilderStep bStep;
    union {
        MapStep map;
        ReadStep read;
        CollectStep collect;
    };
    size_t index;
}Step;

struct ExecutionBuilder {
    ARR(ExecutionBuilderStep) steps;
};

#define ExecutionFlag_Initiator 1<<0
#define ExecutionFlag_Local 1<<1

typedef struct ExecutionCallbackData {
    void* pd;
    ExecutionCallback callback;
}ExecutionCallbackData;

typedef struct ExecutionCallbacks {
    ExecutionCallbackData done;
    ExecutionCallbackData resume;
    ExecutionCallbackData hold;
}ExecutionCallbacks;

struct Execution {
    int flags;
    size_t refCount;
    char id[ID_LEN];
    char idStr[STR_ID_LEN];
    ARR(Step) steps;
    pthread_mutex_t eLock; /* lock for critical sections of the execution */
    mr_list* tasks;

    size_t nRecieved;
    size_t nCompleted;
    ARR(Record*) results;
    ARR(char*) errors;

    ExecutionCallbacks callbacks;
    MR_LoopTaskCtx* timeoutTask;
    size_t timeoutMS;
};

struct ExecutionCtx {
    Execution* e;
};

typedef struct mr_BufferWriter WriteSerializationCtx;
typedef struct mr_BufferReader ReaderSerializationCtx;

typedef struct ExecutionTask {
    ExecutionTaskCallback callback;
    void* pd;
}ExecutionTask;

ExecutionBuilder* MR_CreateExecutionBuilder(const char* readerName, void* args) {
    ExecutionBuilder* ret = MR_ALLOC(sizeof(*ret));
    ret->steps = array_new(ExecutionBuilderStep, 10);

    StepDefinition* rsd = mr_dictFetchValue(mrCtx.readerDict, readerName);
    RedisModule_Assert(rsd);
    ExecutionBuilderStep s = {
            .args = args,
            .argsType = rsd->type,
            .name = MR_STRDUP(readerName),
            .type = StepType_Reader,
    };
    ret->steps = array_append(ret->steps, s);

    return ret;
}

void MR_ExecutionBuilderCollect(ExecutionBuilder* builder) {
    ExecutionBuilderStep s = (ExecutionBuilderStep){
            .args = NULL,
            .argsType = NULL,
            .name = NULL,
            .type = StepType_Collect,
    };
    builder->steps = array_append(builder->steps, s);
}

void MR_ExecutionBuilderMap(ExecutionBuilder* builder, const char* name, void* args) {
    StepDefinition* msd = mr_dictFetchValue(mrCtx.mappersDict, name);
    RedisModule_Assert(msd);
    ExecutionBuilderStep s = {
            .args = args,
            .argsType = msd->type,
            .name = MR_STRDUP(name),
            .type = StepType_Mapper,
    };
    builder->steps = array_append(builder->steps, s);
}

void MR_FreeExecutionBuilder(ExecutionBuilder* builder) {
    for (size_t i = 0 ; i < array_len(builder->steps) ; ++i) {
        ExecutionBuilderStep* s = builder->steps + i;
        if (s->name) {
            MR_FREE(s->name);
        }
        if (s->args) {
            s->argsType->free(s->args);
        }
    }
    array_free(builder->steps);
    MR_FREE(builder);
}

static void SetId(char* idBuf, char* idBufStr, size_t id){
    char noneClusterId[REDISMODULE_NODE_ID_LEN] = {0};
    char* sharId;
    if(MR_ClusterIsClusterMode()){
        sharId = MR_ClusterGetMyId();
    }else{
        memset(noneClusterId, '0', REDISMODULE_NODE_ID_LEN);
        sharId = noneClusterId;
    }
    memcpy(idBuf, sharId, REDISMODULE_NODE_ID_LEN);
    memcpy(idBuf + REDISMODULE_NODE_ID_LEN, &id, sizeof(size_t));
    snprintf(idBufStr, STR_ID_LEN, "%.*s-%lld", REDISMODULE_NODE_ID_LEN, idBuf, *(long long*)&idBuf[REDISMODULE_NODE_ID_LEN]);
}

static void MR_InitializeFromStepDef(Step*s, StepDefinition* sd) {
    switch (s->bStep.type) {
    case StepType_Reader:
        s->read.readCallback = sd->callback;
        break;
    case StepType_Mapper:
        s->map.mapCallback = sd->callback;
        break;
    case StepType_Collect:
        s->collect.collectedRecords = array_new(Record*, 20);
        s->collect.nDone = 0;
        break;
    default:
        RedisModule_Assert(false);
    }
}

static StepDefinition* MR_GetStepDefinition(StepType type, const char* name) {
    StepDefinition* sd = NULL;
    switch (type) {
    case StepType_Mapper:
        sd = mr_dictFetchValue(mrCtx.mappersDict, name);
        break;
    case StepType_Reader:
        sd = mr_dictFetchValue(mrCtx.readerDict, name);
        break;
    default:
        sd = NULL;
    }
    return sd;
}

static void MR_CreateExecutionStep(Step*s, ExecutionBuilderStep* builderStep) {
    s->bStep.type = builderStep->type;
    s->bStep.argsType = builderStep->argsType;
    s->bStep.name = builderStep->name? MR_STRDUP(builderStep->name) : NULL;
    if (builderStep->args) {
        s->bStep.args = s->bStep.argsType->dup(builderStep->args);
    } else {
        s->bStep.args = NULL;
    }

    StepDefinition* sDef = MR_GetStepDefinition(s->bStep.type, s->bStep.name);
    MR_InitializeFromStepDef(s, sDef);
}

static Execution* MR_ExecutionAlloc() {
    Execution* e = MR_ALLOC(sizeof(*e));
    e->refCount = 1;
    e->steps = array_new(Step, 10);
    pthread_mutex_init(&e->eLock, NULL);
    e->tasks = mr_listCreate();
    mr_listSetFreeMethod(e->tasks, MR_FREE);
    e->nRecieved = 0;
    e->nCompleted = 0;
    e->results = array_new(Record*, 10);
    e->errors = array_new(char*, 10);
    e->callbacks = (ExecutionCallbacks){
            .done = {.pd = NULL, .callback = NULL},
            .resume = {.pd = NULL, .callback = NULL},
            .hold = {.pd = NULL, .callback = NULL},
    };
    e->timeoutTask = NULL;
    e->timeoutMS = EXECUTION_DEFAULT_MAX_IDLE_MS;
    e->flags = 0;
    return e;
}

Execution* MR_CreateExecution(ExecutionBuilder* builder) {
    Execution* e = MR_ExecutionAlloc();

    /* Set execution id */
    size_t id = __atomic_add_fetch(&mrCtx.lastExecutionId, 1, __ATOMIC_RELAXED);
    SetId(e->id, e->idStr, id);

    /* Copy steps array. */
    for (size_t i = 0 ; i < array_len(builder->steps) ; ++i) {
        ExecutionBuilderStep* builderStep = builder->steps + i;
        Step s;
        MR_CreateExecutionStep(&s, builderStep);
        s.index = array_len(e->steps);
        s.flags = 0;
        e->steps = array_append(e->steps, s);
    }

    e->flags |= ExecutionFlag_Initiator;
    if (!MR_ClusterIsClusterMode()) {
        e->flags |= ExecutionFlag_Local;
    }

    return e;
}

static size_t MR_SetRecordToStep(Execution* e, size_t stepIndex, Record* r) {
    RedisModule_Assert(stepIndex < array_len(e->steps));
    Step* s = e->steps + stepIndex;
    switch (s->bStep.type) {
    case StepType_Collect:
        s->collect.collectedRecords = array_append(s->collect.collectedRecords, r);
        return array_len(s->collect.collectedRecords);
        break;
    default:
        RedisModule_Assert(0);
    }
    RedisModule_Assert(0);
    return 0;
}

static size_t MR_PerformStepDoneOp(Execution* e, size_t stepIndex) {
    RedisModule_Assert(stepIndex < array_len(e->steps));
    Step* s = e->steps + stepIndex;
    switch (s->bStep.type) {
    case StepType_Collect:
        ++s->collect.nDone;
        return s->collect.nDone;
        break;
    default:
        RedisModule_Assert(0);
    }
    RedisModule_Assert(0);
    return 0;
}

/* Execution task */
static void MR_StepDone(Execution* e, void* pd) {
    RedisModuleString* payload = pd;

    /* deserialize record, set it on the right step. */
    size_t dataLen;
    const char* data = RedisModule_StringPtrLen(payload, &dataLen);
    mr_Buffer buff = (mr_Buffer){
            .buff = (char*)data,
            .size = dataLen,
            .cap = dataLen,
    };
    mr_BufferReader reader;
    mr_BufferReaderInit(&reader, &buff);
    size_t executionIdLen;
    const char* executionId = mr_BufferReaderReadBuff(&reader, &executionIdLen);
    RedisModule_Assert(executionIdLen == ID_LEN);

    size_t stepIndex = mr_BufferReaderReadLong(&reader);

    RedisModule_ThreadSafeContextLock(mr_staticCtx);
    RedisModule_FreeString(NULL, payload);
    RedisModule_ThreadSafeContextUnlock(mr_staticCtx);

    if (MR_PerformStepDoneOp(e, stepIndex) == MR_ClusterGetSize() - 1){
        /* All shards are done running the step, we can continue the execution. */
        MR_RunExecution(e, NULL);
    }
}

/* Execution task */
static void MR_SetRecord(Execution* e, void* pd) {
    RedisModuleString* payload = pd;

    /* deserialize record, set it on the right step. */
    size_t dataLen;
    const char* data = RedisModule_StringPtrLen(payload, &dataLen);
    mr_Buffer buff = (mr_Buffer){
            .buff = (char*)data,
            .size = dataLen,
            .cap = dataLen,
    };
    mr_BufferReader reader;
    mr_BufferReaderInit(&reader, &buff);
    size_t executionIdLen;
    const char* executionId = mr_BufferReaderReadBuff(&reader, &executionIdLen);
    RedisModule_Assert(executionIdLen == ID_LEN);

    size_t stepIndex = mr_BufferReaderReadLong(&reader);

    Record* r = MR_RecordDeSerialize(&reader);

    RedisModule_ThreadSafeContextLock(mr_staticCtx);
    RedisModule_FreeString(NULL, payload);
    RedisModule_ThreadSafeContextUnlock(mr_staticCtx);

    if (MR_SetRecordToStep(e, stepIndex, r) > 10000){
        /* There is enough records to process, lets continue running. */
        MR_RunExecution(e, NULL);
    }
}

/* Remote function call, runs on the event loop */
static void MR_PassRecord(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    size_t dataLen;
    const char* data = RedisModule_StringPtrLen(payload, &dataLen);
    mr_Buffer buff = (mr_Buffer){
            .buff = (char*)data,
            .size = dataLen,
            .cap = dataLen,
    };
    mr_BufferReader reader;
    mr_BufferReaderInit(&reader, &buff);
    size_t executionIdLen;
    const char* executionId = mr_BufferReaderReadBuff(&reader, &executionIdLen);
    RedisModule_Assert(executionIdLen == ID_LEN);

    Execution* e = mr_dictFetchValue(mrCtx.executionsDict, executionId);
    if (!e) {
        ++mrCtx.stats.nMissedExecutions;
        return;
    }

    /* run the execution on the thread pool */
    MR_ExecutionAddTask(e, MR_SetRecord, RedisModule_HoldString(NULL, payload));
}

static void MR_SendRecord(Execution* e, Step* s, Record** r, const char* nodeId) {
    mr_Buffer buff;
    mr_BufferInitialize(&buff);
    mr_BufferWriter buffWriter;
    mr_BufferWriterInit(&buffWriter, &buff);
    /* write the execution id */
    mr_BufferWriterWriteBuff(&buffWriter, e->id, ID_LEN);
    /* write the step index to add the record to */
    mr_BufferWriterWriteLong(&buffWriter, s->index);
    /* Write the record */
    MR_RecordSerialize(*r, &buffWriter);

    MR_ClusterSendMsg(nodeId, PASS_RECORD_FUNCTION_ID, buff.buff, buff.size);
}

/* Remote function call, runs on the event loop */
static void MR_NotifyStepDone(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    size_t dataLen;
    const char* data = RedisModule_StringPtrLen(payload, &dataLen);
    mr_Buffer buff = (mr_Buffer){
            .buff = (char*)data,
            .size = dataLen,
            .cap = dataLen,
    };
    mr_BufferReader reader;
    mr_BufferReaderInit(&reader, &buff);
    size_t executionIdLen;
    const char* executionId = mr_BufferReaderReadBuff(&reader, &executionIdLen);
    RedisModule_Assert(executionIdLen == ID_LEN);
    Execution* e = mr_dictFetchValue(mrCtx.executionsDict, executionId);
    if (!e) {
        ++mrCtx.stats.nMissedExecutions;
        return;
    }

    /* run the execution on the thread pool */
    MR_ExecutionAddTask(e, MR_StepDone, RedisModule_HoldString(NULL, payload));
}

static StepRunResult MR_RunCollectStep(ExecutionCtx* eCtx, Step* s, Record** r) {
    if (eCtx->e->flags & ExecutionFlag_Local) {
        /* on local execution, collect does nothing */
        return StepRunResult_Continue;
    }
    if (!(s->flags & StepFlag_Done)) {
        if (*r) {
            if (eCtx->e->flags & ExecutionFlag_Initiator) {
                /* We are the initiator, lets continue processing the Record. */
                return StepRunResult_Continue;
            }
            /* send record to the initiator */
            MR_SendRecord(eCtx->e, s, r, eCtx->e->id);
            /* we pass the record to the initiator, we can free it now */
            MR_RecordFree(*r);
            return StepRunResult_More;
        }
        if (!(eCtx->e->flags & ExecutionFlag_Initiator)) {
            /* got all the record, notify initiator that we are done passing all the record. */
            mr_Buffer buff;
            mr_BufferInitialize(&buff);
            mr_BufferWriter buffWriter;
            mr_BufferWriterInit(&buffWriter, &buff);
            /* write the execution id */
            mr_BufferWriterWriteBuff(&buffWriter, eCtx->e->id, ID_LEN);
            /* write the step index to add the record to */
            mr_BufferWriterWriteLong(&buffWriter, s->index);
            MR_ClusterSendMsg(eCtx->e->id, NOTIFY_STEP_DONE_FUNCTION_ID, buff.buff, buff.size);
        }
    }

    if (!(eCtx->e->flags & ExecutionFlag_Initiator)) {
        /* we are not the initiator, we have nothing to give here */
        s->flags &= StepFlag_Done;
        *r = NULL;
        return StepRunResult_Continue;
    }

    if (array_len(s->collect.collectedRecords) > 0) {
        /* process Records that came from other shards */
        *r = array_pop(s->collect.collectedRecords);
        return StepRunResult_Continue;
    }

    if (s->collect.nDone == MR_ClusterGetSize() - 1) {
        /* all shards finished sending all the record, we are done. */
        s->flags &= StepFlag_Done;
        *r = NULL;
        return StepRunResult_Continue;
    } else {
        /* hold the execution, wait for shards to send data */
        return StepRunResult_Hold;
    }
}

static StepRunResult MR_RunMapStep(ExecutionCtx* eCtx, Step* s, Record** r) {
    if (!*r) {
        s->flags &= StepFlag_Done;
        return StepRunResult_Continue;
    }
    *r = s->map.mapCallback(eCtx, *r, s->bStep.args);
    return StepRunResult_Continue;
}

static StepRunResult MR_RunReaderStep(ExecutionCtx* eCtx, Step* s, Record** r) {
    *r = s->read.readCallback(eCtx, s->bStep.args);
    if (!*r) {
        s->flags &= StepFlag_Done;
    }
    return StepRunResult_Continue;
}

static StepRunResult MR_RunStep(ExecutionCtx* eCtx, Step* s, Record** r) {
    switch(s->bStep.type){
    case StepType_Reader:
        return MR_RunReaderStep(eCtx, s, r);
    case StepType_Mapper:
        return MR_RunMapStep(eCtx, s, r);
    case StepType_Collect:
        return MR_RunCollectStep(eCtx, s, r);
    default:
        RedisModule_Assert(false);
    }
    RedisModule_Assert(false);
    return StepRunResult_Continue;
}

static void MR_ResetStack(ExecutionCtx* eCtx, Step** stack) {
    array_trimm_len(stack, 0);
    /* Find the first step which is not done and take is
     * as the step to resume the run */
    for (size_t i = 0 ; i < array_len(eCtx->e->steps) ; ++i){
        Step* s = eCtx->e->steps + i;
        if (!(s->flags & StepFlag_Done)) {
            stack = array_append(stack, s);
            break;
        }
    }
}

static int MR_RunExecutionInternal(ExecutionCtx* eCtx) {
    array_new_on_stack(Step*, 30, stack);
    MR_ResetStack(eCtx, stack);
    Record* currRecord = NULL;
    while (1) {
        Step* currStep = stack[array_len(stack) - 1];
        StepRunResult res = MR_RunStep(eCtx, currStep, &currRecord);
        switch(res) {
        case StepRunResult_Continue:
            if (currStep->index == array_len(eCtx->e->steps) - 1) {
                /* last step */
                if (!currRecord) {
                    /* currRecord is NULL and we reach the last step, we are done. */
                    array_free(stack);
                    return 1;
                }
                /* no more steps, currRecord is considered a result */
                eCtx->e->results = array_append(eCtx->e->results, currRecord);
                MR_ResetStack(eCtx, stack);
            } else {
                /* more steps to run, add the next step to the stack */
                stack = array_append(stack, eCtx->e->steps + currStep->index + 1);
            }
            break;
        case StepRunResult_More:
            MR_ResetStack(eCtx, stack);
            break;
        case StepRunResult_Hold:
            /* hold the execution */
            array_free(stack);
            return 0;
        default:
            RedisModule_Assert(false);
        }
    }
    RedisModule_Assert(false);
    return 1;
}

static void MR_ExecutionInvokeCallback(ExecutionCtx* eCtx, ExecutionCallbackData* callback) {
    if (callback->callback) {
        callback->callback(eCtx, callback->pd);
    }
}

static void MR_DisposeExecution(Execution* e, void* pd) {
    MR_FreeExecution(e);
}

/* runs on the event loop, remove the execution from the
 * executions dictionary and send a dispose task */
static void MR_DeleteExecution(void* ctx) {
    Execution* e = ctx;
    mr_dictDelete(mrCtx.executionsDict, e->id);
    /* Send dispose execution task, this will be last task this execution will ever recieve. */
    MR_ExecutionAddTask(e, MR_DisposeExecution, NULL);
}

/* Remote function call, runs on the event loop */
static void MR_DropExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    size_t idLen;
    const char* executionId = RedisModule_StringPtrLen(payload, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    Execution* e = mr_dictFetchValue(mrCtx.executionsDict, executionId);
    if (!e) {
        ++mrCtx.stats.nMissedExecutions;
        return;
    }

    MR_DeleteExecution(e);
}

/* Remote function call, runs on the event loop */
static void MR_NotifyDone(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    size_t idLen;
    const char* executionId = RedisModule_StringPtrLen(payload, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    Execution* e = mr_dictFetchValue(mrCtx.executionsDict, executionId);
    if (!e) {
        ++mrCtx.stats.nMissedExecutions;
        return;
    }

    ++e->nCompleted;
    if (e->nCompleted == MR_ClusterGetSize() - 1) {
        /* Execution is finished on all the shards,
         * We will not recieve any more messages on it.
         * We can ask all the shards to drop it and we can
         * drop it ourself. */
        MR_ClusterCopyAndSendMsg(NULL, DROP_EXECUTION_FUNCTION_ID, e->id, ID_LEN);
        MR_DeleteExecution(e);
    }
}

static void MR_RunExecution(Execution* e, void* pd) {
    ExecutionCtx eCtx = {
            .e = e,
    };
    MR_ExecutionInvokeCallback(&eCtx, &e->callbacks.resume);
    if (MR_RunExecutionInternal(&eCtx)) {
        /* we are done, invoke on done callback and perform termination process. */
        MR_ExecutionInvokeCallback(&eCtx, &e->callbacks.done);
        if (e->flags & ExecutionFlag_Local) {
            /* not need to wait to any shard, delete the execution */
            MR_EventLoopAddTask(MR_DeleteExecution, e);
            return;
        }
        if (!(e->flags & ExecutionFlag_Initiator)) {
            MR_ClusterCopyAndSendMsg(e->id, NOTIFY_DONE_FUNCTION_ID, e->id, ID_LEN);
        }
    } else {
        MR_ExecutionInvokeCallback(&eCtx, &e->callbacks.hold);
    }
}

/* Remote function call, runs on the event loop */
static void MR_InvokeExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    size_t idLen;
    const char* executionId = RedisModule_StringPtrLen(payload, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    Execution* e = mr_dictFetchValue(mrCtx.executionsDict, executionId);
    if (!e) {
        ++mrCtx.stats.nMissedExecutions;
        return;
    }

    /* run the execution on the thread pool */
    MR_ExecutionAddTask(e, MR_RunExecution, NULL);
}

/* Remote function call, runs on the event loop */
static void MR_AckExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    size_t idLen;
    const char* executionId = RedisModule_StringPtrLen(payload, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    Execution* e = mr_dictFetchValue(mrCtx.executionsDict, executionId);
    if (!e) {
        ++mrCtx.stats.nMissedExecutions;
        return;
    }

    ++e->nRecieved;
    if (e->nRecieved == MR_ClusterGetSize() - 1) {
        /* all shards have recieved the execution, we can invoke it. */
        MR_ClusterCopyAndSendMsg(NULL, INVOKE_EXECUTION_FUNCTION_ID, e->id, ID_LEN);
        /* run the execution on the thread pool */
        MR_ExecutionAddTask(e, MR_RunExecution, NULL);
    }
}

/* Runs in the event loop, save the execution
 * in the executions dictionary. Send ack on
 * recieving the execution to the initiator */
static void MR_RecievedExecution(void* ctx) {
    Execution* e = ctx;

    /* add the execution to the execution dictionary */
    mr_dictAdd(mrCtx.executionsDict, e->id, e);

    /* tell the initiator that we recieved the execution */
    MR_ClusterCopyAndSendMsg(e->id, ACK_EXECUTION_FUNCTION_ID, e->id, ID_LEN);
}

static Execution* MR_ExecutionDeserialize(mr_BufferReader* buffReader) {
    size_t executionIdLen;
    const char* executionId = mr_BufferReaderReadBuff(buffReader, &executionIdLen);
    RedisModule_Assert(executionIdLen == ID_LEN);
    size_t nSteps = mr_BufferReaderReadLong(buffReader);

    Execution* e = MR_ExecutionAlloc();
    memcpy(e->id, executionId, ID_LEN);
    snprintf(e->idStr, STR_ID_LEN, "%.*s-%lld", REDISMODULE_NODE_ID_LEN, e->id, *(long long*)&e->id[REDISMODULE_NODE_ID_LEN]);

    for (size_t i = 0 ; i < nSteps ; ++i) {
        Step s;
        s.bStep.type = mr_BufferReaderReadLong(buffReader);
        if (mr_BufferReaderReadLong(buffReader)) {
            /* read step name */
            s.bStep.name = MR_STRDUP(mr_BufferReaderReadString(buffReader));
        } else {
            s.bStep.name = NULL;
        }

        StepDefinition* sd = MR_GetStepDefinition(s.bStep.type, s.bStep.name);
        if (sd) {
            s.bStep.argsType = sd->type;
        } else {
            s.bStep.argsType = NULL;
        }

        if (mr_BufferReaderReadLong(buffReader)) {
            /* read step args */
            s.bStep.args = s.bStep.argsType->deserialize(buffReader);
        } else {
            s.bStep.args = NULL;
        }

        MR_InitializeFromStepDef(&s, sd);
        s.flags = 0;
        s.index = i;
        e->steps = array_append(e->steps, s);
    }
    return e;
}

static void MR_RecieveExecution(void* pd) {
    RedisModuleString* payload = pd;
    size_t dataSize;
    const char* data = RedisModule_StringPtrLen(payload, &dataSize);
    mr_Buffer buff = {
            .buff = (char*)data,
            .size = dataSize,
            .cap = dataSize,
    };
    mr_BufferReader buffReader;
    mr_BufferReaderInit(&buffReader, &buff);
    Execution* e = MR_ExecutionDeserialize(&buffReader);

    /* We must take the Redis GIL to free the payload,
     * RedisModuleString refcount are not thread safe.
     * We better do it here and stuck on of the threads
     * in the thread pool then do it on the event loop.
     * Possible optimization would be to batch multiple
     * payloads into one GIL locking */
    RedisModule_ThreadSafeContextLock(mr_staticCtx);
    RedisModule_FreeString(NULL, payload);
    RedisModule_ThreadSafeContextUnlock(mr_staticCtx);

    /* Finish deserializing the execution, we need to
     * return to the event loop and save the execution
     * in the executions dictionary */
    MR_EventLoopAddTask(MR_RecievedExecution, e);

}

/* Remote function call, runs on the event loop */
static void MR_NewExecutionRecieved(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, RedisModuleString* payload) {
    /* We can directly move the job to the thread pool.
     * We need to deserialize the execution and reply to the initiator. */
    mr_thpool_add_work(mrCtx.executionsThreadPool, MR_RecieveExecution, RedisModule_HoldString(NULL, payload));
}

static void MR_ExecutionStepSerialize(mr_BufferWriter* buffWriter, Step* s) {
    mr_BufferWriterWriteLong(buffWriter, s->bStep.type); /* write the step type */
    if (s->bStep.name) {
        mr_BufferWriterWriteLong(buffWriter, 1); /* name exists */
        mr_BufferWriterWriteString(buffWriter, s->bStep.name); /* write the step name */
    } else {
        mr_BufferWriterWriteLong(buffWriter, 0); /* name does not exists */
    }
    /* serialize step args */
    if (s->bStep.args) {
        mr_BufferWriterWriteLong(buffWriter, 1); /* args exists */
        s->bStep.argsType->serialize(buffWriter, s->bStep.args);
    } else {
        mr_BufferWriterWriteLong(buffWriter, 0); /* args does not exists */
    }
}

static void MR_ExecutionSerialize(mr_BufferWriter* buffWriter, Execution* e) {
    mr_BufferWriterWriteBuff(buffWriter, e->id, ID_LEN); /* write the exectuion id */
    mr_BufferWriterWriteLong(buffWriter, array_len(e->steps)); /* number of steps */
    for (size_t i = 0 ; i < array_len(e->steps) ; ++i) {
        MR_ExecutionStepSerialize(buffWriter, e->steps + i);
    }
}

/* Execution task distribute callback */
static void MR_ExecutionDistribute(Execution* e, void* pd) {
    mr_Buffer buff;
    mr_BufferInitialize(&buff);
    mr_BufferWriter buffWriter;
    mr_BufferWriterInit(&buffWriter, &buff);
    MR_ExecutionSerialize(&buffWriter, e);
    MR_ClusterSendMsg(NULL, NEW_EXECUTION_RECIEVED_FUNCTION_ID, buff.buff, buff.size);

    /* now we wait for shards to respond that they got the execution */
}

static void MR_ExecutionTimedOutInternal(Execution* e, void* pd) {
    e->errors = array_append(e->errors, MR_STRDUP("execution max idle reached"));
    /* we are done, invoke on done callback. */
    ExecutionCtx eCtx = {
            .e = e,
    };
    MR_ExecutionInvokeCallback(&eCtx, &e->callbacks.done);
    MR_FreeExecution(e);
}

/* runs on the event loop */
static void MR_ExecutionTimedOut(void* ctx) {
    Execution* e = ctx;
    /* execution timed out */
    e->timeoutTask = NULL;
    ++mrCtx.stats.nMaxIdleReached;
    /* Delete the execution from the executions dictionary,
     * We will ignore further messages on this execution. */
    mr_dictDelete(mrCtx.executionsDict, e->id);
    MR_ExecutionAddTask(e, MR_ExecutionTimedOutInternal, NULL);
}

static void MR_ExecutionMain(void* pd) {
    Execution* e = pd;
    pthread_mutex_lock(&e->eLock);
    mr_listNode *head = mr_listFirst(e->tasks);
    ExecutionTask* task = mr_listNodeValue(head);
    pthread_mutex_unlock(&e->eLock);

    ExecutionTaskCallback callback = task->callback;
    callback(e, task->pd);
    if (callback == MR_DisposeExecution || callback == MR_ExecutionTimedOutInternal) {
        /* MR_DisposeExecution means we will not longer gets any events
         * on this execution and we should not longer touch it. */
        return;
    }

    pthread_mutex_lock(&e->eLock);
    /* pop current task out */
    mr_listDelNode(e->tasks, head);

    /* check if there are more tasks to run */
    if (mr_listLength(e->tasks) > 0) {
        /* more work to do, for fairness we will not run now.
         * We will add ourselfs to the thread pool */
        mr_thpool_add_work(mrCtx.executionsThreadPool, MR_ExecutionMain, e);
    } else {
        e->timeoutTask = MR_EventLoopAddTaskWithDelay(MR_ExecutionTimedOut, e, e->timeoutMS);
    }

    pthread_mutex_unlock(&e->eLock);
}

/* should be invoked only from the event loop */
static void MR_ExecutionAddTask(Execution* e, ExecutionTaskCallback callback, void* pd) {
    ExecutionTask* task = MR_ALLOC(sizeof(*task));
    task->callback = callback;
    task->pd = pd;
    pthread_mutex_lock(&e->eLock);
    if (e->timeoutTask) {
        MR_EventLoopDelayTaskCancel(e->timeoutTask);
        e->timeoutTask = NULL;
    }
    size_t lenBeforeTask = mr_listLength(e->tasks);
    mr_listAddNodeTail(e->tasks, task);

    if (lenBeforeTask == 0) {
        /* nothing is currently running, add a task to the thread pool */
        mr_thpool_add_work(mrCtx.executionsThreadPool, MR_ExecutionMain, e);
    }

    pthread_mutex_unlock(&e->eLock);
}

/* Happends on the event loop,
 * Preper the execution to run and send
 * it to run on the thread pool. */
static void MR_ExecutionStart(void* ctx) {
    Execution* e = ctx;

    /* add the execution to the execution dictionary */
    mr_dictAdd(mrCtx.executionsDict, e->id, e);

    if (e->flags & ExecutionFlag_Local) {
        /* not need to distribute the executio,
         * we can simply start running it */
        MR_ExecutionAddTask(e, MR_RunExecution, NULL);
    } else {
        MR_ExecutionAddTask(e, MR_ExecutionDistribute, NULL);
    }
}

void MR_ExecutionSetOnDoneHandler(Execution* e, ExecutionCallback onDone, void* pd) {
    e->callbacks.done = (ExecutionCallbackData){
        .callback = onDone,
        .pd = pd,
    };
}

void MR_Run(Execution* e) {
    /* take ownership on the execution */
    __atomic_add_fetch(&e->refCount, 1, __ATOMIC_RELAXED);

    /* add the execution to the event loop */
    MR_EventLoopAddTask(MR_ExecutionStart, e);
}

Record* MR_ExecutionCtxGetResult(ExecutionCtx* ectx, size_t i) {
    return ectx->e->results[i];
}

size_t MR_ExecutionCtxGetResultsLen(ExecutionCtx* ectx) {
    return array_len(ectx->e->results);
}

const char* MR_ExecutionCtxGetError(ExecutionCtx* ectx, size_t i) {
    return ectx->e->errors[i];
}

size_t MR_ExecutionCtxGetErrorsLen(ExecutionCtx* ectx){
    return array_len(ectx->e->errors);
}

static void MR_StepDispose(Step* s) {
    if (s->bStep.name) {
        MR_FREE(s->bStep.name);
    }
    if (s->bStep.args) {
        s->bStep.argsType->free(s->bStep.args);
    }
    switch (s->bStep.type) {
    case StepType_Mapper:
    case StepType_Reader:
        break;
    case StepType_Collect:
        for (size_t i = 0 ; i < array_len(s->collect.collectedRecords) ; ++i){
            MR_RecordFree(s->collect.collectedRecords[i]);
        }
        array_free(s->collect.collectedRecords);
        break;
    default:
        RedisModule_Assert(0);
    }
}

void MR_FreeExecution(Execution* e) {
    if (__atomic_sub_fetch(&e->refCount, 1, __ATOMIC_RELAXED) > 0) {
        return;
    }
    for (size_t i = 0 ; i < array_len(e->steps) ; ++i) {
        MR_StepDispose(e->steps + i);
    }
    array_free(e->steps);
    mr_listRelease(e->tasks);
    for (size_t i = 0 ; i < array_len(e->results) ; ++i) {
        MR_RecordFree(e->results[i]);
    }
    array_free(e->results);
    for (size_t i = 0 ; i < array_len(e->errors) ; ++i) {
        MR_FREE(e->errors[i]);
    }
    array_free(e->errors);
    MR_FREE(e);
}

int MR_Init(RedisModuleCtx* ctx, size_t numThreads) {
    mr_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    if (MR_ClusterInit(ctx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    mrCtx.lastExecutionId = 0;
    mrCtx.executionsDict = mr_dictCreate(&dictTypeHeapIds, NULL);

    mrCtx.objectTypesDict = array_new(MRObjectType*, 10);

    mrCtx.readerDict = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);
    mrCtx.mappersDict = mr_dictCreate(&mr_dictTypeHeapStrings, NULL);

    mrCtx.executionsThreadPool = mr_thpool_init(numThreads);
    mrCtx.stats = (MRStats){
            .nMissedExecutions = 0,
            .nMaxIdleReached = 0,
    };

    for (size_t i = 0 ; i < sizeof(remoteFunctions)/sizeof(RemoteFunctionDef) ; ++i) {
        RemoteFunctionDef* rf = remoteFunctions + i;
        *(rf->funcIdPointer) = MR_ClusterRegisterMsgReceiver(rf->functionPointer);
    }

    MR_EventLoopStart();

    return REDISMODULE_OK;
}

int MR_RegisterObject(MRObjectType* t) {
    mrCtx.objectTypesDict = array_append(mrCtx.objectTypesDict, t);
    t->id = array_len(mrCtx.objectTypesDict) - 1;
    return REDISMODULE_OK;
}

MRObjectType* MR_GetObjectType(size_t id) {
    if (id >= array_len(mrCtx.objectTypesDict)) {
        return NULL;
    }
    return mrCtx.objectTypesDict[id];
}

void MR_RegisterReader(const char* name, ExecutionReader reader, MRObjectType* argType) {
    RedisModule_Assert(!mr_dictFetchValue(mrCtx.readerDict, name));
    StepDefinition* rsd = MR_ALLOC(sizeof(*rsd));
    *rsd = (StepDefinition) {
        .name = MR_STRDUP(name),
        .type = argType,
        .callback = reader,
    };
    mr_dictAdd(mrCtx.readerDict, rsd->name, rsd);
}

void MR_RegisterMapper(const char* name, ExecutionMapper mapper, MRObjectType* argType) {
    RedisModule_Assert(!mr_dictFetchValue(mrCtx.mappersDict, name));
    StepDefinition* msd = MR_ALLOC(sizeof(*msd));
    *msd = (StepDefinition) {
        .name = MR_STRDUP(name),
        .type = argType,
        .callback = mapper,
    };
    mr_dictAdd(mrCtx.mappersDict, msd->name, msd);
}

long long MR_SerializationCtxReadeLongLong(ReaderSerializationCtx* sctx) {
    return mr_BufferReaderReadLong(sctx);
}

const char* MR_SerializationCtxReadeBuffer(ReaderSerializationCtx* sctx, size_t* len) {
    return mr_BufferReaderReadBuff(sctx, len);
}

double MR_SerializationCtxReadeDouble(ReaderSerializationCtx* sctx) {
    return (double)mr_BufferReaderReadLong(sctx);
}

void MR_SerializationCtxWriteLongLong(WriteSerializationCtx* sctx, long long val) {
    mr_BufferWriterWriteLong(sctx, val);
}

void MR_SerializationCtxWriteBuffer(WriteSerializationCtx* sctx, const char* buff, size_t len) {
    mr_BufferWriterWriteBuff(sctx, buff, len);
}

void MR_SerializationCtxWriteDouble(WriteSerializationCtx* sctx, double val) {
    mr_BufferWriterWriteLong(sctx, (long) val);
}

int MR_WriteSerializationCtxIsError(WriteSerializationCtx* sctx) {
    return 0;
}

int MR_ReadSerializationCtxIsError(ReaderSerializationCtx* sctx) {
    return 0;
}
