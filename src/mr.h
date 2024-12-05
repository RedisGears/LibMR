/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef SRC_MR_H_
#define SRC_MR_H_

#include <limits.h>
#include <stddef.h>

#define LIBMR_API __attribute__ ((visibility("default")))

typedef struct MRError MRError;

extern struct RedisModuleCtx* mr_staticCtx;

/* Opaque struct build an execution */
typedef struct ExecutionBuilder ExecutionBuilder;

/* Opaque struct represents an execution */
typedef struct Execution Execution;

/* Opaque struct represents a record that pass in the execution pipe */
typedef struct Record Record;

/* Opaque struct that allow to serialize and deserialize objects */
typedef struct mr_BufferReader ReaderSerializationCtx;
typedef struct mr_BufferWriter WriteSerializationCtx;

/* MRObjectType callbacks definition */
typedef void (*ObjectFree)(void* arg);
typedef void* (*ObjectDuplicate)(void* arg);
typedef void (*ObjectSerialize)(WriteSerializationCtx* sctx, void* arg, MRError** error);
typedef void* (*ObjectDeserialize)(ReaderSerializationCtx* sctx, MRError** error);
typedef char* (*ObjectToString)(void* arg);

/* represent map reduce object type */
typedef struct MRObjectType{
    char* type;
    size_t id;
    ObjectFree free;
    ObjectDuplicate dup;
    ObjectSerialize serialize;
    ObjectDeserialize deserialize;
    ObjectToString tostring;
}MRObjectType;

LIBMR_API int MR_ClusterIsInClusterMode();

/* Opaque struct that is given to execution steps */
typedef struct ExecutionCtx ExecutionCtx;
LIBMR_API Record* MR_ExecutionCtxGetResult(ExecutionCtx* ectx, size_t i);
LIBMR_API size_t MR_ExecutionCtxGetResultsLen(ExecutionCtx* ectx);
LIBMR_API const char* MR_ExecutionCtxGetError(ExecutionCtx* ectx, size_t i);
LIBMR_API size_t MR_ExecutionCtxGetErrorsLen(ExecutionCtx* ectx);
LIBMR_API void MR_ExecutionCtxSetError(ExecutionCtx* ectx, const char* err, size_t len);

/* Execution Callback definition */
typedef void(*ExecutionCallback)(ExecutionCtx* ectx, void* pd);

/* step functions signiture */
typedef Record* (*ExecutionReader)(ExecutionCtx* ectx, void* args);
typedef Record* (*ExecutionMapper)(ExecutionCtx* ectx, Record* r, void* args);
typedef int (*ExecutionFilter)(ExecutionCtx* ectx, Record* r, void* args);
typedef Record* (*ExecutionAccumulator)(ExecutionCtx* ectx, Record* accumulator, Record* r, void* args);
typedef void (*RemoteTask)(Record* r, void* args, void (*onDone)(void* PD, Record *r), void (*onError)(void* PD, MRError *r), void *pd);

typedef void (*MR_RunOnKey_OnError)(void *pd, MRError* err);
typedef void (*MR_RunOnKey_OnDone)(void *pd, Record* result);

/* Run a remote task on a shard responsible for a given key.
 * There is not guarantee on which thread the task will run, if
 * the current shard is responsible for the given key or if its
 * a none cluster environment, then the callback will be called
 * immediately (an so the onDone/onError) callbacks.
 * If the key located on the remote shard, the task will
 * be invoke on the thread pool of this remote shard, the onDone/onError
 * callback will be invoke on the thread pool of the current shard. */
LIBMR_API void MR_RunOnKey(const char* keyName,
                           size_t keyNameSize,
                           const char* remoteTaskName,
                           void* args,
                           Record* r,
                           MR_RunOnKey_OnDone onDone,
                           MR_RunOnKey_OnError onError,
                           void *pd,
                           size_t timeout);

typedef void (*MR_RunOnShards_OnDone)(void *pd, Record** result, size_t nResults, MRError** errs, size_t nErrs);

LIBMR_API void MR_RunOnAllShards(const char* remoteTaskName,
                                 void* args,
                                 Record* r,
                                 MR_RunOnShards_OnDone onDone,
                                 void *pd,
                                 size_t timeout);

/* Creatign a new execution builder */
LIBMR_API ExecutionBuilder* MR_CreateExecutionBuilder(const char* readerName, void* args);

/* Add map step to the given builder.
 * The function takes ownership on the given
 * args so the user is not allow to use it anymore. */
LIBMR_API void MR_ExecutionBuilderMap(ExecutionBuilder* builder, const char* name, void* args);

/* Add filter step to the given builder.
 * The function takes ownership on the given
 * args so the user is not allow to use it anymore. */
LIBMR_API void MR_ExecutionBuilderFilter(ExecutionBuilder* builder, const char* name, void* args);

/* Add accumulate step to the given builder.
 * The function takes ownership on the given
 * args so the user is not allow to use it anymore. */
LIBMR_API void MR_ExecutionBuilderBuilAccumulate(ExecutionBuilder* builder, const char* name, void* args);

/* Add a collect step to the builder.
 * Will return all the records to the initiator */
LIBMR_API void MR_ExecutionBuilderCollect(ExecutionBuilder* builder);

/* Add a reshuffle step to the builder. */
LIBMR_API void MR_ExecutionBuilderReshuffle(ExecutionBuilder* builder);

/* Free the give execution builder */
LIBMR_API void MR_FreeExecutionBuilder(ExecutionBuilder* builder);

/* Create execution from the given builder.
 * Returns Execution which need to be freed using RM_FreeExecution.
 * The user can use the returned Execution to set
 * different callbacks, such as on_done callback and hold/resume callbacks.
 *
 * After callbacks are set the user can run the execution using MR_Run
 *
 * The function borrow the builder, which means that once returned
 * the user can still use the builder, change it, or create more executions
 * from it.
 *
 * Return NULL on error and set the error on err out param */
LIBMR_API Execution* MR_CreateExecution(ExecutionBuilder* builder, MRError** err);

/* Set max idle time (in ms) for the given execution */
LIBMR_API void MR_ExecutionSetMaxIdle(Execution* e, size_t maxIdle);

/* Set on execution done callbac */
LIBMR_API void MR_ExecutionSetOnDoneHandler(Execution* e, ExecutionCallback onDone, void* pd);

/* Run the given execution, should at most once on each execution. */
LIBMR_API void MR_Run(Execution* e);

/* Free the given execution */
LIBMR_API void MR_FreeExecution(Execution* e);

/* Initialize mr library */
LIBMR_API int MR_Init(struct RedisModuleCtx* ctx, size_t numThreads, char *username, char *password);

/* Register a new object type */
LIBMR_API int MR_RegisterObject(MRObjectType* t);

/* Register a reader */
LIBMR_API void MR_RegisterReader(const char* name, ExecutionReader reader, MRObjectType* argType);

/* Register a map step */
LIBMR_API void MR_RegisterMapper(const char* name, ExecutionMapper mapper, MRObjectType* argType);

/* Register a filter step */
LIBMR_API void MR_RegisterFilter(const char* name, ExecutionFilter filter, MRObjectType* argType);

/* Register an accumulate step */
LIBMR_API void MR_RegisterAccumulator(const char* name, ExecutionAccumulator accumulator, MRObjectType* argType);

/* Register a remote task */
LIBMR_API void MR_RegisterRemoteTask(const char* name, RemoteTask remote, MRObjectType* argType);

/* Serialization Context functions */
LIBMR_API long long MR_SerializationCtxReadLongLong(ReaderSerializationCtx* sctx, MRError** err);
LIBMR_API const char* MR_SerializationCtxReadBuffer(ReaderSerializationCtx* sctx, size_t* len, MRError** err);
LIBMR_API double MR_SerializationCtxReadDouble(ReaderSerializationCtx* sctx, MRError** err);
LIBMR_API void MR_SerializationCtxWriteLongLong(WriteSerializationCtx* sctx, long long val, MRError** err);
LIBMR_API void MR_SerializationCtxWriteBuffer(WriteSerializationCtx* sctx, const char* buff, size_t len, MRError** err);
LIBMR_API void MR_SerializationCtxWriteDouble(WriteSerializationCtx* sctx, double val, MRError** err);

/* records functions */
typedef void (*SendAsRedisReply)(struct RedisModuleCtx*, void* record);
typedef size_t (*HashTag)(void* record);

/* represent record type */
typedef struct MRRecordType{
    MRObjectType type;
    SendAsRedisReply sendReply;
    HashTag hashTag;
}MRRecordType;

/* Base record struct, each record should have it
 * as first value */
struct Record {
    MRRecordType* recordType;
};

/* Register a new Record type */
LIBMR_API int MR_RegisterRecord(MRRecordType* t);

/* Free the give Record */
LIBMR_API void MR_RecordFree(Record* r);

/* Serialize the given Record */
LIBMR_API void MR_RecordSerialize(Record* r, WriteSerializationCtx* writer);

/* Deserialize the given Record */
LIBMR_API Record* MR_RecordDeSerialize(ReaderSerializationCtx* reader);

/* Calculate slot on the given buffer */
LIBMR_API size_t MR_CalculateSlot(const char* buff, size_t len);

/* Check if the given slot is in the current shard slot range */
LIBMR_API int MR_IsMySlot(size_t slot);

/* Create a new error object */
LIBMR_API MRError* MR_ErrorCreate(const char* msg, size_t len);

/* Get error message from the error object */
LIBMR_API const char* MR_ErrorGetMessage(MRError* err);

/* Free the error object */
LIBMR_API void MR_ErrorFree(MRError* err);

/***************** no public API **********************/
MRObjectType* MR_GetObjectType(size_t id);

#endif /* SRC_MR_H_ */
