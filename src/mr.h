#ifndef SRC_MR_H_
#define SRC_MR_H_

#include <limits.h>
#include <stddef.h>

#define LIBMR_API __attribute__ ((visibility("default")))

typedef struct RedisModuleCtx RedisModuleCtx;

extern RedisModuleCtx* mr_staticCtx;

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
typedef int (*ObjectSerialize)(WriteSerializationCtx* sctx, void* arg);
typedef void* (*ObjectDeserialize)(ReaderSerializationCtx* sctx);
typedef char* (*ObjectToString)(void* arg);
typedef void (*SendAsRedisReply)(RedisModuleCtx*);

/* represent map reduce object type */
typedef struct MRObjectType{
    char* type;
    size_t id;
    ObjectFree free;
    ObjectDuplicate dup;
    ObjectSerialize serialize;
    ObjectDeserialize deserialize;
    ObjectToString tostring;
    SendAsRedisReply sendReply;
}MRObjectType;

/* Opaque struct that is given to execution steps */
typedef struct ExecutionCtx ExecutionCtx;
LIBMR_API Record* MR_ExecutionCtxGetResult(ExecutionCtx* ectx, size_t i);
LIBMR_API size_t MR_ExecutionCtxGetResultsLen(ExecutionCtx* ectx);
LIBMR_API const char* MR_ExecutionCtxGetError(ExecutionCtx* ectx, size_t i);
LIBMR_API size_t MR_ExecutionCtxGetErrorsLen(ExecutionCtx* ectx);

/* Execution Callback definition */
typedef void(*ExecutionCallback)(ExecutionCtx* ectx, void* pd);

/* step functions signiture */
typedef Record* (*ExecutionReader)(ExecutionCtx* ectx, void* args);
typedef Record* (*ExecutionMapper)(ExecutionCtx* ectx, Record* r, void* args);

/* Creatign a new execution builder */
LIBMR_API ExecutionBuilder* MR_CreateExecutionBuilder(const char* readerName, void* args);

/* Add map step to the given builder.
 * The function takes ownership on the given
 * args so the user is not allow to use it anymore. */
LIBMR_API void MR_ExecutionBuilderMap(ExecutionBuilder* builder, const char* name, void* args);

/* Add a collect step to the builder.
 * Will return all the records to the initiator */
LIBMR_API void MR_ExecutionBuilderCollect(ExecutionBuilder* builder);

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
 * from it. */
LIBMR_API Execution* MR_CreateExecution(ExecutionBuilder* builder);

LIBMR_API void MR_ExecutionSetOnDoneHandler(Execution* e, ExecutionCallback onDone, void* pd);

LIBMR_API void MR_Run(Execution* e);

/* Free the given execution */
LIBMR_API void MR_FreeExecution(Execution* e);

/* Initialize mr library */
LIBMR_API int MR_Init(RedisModuleCtx* ctx, size_t numThreads);

/* Register a new object type */
LIBMR_API int MR_RegisterObject(MRObjectType* t);

LIBMR_API MRObjectType* MR_GetObjectType(size_t id);

/* Register a mapper step */
LIBMR_API void MR_RegisterReader(const char* name, ExecutionReader reader, MRObjectType* argType);
LIBMR_API void MR_RegisterMapper(const char* name, ExecutionMapper mapper, MRObjectType* argType);

/* Serialization Context functions */
#define LONG_READ_ERROR LLONG_MAX
#define BUFF_READ_ERROR NULL
LIBMR_API long long MR_SerializationCtxReadeLongLong(ReaderSerializationCtx* sctx);
LIBMR_API const char* MR_SerializationCtxReadeBuffer(ReaderSerializationCtx* sctx, size_t* len);
LIBMR_API double MR_SerializationCtxReadeDouble(ReaderSerializationCtx* sctx);
LIBMR_API void MR_SerializationCtxWriteLongLong(WriteSerializationCtx* sctx, long long val);
LIBMR_API void MR_SerializationCtxWriteBuffer(WriteSerializationCtx* sctx, const char* buff, size_t len);
LIBMR_API void MR_SerializationCtxWriteDouble(WriteSerializationCtx* sctx, double val);
LIBMR_API int MR_WriteSerializationCtxIsError(WriteSerializationCtx* sctx);
LIBMR_API int MR_ReadSerializationCtxIsError(ReaderSerializationCtx* sctx);

/* records functions */

/* Base record struct, each record should have it
 * as first value */
struct Record {
    MRObjectType* type;
};
LIBMR_API void MR_RecordFree(Record* r);

#endif /* SRC_MR_H_ */
