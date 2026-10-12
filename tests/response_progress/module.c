/* Integration fixture for LibMR's public response-timeout API. */
#include "redismodule.h"
#include "mr.h"
#include <string.h>
#include <unistd.h>

static char myid[41];
typedef struct Args {
    long first_ms, gap_ms, count, emitted;
    int local;
    char stalled_node[41];
} Args;
typedef struct Blob { Record base; size_t length; } Blob;
static MRRecordType blob_type;

static void release(void* p) { RedisModule_Free(p); }
static void* copy_args(void* p) {
    Args* copy = RedisModule_Alloc(sizeof(*copy));
    *copy = *(Args*)p;
    return copy;
}
static void write_args(WriteSerializationCtx* w, void* p, MRError** err) {
    Args* a = p;
    MR_SerializationCtxWriteLongLong(w, a->first_ms, err);
    MR_SerializationCtxWriteLongLong(w, a->gap_ms, err);
    MR_SerializationCtxWriteLongLong(w, a->count, err);
    MR_SerializationCtxWriteBuffer(w, a->stalled_node, 40, err);
}
static void* read_args(ReaderSerializationCtx* r, MRError** err) {
    Args* a = RedisModule_Calloc(1, sizeof(*a));
    a->first_ms = MR_SerializationCtxReadLongLong(r, err);
    a->gap_ms = MR_SerializationCtxReadLongLong(r, err);
    a->count = MR_SerializationCtxReadLongLong(r, err);
    size_t length;
    const char* id = MR_SerializationCtxReadBuffer(r, &length, err);
    if (id && length == 40) memcpy(a->stalled_node, id, 40);
    return a;
}
static MRObjectType args_type = {
    .type = "progress-args", .free = release, .dup = copy_args,
    .serialize = write_args, .deserialize = read_args,
};
static void write_blob(WriteSerializationCtx* w, void* p, MRError** err) {
    Blob* b = p;
    char* bytes = RedisModule_Alloc(b->length);
    memset(bytes, 'p', b->length);
    MR_SerializationCtxWriteBuffer(w, bytes, b->length, err);
    RedisModule_Free(bytes);
}
static void* read_blob(ReaderSerializationCtx* r, MRError** err) {
    size_t length;
    const char* bytes = MR_SerializationCtxReadBuffer(r, &length, err);
    if (!bytes) return NULL;
    for (size_t i = 0; i < length; ++i) RedisModule_Assert(bytes[i] == 'p');
    Blob* b = RedisModule_Alloc(sizeof(*b));
    b->base.recordType = &blob_type;
    b->length = length;
    return b;
}
static MRRecordType blob_type = { .type = {
    .type = "progress-blob", .free = release,
    .serialize = write_blob, .deserialize = read_blob,
}};
static Record* read_record(ExecutionCtx* ctx, void* p) {
    Args* a = p;
    if (a->local || a->emitted == a->count) return NULL;
    long delay = a->emitted ? a->gap_ms : a->first_ms;
    /* With an explicit stalled node, all other shards keep sending data. */
    if (a->emitted && a->stalled_node[0] && memcmp(myid, a->stalled_node, 40))
        delay = 100;
    usleep(delay * 1000);
    ++a->emitted;
    Blob* b = RedisModule_Alloc(sizeof(*b));
    b->base.recordType = &blob_type;
    b->length = 128 * 1024; /* exercises multi-frame reassembly */
    return &b->base;
}
static void done(ExecutionCtx* e, void* pd) {
    RedisModuleBlockedClient* bc = pd;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(bc);
    if (MR_ExecutionCtxGetErrorsLen(e))
        RedisModule_ReplyWithError(ctx, MR_ExecutionCtxGetError(e, 0));
    else {
        long long size = 0;
        for (size_t i = 0; i < MR_ExecutionCtxGetResultsLen(e); ++i)
            size += ((Blob*)MR_ExecutionCtxGetResult(e, i))->length;
        RedisModule_ReplyWithLongLong(ctx, size);
    }
    RedisModule_FreeThreadSafeContext(ctx);
    RedisModule_UnblockClient(bc, NULL);
}
static int refresh(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
    memcpy(myid, RedisModule_GetMyClusterID(), 40);
    MR_UpdateClusterTopology();
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
static int query(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
    if (argc != 6 && argc != 7) return RedisModule_WrongArity(ctx);
    long long v[5];
    for (int i = 0; i < 5; ++i)
        if (RedisModule_StringToLongLong(argv[i+1], &v[i]) != REDISMODULE_OK ||
            v[i] < 0 || v[i] > 10000)
            return RedisModule_ReplyWithError(ctx, "bad test argument");
    Args* a = RedisModule_Calloc(1, sizeof(*a));
    a->local = 1; a->first_ms = v[0]; a->gap_ms = v[1]; a->count = v[2];
    if (argc == 7) {
        size_t n; const char* id = RedisModule_StringPtrLen(argv[6], &n);
        if (n != 40) { release(a); return RedisModule_ReplyWithError(ctx, "bad node id"); }
        memcpy(a->stalled_node, id, 40);
    }
    ExecutionBuilder* b = MR_CreateExecutionBuilder("progress-reader", a);
    MR_ExecutionBuilderCollect(b);
    MRError* err = NULL;
    Execution* e = MR_CreateExecution(b, &err);
    if (!MR_ExecutionSetResponseTimeouts(e, v[3], v[4])) {
        MR_FreeExecution(e); MR_FreeExecutionBuilder(b);
        return RedisModule_ReplyWithError(ctx, "invalid response timeout policy");
    }
    RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    MR_ExecutionSetOnDoneHandler(e, done, bc);
    MR_Run(e);
    MR_FreeExecution(e); MR_FreeExecutionBuilder(b);
    return REDISMODULE_OK;
}
int RedisModule_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
    if (RedisModule_Init(ctx, "progressfixture", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK)
        return REDISMODULE_ERR;
    if (MR_Init(ctx, 4, NULL, false) != REDISMODULE_OK) return REDISMODULE_ERR;
    MR_RegisterObject(&args_type);
    MR_RegisterRecord(&blob_type);
    MR_RegisterReader("progress-reader", read_record, &args_type);
    RedisModule_CreateCommand(ctx, "progress.refresh", refresh, "", 0, 0, 0);
    RedisModule_CreateCommand(ctx, "progress.query", query, "", 0, 0, 0);
    return REDISMODULE_OK;
}
