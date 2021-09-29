#include "record.h"
#include "mr_memory.h"

static Record holdRecord;

typedef struct ErrorRecord {
    Record base;
    char* error;
}ErrorRecord;

static void ErrorRecord_ObjectFree(void* arg) {
    ErrorRecord* errorRecord = arg;
    MR_FREE(errorRecord->error);
    MR_FREE(errorRecord);
}

static void* ErrorRecord_ObjectDuplicate(void* arg) {
    ErrorRecord* errorRecord = arg;
    return MR_ErrorRecordCreate(errorRecord->error);
}
static int ErrorRecord_ObjectSerialize(WriteSerializationCtx* sctx, void* arg) {
    ErrorRecord* errorRecord = arg;
    MR_SerializationCtxWriteBuffer(sctx, errorRecord->error, strlen(errorRecord->error) + 1);
    return 0;
}

static void* ErrorRecord_ObjectDeserialize(ReaderSerializationCtx* sctx) {
    size_t len;
    const char* str = MR_SerializationCtxReadeBuffer(sctx, &len);
    return MR_ErrorRecordCreate(str);
}

static char* ErrorRecord_ObjectToString(void* arg) {
    ErrorRecord* errorRecord = arg;
    return errorRecord->error;
}

static void ErrorRecord_SendAsRedisReply(RedisModuleCtx* ctx, void* record) {
    ErrorRecord* errorRecord = record;
    RedisModule_ReplyWithError(ctx, errorRecord->error);
}

static MRRecordType ErrorRecordType = {
    .type = {
            .type = "ErrorRecordType",
            .id = 0,
            .free = ErrorRecord_ObjectFree,
            .dup = ErrorRecord_ObjectDuplicate,
            .serialize = ErrorRecord_ObjectSerialize,
            .deserialize = ErrorRecord_ObjectDeserialize,
            .tostring = ErrorRecord_ObjectToString,
    },
    .sendReply = ErrorRecord_SendAsRedisReply,
    .hashTag = NULL,
};

void MR_RecorInitialize() {
    MR_RegisterRecord(&ErrorRecordType);
}

Record* MR_ErrorRecordCreate(const char* err) {
    ErrorRecord* errorRecord = MR_ALLOC(sizeof(*errorRecord));
    errorRecord->base.recordType = &ErrorRecordType;
    errorRecord->error = MR_STRDUP(err);
    return &errorRecord->base;
}

int MR_IsError(Record* record) {
    return record && record->recordType == &ErrorRecordType;
}

const char* MR_ErrorRecordGetError(Record* record) {
    ErrorRecord* errorRecord = (ErrorRecord*)record;
    return errorRecord->error;
}

Record* MR_HoldRecordGet() {
    return &holdRecord;
}

int MR_IsHold(Record* record) {
    return record == &holdRecord;
}

size_t MR_RecordGetHslot(Record* record) {
    return record->recordType->hashTag(record);
}

void MR_RecordSerialize(Record* r, mr_BufferWriter* writer){
    mr_BufferWriterWriteLong(writer, r->recordType->type.id);
    r->recordType->type.serialize(writer, r);
}

Record* MR_RecordDeSerialize(mr_BufferReader* reader) {
    size_t id = mr_BufferReaderReadLong(reader);
    MRObjectType* type = MR_GetObjectType(id);
    Record* r = type->deserialize(reader);
    r->recordType = (MRRecordType*)type;
    return r;
}
void MR_RecordFree(Record* r){
    r->recordType->type.free(r);
}
