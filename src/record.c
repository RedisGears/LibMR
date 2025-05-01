/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

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
static void ErrorRecord_ObjectSerialize(WriteSerializationCtx* sctx, void* arg, MRError** err) {
    ErrorRecord* errorRecord = arg;
    MR_SerializationCtxWriteBuffer(sctx, errorRecord->error, strlen(errorRecord->error) + 1, err);
}

static void* ErrorRecord_ObjectDeserialize(ReaderSerializationCtx* sctx, MRError** err) {
    size_t len;
    const char* str = MR_SerializationCtxReadBuffer(sctx, &len, err);
    if (*err) {
        return NULL;
    }
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
    mr_BufferWriterWriteLongLong(writer, r->recordType->type.id);
    MRError* err = NULL;
    r->recordType->type.serialize(writer, r, &err);
    // todo: handle serilization failure
    RedisModule_Assert(!err);

}

Record* MR_RecordDeSerialize(mr_BufferReader* reader) {
    size_t id = mr_BufferReaderReadLongLong(reader, NULL);
    MRObjectType* type = MR_GetObjectType(id);
    MRError* err = NULL;
    Record* r = type->deserialize(reader, &err);
    // todo: handle deserialization failure
    RedisModule_Assert(!err);
    r->recordType = (MRRecordType*)type;
    return r;
}
void MR_RecordFree(Record* r){
    r->recordType->type.free(r);
}
