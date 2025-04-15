
/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "buffer.h"
#include <string.h>
#include "../mr_memory.h"
#include "../redismodule.h"

void mr_BufferInit(mr_Buffer* buff, size_t initialCap){
    buff->cap = initialCap;
    buff->size = 0;
    buff->buff = MR_ALLOC(initialCap * sizeof(char));
}

mr_Buffer* mr_BufferNew(size_t initialCap){
    mr_Buffer* ret = MR_ALLOC(sizeof(*ret));
    ret->cap = initialCap;
    ret->size = 0;
    ret->buff = MR_ALLOC(initialCap * sizeof(char));
    return ret;
}

void mr_BufferFree(mr_Buffer* buff){
    MR_FREE(buff->buff);
    MR_FREE(buff);
}

void mr_BufferAdd(mr_Buffer* buff, const char* data, size_t len){
    if (buff->size + len >= buff->cap){
        buff->cap = buff->size + len;
        buff->buff = MR_REALLOC(buff->buff, buff->cap);
    }
    memcpy(buff->buff + buff->size, data, len);
    buff->size += len;
}

void mr_BufferClear(mr_Buffer* buff){
    buff->size = 0;
}

void mr_BufferWriterInit(mr_BufferWriter* bw, mr_Buffer* buff){
    bw->buff = buff;
}

void mr_BufferWriterWriteLongLong(mr_BufferWriter* bw, long long val){
    mr_BufferAdd(bw->buff, (char*)&val, sizeof(long));
}

void mr_BufferWriterWriteString(mr_BufferWriter* bw, const char* str){
    mr_BufferWriterWriteBuff(bw, str, strlen(str) + 1);
}

void mr_BufferWriterWriteBuff(mr_BufferWriter* bw, const char* buff, size_t len){
    mr_BufferWriterWriteLongLong(bw, len);
    mr_BufferAdd(bw->buff, buff, len);
}

void mr_BufferReaderInit(mr_BufferReader* br, mr_Buffer* buff){
    br->buff = buff;
    br->location = 0;
}

long long mr_BufferReaderReadLongLong(mr_BufferReader* br, int* error){
    if(br->location + sizeof(long) > br->buff->size){
        RedisModule_Assert(error);
        *error = 1;
        return 0;
    }
    long ret = *(long*)(&br->buff->buff[br->location]);
    br->location += sizeof(long);
    return ret;
}

char* mr_BufferReaderReadBuff(mr_BufferReader* br, size_t* len, int* error){
    int internalErr = 0;
    *len = (size_t)mr_BufferReaderReadLongLong(br, &internalErr);
    if(internalErr || (br->location + *len > br->buff->size)){
        RedisModule_Assert(error);
        *error = 1;
        return NULL;
    }
    char* ret = br->buff->buff + br->location;
    br->location += *len;
    return ret;
}

char* mr_BufferReaderReadString(mr_BufferReader* br, int* error){
    size_t len;
    return mr_BufferReaderReadBuff(br, &len, error);
}

void mr_BufferReaderRewind(mr_BufferReader* br) {
    br->location = 0;
}
void mr_BufferWriterRewind(mr_BufferWriter* bw) {
    bw->buff->size = 0;
}
bool mr_BufferReaderIsDepleted(mr_BufferReader* br) {
    return br->location >= br->buff->size;
}
bool mr_BufferWriterIsDepleted(mr_BufferWriter* bw) {
    return bw->buff->size >= bw->buff->cap;
}
