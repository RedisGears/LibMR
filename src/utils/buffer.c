/*
 * buffer.c
 *
 *  Created on: Sep 23, 2018
 *      Author: meir
 */

#include "buffer.h"
#include <string.h>
#include "../mr_memory.h"
#include "../mr.h"

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

void mr_BufferWriterWriteLong(mr_BufferWriter* bw, long val){
    mr_BufferAdd(bw->buff, (char*)&val, sizeof(long));
}

void mr_BufferWriterWriteString(mr_BufferWriter* bw, const char* str){
    mr_BufferWriterWriteBuff(bw, str, strlen(str) + 1);
}

void mr_BufferWriterWriteBuff(mr_BufferWriter* bw, const char* buff, size_t len){
    mr_BufferWriterWriteLong(bw, len);
    mr_BufferAdd(bw->buff, buff, len);
}

void mr_BufferReaderInit(mr_BufferReader* br, mr_Buffer* buff){
    br->buff = buff;
    br->location = 0;
}

long mr_BufferReaderReadLong(mr_BufferReader* br){
    if(br->location + sizeof(long) > br->buff->size){
        return LONG_READ_ERROR;
    }
    long ret = *(long*)(&br->buff->buff[br->location]);
    br->location += sizeof(long);
    return ret;
}

char* mr_BufferReaderReadBuff(mr_BufferReader* br, size_t* len){
    *len = (size_t)mr_BufferReaderReadLong(br);
    if((*len == LONG_READ_ERROR) || (br->location + *len > br->buff->size)){
        return BUFF_READ_ERROR;
    }
    char* ret = br->buff->buff + br->location;
    br->location += *len;
    return ret;
}

char* mr_BufferReaderReadString(mr_BufferReader* br){
    size_t len;
    return mr_BufferReaderReadBuff(br, &len);
}

