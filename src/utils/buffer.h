/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef SRC_UTILS_BUFFER_H_
#define SRC_UTILS_BUFFER_H_

#include <stddef.h>

#define DEFAULT_INITIAL_CAP 50

typedef struct mr_Buffer{
    size_t cap;
    size_t size;
    char* buff;
}mr_Buffer;

#define mr_BufferCreate() mr_BufferNew(DEFAULT_INITIAL_CAP)
#define mr_BufferInitialize(buff) mr_BufferInit(buff, DEFAULT_INITIAL_CAP)

void mr_BufferInit(mr_Buffer* buff, size_t initialCap);
mr_Buffer* mr_BufferNew(size_t initialCap);
void mr_BufferFree(mr_Buffer* buff);
void mr_BufferAdd(mr_Buffer* buff, const char* data, size_t len);
void mr_BufferClear(mr_Buffer* buff);

typedef struct mr_BufferWriter{
    mr_Buffer* buff;
}mr_BufferWriter;

void mr_BufferWriterInit(mr_BufferWriter* bw, mr_Buffer* buff);
void mr_BufferWriterWriteLongLong(mr_BufferWriter* bw, long long val);
void mr_BufferWriterWriteString(mr_BufferWriter* bw, const char* str);
void mr_BufferWriterWriteBuff(mr_BufferWriter* bw, const char* buff, size_t len);

typedef struct mr_BufferReader{
    mr_Buffer* buff;
    size_t location;
}mr_BufferReader;

void mr_BufferReaderInit(mr_BufferReader* br, mr_Buffer* buff);
long long mr_BufferReaderReadLongLong(mr_BufferReader* br, int* error);
char* mr_BufferReaderReadBuff(mr_BufferReader* br, size_t* len, int* error);
char* mr_BufferReaderReadString(mr_BufferReader* br, int* error);




#endif /* SRC_UTILS_BUFFER_H_ */
