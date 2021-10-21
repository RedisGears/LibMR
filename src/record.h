#ifndef SRC_RECORD_H_
#define SRC_RECORD_H_

#include "mr.h"
#include "utils/buffer.h"

void MR_RecorInitialize();
LIBMR_API void MR_RecordSerialize(Record* r, mr_BufferWriter* writer);
LIBMR_API Record* MR_RecordDeSerialize(mr_BufferReader* reader);

LIBMR_API Record* MR_ErrorRecordCreate(const char* err);
LIBMR_API int MR_IsError(Record* record);
const char* MR_ErrorRecordGetError(Record* record);

Record* MR_HoldRecordGet();
int MR_IsHold(Record* record);

size_t MR_RecordGetHslot(Record* record);

LIBMR_API Record* MR_RecordCreate(MRRecordType* type, size_t size);

LIBMR_API MRRecordType* MR_RecordTypeCreate(char* type,
                                    size_t id,
                                    ObjectFree free,
                                    ObjectDuplicate dup,
                                    ObjectSerialize serialize,
                                    ObjectDeserialize deserialize,
                                    ObjectToString tostring,
                                    SendAsRedisReply sendReply,
                                    HashTag hashTag
);

#endif /* SRC_RECORD_H_ */
