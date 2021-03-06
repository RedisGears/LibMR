#ifndef SRC_RECORD_H_
#define SRC_RECORD_H_

#include "mr.h"
#include "utils/buffer.h"

void MR_RecorInitialize();
void MR_RecordSerialize(Record* r, mr_BufferWriter* writer);
Record* MR_RecordDeSerialize(mr_BufferReader* reader);

Record* MR_ErrorRecordCreate(const char* err);
int MR_IsError(Record* record);
const char* MR_ErrorRecordGetError(Record* record);

Record* MR_HoldRecordGet();
int MR_IsHold(Record* record);

size_t MR_RecordGetHslot(Record* record);

#endif /* SRC_RECORD_H_ */
