#ifndef SRC_RECORD_H_
#define SRC_RECORD_H_

#include "mr.h"
#include "utils/buffer.h"

void MR_RecordSerialize(Record* r, mr_BufferWriter* writer);
Record* MR_RecordDeSerialize(mr_BufferReader* reader);

#endif /* SRC_RECORD_H_ */
