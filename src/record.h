/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

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
