#include "record.h"
#include "mr_memory.h"

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
