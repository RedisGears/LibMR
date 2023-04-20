/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::libmr_c_raw::bindings::{
    MRError, MRObjectType, MRRecordType, MR_RegisterRecord, MR_SerializationCtxReadBuffer,
    MR_SerializationCtxWriteBuffer, ReaderSerializationCtx, RedisModuleCtx, WriteSerializationCtx,
};

use redis_module::RedisValue;

use serde_json::{from_str, to_string};

use std::os::raw::{c_char, c_void};

use crate::libmr::base_object::BaseObject;

use std::collections::HashMap;

use std::slice;
use std::str;

#[repr(C)]
#[derive(Clone, serde::Serialize)]
pub(crate) struct MRBaseRecord<T: Record> {
    #[serde(skip)]
    base: crate::libmr_c_raw::bindings::Record,
    pub(crate) record: Option<T>,
}

impl<T: Record> MRBaseRecord<T> {
    pub(crate) fn new(record: T) -> MRBaseRecord<T> {
        MRBaseRecord {
            base: crate::libmr_c_raw::bindings::Record {
                recordType: get_record_type(T::get_name()).unwrap(),
            },
            record: Some(record),
        }
    }
}

pub extern "C" fn rust_obj_free<T: Record>(ctx: *mut c_void) {
    unsafe { Box::from_raw(ctx as *mut MRBaseRecord<T>) };
}

pub extern "C" fn rust_obj_dup<T: Record>(arg: *mut c_void) -> *mut c_void {
    let obj = unsafe { &mut *(arg as *mut MRBaseRecord<T>) };
    let mut obj = obj.clone();
    obj.record.as_mut().unwrap().init();
    Box::into_raw(Box::new(obj)) as *mut c_void
}

pub extern "C" fn rust_obj_serialize<T: Record>(
    sctx: *mut WriteSerializationCtx,
    arg: *mut c_void,
    error: *mut *mut MRError,
) {
    let obj = unsafe { &mut *(arg as *mut MRBaseRecord<T>) };
    let s = to_string(obj.record.as_ref().unwrap()).unwrap();
    unsafe {
        MR_SerializationCtxWriteBuffer(sctx, s.as_ptr() as *const c_char, s.len(), error);
    }
}

pub extern "C" fn rust_obj_deserialize<T: Record>(
    sctx: *mut ReaderSerializationCtx,
    error: *mut *mut MRError,
) -> *mut c_void {
    let mut len: usize = 0;
    let s = unsafe { MR_SerializationCtxReadBuffer(sctx, &mut len as *mut usize, error) };
    if !(unsafe { *error }).is_null() {
        return 0 as *mut c_void;
    }
    let s = str::from_utf8(unsafe { slice::from_raw_parts(s as *const u8, len) }).unwrap();
    let mut obj: T = from_str(s).unwrap();
    obj.init();
    Box::into_raw(Box::new(MRBaseRecord::new(obj))) as *mut c_void
}

pub extern "C" fn rust_obj_to_string(_arg: *mut c_void) -> *mut c_char {
    0 as *mut c_char
}

pub extern "C" fn rust_obj_send_reply(
    _arg1: *mut RedisModuleCtx,
    _record: *mut ::std::os::raw::c_void,
) {
}

pub extern "C" fn rust_obj_hash_slot<T: Record>(record: *mut ::std::os::raw::c_void) -> usize {
    let record = unsafe { &mut *(record as *mut MRBaseRecord<T>) };
    record.record.as_ref().unwrap().hash_slot()
}

fn register_record<T: Record>() -> *mut MRRecordType {
    unsafe {
        let obj = Box::into_raw(Box::new(MRRecordType {
            type_: MRObjectType {
                type_: T::get_name().as_ptr() as *mut c_char,
                id: 0,
                free: Some(rust_obj_free::<T>),
                dup: Some(rust_obj_dup::<T>),
                serialize: Some(rust_obj_serialize::<T>),
                deserialize: Some(rust_obj_deserialize::<T>),
                tostring: Some(rust_obj_to_string),
            },
            sendReply: Some(rust_obj_send_reply),
            hashTag: Some(rust_obj_hash_slot::<T>),
        }));

        MR_RegisterRecord(obj);

        obj
    }
}

static mut RECORD_TYPES: Option<HashMap<String, *mut MRRecordType>> = None;

fn get_record_types_mut() -> &'static mut HashMap<String, *mut MRRecordType> {
    unsafe { RECORD_TYPES.as_mut().unwrap() }
}

fn get_record_type(name: &str) -> Option<*mut MRRecordType> {
    match unsafe { RECORD_TYPES.as_ref().unwrap() }.get(name) {
        Some(r) => Some(*r),
        None => None,
    }
}

pub(crate) fn init() {
    unsafe {
        RECORD_TYPES = Some(HashMap::new());
    }
}

pub trait Record: BaseObject {
    fn register() {
        let record_type = register_record::<Self>();
        get_record_types_mut().insert(Self::get_name().to_string(), record_type);
    }
    fn to_redis_value(&mut self) -> RedisValue;
    fn hash_slot(&self) -> usize;
}
