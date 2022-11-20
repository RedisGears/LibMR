/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::marker::PhantomData;
use crate::libmrraw::bindings::{
    ExecutionBuilder,
    MR_CreateExecutionBuilder,
    MR_FreeExecutionBuilder,
    MR_ExecutionBuilderCollect,
    MR_ExecutionBuilderMap,
    MRObjectType,
    WriteSerializationCtx,
    ReaderSerializationCtx,
    MR_SerializationCtxWriteBuffer,
    MR_SerializationCtxReadeBuffer,
    RedisModuleCtx,
    MR_RegisterObject,
    MR_RegisterMapper,
    ExecutionCtx,
    MR_RegisterReader,
    MR_CreateExecution,
    MR_Run,
    Execution,
    MR_ExecutionSetOnDoneHandler,
    MR_FreeExecution,
    MR_ExecutionCtxGetResultsLen,
    MR_ExecutionCtxGetResult,
    MR_ExecutionCtxGetErrorsLen,
    MR_ExecutionCtxGetError,
    MRRecordType,
    MR_RegisterRecord,
    MR_ExecutionCtxSetError,
    MR_ExecutionBuilderFilter,
    MR_RegisterFilter,
    MR_ExecutionBuilderReshuffle,
    MR_ExecutionSetMaxIdle,
    MR_RegisterAccumulator,
    MR_ExecutionBuilderBuilAccumulate,
    MRError,
    MR_ErrorGetMessage,
};

use serde::ser::{
    Serialize,
};

use serde::de::{
    Deserialize,
};

use serde_json::{
    to_string,
    from_str,
};

use std::os::raw::{
    c_char,
    c_void,
    c_int,
};

use std::slice;
use std::str;

use redis_module::{
    RedisValue,
};

use libc::{
    strlen,
};

pub type RustMRError = String;

pub extern "C" fn rust_obj_free<T: BaseObject>(ctx: *mut c_void) {
    unsafe{Box::from_raw(ctx as *mut T)};
}

pub extern "C" fn rust_obj_dup<T:BaseObject>(arg: *mut c_void) -> *mut c_void {
    let obj = unsafe{&mut *(arg as *mut T)};
    let mut obj = obj.clone();
    obj.init();
    Box::into_raw(Box::new(obj)) as *mut c_void
}

pub extern "C" fn rust_obj_serialize<T:BaseObject>(sctx: *mut WriteSerializationCtx, arg: *mut c_void, error: *mut *mut MRError) {
    let obj = unsafe{&mut *(arg as *mut T)};
    let s = to_string(obj).unwrap();
    unsafe{
        MR_SerializationCtxWriteBuffer(sctx, s.as_ptr() as *const c_char, s.len(), error);
    }
}

pub extern "C" fn rust_obj_deserialize<T:BaseObject>(sctx: *mut ReaderSerializationCtx, error: *mut *mut MRError) -> *mut c_void {
    let mut len: usize = 0;
    let s = unsafe {
        MR_SerializationCtxReadeBuffer(sctx, &mut len as *mut usize, error)
    };
    if !(unsafe{*error}).is_null() {
        return 0 as *mut c_void;
    }
    let s = str::from_utf8(unsafe { slice::from_raw_parts(s as *const u8, len) }).unwrap();
    let mut obj: T = from_str(s).unwrap();
    obj.init();
    Box::into_raw(Box::new(obj)) as *mut c_void
}

pub extern "C" fn rust_obj_to_string(_arg: *mut c_void) -> *mut c_char {
    0 as *mut c_char
}

pub extern "C" fn rust_obj_send_reply(_arg1: *mut RedisModuleCtx, _record: *mut ::std::os::raw::c_void) {
    
}

pub extern "C" fn rust_obj_hash_slot<T:Record>(record: *mut ::std::os::raw::c_void) -> usize {
    let record = unsafe{&mut *(record as *mut T)};
    record.hash_slot()
}

pub trait BaseObject: Clone + Serialize + Deserialize<'static> {
    fn get_name() -> &'static str;
    fn init(&mut self) {}
}

fn register<T: BaseObject>() -> *mut MRObjectType {
    unsafe {
        let obj = Box::into_raw(Box::new(MRObjectType {
            type_: T::get_name().as_ptr() as *mut c_char,
            id: 0,
            free: Some(rust_obj_free::<T>),
            dup: Some(rust_obj_dup::<T>),
            serialize: Some(rust_obj_serialize::<T>),
            deserialize: Some(rust_obj_deserialize::<T>),
            tostring: Some(rust_obj_to_string),
        }));
    
        MR_RegisterObject(obj);

        obj
    }
}

fn register_record<T: Record>() -> *mut MRRecordType {
    unsafe {
        let obj = Box::into_raw(Box::new(MRRecordType {
            type_: MRObjectType{
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

pub struct RecordType<R: BaseObject> {
    t: *mut MRRecordType,
    phantom: PhantomData<R>,
}

impl<R: Record> RecordType<R> {
    pub fn new() -> RecordType<R> {
        let obj = register_record::<R>();
        RecordType {
            t: obj,
            phantom: PhantomData,
        }
    }

    pub fn create(&self) -> R {
        R::new(self.t)
    }
}

pub trait Record: BaseObject{
    fn new(t: *mut MRRecordType) -> Self;
    fn to_redis_value(&mut self) -> RedisValue;
    fn hash_slot(&self) -> usize;
}

pub extern "C" fn rust_reader<Step:Reader>(ectx: *mut ExecutionCtx, args: *mut ::std::os::raw::c_void) -> *mut crate::libmrraw::bindings::Record {
    let r = unsafe{&mut *(args as *mut Step)};
    match r.read() {
        Some(res) => {
            match res {
                Ok(res) => Box::into_raw(Box::new(res)) as *mut crate::libmrraw::bindings::Record,
                Err(e) => {
                    unsafe{MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len())};
                    0 as *mut crate::libmrraw::bindings::Record
                },
            }
        },
        None => 0 as *mut crate::libmrraw::bindings::Record,
    }  
}

pub trait Reader : BaseObject{
    type R: Record;

    fn read(&mut self) -> Option<Result<Self::R, RustMRError>>;

    fn register() {
        let obj = register::<Self>();
        unsafe{
            MR_RegisterReader(Self::get_name().as_ptr() as *mut c_char, Some(rust_reader::<Self>), obj);
        }
    }
}

pub extern "C" fn rust_map<Step:MapStep>(ectx: *mut ExecutionCtx, r: *mut crate::libmrraw::bindings::Record, args: *mut c_void) -> *mut crate::libmrraw::bindings::Record {
    let s = unsafe{&*(args as *mut Step)};
    let r = unsafe{Box::from_raw(r as *mut Step::InRecord)};
    match s.map(*r) {
        Ok(res) => Box::into_raw(Box::new(res)) as *mut crate::libmrraw::bindings::Record,
        Err(e) => {
            unsafe{MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len())};
            0 as *mut crate::libmrraw::bindings::Record
        }
    }
    
}

pub trait MapStep: BaseObject{
    type InRecord: Record;
    type OutRecord: Record;

    fn map(&self, r: Self::InRecord) -> Result<Self::OutRecord, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe{
            MR_RegisterMapper(Self::get_name().as_ptr() as *mut c_char, Some(rust_map::<Self>), obj);
        }
    }
}

pub extern "C" fn rust_filter<Step:FilterStep>(ectx: *mut ExecutionCtx, r: *mut crate::libmrraw::bindings::Record, args: *mut c_void) -> c_int {
    let s = unsafe{&*(args as *mut Step)};
    let r = unsafe{&*(r as *mut Step::R)}; // do not take ownership on the record
    match s.filter(r) {
        Ok(res) => res as c_int,
        Err(e) => {
            unsafe{MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len())};
            0 as c_int
        }
    }
    
}

pub trait FilterStep: BaseObject{
    type R: Record;

    fn filter(&self, r: &Self::R) -> Result<bool, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe{
            MR_RegisterFilter(Self::get_name().as_ptr() as *mut c_char, Some(rust_filter::<Self>), obj);
        }
    }
    
}

pub extern "C" fn rust_accumulate<Step:AccumulateStep>(ectx: *mut ExecutionCtx, accumulator: *mut crate::libmrraw::bindings::Record, r: *mut crate::libmrraw::bindings::Record, args: *mut c_void) -> *mut crate::libmrraw::bindings::Record {
    let s = unsafe{&*(args as *mut Step)};
    let accumulator = if accumulator.is_null() {
        None
    } else {
        Some(unsafe{*Box::from_raw(accumulator as *mut Step::Accumulator)})
    };
    let r = unsafe{Box::from_raw(r as *mut Step::InRecord)};
    match s.accumulate(accumulator, *r) {
        Ok(res) => Box::into_raw(Box::new(res)) as *mut crate::libmrraw::bindings::Record,
        Err(e) => {
            unsafe{MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len())};
            0 as *mut crate::libmrraw::bindings::Record
        }
    }
    
}

pub trait AccumulateStep: BaseObject{
    type InRecord: Record;
    type Accumulator: Record;

    fn accumulate(&self, accumulator: Option<Self::Accumulator>, r: Self::InRecord) -> Result<Self::Accumulator, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe{
            MR_RegisterAccumulator(Self::get_name().as_ptr() as *mut c_char, Some(rust_accumulate::<Self>), obj);
        }
    }   
}

pub struct Builder<R: Record> {
    inner_builder: Option<*mut ExecutionBuilder>,
    phantom: PhantomData<R>,
}

pub fn create_builder<Re:Reader>(reader: Re) -> Builder<Re::R> {
    let reader = Box::into_raw(Box::new(reader));
    let inner_builder = unsafe{
        MR_CreateExecutionBuilder(Re::get_name().as_ptr() as *const c_char, reader as *mut c_void)
    };
    Builder::<Re::R> {
        inner_builder: Some(inner_builder),
        phantom: PhantomData,
    }
}

impl<R: Record> Builder<R> {
    fn take(&mut self) -> *mut ExecutionBuilder{
        self.inner_builder.take().unwrap()
    }

    pub fn map<Step: MapStep::<InRecord=R>>(mut self, step: Step) -> Builder<Step::OutRecord> {
        let inner_builder = self.take();
        unsafe {
            MR_ExecutionBuilderMap(inner_builder, Step::get_name().as_ptr() as *const c_char, Box::into_raw(Box::new(step)) as *const Step as *mut c_void)
        }
        Builder::<Step::OutRecord> {
            inner_builder: Some(inner_builder),
            phantom: PhantomData,
        }
    }

    pub fn filter<Step: FilterStep::<R=R>>(self, step: Step) -> Builder<Step::R> {
        unsafe {
            MR_ExecutionBuilderFilter(self.inner_builder.unwrap(), Step::get_name().as_ptr() as *const c_char, Box::into_raw(Box::new(step)) as *const Step as *mut c_void)
        }
        self
    }

    pub fn accumulate<Step: AccumulateStep::<InRecord=R>>(mut self, step: Step) -> Builder<Step::Accumulator> {
        let inner_builder = self.take();
        unsafe {
            MR_ExecutionBuilderBuilAccumulate(inner_builder, Step::get_name().as_ptr() as *const c_char, Box::into_raw(Box::new(step)) as *const Step as *mut c_void)
        }
        Builder::<Step::Accumulator> {
            inner_builder: Some(inner_builder),
            phantom: PhantomData,
        }
    }

    pub fn collect(self) -> Self {
        unsafe {
            MR_ExecutionBuilderCollect(self.inner_builder.unwrap());
        }
        self
    }

    pub fn reshuffle(self) -> Self {
        unsafe {
            MR_ExecutionBuilderReshuffle(self.inner_builder.unwrap());
        }
        self
    }

    pub fn create_execution(&self) -> Result<ExecutionObj<R>, RustMRError> {
        let execution = unsafe {
            let mut err: *mut MRError = 0 as *mut MRError;
            let res = MR_CreateExecution(self.inner_builder.unwrap(), &mut err);
            if !err.is_null() {
                let c_msg = MR_ErrorGetMessage(err);
                let r_str = str::from_utf8(slice::from_raw_parts(c_msg.cast::<u8>(), strlen(c_msg))).unwrap();
                return Err(r_str.to_string());
            }
            res
        };
        Ok(ExecutionObj{inner_e: execution, phantom: PhantomData,})
    }
}

impl<R: Record> Drop for Builder<R> {
    fn drop(&mut self) {
        if let Some(innder_builder) = self.inner_builder {
            unsafe{MR_FreeExecutionBuilder(innder_builder)}
        }
    }
}

pub struct ExecutionObj<R: Record> {
    inner_e: *mut Execution,
    phantom: PhantomData<R>,
}

pub extern "C" fn rust_on_done<R: Record, F:FnOnce(Vec<&mut R>, Vec<&str>)>(ectx: *mut ExecutionCtx, pd: *mut c_void) {
    let f = unsafe{Box::from_raw(pd as *mut F)};
    let mut res = Vec::new();
    let res_len = unsafe{MR_ExecutionCtxGetResultsLen(ectx)};
    for i in 0..res_len {
        let r = unsafe{&mut *(MR_ExecutionCtxGetResult(ectx, i) as *mut R)};
        res.push(r);
    }
    let mut errs = Vec::new();
    let errs_len = unsafe{MR_ExecutionCtxGetErrorsLen(ectx)};
    for i in 0..errs_len {
        let r = unsafe{MR_ExecutionCtxGetError(ectx, i)};
        let s = str::from_utf8(unsafe { slice::from_raw_parts(r.cast::<u8>(), strlen(r))}).unwrap();
        errs.push(s);
    }
    f(res, errs);
}

impl<R: Record> ExecutionObj<R> {

    pub fn set_max_idle(&self, max_idle: usize) {
        unsafe{MR_ExecutionSetMaxIdle(self.inner_e, max_idle)};
    }

    pub fn set_done_hanlder<F:FnOnce(Vec<&mut R>, Vec<&str>)>(&self, f: F) {
        let f = Box::into_raw(Box::new(f));
        unsafe{MR_ExecutionSetOnDoneHandler(self.inner_e, Some(rust_on_done::<R, F>), f as *mut c_void)};
    }

    pub fn run(&self) {
        unsafe{MR_Run(self.inner_e)};
    }
}

impl<R: Record> Drop for ExecutionObj<R> {
    fn drop(&mut self) {
        unsafe{MR_FreeExecution(self.inner_e)};
    }
}