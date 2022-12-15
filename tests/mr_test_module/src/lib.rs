/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#[macro_use]
extern crate serde_derive;

use redis_module::redisraw::bindings::{
    RedisModule_ScanCursorCreate,
    RedisModuleScanCursor,
    RedisModule_Scan,
    RedisModule_GetDetachedThreadSafeContext,
    RedisModuleCtx,
    RedisModuleString,
    RedisModuleKey,
    RedisModule_ThreadSafeContextLock,
    RedisModule_ThreadSafeContextUnlock,
    RedisModule_ScanCursorDestroy,
    RedisModule_StringPtrLen,
};

use redis_module::{
    redis_module,
    redis_command,
    Context,
    RedisError,
    RedisResult,
    RedisString,
    RedisValue,
    Status,
    ThreadSafeContext,
};

use std::ptr;
use std::str;

mod libmrraw;
mod libmr;

use libmr::{
    create_builder,
    BaseObject,
    Record,
    Reader,
    MapStep,
    RecordType,
    RustMRError,
    FilterStep,
    AccumulateStep,
};

use libmrraw::bindings::{
    MR_Init,
    MRRecordType,
    MR_CalculateSlot,
};

use std::os::raw::{
    c_void,
    c_char,
};

use std::{thread, time};

#[allow(improper_ctypes)]
#[link(name = "mr", kind = "static")]
extern "C" {}

#[allow(improper_ctypes)]
#[link(name = "ssl")]
extern "C" {}

#[allow(improper_ctypes)]
#[link(name = "crypto")]
extern "C" {}

static mut DETACHED_CTX: *mut RedisModuleCtx = 0 as *mut RedisModuleCtx;

fn get_redis_ctx() -> *mut RedisModuleCtx {
    unsafe {
        DETACHED_CTX
    }
}

fn get_ctx() -> Context {
    let inner = get_redis_ctx();
    Context::new(inner)
}

fn ctx_lock() {
    let inner = get_redis_ctx();
    unsafe{
        RedisModule_ThreadSafeContextLock.unwrap()(inner);
    }
}

fn ctx_unlock() {
    let inner = get_redis_ctx();
    unsafe{
        RedisModule_ThreadSafeContextUnlock.unwrap()(inner);
    }
}

fn strin_record_new(s: String) -> StringRecord {
    let mut r = unsafe{
        HASH_RECORD_TYPE.as_ref().unwrap().create()
    };
    r.s = Some(s);
    r
}

fn int_record_new(i: i64) -> IntRecord {
    let mut r = unsafe{
        INT_RECORD_TYPE.as_ref().unwrap().create()
    };
    r.i = i;
    r
}

fn lmr_map_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    map(ErrorMapper).
                    filter(DummyFilter).
                    reshuffle().
                    collect().
                    accumulate(CountAccumulator).
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let mut final_res = Vec::new();
        final_res.push(RedisValue::Integer(res.len() as i64));
        final_res.push(RedisValue::Integer(errs.len() as i64));
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_filter_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    filter(ErrorFilter).
                    map(DummyMapper).
                    reshuffle().
                    collect().
                    accumulate(CountAccumulator).
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let mut final_res = Vec::new();
        final_res.push(RedisValue::Integer(res.len() as i64));
        final_res.push(RedisValue::Integer(errs.len() as i64));
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_accumulate_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    accumulate(ErrorAccumulator).
                    map(DummyMapper).
                    filter(DummyFilter).
                    reshuffle().
                    collect().
                    accumulate(CountAccumulator).
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let mut final_res = Vec::new();
        final_res.push(RedisValue::Integer(res.len() as i64));
        final_res.push(RedisValue::Integer(errs.len() as i64));
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_uneven_work(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(MaxIdleReader::new(1)).
                    map(UnevenWorkMapper::new()).
                    create_execution().map_err(|e|RedisError::String(e))?;
    execution.set_max_idle(2000);
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_read_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(ErrorReader::new()).
                    map(DummyMapper).
                    filter(DummyFilter).
                    reshuffle().
                    collect().
                    accumulate(CountAccumulator).
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let mut final_res = Vec::new();
        final_res.push(RedisValue::Integer(res.len() as i64));
        final_res.push(RedisValue::Integer(errs.len() as i64));
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_count_key(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    collect().
                    accumulate(CountAccumulator).
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_reach_max_idle(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(MaxIdleReader::new(50)).
                    collect().
                    create_execution().map_err(|e|RedisError::String(e))?;
    execution.set_max_idle(20);
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_read_keys_type(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    map(TypeMapper).
                    collect().
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn replace_keys_values(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let prefix = args.next().ok_or(RedisError::Str("not prefix was given"))?.try_as_str()?;
    let execution = create_builder(KeysReader::new(Some(prefix.to_string()))).
                    filter(TypeFilter::new("string".to_string())).
                    map(ReadStringMapper{}).
                    reshuffle().
                    map(WriteDummyString{}).
                    collect().
                    create_execution().map_err(|e|RedisError::String(e))?;
    
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_read_string_keys(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    filter(TypeFilter::new("string".to_string())).
                    collect().
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_read_all_keys(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    collect().
                    create_execution().map_err(|e|RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs|{
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if errs.len() > 0 {
            let err = errs.pop().unwrap();
            thread_ctx.reply(Err(RedisError::String(err.to_string())));
        } else {
            let res: Vec<RedisValue> = res.drain(..).map(|r| r.to_redis_value()).collect();
            thread_ctx.reply(Ok(RedisValue::Array(res)));
        }
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

impl Default for  crate::libmrraw::bindings::Record {
    fn default() -> Self {
        crate::libmrraw::bindings::Record {
            recordType: 0 as *mut MRRecordType,
        }
    }
}

#[repr(C)]
#[derive(Clone, Serialize, Deserialize)]
struct StringRecord {
    #[serde(skip)]
    base: crate::libmrraw::bindings::Record,
    pub s: Option<String>,
}

impl Record for StringRecord {
    fn new(t: *mut MRRecordType) -> Self {
        StringRecord {
            base: crate::libmrraw::bindings::Record {
                recordType: t,
            },
            s: None,
        }
    }

    fn to_redis_value(&mut self) -> RedisValue {
        match self.s.take() {
            Some(s) => RedisValue::BulkString(s),
            None => RedisValue::Null,
        }
        
    }

    fn hash_slot(&self) -> usize {
        unsafe{MR_CalculateSlot(self.s.as_ref().unwrap().as_ptr() as *const c_char, self.s.as_ref().unwrap().len())}
    }
}

impl BaseObject for StringRecord {
    fn get_name() -> &'static str {
        "StringRecord\0"
    }
}

#[repr(C)]
#[derive(Clone, Serialize, Deserialize)]
struct IntRecord {
    #[serde(skip)]
    base: crate::libmrraw::bindings::Record,
    pub i: i64,
}

impl Record for IntRecord {
    fn new(t: *mut MRRecordType) -> Self {
        IntRecord {
            base: crate::libmrraw::bindings::Record {
                recordType: t,
            },
            i: 0,
        }
    }

    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Integer(self.i)
    }

    fn hash_slot(&self) -> usize {
        let s = self.i.to_string();
        unsafe{MR_CalculateSlot(s.as_ptr() as *const c_char, s.len())}
    }
}

impl BaseObject for IntRecord {
    fn get_name() -> &'static str {
        "IntRecord\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct CountAccumulator;

impl AccumulateStep for CountAccumulator {
    type InRecord = StringRecord;
    type Accumulator = IntRecord;

    fn accumulate(&self, accumulator: Option<Self::Accumulator>, _r: Self::InRecord) -> Result<Self::Accumulator, RustMRError> {
        let mut accumulator = match accumulator {
            Some(a) => a,
            None => int_record_new(0)
        };
        accumulator.i+=1;
        Ok(accumulator)
    }
}

impl BaseObject for CountAccumulator {
    fn get_name() -> &'static str {
        "CountAccumulator\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ErrorAccumulator;

impl AccumulateStep for ErrorAccumulator {
    type InRecord = StringRecord;
    type Accumulator = StringRecord;

    fn accumulate(&self, _accumulator: Option<Self::Accumulator>, _r: Self::InRecord) -> Result<Self::Accumulator, RustMRError> {
        Err("accumulate_error".to_string())
    }
}

impl BaseObject for ErrorAccumulator {
    fn get_name() -> &'static str {
        "ErrorAccumulator\0"
    }
}

/* filter by key type */
#[derive(Clone, Serialize, Deserialize)]
struct DummyFilter;

impl FilterStep for DummyFilter {
    type R = StringRecord;

    fn filter(&self, _r: &Self::R) -> Result<bool, RustMRError> {
        Ok(true)
    }
}

impl BaseObject for DummyFilter {
    fn get_name() -> &'static str {
        "DummyFilter\0"
    }
}

/* filter by key type */
#[derive(Clone, Serialize, Deserialize)]
struct ErrorFilter;

impl FilterStep for ErrorFilter {
    type R = StringRecord;

    fn filter(&self, _r: &Self::R) -> Result<bool, RustMRError> {
        Err("filter_error".to_string())
    }
}

impl BaseObject for ErrorFilter {
    fn get_name() -> &'static str {
        "ErrorFilter\0"
    }
}

/* filter by key type */
#[derive(Clone, Serialize, Deserialize)]
struct TypeFilter {
    t: String,
}

impl TypeFilter {
    pub fn new(t: String) -> TypeFilter{
        TypeFilter{
            t: t,
        }
    }
}

impl FilterStep for TypeFilter {
    type R = StringRecord;

    fn filter(&self, r: &Self::R) -> Result<bool, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("type",&[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::SimpleString(res) = res {
                if res == self.t {
                    Ok(true)
                } else {
                    Ok(false)
                }
            } else {
                Err("bad result returned from type command".to_string())
            }
        } else {
            Err("bad result returned from type command".to_string())
        }
    }
}

impl BaseObject for TypeFilter {
    fn get_name() -> &'static str {
        "TypeFilter\0"
    }
}

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize)]
struct TypeMapper;

impl MapStep for TypeMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("type",&[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::SimpleString(res) = res {
                r.s = Some(res);
                Ok(r)
            } else {
                Err("bad result returned from type command".to_string())
            }
        } else {
            Err("bad result returned from type command".to_string())
        }
    }
}

impl BaseObject for TypeMapper {
    fn get_name() -> &'static str {
        "TypeMapper\0"
    }
}

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize)]
struct ErrorMapper;

impl MapStep for ErrorMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut _r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        Err("error".to_string())
    }
}

impl BaseObject for ErrorMapper {
    fn get_name() -> &'static str {
        "ErrorMapper\0"
    }
}
/* map key name to its type */
#[derive(Clone, Serialize, Deserialize)]
struct DummyMapper;

impl MapStep for DummyMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        Ok(r)
    }
}

impl BaseObject for DummyMapper {
    fn get_name() -> &'static str {
        "DummyMapper\0"
    }
}

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize)]
struct UnevenWorkMapper {
    #[serde(skip)]
    is_initiator: bool
}

impl UnevenWorkMapper {
    fn new() -> UnevenWorkMapper {
        UnevenWorkMapper{ is_initiator: true }
    }
}

impl MapStep for UnevenWorkMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        if !self.is_initiator {
            let millis = time::Duration::from_millis(30000 as u64);
            thread::sleep(millis);
        }
        Ok(r)
    }
}

impl BaseObject for UnevenWorkMapper {
    fn get_name() -> &'static str {
        "UnevenWorkMapper\0"
    }
}


#[derive(Clone, Serialize, Deserialize)]
struct ReadStringMapper;

impl MapStep for ReadStringMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("get",&[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::SimpleString(res) = res {
                r.s = Some(res);
                Ok(r)
            } else {
                Err("bad result returned from type command".to_string())
            }
        } else {
            Err("bad result returned from type command".to_string())
        }
    }
}

impl BaseObject for ReadStringMapper {
    fn get_name() -> &'static str {
        "ReadStringMapper\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct WriteDummyString;

impl MapStep for WriteDummyString {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("set",&[r.s.as_ref().unwrap(), "val"]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::SimpleString(res) = res {
                r.s = Some(res);
                Ok(r)
            } else {
                Err("bad result returned from type command".to_string())
            }
        } else {
            Err("bad result returned from type command".to_string())
        }
    }
}

impl BaseObject for WriteDummyString {
    fn get_name() -> &'static str {
        "WriteDummyString\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MaxIdleReader {
    #[serde(skip)]
    is_initiator: bool,
    sleep_time: usize,
    is_done: bool,
}

impl MaxIdleReader {
    fn new(sleep_time: usize) -> MaxIdleReader {
        MaxIdleReader {is_initiator: true, sleep_time: sleep_time, is_done: false}
    }
}
impl Reader for MaxIdleReader {
    type R = StringRecord;

    fn read(&mut self) -> Option<Result<Self::R, RustMRError>> {
        if self.is_done {
            return None;
        }
        self.is_done = true;
        if !self.is_initiator {
            let ten_millis = time::Duration::from_millis(self.sleep_time as u64);
            thread::sleep(ten_millis);
        }
        Some(Ok(strin_record_new("record".to_string())))
    }
}

impl BaseObject for MaxIdleReader {
    fn get_name() -> &'static str {
        "MaxIdleReader\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ErrorReader{
    is_done: bool,
}

impl ErrorReader {
    fn new() -> ErrorReader {
        ErrorReader {is_done: false}
    }
}

impl Reader for ErrorReader {
    type R = StringRecord;

    fn read(&mut self) -> Option<Result<Self::R, RustMRError>> {
        if self.is_done {
            return None;
        }
        self.is_done = true;
        Some(Err("read_error".to_string()))
    }
}

impl BaseObject for ErrorReader {
    fn get_name() -> &'static str {
        "ErrorReader\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct KeysReader {
    #[serde(skip)]
    cursor: Option<*mut RedisModuleScanCursor>,
    #[serde(skip)]
    pending: Vec<StringRecord>,
    #[serde(skip)]
    is_done: bool,
    prefix: Option<String>
}

impl KeysReader {
    fn new(prefix: Option<String>) -> KeysReader {
        let mut reader = KeysReader {cursor: None,
            pending:Vec::new(),
            is_done: false,
            prefix: prefix,
        };
        reader.init();
        reader
    }
}

extern "C" fn cursor_callback(_ctx: *mut RedisModuleCtx,
     keyname: *mut RedisModuleString,
     _key: *mut RedisModuleKey,
     privdata: *mut c_void) {

    let reader = unsafe{&mut *(privdata as *mut KeysReader)};

    let key_str = RedisString::from_ptr(keyname).unwrap();
    if let Some(pre) = &reader.prefix {
        if !key_str.starts_with(pre) {
            return;
        }
    }

    let r = strin_record_new(key_str.to_string());

    reader.pending.push(r);
}

impl Reader for KeysReader {
    type R = StringRecord;

    fn read(&mut self) -> Option<Result<Self::R, RustMRError>> {
        let cursor = *self.cursor.as_ref()?;
        loop {
            if let Some(element) = self.pending.pop() {
                return Some(Ok(element));
            }
            if self.is_done {
                return None;
            }
            ctx_lock();
            let res = unsafe{
                let res = RedisModule_Scan.unwrap()(get_redis_ctx(), cursor, Some(cursor_callback), self as *mut KeysReader as *mut c_void);
                res
            };
            ctx_unlock();
            if res == 0 {
                self.is_done = true;
            }
        }
    }
}

impl BaseObject for KeysReader {
    fn get_name() -> &'static str {
        "KeysReader\0"
    }

    fn init(&mut self) {
        self.cursor = Some(unsafe{
            RedisModule_ScanCursorCreate.unwrap()()
        });
        self.is_done = false;
    }
}

impl Drop for KeysReader {
    fn drop(&mut self) {
        if let Some(c) = self.cursor {
            unsafe{RedisModule_ScanCursorDestroy.unwrap()(c)};
        }
    }
}

static mut HASH_RECORD_TYPE: Option<RecordType<StringRecord>> = None;
static mut INT_RECORD_TYPE: Option<RecordType<IntRecord>> = None;

fn init_func(ctx: &Context, args: &Vec<RedisString>) -> Status {
    unsafe{
        DETACHED_CTX = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
        let passwd = args.get(0).map(|v| RedisModule_StringPtrLen.unwrap()(v.inner, ptr::null_mut())).unwrap_or(ptr::null_mut());
        MR_Init(ctx.ctx as *mut libmrraw::bindings::RedisModuleCtx, 3, passwd as *mut c_char);
    }

    unsafe{
        HASH_RECORD_TYPE = Some(RecordType::new());
        INT_RECORD_TYPE = Some(RecordType::new());
    };
	KeysReader::register();
    MaxIdleReader::register();
    ErrorReader::register();
    TypeMapper::register();
    ErrorMapper::register();
    DummyMapper::register();
    TypeFilter::register();
    DummyFilter::register();
    ErrorFilter::register();
    WriteDummyString::register();
    ReadStringMapper::register();
    CountAccumulator::register();
    ErrorAccumulator::register();
    UnevenWorkMapper::register();
	Status::Ok
}

redis_module!{
    name: "lmrtest",
    version: 99_99_99,
    data_types: [],
    init: init_func,
    commands: [
        ["lmrtest.readallkeys", lmr_read_all_keys, "readonly", 0,0,0],
        ["lmrtest.readallkeystype", lmr_read_keys_type, "readonly", 0,0,0],
        ["lmrtest.readallstringkeys", lmr_read_string_keys, "readonly", 0,0,0],
        ["lmrtest.replacekeysvalues", replace_keys_values, "readonly", 0,0,0],
        ["lmrtest.reachmaxidle", lmr_reach_max_idle, "readonly", 0,0,0],
        ["lmrtest.countkeys", lmr_count_key, "readonly", 0,0,0],
        ["lmrtest.maperror", lmr_map_error, "readonly", 0,0,0],
        ["lmrtest.filtererror", lmr_filter_error, "readonly", 0,0,0],
        ["lmrtest.accumulatererror", lmr_accumulate_error, "readonly", 0,0,0],
        ["lmrtest.readerror", lmr_read_error, "readonly", 0,0,0],
        ["lmrtest.unevenwork", lmr_uneven_work, "readonly", 0,0,0],
    ],
}
