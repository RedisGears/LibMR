/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use mr::redis_module;

use redis_module::redisraw::bindings::{
    RedisModuleCtx, RedisModuleKey, RedisModuleScanCursor, RedisModuleString,
    RedisModule_GetDetachedThreadSafeContext, RedisModule_Scan, RedisModule_ScanCursorCreate,
    RedisModule_ScanCursorDestroy, RedisModule_ThreadSafeContextLock,
    RedisModule_ThreadSafeContextUnlock,
};

use redis_module::{
    alloc::RedisAlloc, redis_command, redis_module, AclCategory, Context, RedisError, RedisResult,
    RedisString, RedisValue, Status, ThreadSafeContext,
};

use mr::libmr::{
    accumulator::AccumulateStep, base_object::BaseObject, calc_slot,
    execution_builder::create_builder, filter::FilterStep, mapper::MapStep, mr_init,
    reader::Reader, record::Record, remote_task::run_on_all_shards, remote_task::run_on_key,
    remote_task::RemoteTask, RustMRError,
};
use serde::{Deserialize, Serialize};

use std::os::raw::c_void;

use std::{thread, time};

use mr_derive::BaseObject;

static mut DETACHED_CTX: *mut RedisModuleCtx = std::ptr::null_mut();

fn get_redis_ctx() -> *mut RedisModuleCtx {
    unsafe { DETACHED_CTX }
}

fn get_ctx() -> Context {
    let inner = get_redis_ctx();
    Context::new(inner)
}

fn ctx_lock() {
    let inner = get_redis_ctx();
    unsafe {
        RedisModule_ThreadSafeContextLock.unwrap()(inner);
    }
}

fn ctx_unlock() {
    let inner = get_redis_ctx();
    unsafe {
        RedisModule_ThreadSafeContextUnlock.unwrap()(inner);
    }
}

fn strin_record_new(s: String) -> StringRecord {
    StringRecord { s: Some(s) }
}

fn int_record_new(i: i64) -> IntRecord {
    IntRecord { i }
}

fn lmr_map_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
        .map(ErrorMapper)
        .filter(DummyFilter)
        .reshuffle()
        .collect()
        .accumulate(CountAccumulator)
        .create_execution()
        .map_err(RedisError::String)?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs| {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let final_res = vec![
            RedisValue::Integer(res.len() as i64),
            RedisValue::Integer(errs.len() as i64),
        ];
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_filter_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
        .filter(ErrorFilter)
        .map(DummyMapper)
        .reshuffle()
        .collect()
        .accumulate(CountAccumulator)
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs| {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let final_res = vec![
            RedisValue::Integer(res.len() as i64),
            RedisValue::Integer(errs.len() as i64),
        ];
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_accumulate_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
        .accumulate(ErrorAccumulator)
        .map(DummyMapper)
        .filter(DummyFilter)
        .reshuffle()
        .collect()
        .accumulate(CountAccumulator)
        .create_execution()
        .map_err(RedisError::String)?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs| {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let final_res = vec![
            RedisValue::Integer(res.len() as i64),
            RedisValue::Integer(errs.len() as i64),
        ];
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_uneven_work(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(MaxIdleReader::new(1))
        .map(UnevenWorkMapper::new())
        .create_execution()
        .map_err(RedisError::String)?;
    execution.set_max_idle(2000);
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if !errs.is_empty() {
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
    let execution = create_builder(ErrorReader::new())
        .map(DummyMapper)
        .filter(DummyFilter)
        .reshuffle()
        .collect()
        .accumulate(CountAccumulator)
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs| {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let final_res = vec![
            RedisValue::Integer(res.len() as i64),
            RedisValue::Integer(errs.len() as i64),
        ];
        thread_ctx.reply(Ok(RedisValue::Array(final_res)));
    });
    execution.run();

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

fn lmr_count_key(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
        .collect()
        .accumulate(CountAccumulator)
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
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
    let execution = create_builder(MaxIdleReader::new(200))
        .collect()
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    execution.set_max_idle(10);
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if !errs.is_empty() {
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
    let execution = create_builder(KeysReader::new(None))
        .map(TypeMapper)
        .collect()
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
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
    let prefix = args
        .next()
        .ok_or(RedisError::Str("not prefix was given"))?
        .try_as_str()?;
    let execution = create_builder(KeysReader::new(Some(prefix.to_string())))
        .filter(TypeFilter::new("string".to_string()))
        .map(ReadStringMapper {})
        .reshuffle()
        .map(WriteDummyString {})
        .collect()
        .create_execution()
        .map_err(|e| RedisError::String(e))?;

    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
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
    let execution = create_builder(KeysReader::new(None))
        .filter(TypeFilter::new("string".to_string()))
        .collect()
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
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

fn lmr_dbsize(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    run_on_all_shards(
        RemoteTaskDBSize,
        int_record_new(0),
        move |results: Vec<IntRecord>, mut errs: Vec<RustMRError>| {
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            if errs.len() > 0 {
                let err = errs.pop().unwrap();
                thread_ctx.reply(Err(RedisError::String(err)));
            } else {
                let sum: i64 = results.into_iter().map(|e| e.i).sum();
                thread_ctx.reply(Ok(RedisValue::Integer(sum)));
            }
        },
        usize::MAX,
    );
    Ok(RedisValue::NoReply)
}

fn lmr_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let ke_redis_string = args.next().ok_or(RedisError::Str("not prefix was given"))?;
    let key = ke_redis_string.try_as_str()?;
    let blocked_client = ctx.block_client();
    thread::spawn(move || {
        let record = strin_record_new(key.to_string());
        run_on_key(
            key.as_bytes(),
            RemoteTaskGet,
            record,
            move |res: Result<StringRecord, RustMRError>| {
                let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
                match res {
                    Ok(mut r) => {
                        thread_ctx.reply(Ok(r.to_redis_value()));
                    }
                    Err(e) => {
                        thread_ctx.reply(Err(RedisError::String(e)));
                    }
                }
            },
            usize::MAX,
        );
    });
    Ok(RedisValue::NoReply)
}

fn lmr_read_all_keys(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
        .collect()
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    execution.set_max_idle(90000);
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|mut res, mut errs| {
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

#[derive(Clone, serde::Serialize, serde::Deserialize, BaseObject)]
struct RemoteTaskGet;

impl RemoteTask for RemoteTaskGet {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn task(
        self,
        mut r: Self::InRecord,
        on_done: Box<dyn FnOnce(Result<Self::OutRecord, RustMRError>) + Send>,
    ) {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("get", &[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::SimpleString(res) = res {
                r.s = Some(res);
                on_done(Ok(r));
            } else {
                on_done(Err("bad result returned from `get` command".to_string()))
            }
        } else {
            on_done(Err("bad result returned from `get` command".to_string()))
        }
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct RemoteTaskDBSize;

impl RemoteTask for RemoteTaskDBSize {
    type InRecord = IntRecord;
    type OutRecord = IntRecord;

    fn task(
        self,
        mut r: Self::InRecord,
        on_done: Box<dyn FnOnce(Result<Self::OutRecord, RustMRError>) + Send>,
    ) {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call::<&[&str; 0]>("dbsize", &[]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::Integer(res) = res {
                r.i = res;
                on_done(Ok(r));
            } else {
                on_done(Err("bad result returned from `dbsize` command".to_string()))
            }
        } else {
            on_done(Err("bad result returned from `dbsize` command".to_string()))
        }
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct StringRecord {
    pub s: Option<String>,
}

impl Record for StringRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        match self.s.take() {
            Some(s) => RedisValue::BulkString(s),
            None => RedisValue::Null,
        }
    }

    fn hash_slot(&self) -> usize {
        calc_slot(self.s.as_ref().unwrap().as_bytes())
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct IntRecord {
    pub i: i64,
}

impl Record for IntRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Integer(self.i)
    }

    fn hash_slot(&self) -> usize {
        let s = self.i.to_string();
        calc_slot(s.as_bytes())
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct CountAccumulator;

impl AccumulateStep for CountAccumulator {
    type InRecord = StringRecord;
    type Accumulator = IntRecord;

    fn accumulate(
        &self,
        accumulator: Option<Self::Accumulator>,
        _r: Self::InRecord,
    ) -> Result<Self::Accumulator, RustMRError> {
        let mut accumulator = match accumulator {
            Some(a) => a,
            None => int_record_new(0),
        };
        accumulator.i += 1;
        Ok(accumulator)
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct ErrorAccumulator;

impl AccumulateStep for ErrorAccumulator {
    type InRecord = StringRecord;
    type Accumulator = StringRecord;

    fn accumulate(
        &self,
        _accumulator: Option<Self::Accumulator>,
        _r: Self::InRecord,
    ) -> Result<Self::Accumulator, RustMRError> {
        Err("accumulate_error".to_string())
    }
}

/* filter by key type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct DummyFilter;

impl FilterStep for DummyFilter {
    type R = StringRecord;

    fn filter(&self, _r: &Self::R) -> Result<bool, RustMRError> {
        Ok(true)
    }
}

/* filter by key type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct ErrorFilter;

impl FilterStep for ErrorFilter {
    type R = StringRecord;

    fn filter(&self, _r: &Self::R) -> Result<bool, RustMRError> {
        Err("filter_error".to_string())
    }
}

/* filter by key type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct TypeFilter {
    t: String,
}

impl TypeFilter {
    pub fn new(t: String) -> TypeFilter {
        TypeFilter { t }
    }
}

impl FilterStep for TypeFilter {
    type R = StringRecord;

    fn filter(&self, r: &Self::R) -> Result<bool, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("type", &[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::SimpleString(s) = res {
                if s == self.t {
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

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct TypeMapper;

impl MapStep for TypeMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("type", &[r.s.as_ref().unwrap()]);
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

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct ErrorMapper;

impl MapStep for ErrorMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut _r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        Err("error".to_string())
    }
}

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct DummyMapper;

impl MapStep for DummyMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        Ok(r)
    }
}

/* map key name to its type */
#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct UnevenWorkMapper {
    #[serde(skip)]
    is_initiator: bool,
}

impl UnevenWorkMapper {
    fn new() -> UnevenWorkMapper {
        UnevenWorkMapper { is_initiator: true }
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

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct ReadStringMapper;

impl MapStep for ReadStringMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("get", &[r.s.as_ref().unwrap()]);
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

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct WriteDummyString;

impl MapStep for WriteDummyString {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, RustMRError> {
        let ctx = get_ctx();
        ctx_lock();
        let res = ctx.call("set", &[r.s.as_ref().unwrap(), "val"]);
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

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct MaxIdleReader {
    #[serde(skip)]
    is_initiator: bool,
    sleep_time: usize,
    is_done: bool,
}

impl MaxIdleReader {
    fn new(sleep_time: usize) -> MaxIdleReader {
        MaxIdleReader {
            is_initiator: true,
            sleep_time,
            is_done: false,
        }
    }
}
impl Reader for MaxIdleReader {
    type R = StringRecord;

    fn read(&mut self) -> Result<Option<Self::R>, RustMRError> {
        if self.is_done {
            return Ok(None);
        }
        self.is_done = true;
        if !self.is_initiator {
            let ten_millis = time::Duration::from_millis(self.sleep_time as u64);
            thread::sleep(ten_millis);
        }
        Ok(Some(strin_record_new("record".to_string())))
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
struct ErrorReader {
    is_done: bool,
}

impl ErrorReader {
    fn new() -> ErrorReader {
        ErrorReader { is_done: false }
    }
}

impl Reader for ErrorReader {
    type R = StringRecord;

    fn read(&mut self) -> Result<Option<Self::R>, RustMRError> {
        if self.is_done {
            return Ok(None);
        }
        self.is_done = true;
        Err("read_error".to_string())
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
    prefix: Option<String>,
}

impl KeysReader {
    fn new(prefix: Option<String>) -> KeysReader {
        let mut reader = KeysReader {
            cursor: None,
            pending: Vec::new(),
            is_done: false,
            prefix,
        };
        reader.init();
        reader
    }
}

extern "C" fn cursor_callback(
    _ctx: *mut RedisModuleCtx,
    keyname: *mut RedisModuleString,
    _key: *mut RedisModuleKey,
    privdata: *mut c_void,
) {
    let reader = unsafe { &mut *(privdata as *mut KeysReader) };

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

    fn read(&mut self) -> Result<Option<Self::R>, RustMRError> {
        let cursor = *match self.cursor.as_ref() {
            Some(s) => s,
            None => return Ok(None),
        };
        loop {
            if let Some(element) = self.pending.pop() {
                return Ok(Some(element));
            }
            if self.is_done {
                return Ok(None);
            }
            ctx_lock();
            let res = unsafe {
                let res = RedisModule_Scan.unwrap()(
                    get_redis_ctx(),
                    cursor,
                    Some(cursor_callback),
                    self as *mut KeysReader as *mut c_void,
                );
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
        self.cursor = Some(unsafe { RedisModule_ScanCursorCreate.unwrap()() });
        self.is_done = false;
    }
}

impl Drop for KeysReader {
    fn drop(&mut self) {
        if let Some(c) = self.cursor {
            unsafe { RedisModule_ScanCursorDestroy.unwrap()(c) };
        }
    }
}

fn init_func(ctx: &Context, args: &[RedisString]) -> Status {
    unsafe {
        DETACHED_CTX = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
    }

    let mut args_iter = args.iter();
    let pass = args_iter
        .next()
        .map(|v| Some(v.to_string())).flatten();
    mr_init(ctx, 5, pass.as_deref());

    KeysReader::register();
    Status::Ok
}

#[cfg(not(test))]
redis_module! {
    name: "lmrtest",
    version: 99_99_99,
    allocator: (RedisAlloc, RedisAlloc),
    data_types: [],
    init: init_func,
    commands: [
        ["lmrtest.dbsize", lmr_dbsize, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.get", lmr_get, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.readallkeys", lmr_read_all_keys, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.readallkeystype", lmr_read_keys_type, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.readallstringkeys", lmr_read_string_keys, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.replacekeysvalues", replace_keys_values, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.reachmaxidle", lmr_reach_max_idle, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.countkeys", lmr_count_key, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.maperror", lmr_map_error, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.filtererror", lmr_filter_error, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.accumulatererror", lmr_accumulate_error, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.readerror", lmr_read_error, "readonly", 0,0,0, AclCategory::None],
        ["lmrtest.unevenwork", lmr_uneven_work, "readonly", 0,0,0, AclCategory::None],
    ],
}
