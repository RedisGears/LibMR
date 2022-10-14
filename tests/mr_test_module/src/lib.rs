#[macro_use]
extern crate serde_derive;

use redis_module::redisraw::bindings::{
    RedisModuleCtx, RedisModuleKey, RedisModuleScanCursor, RedisModuleString,
    RedisModule_GetDetachedThreadSafeContext, RedisModule_Scan, RedisModule_ScanCursorCreate,
    RedisModule_ScanCursorDestroy, RedisModule_ThreadSafeContextLock,
    RedisModule_ThreadSafeContextUnlock,
};

use redis_module::{
    redis_command, redis_module, Context, RedisError, RedisResult, RedisString, RedisValue, Status,
    ThreadSafeContext,
};

use std::str;

use mr::libmr::{
    accumulator::AccumulateStep, base_object::BaseObject, calc_slot,
    execution_builder::create_builder, filter::FilterStep, mapper::MapStep, mr_init,
    reader::Reader, record::Record, remote_task::run_on_key, remote_task::RemoteTask, RustMRError,
};

use std::os::raw::c_void;

use std::{thread, time};

static mut DETACHED_CTX: *mut RedisModuleCtx = 0 as *mut RedisModuleCtx;

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
    IntRecord { i: i }
}

fn lmr_map_error(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
        .map(ErrorMapper)
        .filter(DummyFilter)
        .reshuffle()
        .collect()
        .accumulate(CountAccumulator)
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    let blocked_client = ctx.block_client();
    execution.set_done_hanlder(|res, errs| {
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
    let execution = create_builder(KeysReader::new(None))
        .accumulate(ErrorAccumulator)
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
    let execution = create_builder(MaxIdleReader::new(1))
        .map(UnevenWorkMapper::new())
        .create_execution()
        .map_err(|e| RedisError::String(e))?;
    execution.set_max_idle(2000);
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
        );
    });
    Ok(RedisValue::NoReply)
}

fn lmr_read_all_keys(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None))
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

#[derive(Clone, Serialize, Deserialize)]
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
            if let RedisValue::StringBuffer(res) = res {
                r.s = Some(std::str::from_utf8(&res).unwrap().to_string());
                on_done(Ok(r));
            } else {
                on_done(Err("bad result returned from `get` command".to_string()))
            }
        } else {
            on_done(Err("bad result returned from `get` command".to_string()))
        }
    }
}

impl BaseObject for RemoteTaskGet {
    fn get_name() -> &'static str {
        "RemoteTaskGet\0"
    }

    fn init(&mut self) {}
}

#[derive(Clone, Serialize, Deserialize)]
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

impl BaseObject for StringRecord {
    fn get_name() -> &'static str {
        "StringRecord\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
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

    fn accumulate(
        &self,
        _accumulator: Option<Self::Accumulator>,
        _r: Self::InRecord,
    ) -> Result<Self::Accumulator, RustMRError> {
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
    pub fn new(t: String) -> TypeFilter {
        TypeFilter { t: t }
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
            if let RedisValue::StringBuffer(res) = res {
                if std::str::from_utf8(&res).unwrap() == self.t {
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
        let res = ctx.call("type", &[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::StringBuffer(res) = res {
                r.s = Some(std::str::from_utf8(&res).unwrap().to_string());
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
        let res = ctx.call("get", &[r.s.as_ref().unwrap()]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::StringBuffer(res) = res {
                r.s = Some(std::str::from_utf8(&res).unwrap().to_string());
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
        let res = ctx.call("set", &[r.s.as_ref().unwrap(), "val"]);
        ctx_unlock();
        if let Ok(res) = res {
            if let RedisValue::StringBuffer(res) = res {
                r.s = Some(std::str::from_utf8(&res).unwrap().to_string());
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
        MaxIdleReader {
            is_initiator: true,
            sleep_time: sleep_time,
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

impl BaseObject for MaxIdleReader {
    fn get_name() -> &'static str {
        "MaxIdleReader\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
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
    prefix: Option<String>,
}

impl KeysReader {
    fn new(prefix: Option<String>) -> KeysReader {
        let mut reader = KeysReader {
            cursor: None,
            pending: Vec::new(),
            is_done: false,
            prefix: prefix,
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

fn init_func(ctx: &Context, _args: &Vec<RedisString>) -> Status {
    unsafe {
        DETACHED_CTX = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);

        mr_init(ctx, 3);
    }

    StringRecord::register();
    IntRecord::register();
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
    RemoteTaskGet::register();
    Status::Ok
}

redis_module! {
    name: "lmrtest",
    version: 99_99_99,
    data_types: [],
    init: init_func,
    commands: [
        ["lmrtest.get", lmr_get, "readonly", 0,0,0],
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
