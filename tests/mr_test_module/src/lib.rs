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
    MRError,
    FilterStep,
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

#[allow(improper_ctypes)]
#[link(name = "mr", kind = "static")]
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

fn lmr_read_keys_type(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new(None)).
                    map(TypeMapper).
                    collect().
                    create_execution();
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
                    create_execution();
    
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
                    create_execution();
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
                    create_execution();
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

    fn filter(&self, r: &Self::R) -> Result<bool, MRError> {
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

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, MRError> {
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

#[derive(Clone, Serialize, Deserialize)]
struct ReadStringMapper;

impl MapStep for ReadStringMapper {
    type InRecord = StringRecord;
    type OutRecord = StringRecord;

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, MRError> {
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

    fn map(&self, mut r: Self::InRecord) -> Result<Self::OutRecord, MRError> {
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

    fn read(&mut self) -> Option<Result<Self::R, MRError>> {
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

fn init_func(ctx: &Context, _args: &Vec<RedisString>) -> Status {
    unsafe{
        DETACHED_CTX = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);

        MR_Init(ctx.ctx as *mut libmrraw::bindings::RedisModuleCtx, 3);
    }

    unsafe{
        HASH_RECORD_TYPE = Some(RecordType::new())
    };
	KeysReader::register();
    TypeMapper::register();
    TypeFilter::register();
    WriteDummyString::register();
    ReadStringMapper::register();
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
    ],
}
