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
    RecordType
};

use libmrraw::bindings::{
    MRObjectType,
    MR_Init,
};

use std::os::raw::{
    c_void,
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

fn lmr_read_all_keys(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let execution = create_builder(KeysReader::new()).
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
            type_: 0 as *mut MRObjectType,
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
    fn new(t: *mut MRObjectType) -> Self {
        StringRecord {
            base: crate::libmrraw::bindings::Record {
                type_: t,
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
}

impl BaseObject for StringRecord {
    fn get_name() -> &'static str {
        "StringRecord\0"
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
}

impl KeysReader {
    fn new() -> KeysReader {
        let mut reader = KeysReader {cursor: None,
            pending:Vec::new(),
            is_done: false,
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

    let mut r = unsafe{
        HASH_RECORD_TYPE.as_ref().unwrap().create()
    };

    r.s = Some(key_str.to_string());

    reader.pending.push(r);
}

impl Reader for KeysReader {
    type R = StringRecord;

    fn read(&mut self) -> Option<Self::R> {
        let cursor = *self.cursor.as_ref()?;
        loop {
            if let Some(element) = self.pending.pop() {
                return Some(element);
            }
            if self.is_done {
                return None;
            }
            let res = unsafe{
                RedisModule_ThreadSafeContextLock.unwrap()(get_redis_ctx());
                let res = RedisModule_Scan.unwrap()(get_redis_ctx(), cursor, Some(cursor_callback), self as *mut KeysReader as *mut c_void);
                RedisModule_ThreadSafeContextUnlock.unwrap()(get_redis_ctx());
                res
            };
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
	Status::Ok
}

redis_module!{
    name: "lmrtest",
    version: 99_99_99,
    data_types: [],
    init: init_func,
    commands: [
        ["lmrtest.readallkeys", lmr_read_all_keys, "readonly", 0,0,0],
    ],
}
