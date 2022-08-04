use crate::libmr_c_raw::bindings::{
    MRError, MR_ErrorCreate, MR_ErrorFree, MR_ErrorGetMessage, MR_RegisterRemoteTask, MR_RunOnKey,
    Record,
};

use crate::libmr::base_object::{register, BaseObject};
use crate::libmr::record;
use crate::libmr::record::MRBaseRecord;
use crate::libmr::RustMRError;

use libc::strlen;
use std::os::raw::{c_char, c_void};

extern "C" fn rust_remote_task<Step: RemoteTask>(
    r: *mut Record,
    args: *mut ::std::os::raw::c_void,
    on_done: ::std::option::Option<
        unsafe extern "C" fn(PD: *mut ::std::os::raw::c_void, r: *mut Record),
    >,
    on_error: ::std::option::Option<
        unsafe extern "C" fn(PD: *mut ::std::os::raw::c_void, r: *mut MRError),
    >,
    pd: *mut ::std::os::raw::c_void,
) {
    let s = unsafe { Box::from_raw(args as *mut Step) };
    let mut r = unsafe { Box::from_raw(r as *mut MRBaseRecord<Step::InRecord>) };
    s.task(
        r.record.take().unwrap(),
        Box::new(move |res| match res {
            Ok(r) => {
                let record = Box::new(MRBaseRecord::new(r));
                unsafe { on_done.unwrap()(pd, Box::into_raw(record) as *mut Record) }
            }
            Err(e) => {
                let error = unsafe { MR_ErrorCreate(e.as_ptr() as *const c_char, e.len()) };
                unsafe { on_error.unwrap()(pd, error) };
            }
        }),
    );
}

pub trait RemoteTask: BaseObject {
    type InRecord: record::Record;
    type OutRecord: record::Record;

    fn task(
        self,
        r: Self::InRecord,
        on_done: Box<dyn FnOnce(Result<Self::OutRecord, RustMRError>)>,
    );

    fn register() {
        let obj = register::<Self>();
        unsafe {
            MR_RegisterRemoteTask(
                Self::get_name().as_ptr() as *mut c_char,
                Some(rust_remote_task::<Self>),
                obj,
            );
        }
    }
}

extern "C" fn on_done<
    OutRecord: record::Record,
    DoneCallback: FnOnce(Result<OutRecord, RustMRError>),
>(
    pd: *mut ::std::os::raw::c_void,
    result: *mut Record,
) {
    let callback = unsafe { Box::<DoneCallback>::from_raw(pd as *mut DoneCallback) };
    let mut r = unsafe { Box::from_raw(result as *mut MRBaseRecord<OutRecord>) };
    callback(Ok(r.record.take().unwrap()));
}

extern "C" fn on_error<
    OutRecord: record::Record,
    DoneCallback: FnOnce(Result<OutRecord, RustMRError>),
>(
    pd: *mut ::std::os::raw::c_void,
    error: *mut MRError,
) {
    let callback = unsafe { Box::<DoneCallback>::from_raw(pd as *mut DoneCallback) };
    let err_msg = unsafe { MR_ErrorGetMessage(error) };
    let r_str = std::str::from_utf8(unsafe {
        std::slice::from_raw_parts(err_msg.cast::<u8>(), strlen(err_msg))
    })
    .unwrap();
    callback(Err(r_str.to_string()));
    unsafe { MR_ErrorFree(error) };
}

pub fn run_on_key<
    Remote: RemoteTask,
    InRecord: record::Record,
    OutRecord: record::Record,
    DoneCallback: FnOnce(Result<OutRecord, RustMRError>),
>(
    key_name: &str,
    remote_task: Remote,
    r: InRecord,
    done: DoneCallback,
) {
    unsafe {
        MR_RunOnKey(
            key_name.as_ptr() as *mut c_char,
            key_name.len(),
            Remote::get_name().as_ptr() as *mut c_char,
            Box::into_raw(Box::new(remote_task)) as *mut c_void,
            Box::into_raw(Box::new(MRBaseRecord::new(r))) as *mut Record,
            Some(on_done::<OutRecord, DoneCallback>),
            Some(on_error::<OutRecord, DoneCallback>),
            Box::into_raw(Box::new(done)) as *mut c_void,
        )
    }
}
