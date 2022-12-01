/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::marker::PhantomData;

use crate::libmr_c_raw::bindings::{
    Execution, ExecutionCtx, MR_ExecutionCtxGetError, MR_ExecutionCtxGetErrorsLen,
    MR_ExecutionCtxGetResult, MR_ExecutionCtxGetResultsLen, MR_ExecutionSetMaxIdle,
    MR_ExecutionSetOnDoneHandler, MR_FreeExecution, MR_Run,
};

use crate::libmr::record;

use std::os::raw::c_void;

use std::slice;
use std::str;

use libc::strlen;

pub struct ExecutionObj<R: record::Record> {
    pub(crate) inner_e: *mut Execution,
    pub(crate) phantom: PhantomData<R>,
}

pub extern "C" fn rust_on_done<R: record::Record, F: FnOnce(Vec<&mut R>, Vec<&str>)>(
    ectx: *mut ExecutionCtx,
    pd: *mut c_void,
) {
    let f = unsafe { Box::from_raw(pd as *mut F) };
    let mut res = Vec::new();
    let res_len = unsafe { MR_ExecutionCtxGetResultsLen(ectx) };
    for i in 0..res_len {
        let r =
            unsafe { &mut *(MR_ExecutionCtxGetResult(ectx, i) as *mut record::MRBaseRecord<R>) };
        res.push(r.record.as_mut().unwrap());
    }
    let mut errs = Vec::new();
    let errs_len = unsafe { MR_ExecutionCtxGetErrorsLen(ectx) };
    for i in 0..errs_len {
        let r = unsafe { MR_ExecutionCtxGetError(ectx, i) };
        let s =
            str::from_utf8(unsafe { slice::from_raw_parts(r.cast::<u8>(), strlen(r)) }).unwrap();
        errs.push(s);
    }
    f(res, errs);
}

impl<R: record::Record> ExecutionObj<R> {
    pub fn set_max_idle(&self, max_idle: usize) {
        unsafe { MR_ExecutionSetMaxIdle(self.inner_e, max_idle) };
    }

    pub fn set_done_hanlder<F: FnOnce(Vec<&mut R>, Vec<&str>)>(&self, f: F) {
        let f = Box::into_raw(Box::new(f));
        unsafe {
            MR_ExecutionSetOnDoneHandler(self.inner_e, Some(rust_on_done::<R, F>), f as *mut c_void)
        };
    }

    pub fn run(&self) {
        unsafe { MR_Run(self.inner_e) };
    }
}

impl<R: record::Record> Drop for ExecutionObj<R> {
    fn drop(&mut self) {
        unsafe { MR_FreeExecution(self.inner_e) };
    }
}
