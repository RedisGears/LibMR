/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use crate::libmr_c_raw::bindings::{
    ExecutionCtx, MR_ExecutionCtxSetError, MR_RegisterReader, Record,
};

use crate::libmr::base_object::{register, BaseObject};
use crate::libmr::record;
use crate::libmr::record::MRBaseRecord;
use crate::libmr::RustMRError;

use std::os::raw::{c_char, c_void};

use std::ptr;

extern "C" fn rust_reader<Step: Reader>(ectx: *mut ExecutionCtx, args: *mut c_void) -> *mut Record {
    let r = unsafe { &mut *(args as *mut Step) };
    let res = match r.read() {
        Ok(res) => match res {
            Some(res) => res,
            None => return ptr::null_mut(),
        },
        Err(e) => {
            unsafe { MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len()) };
            return ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(MRBaseRecord::new(res))) as *mut Record
}

pub trait Reader: BaseObject {
    type R: record::Record;

    fn read(&mut self) -> Result<Option<Self::R>, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe {
            MR_RegisterReader(
                Self::get_name().as_ptr() as *mut c_char,
                Some(rust_reader::<Self>),
                obj,
            );
        }
    }
}
