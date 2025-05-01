/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use crate::libmr_c_raw::bindings::{
    ExecutionCtx, MR_ExecutionCtxSetError, MR_RegisterFilter, Record,
};

use crate::libmr::base_object::{register, BaseObject};
use crate::libmr::record;
use crate::libmr::record::MRBaseRecord;
use crate::libmr::RustMRError;

use std::os::raw::{c_char, c_int, c_void};

pub extern "C" fn rust_filter<Step: FilterStep>(
    ectx: *mut ExecutionCtx,
    r: *mut Record,
    args: *mut c_void,
) -> c_int {
    let s = unsafe { &*(args as *mut Step) };
    let r = unsafe { &*(r as *mut MRBaseRecord<Step::R>) }; // do not take ownership on the record
    match s.filter(&r.record.as_ref().unwrap()) {
        Ok(res) => res as c_int,
        Err(e) => {
            unsafe { MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len()) };
            0 as c_int
        }
    }
}

pub trait FilterStep: BaseObject {
    type R: record::Record;

    fn filter(&self, r: &Self::R) -> Result<bool, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe {
            MR_RegisterFilter(
                Self::get_name().as_ptr() as *mut c_char,
                Some(rust_filter::<Self>),
                obj,
            );
        }
    }
}
