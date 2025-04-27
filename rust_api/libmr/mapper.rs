/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use crate::libmr_c_raw::bindings::{
    ExecutionCtx, MR_ExecutionCtxSetError, MR_RegisterMapper, Record,
};

use crate::libmr::base_object::{register, BaseObject};
use crate::libmr::record;
use crate::libmr::record::MRBaseRecord;
use crate::libmr::RustMRError;

use std::os::raw::{c_char, c_void};

use std::ptr;

pub extern "C" fn rust_map<Step: MapStep>(
    ectx: *mut ExecutionCtx,
    r: *mut Record,
    args: *mut c_void,
) -> *mut Record {
    let s = unsafe { &*(args as *mut Step) };
    let mut r = unsafe { Box::from_raw(r as *mut MRBaseRecord<Step::InRecord>) };
    let res = match s.map(r.record.take().unwrap()) {
        Ok(res) => res,
        Err(e) => {
            unsafe { MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len()) };
            return ptr::null_mut();
        }
    };
    Box::into_raw(Box::new(MRBaseRecord::new(res))) as *mut Record
}

pub trait MapStep: BaseObject {
    type InRecord: record::Record;
    type OutRecord: record::Record;

    fn map(&self, r: Self::InRecord) -> Result<Self::OutRecord, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe {
            MR_RegisterMapper(
                Self::get_name().as_ptr() as *mut c_char,
                Some(rust_map::<Self>),
                obj,
            );
        }
    }
}
