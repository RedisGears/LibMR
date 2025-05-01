/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use crate::libmr_c_raw::bindings::{
    ExecutionCtx, MR_ExecutionCtxSetError, MR_RegisterAccumulator, Record,
};

use crate::libmr::base_object::{register, BaseObject};
use crate::libmr::record;
use crate::libmr::record::MRBaseRecord;
use crate::libmr::RustMRError;

use std::os::raw::{c_char, c_void};

use std::ptr;

pub extern "C" fn rust_accumulate<Step: AccumulateStep>(
    ectx: *mut ExecutionCtx,
    accumulator: *mut Record,
    r: *mut Record,
    args: *mut c_void,
) -> *mut Record {
    let s = unsafe { &*(args as *mut Step) };
    let accumulator = if accumulator.is_null() {
        None
    } else {
        let mut accumulator =
            unsafe { *Box::from_raw(accumulator as *mut MRBaseRecord<Step::Accumulator>) };
        Some(accumulator.record.take().unwrap())
    };
    let mut r = unsafe { Box::from_raw(r as *mut MRBaseRecord<Step::InRecord>) };
    let res = match s.accumulate(accumulator, r.record.take().unwrap()) {
        Ok(res) => res,
        Err(e) => {
            unsafe { MR_ExecutionCtxSetError(ectx, e.as_ptr() as *mut c_char, e.len()) };
            return ptr::null_mut();
        }
    };
    Box::into_raw(Box::new(MRBaseRecord::new(res))) as *mut Record
}

pub trait AccumulateStep: BaseObject {
    type InRecord: record::Record;
    type Accumulator: record::Record;

    fn accumulate(
        &self,
        accumulator: Option<Self::Accumulator>,
        r: Self::InRecord,
    ) -> Result<Self::Accumulator, RustMRError>;

    fn register() {
        let obj = register::<Self>();
        unsafe {
            MR_RegisterAccumulator(
                Self::get_name().as_ptr() as *mut c_char,
                Some(rust_accumulate::<Self>),
                obj,
            );
        }
    }
}
