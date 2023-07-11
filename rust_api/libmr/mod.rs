/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
//! This module provides with the Map-Reduce operations supported.

use crate::libmr_c_raw::bindings::{
    MRRecordType, MR_CalculateSlot, MR_Init, MR_IsMySlot, RedisModuleCtx,
};
use redis_module::Context;

use std::{ffi::CString, os::raw::c_char, ptr};

use linkme::distributed_slice;

pub mod accumulator;
pub mod base_object;
pub mod execution_builder;
pub mod execution_object;
pub mod filter;
pub mod mapper;
pub mod reader;
pub mod record;
pub mod remote_task;

#[distributed_slice()]
pub static REGISTER_LIST: [fn()] = [..];

impl Default for crate::libmr_c_raw::bindings::Record {
    fn default() -> Self {
        crate::libmr_c_raw::bindings::Record {
            recordType: 0 as *mut MRRecordType,
        }
    }
}

pub type RustMRError = String;

pub fn mr_init(ctx: &Context, num_threads: usize, password: Option<&str>) {
    let password = password.map(|v| CString::new(v).unwrap());
    unsafe {
        MR_Init(
            ctx.ctx as *mut RedisModuleCtx,
            num_threads,
            password
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(ptr::null_mut()) as *mut c_char,
        )
    };
    record::init();

    for register in REGISTER_LIST {
        register();
    }
}

pub fn calc_slot(s: &[u8]) -> usize {
    unsafe { MR_CalculateSlot(s.as_ptr() as *const c_char, s.len()) }
}

pub fn is_my_slot(slot: usize) -> bool {
    (unsafe { MR_IsMySlot(slot) }) != 0
}
