/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use std::marker::PhantomData;

use crate::libmr_c_raw::bindings::{
    ExecutionBuilder, MRError, MR_CreateExecution, MR_CreateExecutionBuilder, MR_ErrorGetMessage,
    MR_ExecutionBuilderBuilAccumulate, MR_ExecutionBuilderCollect, MR_ExecutionBuilderFilter,
    MR_ExecutionBuilderMap, MR_ExecutionBuilderReshuffle, MR_FreeExecutionBuilder,
};

use std::os::raw::{c_char, c_void};

use crate::libmr::accumulator::AccumulateStep;
use crate::libmr::execution_object::ExecutionObj;
use crate::libmr::filter::FilterStep;
use crate::libmr::mapper::MapStep;
use crate::libmr::reader::Reader;
use crate::libmr::record;
use crate::libmr::RustMRError;

use std::slice;
use std::str;

use libc::strlen;

pub struct Builder<R: record::Record> {
    inner_builder: Option<*mut ExecutionBuilder>,
    phantom: PhantomData<R>,
}

pub fn create_builder<Re: Reader>(reader: Re) -> Builder<Re::R> {
    let reader = Box::into_raw(Box::new(reader));
    let inner_builder = unsafe {
        MR_CreateExecutionBuilder(
            Re::get_name().as_ptr() as *const c_char,
            reader as *mut c_void,
        )
    };
    Builder::<Re::R> {
        inner_builder: Some(inner_builder),
        phantom: PhantomData,
    }
}

impl<R: record::Record> Builder<R> {
    fn take(&mut self) -> *mut ExecutionBuilder {
        self.inner_builder.take().unwrap()
    }

    pub fn map<Step: MapStep<InRecord = R>>(mut self, step: Step) -> Builder<Step::OutRecord> {
        let inner_builder = self.take();
        unsafe {
            MR_ExecutionBuilderMap(
                inner_builder,
                Step::get_name().as_ptr() as *const c_char,
                Box::into_raw(Box::new(step)) as *const Step as *mut c_void,
            )
        }
        Builder::<Step::OutRecord> {
            inner_builder: Some(inner_builder),
            phantom: PhantomData,
        }
    }

    pub fn filter<Step: FilterStep<R = R>>(self, step: Step) -> Builder<Step::R> {
        unsafe {
            MR_ExecutionBuilderFilter(
                self.inner_builder.unwrap(),
                Step::get_name().as_ptr() as *const c_char,
                Box::into_raw(Box::new(step)) as *const Step as *mut c_void,
            )
        }
        self
    }

    pub fn accumulate<Step: AccumulateStep<InRecord = R>>(
        mut self,
        step: Step,
    ) -> Builder<Step::Accumulator> {
        let inner_builder = self.take();
        unsafe {
            MR_ExecutionBuilderBuilAccumulate(
                inner_builder,
                Step::get_name().as_ptr() as *const c_char,
                Box::into_raw(Box::new(step)) as *const Step as *mut c_void,
            )
        }
        Builder::<Step::Accumulator> {
            inner_builder: Some(inner_builder),
            phantom: PhantomData,
        }
    }

    pub fn collect(self) -> Self {
        unsafe {
            MR_ExecutionBuilderCollect(self.inner_builder.unwrap());
        }
        self
    }

    pub fn reshuffle(self) -> Self {
        unsafe {
            MR_ExecutionBuilderReshuffle(self.inner_builder.unwrap());
        }
        self
    }

    pub fn create_execution(&self) -> Result<ExecutionObj<R>, RustMRError> {
        let execution = unsafe {
            let mut err: *mut MRError = 0 as *mut MRError;
            let res = MR_CreateExecution(self.inner_builder.unwrap(), &mut err);
            if !err.is_null() {
                let c_msg = MR_ErrorGetMessage(err);
                let r_str =
                    str::from_utf8(slice::from_raw_parts(c_msg.cast::<u8>(), strlen(c_msg)))
                        .unwrap();
                return Err(r_str.to_string());
            }
            res
        };
        Ok(ExecutionObj {
            inner_e: execution,
            phantom: PhantomData,
        })
    }
}

impl<R: record::Record> Drop for Builder<R> {
    fn drop(&mut self) {
        if let Some(innder_builder) = self.inner_builder {
            unsafe { MR_FreeExecutionBuilder(innder_builder) }
        }
    }
}
