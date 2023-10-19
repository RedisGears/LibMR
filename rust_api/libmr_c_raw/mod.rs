/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

#[allow(missing_docs)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/libmr_bindings.rs"));
}

// See: https://users.rust-lang.org/t/bindgen-generate-options-and-some-are-none/14027
