/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
//! # LibMR Rust API
//!
//! This crate provides a Rust API for the LibMR library.

// For now warn, but should be #![deny(missing_docs)].
#![warn(missing_docs)]
// TODO: disable this when refactoring
#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub use redis_module;
pub mod libmr;
pub mod libmr_c_raw;
