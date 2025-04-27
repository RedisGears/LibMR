/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
