use crate::libmr_c_raw::bindings::{
    MRError, MRObjectType, MR_RegisterObject, MR_SerializationCtxReadeBuffer,
    MR_SerializationCtxWriteBuffer, ReaderSerializationCtx, WriteSerializationCtx,
};

use std::os::raw::{c_char, c_void};

use serde_json::{from_str, to_string};

use serde::ser::Serialize;

use serde::de::Deserialize;

use std::slice;
use std::str;

use std::ffi::CString;

pub extern "C" fn rust_obj_free<T: BaseObject>(ctx: *mut c_void) {
    unsafe { Box::from_raw(ctx as *mut T) };
}

pub extern "C" fn rust_obj_dup<T: BaseObject>(arg: *mut c_void) -> *mut c_void {
    let obj = unsafe { &mut *(arg as *mut T) };
    let mut obj = obj.clone();
    obj.init();
    Box::into_raw(Box::new(obj)) as *mut c_void
}

pub extern "C" fn rust_obj_serialize<T: BaseObject>(
    sctx: *mut WriteSerializationCtx,
    arg: *mut c_void,
    error: *mut *mut MRError,
) {
    let obj = unsafe { &mut *(arg as *mut T) };
    let s = to_string(obj).unwrap();
    unsafe {
        MR_SerializationCtxWriteBuffer(sctx, s.as_ptr() as *const c_char, s.len(), error);
    }
}

pub extern "C" fn rust_obj_deserialize<T: BaseObject>(
    sctx: *mut ReaderSerializationCtx,
    error: *mut *mut MRError,
) -> *mut c_void {
    let mut len: usize = 0;
    let s = unsafe { MR_SerializationCtxReadeBuffer(sctx, &mut len as *mut usize, error) };
    if !(unsafe { *error }).is_null() {
        return 0 as *mut c_void;
    }
    let s = str::from_utf8(unsafe { slice::from_raw_parts(s as *const u8, len) }).unwrap();
    let mut obj: T = from_str(s).unwrap();
    obj.init();
    Box::into_raw(Box::new(obj)) as *mut c_void
}

pub extern "C" fn rust_obj_to_string(_arg: *mut c_void) -> *mut c_char {
    0 as *mut c_char
}

pub trait BaseObject: Clone + Serialize + Deserialize<'static> {
    fn get_name() -> &'static str;
    fn init(&mut self) {}
}

pub(crate) fn register<T: BaseObject>() -> *mut MRObjectType {
    let type_name = T::get_name();
    let type_name_cstring = CString::new(type_name).unwrap();
    unsafe {
        let obj = Box::into_raw(Box::new(MRObjectType {
            type_: type_name_cstring.into_raw(),
            id: 0,
            free: Some(rust_obj_free::<T>),
            dup: Some(rust_obj_dup::<T>),
            serialize: Some(rust_obj_serialize::<T>),
            deserialize: Some(rust_obj_deserialize::<T>),
            tostring: Some(rust_obj_to_string),
        }));

        MR_RegisterObject(obj);

        obj
    }
}
