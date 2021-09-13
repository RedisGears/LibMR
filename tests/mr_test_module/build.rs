extern crate bindgen;
extern crate cc;

use std::env;
use std::path::PathBuf;

#[derive(Debug)]
struct RedisModuleCallback;

fn main() {
    let build = bindgen::Builder::default();

    let bindings = build
        .header("src/include/mr.h")
        .size_t_is_usize(true)
        .generate()
        .expect("error generating bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("mr.rs"))
        .expect("failed to write bindings to file");
}
