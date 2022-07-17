extern crate bindgen;

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=src/*.c");
    println!("cargo:rerun-if-changed=src/*.h");
    println!("cargo:rerun-if-changed=src/utils/*.h");
    println!("cargo:rerun-if-changed=src/utils/*.c");

    if !Command::new("make")
        .env(
            "MODULE_NAME",
            std::env::var("MODULE_NAME").unwrap_or_default(),
        )
        .status()
        .expect("failed to compile libmr")
        .success()
    {
        panic!("failed to compile libmr");
    }

    let output_dir = env::var("OUT_DIR").expect("Can not find out directory");

    if !Command::new("cp")
        .args(&["src/libmr.a", &output_dir])
        .status()
        .expect("failed copy libmr.a to output directory")
        .success()
    {
        panic!("failed copy libmr.a to output directory");
    }

    let build = bindgen::Builder::default();

    let bindings = build
        .header("src/mr.h")
        .size_t_is_usize(true)
        .layout_tests(false)
        .generate()
        .expect("error generating bindings");

    let out_path = PathBuf::from(&output_dir);
    bindings
        .write_to_file(out_path.join("libmr_bindings.rs"))
        .expect("failed to write bindings to file");

    println!("cargo:rustc-flags=-L{} -lmr -lssl -lcrypto", output_dir);
}
