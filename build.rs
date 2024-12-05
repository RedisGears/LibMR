use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn probe_paths<'a>(paths: &'a [&'a str]) -> Option<&'a str> {
    paths.iter().find(|path| Path::new(path).exists()).copied()
}

fn find_macos_openssl_prefix_path() -> &'static str {
    const PATHS: [&str; 3] = [
        "/usr/local/opt/openssl",
        "/usr/local/opt/openssl@1.1",
        "/opt/homebrew/opt/openssl@1.1",
    ];
    probe_paths(&PATHS).unwrap_or("")
}

fn main() {
    println!("cargo:rerun-if-changed=src/*.c");
    println!("cargo:rerun-if-changed=src/*.h");
    println!("cargo:rerun-if-changed=src/utils/*.h");
    println!("cargo:rerun-if-changed=src/utils/*.c");

    let mut command = Command::new("make");

    command.env(
        "MODULE_NAME",
        std::env::var("MODULE_NAME").expect("module name was not given"),
    );

    if !command.status().expect("failed to compile libmr").success() {
        panic!("failed to compile libmr");
    }

    let output_dir = env::var("OUT_DIR").expect("Can not find out directory");

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

    let open_ssl_prefix_path = match std::option_env!("OPENSSL_PREFIX") {
        Some(p) => p,
        None if std::env::consts::OS == "macos" => find_macos_openssl_prefix_path(),
        _ => "",
    };

    let open_ssl_lib_path_link_argument = if open_ssl_prefix_path.is_empty() {
        "".to_owned()
    } else {
        format!("-L{open_ssl_prefix_path}/lib/")
    };

    println!(
        "cargo:rustc-flags=-L{} {} -lmr -lssl -lcrypto",
        output_dir, open_ssl_lib_path_link_argument
    );
}
