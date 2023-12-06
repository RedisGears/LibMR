use std::env;

fn main() {
    if env::var("FOR_PROFILE").map_or(false, |v| v.as_str() == "1") {
        println!("cargo:rustc-force-frame-pointers=yes");
    }
}
