extern crate bindgen;

use std::process::Command;
use std::env;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/Sw.h");
    println!("cargo:rerun-if-changed=src/Sw.f95");

    let out_dir = env::var("OUT_DIR").unwrap();

    Command::new("gfortran")
        .args(&["-Wall", "-Wextra", "src/Sw.f95", "-c", "-o"])
        .arg(&format!("{}/soil.o", out_dir))
        .status().unwrap();
    Command::new("ar")
        .args(&["crus", "libsoil.a", "soil.o"])
        .current_dir(&Path::new(&out_dir))
        .status().unwrap();

    println!("cargo:rustc-link-search=native={}", out_dir);
    println!("cargo:rustc-link-lib=static=soil");
    println!("cargo:rustc-link-lib=gfortran");

    let bindings = bindgen::Builder::default()
        .header("src/Sw.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(Path::new(&out_dir).join("bindings.rs"))
        .expect("Couldn't write bindings!");
}