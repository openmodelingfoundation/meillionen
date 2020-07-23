extern crate bindgen;

use std::process::Command;
use std::env;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=../simplecrop/include/SimpleCrop.h");
    println!("cargo:rerun-if-changed=../simplecrop/src/PlantComponent.f03");
    println!("cargo:rerun-if-changed=../simplecrop/src/SoilComponent.f03");

    let out_dir = env::var("OUT_DIR").unwrap();

    Command::new("gfortran")
        .args(&["-Wall", "-Wextra", "../simplecrop/src/PlantComponent.f03", "-c", "-o"])
        .arg(&format!("{}/plant.o", out_dir))
        .status().unwrap();
    Command::new("gfortran")
        .args(&["-Wall", "-Wextra", "../simplecrop/src/SoilComponent.f03", "-c", "-o"])
        .arg(&format!("{}/soil.o", out_dir))
        .status().unwrap();

    Command::new("ar")
        .args(&["crus", "libsimplecrop.a", "plant.o", "soil.o"])
        .current_dir(&Path::new(&out_dir))
        .status().unwrap();

    println!("cargo:rustc-link-search=native={}", out_dir);
    println!("cargo:rustc-link-lib=static=simplecrop");
    println!("cargo:rustc-link-lib=gfortran");

    let bindings = bindgen::Builder::default()
        .header("../simplecrop/include/SimpleCrop.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .derive_default(true)
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(Path::new(&out_dir).join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
