use std::env;
use std::process::Command;

fn main() {
    let python = env::var("PYO3_PYTHON").unwrap_or_else(|_| "python3".to_string());

    let output = Command::new(&python)
        .args(["-c", "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"])
        .output()
        .expect("failed to run python");

    let libdir = String::from_utf8(output.stdout).unwrap().trim().to_string();

    let output = Command::new(&python)
        .args(["-c", "import sysconfig;import os; print(os.path.splitext(os.path.basename(sysconfig.get_config_var(\"LIBRARY\")))[0])"])
        .output()
        .expect("failed to run python");

    let library = String::from_utf8(output.stdout).unwrap().trim().to_string();

    println!("cargo:rustc-link-search=native={}", library.replace("lib",""));

    println!("cargo:rustc-link-lib=dylib={}", library.replace("lib",""));

    println!("cargo:rerun-if-changed=build.rs");
}