use std::env;
use std::process::Command;

fn main() {
    // 현재 파이썬이 사용하는 lib 디렉토리 찾아오기 (예: venv)
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

    // 링커에 라이브러리 검색 경로 전달
    println!("cargo:rustc-link-search=native={}", library.replace("lib",""));

    // 필요 시 직접 링크할 라이브러리도 지정 (플랫폼에 따라 이름 조정)
    println!("cargo:rustc-link-lib=dylib={}", "python3.12");

    // build.rs 변경 시 다시 실행
    println!("cargo:rerun-if-changed=build.rs");
}