use std::env;
use std::process::Command;

fn get_rustc_version() -> String {
    let rustc = env::var("RUSTC").unwrap();

    let rust_version = Command::new(&rustc)
        .arg("--version")
        .output()
        .expect("Failed to invoke `rustc --version`");

    if rust_version.status.code().unwrap_or(1) != 0 {
        panic!(
            "`{} --version` {}\n\n--- stdout\n{}\n--- stderr{}",
            &rustc,
            match rust_version.status.code() {
                Some(code) => format!("exited with status code {}", code),
                None => String::from("was killed killed by signal"),
            },
            String::from_utf8_lossy(rust_version.stdout.as_slice()),
            String::from_utf8_lossy(rust_version.stderr.as_slice())
        );
    }

    let output = String::from_utf8_lossy(rust_version.stdout.as_slice());
    output.trim_start_matches("rustc ").trim().into()
}

fn main() {
    let rustc_version = get_rustc_version();

    let user_agent = format!(
        "neo4j-rust-robsdedude/{} Rust/{} {}",
        env::var("CARGO_PKG_VERSION").unwrap(),
        rustc_version,
        env::var("TARGET").unwrap(),
    );

    println!("Setting default user agent to: {}", user_agent);

    println!("cargo:rustc-env=NEO4J_DEFAULT_USER_AGENT={}", user_agent);
}
