use std::env;
use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};

fn collect_surql_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_surql_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "surql") {
            files.push(path);
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=tests");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let tests_dir = Path::new(&manifest_dir).join("tests");

    let mut files = Vec::new();
    collect_surql_files(&tests_dir, &mut files);

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest = Path::new(&out_dir).join("embedded_tests.rs");

    let mut output = String::from("pub static EMBEDDED_TESTS: &[(&str, &[u8])] = &[\n");
    let tests_dir_str = tests_dir.to_str().unwrap();

    for file in &files {
        let abs = file.to_str().unwrap();
        let rel = file
            .strip_prefix(&tests_dir)
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/");
        let rel_with_sep = format!("/{rel}");
        writeln!(
            output,
            "    (\"{rel_with_sep}\", include_bytes!(\"{abs}\")),",
            rel_with_sep = rel_with_sep,
            abs = abs.replace('\\', "/"),
        )
        .unwrap();
    }

    output.push_str("];\n");

    fs::write(&dest, output).unwrap();

    // Also emit the tests directory path for native builds to use if needed
    println!("cargo:rustc-env=LANGUAGE_TESTS_DIR={tests_dir_str}");
}
