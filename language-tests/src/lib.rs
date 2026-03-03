#![recursion_limit = "256"]

pub mod cli;
pub mod cmd;
pub mod format;
pub mod runner;
#[cfg(not(target_family = "wasm"))]
pub mod temp_dir;
pub mod tests;

#[cfg(target_family = "wasm")]
mod embedded {
    include!(concat!(env!("OUT_DIR"), "/embedded_tests.rs"));
}

#[cfg(target_family = "wasm")]
pub use embedded::EMBEDDED_TESTS;
