#[cfg(not(target_family = "wasm"))]
pub use spawn::spawn;
pub use try_join_all_buffered::try_join_all_buffered;

mod spawn;
mod try_join_all_buffered;
