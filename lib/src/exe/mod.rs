#[cfg(not(target_arch = "wasm32"))]
pub use spawn::spawn;
pub use try_join_all_buffered::try_join_all_buffered;

mod spawn;
mod try_join_all_buffered;
