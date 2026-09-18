/// The Rust target every Surrealism module is compiled for.
///
/// WASI Preview 2, so the output is a component the host can instantiate
/// directly. The guest SDK needs no build flags: its `async fn` support comes
/// from the component model's async ABI, not from an embedded executor.
pub const TARGET: &str = "wasm32-wasip2";
