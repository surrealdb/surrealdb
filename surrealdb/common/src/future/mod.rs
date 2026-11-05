use std::future::Future;
use std::pin::Pin;

pub mod stream;

mod assert_send;
pub use assert_send::assert_send;

/// Type alias for Pin<Box<dyn Future>>;
pub type BoxFut<'a, R> = Pin<Box<dyn Future<Output = R> + Send + 'a>>;
