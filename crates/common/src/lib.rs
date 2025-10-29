#[macro_use]
pub mod ids;

mod error;
pub use error::{Error, ErrorCode, ErrorTrait, TypedError};

pub mod non_max;
pub mod span;
