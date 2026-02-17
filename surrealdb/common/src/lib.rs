#[macro_use]
pub mod ids;

mod error;
pub use error::{Error, ErrorCode, ErrorTrait, TypedError, source as source_error};

pub mod non_max;
pub mod span;
