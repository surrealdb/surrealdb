pub use self::value::*;

pub(super) mod serde;

#[expect(clippy::module_inception)]
mod value;

mod clear;
mod flatten;
mod into_json;
mod put;
mod walk;

mod convert;
pub(crate) use convert::coerce::{Coerce, CoerceError, CoerceErrorExt};
