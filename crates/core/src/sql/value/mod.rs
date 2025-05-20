pub use self::value::*;

pub(super) mod serde;

#[expect(clippy::module_inception)]
mod value;

mod changed;
mod clear;
mod compare;
mod cut;
mod def;
mod diff;
mod flatten;
mod generate;
pub(crate) mod idiom_recursion;
mod into_json;
mod put;
mod walk;

mod convert;
pub(crate) use convert::coerce::{Coerce, CoerceError, CoerceErrorExt};
