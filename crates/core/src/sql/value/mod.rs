pub use self::value::*;

pub(super) mod serde;

#[expect(clippy::module_inception)]
mod value;

mod all;
mod changed;
mod clear;
mod compare;
mod cut;
mod decrement;
mod def;
mod diff;
mod each;
pub(crate) mod every;
mod extend;
mod fetch;
mod first;
mod flatten;
mod generate;
mod get;
pub(crate) mod idiom_recursion;
mod inc;
mod increment;
mod into_json;
mod last;
mod patch;
mod pick;
mod put;
mod rid;
mod set;
mod walk;

mod convert;
pub(crate) use convert::cast::{Cast, CastError};
pub(crate) use convert::coerce::{Coerce, CoerceError, CoerceErrorExt};
