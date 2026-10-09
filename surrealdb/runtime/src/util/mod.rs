//! Shared helpers the families reach for.
//!
//! Not functions in their own right: the statistics behind `math::median`, the
//! slug transliteration behind `string::slug`, the bearing and distance
//! formulae behind the geo family.

pub mod geo;
pub mod math;
#[cfg(feature = "collate")]
pub mod string;
