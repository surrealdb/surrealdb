pub mod idiom;
pub mod parts;
pub mod statements;
pub mod utils;
use std::time;

use arbitrary::{Arbitrary, Result, Unstructured};
pub use idiom::*;
pub use parts::*;
use rust_decimal::Decimal;
pub use utils::*;

use crate::changefeed::ChangeFeed;
use crate::statements::SleepStatement;

impl<'a> Arbitrary<'a> for ChangeFeed {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			expiry: u.arbitrary()?,
			store_diff: bool::arbitrary(u)?,
		})
	}
}

impl<'a> Arbitrary<'a> for SleepStatement {
	fn arbitrary(_u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			// When fuzzing we don't want to sleep, that's slow... we want insomnia.
			duration: time::Duration::new(0, 0),
		})
	}
}

pub fn arb_decimal<'a>(u: &mut Unstructured<'a>) -> Result<Decimal> {
	Ok(Decimal::arbitrary(u)?.normalize())
}

/// Generates an arbitrary `bytes::Bytes` for the sql AST's raw byte-string
/// literal.
///
/// `bytes::Bytes` is foreign (from the `bytes` crate) and so is `Arbitrary`
/// (from the `arbitrary` crate); neither is local to this crate, so a direct
/// `impl Arbitrary for bytes::Bytes` would violate the orphan rule. This
/// free function is used via `#[arbitrary(with = ...)]` instead, mirroring
/// the `Bytes` (val) impl above.
pub fn arb_bytes<'a>(u: &mut Unstructured<'a>) -> Result<::bytes::Bytes> {
	Ok(::bytes::Bytes::copy_from_slice(u.arbitrary()?))
}

/// Generates an arbitrary compilable `regex::Regex` for the sql AST's regex
/// literal.
///
/// `regex::Regex` has no `Arbitrary` impl anywhere in the ecosystem (and, per
/// the orphan rule, can't gain one here), so an arbitrary regex AST is
/// generated instead and rendered to a pattern string, mirroring
/// `surrealdb_types::Regex`'s own fuzzing impl.
pub fn arb_regex<'a>(u: &mut Unstructured<'a>) -> Result<regex::Regex> {
	let ast = regex_syntax::ast::Ast::arbitrary(u)?;
	let src = ast.to_string();
	if src.is_empty() {
		return Err(arbitrary::Error::IncorrectFormat);
	}
	regex::RegexBuilder::new(&src).build().map_err(|_| arbitrary::Error::IncorrectFormat)
}
