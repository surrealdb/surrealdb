use std::ops::Bound;

use token::T;

use crate::parse::ParseResult;
use crate::{ParseSync, Parser};

/// Parses a range with doesn't start with an item, so `..=ITEM`, `..ITEM` or `..`
/// Returns the bound for the tail item of the range.
pub fn parse_prefix_range_sync<'src, 'ast, K, P>(
	parser: &mut Parser<'src, 'ast>,
	peek_has_rhs: K,
) -> ParseResult<Bound<P>>
where
	P: ParseSync,
	K: Fn(&mut Parser<'src, 'ast>) -> ParseResult<bool>,
{
	if let Some(T![=]) = parser.peek_joined1()?.map(|x| x.token) {
		let _ = parser.next()?;
		let _ = parser.next()?;

		let item = parser.parse_sync()?;
		Ok(Bound::Included(item))
	} else {
		let _ = parser.next()?;

		if peek_has_rhs(parser)? {
			let item = parser.parse_sync()?;
			Ok(Bound::Excluded(item))
		} else {
			Ok(Bound::Unbounded)
		}
	}
}

pub enum TryRange<P> {
	None(P),
	Some {
		start: Bound<P>,
		end: Bound<P>,
	},
}

/// Try parsing a range expression with a heading item,
/// So any of `ITEM..ITEM`, `ITEM..`, `ITEM>..`, etc.
///
/// This function will return TryRange::None if the following token is not `..` or `>`.
/// Otherwise it will return TryRange::Some with the correct bounds for the parsed range.
pub fn try_parse_infix_range_sync<'src, 'ast, K, P>(
	parser: &mut Parser<'src, 'ast>,
	head: P,
	peek_has_rhs: K,
) -> ParseResult<TryRange<P>>
where
	P: ParseSync,
	K: Fn(&mut Parser<'src, 'ast>) -> ParseResult<bool>,
{
	let peek_key = parser.peek()?;
	match peek_key.map(|x| x.token) {
		Some(T![..]) => {
			if let Some(T![=]) = parser.peek_joined1()?.map(|x| x.token) {
				let _ = parser.next();
				let _ = parser.next();

				let end = parser.parse_sync()?;

				Ok(TryRange::Some {
					start: Bound::Included(head),
					end: Bound::Included(end),
				})
			} else {
				let _ = parser.next();

				if peek_has_rhs(parser)? {
					let end = parser.parse_sync()?;

					Ok(TryRange::Some {
						start: Bound::Included(head),
						end: Bound::Excluded(end),
					})
				} else {
					Ok(TryRange::Some {
						start: Bound::Included(head),
						end: Bound::Unbounded,
					})
				}
			}
		}
		Some(T![>]) => {
			if !matches!(parser.peek_joined1()?.map(|x| x.token), Some(T![..])) {
				return Err(parser.unexpected("a range operator"));
			}

			if let Some(T![=]) = parser.peek_joined2()?.map(|x| x.token) {
				let _ = parser.next();
				let _ = parser.next();
				let _ = parser.next();

				let end = parser.parse_sync()?;

				Ok(TryRange::Some {
					start: Bound::Excluded(head),
					end: Bound::Included(end),
				})
			} else {
				let _ = parser.next();
				let _ = parser.next();

				if peek_has_rhs(parser)? {
					let end = parser.parse_sync()?;

					Ok(TryRange::Some {
						start: Bound::Excluded(head),
						end: Bound::Excluded(end),
					})
				} else {
					Ok(TryRange::Some {
						start: Bound::Excluded(head),
						end: Bound::Unbounded,
					})
				}
			}
		}
		_ => Ok(TryRange::None(head)),
	}
}
