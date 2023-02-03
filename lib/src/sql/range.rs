use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::id::{id, Id};
use crate::sql::ident::ident_raw;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::character::complete::char;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::sequence::terminated;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Range {
	pub tb: String,
	pub beg: Bound<Id>,
	pub end: Bound<Id>,
}

impl Range {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		Ok(Value::Range(Box::new(Range {
			tb: self.tb.clone(),
			beg: match &self.beg {
				Bound::Included(id) => Bound::Included(id.compute(ctx, opt, txn, doc).await?),
				Bound::Excluded(id) => Bound::Excluded(id.compute(ctx, opt, txn, doc).await?),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match &self.end {
				Bound::Included(id) => Bound::Included(id.compute(ctx, opt, txn, doc).await?),
				Bound::Excluded(id) => Bound::Excluded(id.compute(ctx, opt, txn, doc).await?),
				Bound::Unbounded => Bound::Unbounded,
			},
		})))
	}
}

impl PartialOrd for Range {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match self.tb.partial_cmp(&other.tb) {
			Some(Ordering::Equal) => match &self.beg {
				Bound::Unbounded => match &other.beg {
					Bound::Unbounded => Some(Ordering::Equal),
					_ => Some(Ordering::Less),
				},
				Bound::Included(v) => match &other.beg {
					Bound::Unbounded => Some(Ordering::Greater),
					Bound::Included(w) => match v.partial_cmp(w) {
						Some(Ordering::Equal) => match &self.end {
							Bound::Unbounded => match &other.end {
								Bound::Unbounded => Some(Ordering::Equal),
								_ => Some(Ordering::Greater),
							},
							Bound::Included(v) => match &other.end {
								Bound::Unbounded => Some(Ordering::Less),
								Bound::Included(w) => v.partial_cmp(w),
								_ => Some(Ordering::Greater),
							},
							Bound::Excluded(v) => match &other.end {
								Bound::Excluded(w) => v.partial_cmp(w),
								_ => Some(Ordering::Less),
							},
						},
						ordering => ordering,
					},
					_ => Some(Ordering::Less),
				},
				Bound::Excluded(v) => match &other.beg {
					Bound::Excluded(w) => match v.partial_cmp(w) {
						Some(Ordering::Equal) => match &self.end {
							Bound::Unbounded => match &other.end {
								Bound::Unbounded => Some(Ordering::Equal),
								_ => Some(Ordering::Greater),
							},
							Bound::Included(v) => match &other.end {
								Bound::Unbounded => Some(Ordering::Less),
								Bound::Included(w) => v.partial_cmp(w),
								_ => Some(Ordering::Greater),
							},
							Bound::Excluded(v) => match &other.end {
								Bound::Excluded(w) => v.partial_cmp(w),
								_ => Some(Ordering::Less),
							},
						},
						ordering => ordering,
					},
					_ => Some(Ordering::Greater),
				},
			},
			ordering => ordering,
		}
	}
}

impl fmt::Display for Range {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:", self.tb)?;
		match &self.beg {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(id) => write!(f, "{id}"),
			Bound::Excluded(id) => write!(f, "{id}>"),
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(id) => write!(f, "..{id}"),
			Bound::Included(id) => write!(f, "..={id}"),
		}?;
		Ok(())
	}
}

pub fn range(i: &str) -> IResult<&str, Range> {
	let (i, tb) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, beg) =
		opt(alt((map(terminated(id, char('>')), Bound::Excluded), map(id, Bound::Included))))(i)?;
	let (i, _) = char('.')(i)?;
	let (i, _) = char('.')(i)?;
	let (i, end) =
		opt(alt((map(preceded(char('='), id), Bound::Included), map(id, Bound::Excluded))))(i)?;
	Ok((
		i,
		Range {
			tb,
			beg: beg.unwrap_or(Bound::Unbounded),
			end: end.unwrap_or(Bound::Unbounded),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn range_int() {
		let sql = "person:1..100";
		let res = range(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#"person:1..100"#, format!("{}", out));
	}

	#[test]
	fn range_array() {
		let sql = "person:['USA', 10]..['USA', 100]";
		let res = range(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:['USA', 10]..['USA', 100]", format!("{}", out));
	}

	#[test]
	fn range_object() {
		let sql = "person:{ country: 'USA', position: 10 }..{ country: 'USA', position: 100 }";
		let res = range(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"person:{ country: 'USA', position: 10 }..{ country: 'USA', position: 100 }",
			format!("{}", out)
		);
	}
}
