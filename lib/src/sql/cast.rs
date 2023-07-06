use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::error::IResult;
use crate::sql::idiom::Idiom;
use crate::sql::kind::{kind, Kind};
use crate::sql::value::{single, Value};
use async_recursion::async_recursion;
use nom::character::complete::char;
use nom::sequence::delimited;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Cast";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Cast")]
pub struct Cast(pub Kind, pub Value);

impl PartialOrd for Cast {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Cast {
	/// Convert cast to a field name
	pub fn to_idiom(&self) -> Idiom {
		self.1.to_idiom()
	}
}

impl Cast {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Prevent long cast chains
		let opt = &opt.dive(1)?;
		// Compute the value to be cast and convert it
		self.1.compute(ctx, opt, txn, doc).await?.convert_to(&self.0)
	}
}

impl fmt::Display for Cast {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<{}> {}", self.0, self.1)
	}
}

pub fn cast(i: &str) -> IResult<&str, Cast> {
	let (i, k) = delimited(char('<'), kind, char('>'))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((i, Cast(k, v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn cast_int() {
		let sql = "<int>1.2345";
		let res = cast(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<int> 1.2345f", format!("{}", out));
		assert_eq!(out, Cast(Kind::Int, 1.2345.into()));
	}

	#[test]
	fn cast_string() {
		let sql = "<string>1.2345";
		let res = cast(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<string> 1.2345f", format!("{}", out));
		assert_eq!(out, Cast(Kind::String, 1.2345.into()));
	}
}
