use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::ending::field as ending;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::part::Part;
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Fields(pub Vec<Field>, pub bool);

impl Fields {
	pub fn all(&self) -> bool {
		self.0.iter().any(|v| matches!(v, Field::All))
	}
	pub fn other(&self) -> impl Iterator<Item = &Field> {
		self.0.iter().filter(|v| !matches!(v, Field::All))
	}
	pub fn single(&self) -> Option<&Field> {
		match (self.0.len(), self.1) {
			(1, true) => match self.0.first() {
				Some(Field::All) => None,
				Some(v) => Some(v),
				_ => None,
			},
			_ => None,
		}
	}
}

impl Deref for Fields {
	type Target = Vec<Field>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Fields {
	type Item = Field;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Fields {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self.single() {
			Some(v) => write!(f, "VALUE {}", &v),
			None => Display::fmt(&Fmt::comma_separated(&self.0), f),
		}
	}
}

impl Fields {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
		group: bool,
	) -> Result<Value, Error> {
		// Ensure futures are run
		let opt = &opt.futures(true);
		//
		let doc = doc.unwrap_or(&Value::None);
		// Process the desired output
		let mut out = match self.all() {
			true => doc.compute(ctx, opt, txn, Some(doc)).await?,
			false => Value::base(),
		};
		for v in self.other() {
			match v {
				Field::All => (),
				Field::Alone(v) => match v {
					// This expression is a grouped aggregate function
					Value::Function(f) if group && f.is_aggregate() => {
						let x = match f.args().len() {
							// If no function arguments, then compute the result
							0 => f.compute(ctx, opt, txn, Some(doc)).await?,
							// If arguments, then pass the first value through
							_ => f.args()[0].compute(ctx, opt, txn, Some(doc)).await?,
						};
						// Check if this is a single VALUE field expression
						match self.single().is_some() {
							false => out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?,
							true => out = x,
						}
					}
					// This expression is a multi-output graph traversal
					Value::Idiom(v) if v.is_multi_yield() => {
						// Store the different output yields here
						let mut res: Vec<(&[Part], Value)> = Vec::new();
						// Split the expression by each output alias
						for v in v.split_inclusive(Idiom::split_multi_yield) {
							// Use the last fetched value for each fetch
							let x = match res.last() {
								Some((_, r)) => r,
								None => doc,
							};
							// Continue fetching the next idiom part
							let x = x
								.get(ctx, opt, txn, v)
								.await?
								.compute(ctx, opt, txn, Some(doc))
								.await?
								.flatten();
							// Add the result to the temporary store
							res.push((v, x));
						}
						// Assign each fetched yield to the output
						for (p, x) in res {
							match p.last().unwrap().alias() {
								// This is an alias expression part
								Some(a) => out.set(ctx, opt, txn, a, x).await?,
								// This is the end of the expression
								None => out.set(ctx, opt, txn, v, x).await?,
							}
						}
					}
					// This expression is a normal field expression
					_ => {
						let x = v.compute(ctx, opt, txn, Some(doc)).await?;
						// Check if this is a single VALUE field expression
						match self.single().is_some() {
							false => out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?,
							true => out = x,
						}
					}
				},
				Field::Alias(v, i) => match v {
					// This expression is a grouped aggregate function
					Value::Function(f) if group && f.is_aggregate() => {
						let x = match f.args().len() {
							// If no function arguments, then compute the result
							0 => f.compute(ctx, opt, txn, Some(doc)).await?,
							// If arguments, then pass the first value through
							_ => f.args()[0].compute(ctx, opt, txn, Some(doc)).await?,
						};
						// Check if this is a single VALUE field expression
						match self.single().is_some() {
							false => out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?,
							true => out = x,
						}
					}
					// This expression is a multi-output graph traversal
					Value::Idiom(v) if v.is_multi_yield() => {
						// Store the different output yields here
						let mut res: Vec<(&[Part], Value)> = Vec::new();
						// Split the expression by each output alias
						for v in v.split_inclusive(Idiom::split_multi_yield) {
							// Use the last fetched value for each fetch
							let x = match res.last() {
								Some((_, r)) => r,
								None => doc,
							};
							// Continue fetching the next idiom part
							let x = x
								.get(ctx, opt, txn, v)
								.await?
								.compute(ctx, opt, txn, Some(doc))
								.await?
								.flatten();
							// Add the result to the temporary store
							res.push((v, x));
						}
						// Assign each fetched yield to the output
						for (p, x) in res {
							match p.last().unwrap().alias() {
								// This is an alias expression part
								Some(a) => {
									let v = x.clone();
									out.set(ctx, opt, txn, a, x).await?;
									out.set(ctx, opt, txn, i, v).await?;
								}
								// This is the end of the expression
								None => out.set(ctx, opt, txn, i, x).await?,
							}
						}
					}
					// This expression is a normal field expression
					_ => {
						let x = v.compute(ctx, opt, txn, Some(doc)).await?;
						// Check if this is a single VALUE field expression
						match self.single().is_some() {
							false => out.set(ctx, opt, txn, i, x).await?,
							true => out = x,
						}
					}
				},
			}
		}
		Ok(out)
	}
}

pub fn fields(i: &str) -> IResult<&str, Fields> {
	alt((field_one, field_many))(i)
}

fn field_one(i: &str) -> IResult<&str, Fields> {
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, f) = alt((alias, alone))(i)?;
	let (i, _) = ending(i)?;
	Ok((i, Fields(vec![f], true)))
}

fn field_many(i: &str) -> IResult<&str, Fields> {
	let (i, v) = separated_list1(commas, field)(i)?;
	Ok((i, Fields(v, false)))
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub enum Field {
	All,
	Alone(Value),
	Alias(Value, Idiom),
}

impl Default for Field {
	fn default() -> Self {
		Self::All
	}
}

impl Display for Field {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::All => f.write_char('*'),
			Self::Alone(e) => Display::fmt(e, f),
			Self::Alias(e, a) => write!(f, "{e} AS {a}"),
		}
	}
}

pub fn field(i: &str) -> IResult<&str, Field> {
	alt((all, alias, alone))(i)
}

pub fn all(i: &str) -> IResult<&str, Field> {
	let (i, _) = tag_no_case("*")(i)?;
	Ok((i, Field::All))
}

pub fn alone(i: &str) -> IResult<&str, Field> {
	let (i, f) = value(i)?;
	Ok((i, Field::Alone(f)))
}

pub fn alias(i: &str) -> IResult<&str, Field> {
	let (i, f) = value(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("AS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, a) = idiom(i)?;
	Ok((i, Field::Alias(f, a)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn field_all() {
		let sql = "*";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("*", format!("{}", out));
	}

	#[test]
	fn field_one() {
		let sql = "field";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field", format!("{}", out));
	}

	#[test]
	fn field_value() {
		let sql = "VALUE field";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("VALUE field", format!("{}", out));
	}

	#[test]
	fn field_alias() {
		let sql = "field AS one";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field AS one", format!("{}", out));
	}

	#[test]
	fn field_value_alias() {
		let sql = "VALUE field AS one";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("VALUE field AS one", format!("{}", out));
	}

	#[test]
	fn field_multiple() {
		let sql = "field, other.field";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field, other.field", format!("{}", out));
	}

	#[test]
	fn field_aliases() {
		let sql = "field AS one, other.field AS two";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field AS one, other.field AS two", format!("{}", out));
	}

	#[test]
	fn field_value_only_one() {
		let sql = "VALUE field, other.field";
		let res = fields(sql);
		assert!(res.is_ok());
	}
}
