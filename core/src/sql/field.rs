use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{fmt::Fmt, Idiom, Part, Value};
use crate::syn;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fields(pub Vec<Field>, pub bool);

impl Fields {
	pub fn all() -> Self {
		Self(vec![Field::All], false)
	}
	/// Check to see if this field is a * projection
	pub fn is_all(&self) -> bool {
		self.0.iter().any(|v| matches!(v, Field::All))
	}
	/// Get all fields which are not an * projection
	pub fn other(&self) -> impl Iterator<Item = &Field> {
		self.0.iter().filter(|v| !matches!(v, Field::All))
	}
	/// Check to see if this field is a single VALUE clause
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

impl InfoStructure for Fields {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

impl Fields {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		group: bool,
	) -> Result<Value, Error> {
		if let Some(doc) = doc {
			self.compute_value(stk, ctx, opt, doc, group).await
		} else {
			let doc = Value::None.into();
			self.compute_value(stk, ctx, opt, &doc, group).await
		}
	}

	async fn compute_value(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: &CursorDoc,
		group: bool,
	) -> Result<Value, Error> {
		// Ensure futures are run
		let opt = &opt.new_with_futures(true);
		// Process the desired output
		let mut out = match self.is_all() {
			true => doc.doc.as_ref().compute(stk, ctx, opt, Some(doc)).await?,
			false => Value::base(),
		};
		for v in self.other() {
			match v {
				Field::All => (),
				Field::Single {
					expr,
					alias,
				} => {
					let name = alias
						.as_ref()
						.map(Cow::Borrowed)
						.unwrap_or_else(|| Cow::Owned(expr.to_idiom()));
					match expr {
						// This expression is a grouped aggregate function
						Value::Function(f) if group && f.is_aggregate() => {
							let x = match f.args().len() {
								// If no function arguments, then compute the result
								0 => f.compute(stk, ctx, opt, Some(doc)).await?,
								// If arguments, then pass the first value through
								_ => f.args()[0].compute(stk, ctx, opt, Some(doc)).await?,
							};
							// Check if this is a single VALUE field expression
							match self.single().is_some() {
								false => out.set(stk, ctx, opt, name.as_ref(), x).await?,
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
									None => doc.doc.as_ref(),
								};
								// Continue fetching the next idiom part
								let x = x
									.get(stk, ctx, opt, Some(doc), v)
									.await?
									.compute(stk, ctx, opt, Some(doc))
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
										if let Some(i) = alias {
											out.set(stk, ctx, opt, i, x.clone()).await?;
										}
										out.set(stk, ctx, opt, a, x).await?;
									}
									// This is the end of the expression
									None => {
										out.set(stk, ctx, opt, alias.as_ref().unwrap_or(v), x)
											.await?
									}
								}
							}
						}
						// This expression is a variable fields expression
						Value::Function(f) if f.name() == Some("type::fields") => {
							// Process the function using variable field projections
							let expr = expr.compute(stk, ctx, opt, Some(doc)).await?;
							// Check if this is a single VALUE field expression
							match self.single().is_some() {
								false => {
									// Get the first argument which is guaranteed to exist
									let args = match f.args().first().unwrap() {
										Value::Param(v) => {
											v.compute(stk, ctx, opt, Some(doc)).await?
										}
										v => v.to_owned(),
									};
									// This value is always an array, so we can convert it
									let expr: Vec<Value> = expr.try_into()?;
									// This value is always an array, so we can convert it
									let args: Vec<Value> = args.try_into()?;
									// This value is always an array, so we can convert it
									for (name, expr) in args.into_iter().zip(expr) {
										// This value is always a string, so we can convert it
										let name = syn::idiom(&name.to_raw_string())?;
										// Check if this is a single VALUE field expression
										out.set(stk, ctx, opt, name.as_ref(), expr).await?
									}
								}
								true => out = expr,
							}
						}
						// This expression is a variable field expression
						Value::Function(f) if f.name() == Some("type::field") => {
							// Process the function using variable field projections
							let expr = expr.compute(stk, ctx, opt, Some(doc)).await?;
							// Check if this is a single VALUE field expression
							match self.single().is_some() {
								false => {
									// Get the first argument which is guaranteed to exist
									let name = match f.args().first().unwrap() {
										Value::Param(v) => {
											v.compute(stk, ctx, opt, Some(doc)).await?
										}
										v => v.to_owned(),
									};
									// find the name for the field, either from the argument or the
									// alias.
									let name = if let Some(x) = alias.as_ref().map(Cow::Borrowed) {
										x
									} else {
										Cow::Owned(syn::idiom(&name.to_raw_string())?)
									};
									// Add the projected field to the output document
									out.set(stk, ctx, opt, name.as_ref(), expr).await?
								}
								true => out = expr,
							}
						}
						// This expression is a normal field expression
						_ => {
							let expr = expr.compute(stk, ctx, opt, Some(doc)).await?;
							// Check if this is a single VALUE field expression
							if self.single().is_some() {
								out = expr;
							} else {
								out.set(stk, ctx, opt, name.as_ref(), expr).await?;
							}
						}
					}
				}
			}
		}
		Ok(out)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single {
		expr: Value,
		/// The `quality` in `SELECT rating AS quality FROM ...`
		alias: Option<Idiom>,
	},
}

impl Display for Field {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::All => f.write_char('*'),
			Self::Single {
				expr,
				alias,
			} => {
				Display::fmt(expr, f)?;
				if let Some(alias) = alias {
					f.write_str(" AS ")?;
					Display::fmt(alias, f)
				} else {
					Ok(())
				}
			}
		}
	}
}
