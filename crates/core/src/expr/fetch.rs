use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, Literal};
use crate::syn;
use crate::val::{Array, Value};
use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetchs(pub Vec<Fetch>);

impl Deref for Fetchs {
	type Target = Vec<Fetch>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Fetchs {
	type Item = Fetch;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Fetchs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FETCH {}", Fmt::comma_separated(&self.0))
	}
}

impl InfoStructure for Fetchs {
	fn structure(self) -> Value {
		self.into_iter().map(Fetch::structure).collect::<Vec<_>>().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug,  Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetch(#[revision(start = 1)] pub Expr);

impl Fetch {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		idioms: &mut Vec<Idiom>,
	) -> Result<()> {
		let strand_or_idiom = |v: Value| match v {
			Value::Strand(s) => Ok(Idiom::from(s.0)),
			Expr::Idiom(i) => Ok(i.clone()),
			v => Err(Error::InvalidFetch {
				value: v,
			}),
		};
		match &self.0 {
			Expr::Idiom(idiom) => {
				idioms.push(idiom.to_owned());
				Ok(())
			}
			Expr::Param(param) => {
				let v = param.compute(stk, ctx, opt, None).await?;
				idioms.push(strand_or_idiom(v)?);
				Ok(())
			}
			Expr::FunctionCall(f) => {
				if f.name() == Some("type::field") {
					let v = match f.args().first().unwrap() {
						Expr::Param(v) => v.compute(stk, ctx, opt, None).await?,
						v => v.to_owned(),
					};
					idioms.push(strand_or_idiom(v)?);
					Ok(())
				} else if f.name() == Some("type::fields") {
					// Get the first argument which is guaranteed to exist
					let args = match f.args().first().unwrap() {
						Expr::Param(v) => v.compute(stk, ctx, opt, None).await?,
						v => v.to_owned(),
					};
					// This value is always an array, so we can convert it
					let Array(args) = args.coerce_to()?;
					// This value is always an array, so we can convert it
					for v in args.into_iter() {
						let i = match v {
							Expr::Param(v) => {
								strand_or_idiom(v.compute(stk, ctx, opt, None).await?)?
							}
							Expr::Literal(Literal::Strand(s)) => syn::idiom(s.as_str())?.into(),
							Expr::Idiom(i) => i,
							v => {
								bail!(Error::InvalidFetch {
									value: v,
								})
							}
						};
						idioms.push(i);
					}
					Ok(())
				} else {
					Err(anyhow::Error::new(Error::InvalidFetch {
						value: Value::Function(f.clone()),
					}))
				}
			}
			v => Err(anyhow::Error::new(Error::InvalidFetch {
				value: v.clone(),
			})),
		}
	}
}

impl From<Value> for Fetch {
	fn from(value: Value) -> Self {
		Self(value)
	}
}

impl Deref for Fetch {
	type Target = Value;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Fetch {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl InfoStructure for Fetch {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
