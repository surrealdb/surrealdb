use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;

use super::FlowResultExt as _;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Function, Idiom};
use crate::fnc::args::FromArgs;
use crate::syn;
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Fetch(pub Expr);

impl Fetch {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		idioms: &mut Vec<Idiom>,
	) -> Result<()> {
		match &self.0 {
			Expr::Idiom(idiom) => {
				idioms.push(idiom.to_owned());
				Ok(())
			}
			Expr::Param(param) => {
				let v = param.compute(stk, ctx, opt, None).await?;
				idioms.push(
					syn::idiom(
						v.clone()
							.coerce_to::<Strand>()
							.map_err(|_| Error::InvalidFetch {
								value: v.into_literal(),
							})?
							.as_str(),
					)?
					.into(),
				);
				Ok(())
			}
			Expr::FunctionCall(f) => {
				// NOTE: Behavior here changed with value inversion PR.
				// Previously `type::field(a.b)` would produce a fetch `a.b`.
				// This is somewhat weird because elsewhere this wouldn't work.
				match f.receiver {
					Function::Normal(ref x) if x == "type::field" => {
						// Some manual reimplemenation of type::field to make it
						// more efficient.
						let mut arguments = Vec::new();
						for arg in f.arguments.iter() {
							arguments.push(
								stk.run(|stk| arg.compute(stk, ctx, opt, None))
									.await
									.catch_return()?,
							);
						}

						// replicate the same error that would happen with normal
						// function calls
						let (arg,) = <(String,)>::from_args("type::field", arguments)?;

						// manually do the implementation of type::field
						let idiom: Idiom = syn::idiom(&arg)?.into();
						idioms.push(idiom);
						Ok(())
					}
					Function::Normal(ref x) if x == "type::fields" => {
						let mut arguments = Vec::new();
						for arg in f.arguments.iter() {
							arguments.push(
								stk.run(|stk| arg.compute(stk, ctx, opt, None))
									.await
									.catch_return()?,
							);
						}

						// replicate the same error that would happen with normal
						// function calls
						let (args,) = <(Vec<String>,)>::from_args("type::fields", arguments)?;

						// manually do the implementation of type::fields
						for arg in args {
							idioms.push(syn::idiom(&arg)?.into());
						}
						Ok(())
					}
					_ => Err(anyhow::Error::new(Error::InvalidFetch {
						value: Expr::FunctionCall(f.clone()),
					})),
				}
			}
			v => Err(anyhow::Error::new(Error::InvalidFetch {
				value: v.clone(),
			})),
		}
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
