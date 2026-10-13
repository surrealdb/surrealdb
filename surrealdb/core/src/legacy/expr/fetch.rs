use std::collections::BTreeSet;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::fetch::Fetch;
use crate::expr::{Expr, Function, Idiom};
use crate::fnc::args::FromArgs;
use crate::syn;

#[instrument(level = "trace", name = "Fetch::compute", skip_all)]
pub(crate) async fn fetch_compute(
	this: &Fetch,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	idioms: &mut BTreeSet<Idiom>,
) -> Result<()> {
	crate::legacy::fetch_compute_expr(&this.0, stk, ctx, opt, idioms).await
}

/// [`Fetch::compute`] on a bare expression.
///
/// The newtype carries nothing this needs, and a caller holding only the
/// expression would otherwise have to build one per call. Live-query
/// notification does exactly that, per subscriber per record, so the clone
/// was a deep `Expr` tree copy on the write path.
pub(crate) async fn fetch_compute_expr(
	expr: &Expr,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	idioms: &mut BTreeSet<Idiom>,
) -> Result<()> {
	match expr {
		Expr::Idiom(idiom) => {
			idioms.insert(idiom.to_owned());
			Ok(())
		}
		Expr::Param(param) => {
			let v = crate::legacy::param_compute(param, stk, ctx, opt, None).await?;
			idioms.insert(
				syn::idiom(
					v.clone()
						.coerce_to::<String>()
						.map_err(|_| ExecError::InvalidFetch {
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
							stk.run(|stk| crate::legacy::expr_compute(arg, stk, ctx, opt, None))
								.await
								.catch_return()?,
						);
					}

					// replicate the same error that would happen with normal
					// function calls
					let (arg,) = <(String,)>::from_args("type::field", arguments)?;

					// manually do the implementation of type::field
					let idiom: Idiom = syn::idiom(&arg)?.into();
					idioms.insert(idiom);
					Ok(())
				}
				Function::Normal(ref x) if x == "type::fields" => {
					let mut arguments = Vec::new();
					for arg in f.arguments.iter() {
						arguments.push(
							stk.run(|stk| crate::legacy::expr_compute(arg, stk, ctx, opt, None))
								.await
								.catch_return()?,
						);
					}

					// replicate the same error that would happen with normal
					// function calls
					let (args,) = <(Vec<String>,)>::from_args("type::fields", arguments)?;

					// manually do the implementation of type::fields
					for arg in args {
						idioms.insert(syn::idiom(&arg)?.into());
					}
					Ok(())
				}
				_ => Err(anyhow::Error::new(ExecError::InvalidFetch {
					value: Expr::FunctionCall(f.clone()),
				})),
			}
		}
		v => Err(anyhow::Error::new(ExecError::InvalidFetch {
			value: v.clone(),
		})),
	}
}
