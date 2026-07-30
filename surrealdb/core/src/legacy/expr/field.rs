use std::borrow::Cow;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::expr::field::Fields;
use crate::expr::{Expr, Function, Idiom, Part};
use crate::fnc::args::FromArgs;
use crate::syn;
use crate::val::{Array, Value};

/// Process this type returning a computed simple Value
pub(crate) async fn fields_compute(
	this: &Fields,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	if let Some(doc) = doc {
		crate::legacy::fields_compute_value(this, stk, ctx, opt, doc).await
	} else {
		let doc = Value::None.into();
		crate::legacy::fields_compute_value(this, stk, ctx, opt, &doc).await
	}
}

pub(crate) async fn fields_compute_value(
	this: &Fields,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: &CursorDoc,
) -> Result<Value> {
	// Process the desired output

	// TODO: This makes it so that with selection `SELECT 1 as foo,*,bar` if `foo`
	// is in the document it will be overwritten with 1. It might be slightly more
	// usefull to have the ordering matter and make `1 as foo,*` provide the foo
	// from the document and have `*, 1 as foo` provide the overwritten foo.
	let mut out = if this.has_all_selection() {
		doc.doc.as_ref().clone()
	} else {
		Value::empty_object()
	};

	for v in this.iter_non_all_fields() {
		let name =
			v.alias.as_ref().map(Cow::Borrowed).unwrap_or_else(|| Cow::Owned(v.expr.to_idiom()));
		match &v.expr {
			// This expression is a multi-output graph traversal
			Expr::Idiom(i) if i.is_multi_yield() => {
				// Store the different output yields here
				let mut res: Vec<(&[Part], Value)> = Vec::new();
				// Split the expression by each output alias
				for v in i.split_inclusive(Idiom::part_is_multi_yield) {
					// Use the last fetched value for each fetch
					let x = match res.last() {
						Some((_, r)) => r,
						None => doc.doc.as_ref(),
					};
					// Continue fetching the next idiom part
					let x = crate::legacy::value_get(x, stk, ctx, opt, Some(doc), v)
						.await
						.catch_return()?
						// TODO: Controlflow winding up to here has some strange
						// implications, check validity.
						.flatten();
					// Add the result to the temporary store
					res.push((v, x));
				}
				// Assign each fetched yield to the output
				for (p, x) in res {
					match p.last().expect("idiom is non-empty").alias() {
						// This is an alias expression part
						Some(a) => {
							if let Some(i) = &v.alias {
								crate::legacy::value_set(&mut out, stk, ctx, opt, &i.0, x.clone())
									.await?;
							}
							crate::legacy::value_set(&mut out, stk, ctx, opt, a, x).await?;
						}
						// This is the end of the expression
						None => {
							crate::legacy::value_set(
								&mut out,
								stk,
								ctx,
								opt,
								v.alias.as_ref().unwrap_or(i),
								x,
							)
							.await?
						}
					}
				}
			}
			// TODO: This section should not be handled here, this should be catched by
			// an analysis pass and optimized.
			Expr::FunctionCall(f) => {
				// functions 'type::fields' and 'type::field' are specially handled
				// here as they don't just return a result but also set fields on
				// the document, so `type::field("foo")` results in `{ foo: "value"
				// }` instead of `{ ["type::field('foo')"]: "value" }`
				match f.receiver {
					Function::Normal(ref x) if x == "type::fields" => {
						// Some manual reimplemenation of type::fields to make it
						// more efficient.
						let mut arguments = Vec::new();
						for arg in f.arguments.iter() {
							arguments.push(
								stk.run(|stk| {
									crate::legacy::expr_compute(arg, stk, ctx, opt, Some(doc))
								})
								.await
								.catch_return()?,
							);
						}

						// replicate the same error that would happen with normal
						// function calls
						let (args,) = <(Vec<String>,)>::from_args("type::fields", arguments)?;

						// manually do the implementation of type::fields
						let mut idioms = Vec::<Idiom>::new();
						for arg in args {
							idioms.push(syn::idiom(&arg)?.into())
						}

						let mut idiom_results = Vec::new();
						for idiom in idioms.iter() {
							let res = crate::legacy::idiom_compute(idiom, stk, ctx, opt, Some(doc))
								.await
								.catch_return()?;
							idiom_results.push(res);
						}
						// Check if this is a single VALUE field expression
						if this.is_single() {
							out = Value::Array(Array(idiom_results));
						} else {
							// TODO: Alias is ignored here, figure out the right
							// behaviour. Maybe make an alias result in sub fields?
							// `select type::fields(["foo","faz"]) as bar` resulting
							// in `{ "bar": { foo: value, faz: value} }`?
							for (idiom, idiom_res) in idioms.iter().zip(idiom_results) {
								crate::legacy::value_set(
									&mut out, stk, ctx, opt, &idiom.0, idiom_res,
								)
								.await?;
							}
						}
					}
					Function::Normal(ref x) if x == "type::field" => {
						// Some manual reimplemenation of type::field to make it
						// more efficient.
						let mut arguments = Vec::new();
						for arg in f.arguments.iter() {
							arguments.push(
								stk.run(|stk| {
									crate::legacy::expr_compute(arg, stk, ctx, opt, Some(doc))
								})
								.await
								.catch_return()?,
							);
						}

						// replicate the same error that would happen with normal
						// function calls
						let (arg,) = <(String,)>::from_args("type::field", arguments)?;

						// manually do the implementation of type::field
						let idiom: Idiom = syn::idiom(&arg)?.into();

						let res = crate::legacy::idiom_compute(&idiom, stk, ctx, opt, Some(doc))
							.await
							.catch_return()?;

						if let Some(alias) = &v.alias {
							crate::legacy::value_set(&mut out, stk, ctx, opt, alias, res).await?;
						} else if this.is_single() {
							out = res
						} else {
							crate::legacy::value_set(&mut out, stk, ctx, opt, &idiom.0, res)
								.await?;
						}
					}
					_ => {
						let expr = stk
							.run(|stk| {
								crate::legacy::expr_compute(&v.expr, stk, ctx, opt, Some(doc))
							})
							.await
							.catch_return()?;

						if this.is_single() {
							out = expr;
						} else {
							crate::legacy::value_set(&mut out, stk, ctx, opt, name.as_ref(), expr)
								.await?;
						}
					}
				}
			}

			// This expression is a normal field expression
			_ => {
				let expr = stk
					.run(|stk| crate::legacy::expr_compute(&v.expr, stk, ctx, opt, Some(doc)))
					.await
					.catch_return()?;
				// Check if this is a single VALUE field expression
				if this.is_single() {
					out = expr;
				} else {
					crate::legacy::value_set(&mut out, stk, ctx, opt, name.as_ref(), expr).await?;
				}
			}
		}
	}
	Ok(out)
}
