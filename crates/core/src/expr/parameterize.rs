use std::str::FromStr;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt, Function, Idiom, Part};

pub async fn exprs_to_fields(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &[Expr],
) -> Result<Vec<Idiom>> {
	let mut fields = Vec::new();
	for expr in expr {
		match expr {
			Expr::Idiom(x) => {
				fields.push(x.clone());
			}
			Expr::FunctionCall(x) => match &x.receiver {
				Function::Normal(fnc) if fnc == "type::field" => {
					let Some(arg) = x.arguments.first() else {
						return Err(anyhow::anyhow!(
							"Expected an argument for type::field function call"
						));
					};
					let field = stk
						.run(|stk| arg.compute(stk, ctx, opt, doc))
						.await
						.catch_return()?
						.coerce_to::<String>()
						.map_err(|_| anyhow::anyhow!("Expected a string"))
						.map(|v| crate::syn::idiom(&v).map(Into::into))??;

					fields.push(field);
				}
				Function::Normal(fnc) if fnc == "type::fields" => {
					let Some(arg) = x.arguments.first() else {
						return Err(anyhow::anyhow!(
							"Expected an argument for type::fields function call"
						));
					};

					let mut x = stk
						.run(|stk| arg.compute(stk, ctx, opt, doc))
						.await
						.catch_return()?
						.coerce_to::<Vec<String>>()
						.map_err(|_| anyhow::anyhow!("Expected an array of strings"))?
						.into_iter()
						.map(|v| crate::syn::idiom(&v).map(Into::into))
						.collect::<anyhow::Result<Vec<Idiom>>>()?;

					fields.append(&mut x);
				}
				_ => {
					return Err(anyhow::anyhow!(
						"Expected an idiom or type::field or type::fields function call"
					));
				}
			},
			_ => {
				return Err(anyhow::anyhow!(
					"Expected an idiom or type::field or type::fields function call"
				));
			}
		}
	}
	Ok(fields)
}

pub async fn expr_to_ident(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<String> {
	if let Expr::Idiom(Idiom(x)) = expr
		&& let [Part::Field(x)] = x.as_slice()
	{
		return Ok(x.clone());
	}
	match stk
		.run(|stk| expr.compute(stk, ctx, opt, doc))
		.await
		.catch_return()?
		.coerce_to::<String>()
	{
		Err(crate::val::value::CoerceError::InvalidKind {
			from,
			..
		}) => Err(crate::val::value::CoerceError::InvalidKind {
			from,
			into: into.to_string(),
		}),
		x => x,
	}
	.map_err(anyhow::Error::from)
}

pub async fn expr_to_optional_ident(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<Option<String>> {
	if let Expr::Idiom(Idiom(x)) = expr
		&& let [Part::Field(x)] = x.as_slice()
	{
		return Ok(Some(x.clone()));
	}
	match stk
		.run(|stk| expr.compute(stk, ctx, opt, doc))
		.await
		.catch_return()?
		.coerce_to::<Option<String>>()
	{
		Err(crate::val::value::CoerceError::InvalidKind {
			from,
			..
		}) => Err(crate::val::value::CoerceError::InvalidKind {
			from,
			into: into.to_string(),
		}),
		x => x,
	}
	.map_err(anyhow::Error::from)
}

pub async fn expr_to_idiom(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<Idiom> {
	match expr {
		Expr::Idiom(x) => Ok(x.clone()),
		x => {
			let raw = match stk
				.run(|stk| x.compute(stk, ctx, opt, doc))
				.await
				.catch_return()?
				.coerce_to::<String>()
			{
				Err(crate::val::value::CoerceError::InvalidKind {
					from,
					..
				}) => Err(crate::val::value::CoerceError::InvalidKind {
					from,
					into: into.to_string(),
				}),
				x => x,
			}?;

			Idiom::from_str(&raw)
				.map_err(|e| anyhow::anyhow!("Failed to parse {} from string: {e}", into))
		}
	}
}
