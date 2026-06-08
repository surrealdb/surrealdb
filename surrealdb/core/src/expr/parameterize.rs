use std::str::FromStr;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt, Function, Idiom, Part};

/// Resolve an [`Expr`] to an [`Idiom`].
///
/// - If the expression is an [`Expr::Idiom`] whose first part is a literal [`Part::Field`], the
///   idiom is kept as-is and any inner parameterized indices (`Part::Value` with a non-literal
///   expression) are computed and substituted with literals.
/// - Any other expression (idioms whose head is a [`Part::Start`], parameters, function calls,
///   etc.) is fully computed against the current context, coerced to a string, and re-parsed as an
///   idiom.
///
/// No `Idiom::validate_local` enforcement happens here — callers that need to
/// ensure the result is a valid schema-key path call [`resolve_local_idiom`].
async fn resolve_idiom(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<Idiom> {
	match expr {
		Expr::Idiom(idiom) if matches!(idiom.0.first(), Some(Part::Field(_))) => {
			idiom.clone().substitute_indices(stk, ctx, opt, doc).await
		}
		_ => {
			let raw = match stk
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
			}?;

			Idiom::from_str(&raw)
				.map_err(|e| anyhow::anyhow!("Failed to parse {} from string: {e}", into))
		}
	}
}

/// Same as [`resolve_idiom`], but additionally enforces
/// [`Idiom::validate_local`] so the result is only composed of parts that are
/// meaningful as a static field path (used by DEFINE/ALTER/REMOVE FIELD).
async fn resolve_local_idiom(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<Idiom> {
	let idiom = resolve_idiom(stk, ctx, opt, doc, expr, into).await?;
	idiom.validate_local(into)?;
	Ok(idiom)
}

/// Resolve a list of field-path expressions for callers like `OMIT` and
/// `DEFINE INDEX FIELDS`.
///
/// Unlike [`expr_to_idiom`] (used by `DEFINE/ALTER/REMOVE FIELD`), this does
/// not enforce [`Idiom::validate_local`]. `OMIT` accepts [`Part::Destructure`]
/// (e.g. `OMIT obj.{ a, b }`) and `DEFINE INDEX FIELDS` accepts method calls
/// (e.g. `id.id().val`); both are rejected by `validate_local`. We still
/// substitute parameterized indices so that paths like `OMIT obj[$idx]`
/// resolve to a static literal at compute time.
pub async fn exprs_to_fields(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &[Expr],
) -> Result<Vec<Idiom>> {
	let mut fields = Vec::new();
	for expr in expr {
		match expr {
			Expr::FunctionCall(x) if matches!(&x.receiver, Function::Normal(fnc) if fnc == "type::fields") =>
			{
				let Some(arg) = x.arguments.first() else {
					return Err(anyhow::anyhow!(
						"Expected an argument for type::fields function call"
					));
				};

				let raws = stk
					.run(|stk| arg.compute(stk, ctx, opt, doc))
					.await
					.catch_return()?
					.coerce_to::<Vec<String>>()
					.map_err(|_| anyhow::anyhow!("Expected an array of strings"))?;

				for raw in raws {
					let idiom: Idiom = crate::syn::idiom(&raw)?.into();
					fields.push(idiom);
				}
			}
			Expr::FunctionCall(x) if matches!(&x.receiver, Function::Normal(fnc) if fnc == "type::field") =>
			{
				let Some(arg) = x.arguments.first() else {
					return Err(anyhow::anyhow!(
						"Expected an argument for type::field function call"
					));
				};
				let raw = stk
					.run(|stk| arg.compute(stk, ctx, opt, doc))
					.await
					.catch_return()?
					.coerce_to::<String>()
					.map_err(|_| anyhow::anyhow!("Expected a string"))?;
				let idiom: Idiom = crate::syn::idiom(&raw)?.into();
				fields.push(idiom);
			}
			_ => {
				fields.push(resolve_idiom(stk, ctx, opt, doc, expr, "field name").await?);
			}
		}
	}
	Ok(fields)
}

pub async fn expr_to_ident(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<String> {
	if let Expr::Idiom(Idiom(x)) = expr
		&& let [Part::Field(x)] = x.as_slice()
	{
		return Ok(x.as_str().to_owned());
	}
	// Handle table name expressions (when parsed with table_as_field = false)
	if let Expr::Table(name) = expr {
		return Ok(name.to_string());
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
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<Option<String>> {
	if let Expr::Idiom(Idiom(x)) = expr
		&& let [Part::Field(x)] = x.as_slice()
	{
		return Ok(Some(x.as_str().to_owned()));
	}
	// Handle table name expressions (when parsed with table_as_field = false)
	if let Expr::Table(name) = expr {
		return Ok(Some(name.to_string()));
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
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	into: &str,
) -> Result<Idiom> {
	resolve_local_idiom(stk, ctx, opt, doc, expr, into).await
}
