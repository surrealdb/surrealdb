pub(crate) mod recursion;

use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::idiom::Idiom;
use crate::expr::part::{Next, NextMethod};
use crate::expr::{Expr, FlowResult, Literal, Part};
use crate::val::{Number, Value};

/// Walk this idiom and substitute any [`Part::Value`] containing a
/// non-literal expression by computing it and replacing it with a literal.
/// Used to resolve parameterized indices such as `foo[$n]` to a static
/// `foo[4]` form before storing the idiom as a schema key.
///
/// String substitutions are canonicalised to [`Part::Field`] so that
/// `addr[$key]` with `$key = "city"` stores as `addr.city` rather than
/// `addr['city']`.
///
/// Returns an error if a substituted index does not evaluate to an
/// integer (array index) or string (object key).
pub(crate) async fn idiom_substitute_indices(
	this: Idiom,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> anyhow::Result<Idiom> {
	let mut out = Vec::with_capacity(this.0.len());
	for part in this.0 {
		let part = match part {
			Part::Value(Expr::Literal(Literal::String(s))) => Part::Field(s),
			Part::Value(Expr::Literal(lit)) => Part::Value(Expr::Literal(lit)),
			Part::Value(expr) => {
				let value = stk
					.run(|stk| crate::legacy::expr_compute(&expr, stk, ctx, opt, doc))
					.await
					.catch_return()?;
				match value {
					Value::Number(Number::Int(i)) => {
						Part::Value(Expr::Literal(Literal::Integer(i)))
					}
					Value::String(s) => Part::Field(s),
					other => {
						return Err(anyhow::anyhow!(
							"Field path index must evaluate to an integer or string, found {}",
							other.kind_of()
						));
					}
				}
			}
			other => other,
		};
		out.push(part);
	}
	Ok(Idiom(out))
}

/// Process this type returning a computed simple Value
pub(crate) async fn idiom_compute(
	this: &Idiom,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	match this.first() {
		// The starting part is a value
		Some(Part::Start(v)) => {
			crate::legacy::value_get(
				&stk.run(|stk| crate::legacy::expr_compute(v, stk, ctx, opt, doc)).await?,
				stk,
				ctx,
				opt,
				doc,
				this.as_ref().next(),
			)
			.await
		}
		// Otherwise use the current document
		_ => match doc {
			// There is a current document
			Some(v) => crate::legacy::value_get(v.doc.as_ref(), stk, ctx, opt, doc, this).await,
			// There isn't any document
			None => {
				crate::legacy::value_get(&Value::None, stk, ctx, opt, doc, this.next_method()).await
			}
		},
	}
}
