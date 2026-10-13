//! Invoking a closure value from outside a compiled body.
//!
//! A closure value is a [`Closure`], and every form of it is invoked here, by
//! the engine, in the evaluation frame of the caller:
//!
//! - a closure the engine created from a literal ([`CompiledClosure`]) runs its compiled body, or
//!   the unit the VM holds for it;
//! - the rest of an API chain ([`ApiContinuation`]) runs the chain from the stage after the one
//!   that received it;
//! - a closure stored as written (`Closure::Expr`, as `DEFINE PARAM` keeps one) is lowered and
//!   compiled on its first invocation, and then runs as a [`CompiledClosure`] does.
//!
//! An invocation binds its arguments in an isolated child of the caller's
//! context that holds the captures. The child is one re-entry deeper than the
//! caller. The result is coerced to the declared return kind; see
//! [`CompiledClosure::invoke`].
//!
//! [`invoke`] is the route for a caller that is evaluating inside a plan and
//! holds an [`EvalContext`]. [`invoke_in`] is the route for a caller that holds
//! only the handles a function body is given — a [`FrozenContext`], an
//! [`Options`] and the row it is running against.

use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::EvalContext;
use crate::exec::api::ApiContinuation;
use crate::exec::vm::calls::CompiledClosure;
use crate::expr::{ControlFlow, FlowResult};
use crate::val::{Closure, Value};

/// Invoke `closure` with `args` in the frame `ctx` is evaluating in.
///
/// A `RETURN` in the body is its value; `BREAK` and `CONTINUE` have no loop to
/// reach and are errors.
pub(crate) async fn invoke(
	closure: &Closure,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	match closure {
		Closure::Builtin(obj) => {
			if let Some(compiled) = obj.as_any().downcast_ref::<CompiledClosure>() {
				compiled.invoke(ctx, args).await
			} else if let Some(next) = obj.as_any().downcast_ref::<ApiContinuation>() {
				next.invoke(args, ctx.exec_ctx).await
			} else {
				Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
					"Closure::Builtin carried an unknown implementation type".to_string(),
				))))
			}
		}
		Closure::Expr {
			args: declared,
			returns,
			body,
			captures,
		} => {
			let stored = CompiledClosure::of_stored(declared, returns.as_ref(), body, captures)
				.map_err(ControlFlow::Err)?;
			stored.invoke(ctx, args).await
		}
	}
}

/// Invoke `closure` with `args` from a caller that holds the handles of a
/// function body: the frame is built from `ctx` and `opt`. The body reads the
/// row the closure captured where it was written, none for a stored closure,
/// and never `doc`.
pub(crate) async fn invoke_in(
	closure: &Closure,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
) -> Result<Value> {
	let exec = crate::exec::entry::frame(ctx, opt, doc).await;
	let frame = crate::exec::entry::eval_frame(&exec);
	invoke(closure, args, &frame).await.map_err(|signal| match signal {
		ControlFlow::Err(e) => e,
		ControlFlow::Break | ControlFlow::Continue => {
			anyhow::Error::new(crate::exec::Error::InvalidControlFlow)
		}
		other => anyhow::Error::new(crate::err::EngineError::Internal(format!(
			"unexpected control flow from a closure body: {other}"
		))),
	})
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::invoke_in;
	use crate::dbs::{Session, Variables};
	use crate::expr::{Kind, Param};
	use crate::kvs::{Datastore, TransactionType};
	use crate::val::{Closure, Value};

	/// `|$x: number| $x * 2`, as a closure stored as written.
	fn doubler(returns: Option<Kind>) -> Closure {
		Closure::Expr {
			args: vec![(Param::from("x".to_string()), Kind::Number)],
			returns,
			body: crate::syn::expr("$x * 2").unwrap(),
			captures: Variables::default(),
		}
	}

	async fn call(closure: &Closure, args: Vec<Value>) -> anyhow::Result<Value> {
		let ds = Datastore::new("memory").await.unwrap();
		let session = Session::owner().with_ns("test").with_db("test");
		let mut ctx = ds.setup_ctx().unwrap();
		ctx.set_transaction(Arc::new(ds.transaction(TransactionType::Read).await.unwrap()));
		let ctx = ctx.freeze();
		let opt = ds.setup_options(&session);
		invoke_in(closure, &ctx, &opt, None, args).await
	}

	#[tokio::test]
	async fn a_stored_closure_runs_its_body_on_its_arguments() {
		let value = call(&doubler(None), vec![Value::from(21)]).await.unwrap();
		assert_eq!(value, Value::from(42));
	}

	#[tokio::test]
	async fn a_stored_closure_binds_its_captures() {
		let mut captures = Variables::default();
		captures.insert("by", Value::from(3));
		let closure = Closure::Expr {
			args: vec![(Param::from("x".to_string()), Kind::Number)],
			returns: None,
			body: crate::syn::expr("$x * $by").unwrap(),
			captures,
		};
		assert_eq!(call(&closure, vec![Value::from(5)]).await.unwrap(), Value::from(15));
	}

	#[tokio::test]
	async fn a_stored_closure_refuses_a_missing_or_mistyped_argument() {
		let missing = call(&doubler(None), vec![]).await.unwrap_err().to_string();
		assert!(missing.contains("Expected a value of type 'number'"), "{missing}");
		let mistyped = call(&doubler(None), vec![Value::from("x")]).await.unwrap_err().to_string();
		assert!(mistyped.contains("Expected a value of type 'number'"), "{mistyped}");
	}

	#[tokio::test]
	async fn a_stored_closure_coerces_its_result_to_the_declared_kind() {
		let err =
			call(&doubler(Some(Kind::String)), vec![Value::from(2)]).await.unwrap_err().to_string();
		assert!(err.contains("Couldn't coerce return value"), "{err}");
	}
}
