//! Invoking a closure from below the execution layer.
//!
//! A closure body is an [`Expr`](crate::expr::Expr), and evaluating one needs
//! the whole execution environment: a context, the statement's options, the
//! document under the cursor. Those live above this crate, so a function
//! library that sits here cannot name them — yet `array::map` and its
//! siblings exist precisely to call a closure once per element.
//!
//! The seam is this trait. The implementation *holds* the environment and
//! only the per-call data crosses the boundary, so the caller needs to name
//! nothing but the closure and its arguments.

use std::future::Future;
use std::pin::Pin;

use reblessive::tree::Stk;

use crate::val::{Closure, Value};

/// A boxed future returned by [`ClosureEvaluator::invoke`].
///
/// Boxed at the trait boundary because the call is recursive: invoking a
/// closure evaluates its body, which may invoke another closure. It is
/// deliberately not `Send` — a closure body runs inside the heap-allocated
/// `TreeStack` that erases auto-traits, and the engine's builtin closures
/// return a non-`Send` future of their own.
pub type BoxInvokeFut<'a> = Pin<Box<dyn Future<Output = anyhow::Result<Value>> + 'a>>;

/// Invokes closures on behalf of a caller that cannot reach the execution
/// environment.
///
/// Implemented by the execution layer, which captures the context, options
/// and cursor document the closure body needs. `stk` stays an explicit
/// parameter rather than being captured: the caller is already inside a
/// `TreeStack` frame and must hand down the same one, or a closure that
/// recurses would build a second stack instead of growing the first.
///
/// `Sync` is required so that `&dyn ClosureEvaluator` is itself `Send`: the
/// engine's executor holds one across an await inside a `Send` boxed future.
/// The bound constrains the evaluator, not the future it hands back —
/// [`BoxInvokeFut`] stays non-`Send` for the reasons given above.
pub trait ClosureEvaluator: Sync {
	/// Calls `closure` with `args`, returning what its body evaluates to.
	///
	/// A `return` inside the body is caught and becomes the value, matching
	/// what a caller of an anonymous function expects; `break` and `continue`
	/// escaping the body are errors, as is any failure evaluating it.
	fn invoke<'a>(
		&'a self,
		stk: &'a mut Stk,
		closure: &'a Closure,
		args: Vec<Value>,
	) -> BoxInvokeFut<'a>;
}
