//! Function system for the engine.
//!
//! This module provides a trait-based function system that supports:
//! - Pure scalar functions (no context needed)
//! - Context-aware scalar functions (need execution context)
//! - Async scalar functions (HTTP, crypto, etc.)
//! - Aggregate functions (operate over groups of values)
//! - Projection functions (produce field bindings for output objects)
//!
//! Functions are registered in a `FunctionRegistry` which can be accessed
//! through the execution context.

mod aggregate;
mod builtin;
#[cfg(feature = "scripting")]
pub(crate) use builtin::timezone_at;
pub(crate) use builtin::{evaluate_streaming, run_statements};
#[cfg(all(test, feature = "kv-mem"))]
mod fallibility_probes;
mod index;
mod macros;
mod method;
#[cfg(all(test, feature = "kv-mem"))]
mod parity_tests;
mod projection;
mod registry;
#[cfg(test)]
mod rule_tests;
mod signature;

use std::fmt::Debug;

pub use aggregate::{Accumulator, AggregateFunction};
use anyhow::Result;
pub(crate) use builtin::aggregates::AccumulatorState;
// Read only by the full-text tests that run a datastore, and by `search::score`.
#[cfg(all(test, feature = "kv-mem"))]
pub(crate) use index::DOC_ID_READS;
#[cfg(all(test, any(feature = "kv-mem", feature = "search")))]
pub(crate) use index::ROW_SCORES;
pub use index::{
	IndexContext, IndexContextKind, IndexFunction, KnnContext, MatchContext, MatchesContext,
};
#[cfg(test)]
pub(crate) use index::{TERM_READS, TERM_STATES_READ};
pub use method::{MethodDescriptor, ValueKind};
pub use projection::ProjectionFunction;
pub use registry::FunctionRegistry;
pub(crate) use registry::{FnKind, check_fallibility_row};
pub use signature::{ReturnRule, Signature};

use crate::exec::ir::iface::Fallibility;
use crate::exec::{BoxFut, EvalContext};
use crate::expr::Kind;
pub use crate::expr::logical::EffectSet;
use crate::val::Value;

/// Enforce an argument-count range, producing the same error shape and
/// messages as the `fnc::args::FromArgs` machinery. Used by builtins that
/// decode their arguments by hand instead of going through `FromArgs`.
pub(crate) fn check_arity(
	name: &str,
	args: &[Value],
	lower: usize,
	upper: Option<usize>,
) -> Result<()> {
	if args.len() < lower || upper.map(|x| args.len() > x).unwrap_or(false) {
		let message = if let Some(upper) = upper {
			if upper == lower {
				if upper == 0 {
					"Expected no arguments".to_string()
				} else if upper == 1 {
					"Expected 1 argument".to_string()
				} else {
					format!("Expected {upper} arguments")
				}
			} else {
				format!("Expected {lower} to {upper} arguments")
			}
		} else if lower == 0 {
			"Expected zero or more arguments".to_string()
		} else {
			format!("Expected {lower} or more arguments")
		};

		anyhow::bail!(crate::expr::Error::InvalidFunctionArguments {
			name: name.to_owned(),
			message,
		});
	}
	Ok(())
}

/// The async walk a metered pure function runs over its arguments.
///
/// See [`ScalarFunction::metered`] for when a function provides one.
pub type MeteredFn = fn(Vec<Value>) -> BoxFut<'static, Result<Value>>;

/// An async scalar function implementation taking the evaluation context and raw arguments.
pub type AsyncScalarFn = for<'a> fn(&'a EvalContext<'_>, Vec<Value>) -> BoxFut<'a, Result<Value>>;

/// How a [`BuiltinScalarFn`] executes.
#[derive(Clone, Copy)]
pub enum ScalarExec {
	/// A synchronous context-free function.
	Pure(fn(Vec<Value>) -> Result<Value>),
	/// A metered context-free function whose work grows with its input.
	Metered(MeteredFn),
	/// A synchronous function that reads from [`EvalContext`].
	Context(fn(&EvalContext<'_>, Vec<Value>) -> Result<Value>),
	/// An asynchronous function that takes [`EvalContext`].
	Async(AsyncScalarFn),
}

/// Table-driven descriptor for a built-in scalar function.
///
/// Builtin macro definitions expand to a `const` of this type so the
/// [`ScalarFunction`] vtable and dispatch methods are compiled once rather than
/// per builtin struct.
#[derive(Clone, Copy)]
pub struct BuiltinScalarFn {
	pub name: &'static str,
	pub effects: EffectSet,
	pub fallibility: Fallibility,
	pub closure_args: &'static [usize],
	pub signature: fn() -> Signature,
	pub exec: ScalarExec,
}

impl Debug for BuiltinScalarFn {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("BuiltinScalarFn").field("name", &self.name).finish()
	}
}

impl ScalarFunction for BuiltinScalarFn {
	fn name(&self) -> &'static str {
		self.name
	}

	fn signature(&self) -> Signature {
		(self.signature)()
	}

	fn effects(&self) -> EffectSet {
		self.effects
	}

	fn fallibility(&self) -> Fallibility {
		self.fallibility
	}

	fn closure_args(&self) -> &'static [usize] {
		self.closure_args
	}

	fn is_pure(&self) -> bool {
		matches!(self.exec, ScalarExec::Pure(_) | ScalarExec::Metered(_))
	}

	fn is_async(&self) -> bool {
		matches!(self.exec, ScalarExec::Async(_))
	}

	fn invoke(&self, args: Vec<Value>) -> Result<Value> {
		match self.exec {
			ScalarExec::Pure(f) => f(args),
			ScalarExec::Metered(walk) => run_walk(walk(args).as_mut()),
			ScalarExec::Context(_) => {
				Err(anyhow::anyhow!("Function '{}' requires context", self.name))
			}
			ScalarExec::Async(_) => {
				Err(anyhow::anyhow!("Function '{}' requires async execution", self.name))
			}
		}
	}

	fn metered(&self) -> Option<MeteredFn> {
		match self.exec {
			ScalarExec::Metered(walk) => Some(walk),
			ScalarExec::Pure(_) | ScalarExec::Context(_) | ScalarExec::Async(_) => None,
		}
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> BoxFut<'a, Result<Value>> {
		match self.exec {
			ScalarExec::Pure(_) | ScalarExec::Metered(_) => ready_boxed(self.invoke(args)),
			ScalarExec::Context(f) => ready_boxed(f(ctx, args)),
			ScalarExec::Async(f) => f(ctx, args),
		}
	}
}

/// A future that is ready with `result`: the `invoke_async` of a builtin whose
/// work is synchronous. The work runs before the future is made; polling only
/// hands over the result.
#[inline(never)]
pub(crate) fn ready_boxed(result: Result<Value>) -> BoxFut<'static, Result<Value>> {
	Box::pin(std::future::ready(result))
}

/// Run a metered builtin's walk to its end on this thread without yielding
/// ([`surrealdb_runtime::fuel::now`]): the units it charges stand, so the
/// executor yields at its next yield point. The synchronous `invoke` of every
/// metered builtin, among them plan-time folding.
#[inline(never)]
pub(crate) fn run_walk(
	walk: std::pin::Pin<&mut (dyn std::future::Future<Output = Result<Value>> + Send + '_)>,
) -> Result<Value> {
	surrealdb_runtime::fuel::now(walk)
}

/// Scalar functions operate on individual values and return a single value.
/// They may be:
/// - Pure: operate only on their arguments with no side effects
/// - Context-aware: need access to session/database state
/// - Async: perform I/O operations
pub trait ScalarFunction: Send + Sync + Debug {
	/// The fully qualified function name (e.g., "math::abs", "string::len")
	fn name(&self) -> &'static str;

	/// The function signature describing arguments and return type.
	#[allow(unused)]
	fn signature(&self) -> Signature;

	/// The result kind of a call whose arguments have kinds `arg_types`, for
	/// kind inference in the plan. The default applies the signature's
	/// [`ReturnRule`].
	#[allow(unused)]
	fn return_type(&self, arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().infer(arg_types))
	}

	/// The minimum context level required to execute this function.
	///
	/// Functions that access database state (e.g., analyzers, custom functions)
	/// should override this to return `ContextLevel::Database`.
	/// The default is `Root` (no namespace/database context needed).
	fn required_context(&self) -> crate::exec::ContextLevel {
		crate::exec::ContextLevel::Root
	}

	/// Whether this function is pure (no context needed).
	///
	/// Pure functions can be evaluated with just their arguments.
	/// Non-pure functions need access to the execution context.
	fn is_pure(&self) -> bool {
		true
	}

	/// Whether this function requires async execution.
	///
	/// Async functions perform I/O or other blocking operations.
	fn is_async(&self) -> bool {
		false
	}

	/// What the function does besides compute its result: the effects a
	/// rewrite has to respect before it may duplicate, drop, fold or move a
	/// call. See [`EffectSet`] for the flags and what each licenses.
	///
	/// Declared, never derived: the `define_*_function!` macros require it at
	/// the definition, and the registry's guard test pins every set that is
	/// not [`EffectSet::PURE`] by name, so a builtin's effects are stated once
	/// and read everywhere. This is distinct from [`ScalarFunction::is_pure`],
	/// which reports only whether a function can run without an execution
	/// context: `rand::float` needs no context and still draws entropy.
	fn effects(&self) -> EffectSet;

	/// What a call's failure can depend on: the failure classes of the body
	/// for arguments of the signature's kinds. Argument coercion (arity and
	/// kind) is the call's `VALUE`, not the builtin's.
	///
	/// A class is a superset when unsure: a declared class may name a
	/// dependency the body never exercises, and never omits one it does.
	/// [`Fallibility::INFALLIBLE`] says no well-kinded argument, capability,
	/// principal or stored state makes the body raise. Every implementor
	/// declares a class; there is no default, so an undeclared one does not
	/// compile.
	#[allow(unused)]
	fn fallibility(&self) -> Fallibility;

	/// The argument positions in which this builtin accepts a closure it
	/// then runs — `check` of `array::any`, `mapper` of `array::fold`, the
	/// `closure` of `value::chain`. Empty for a builtin that runs nothing it
	/// is handed.
	///
	/// A body run this way evaluates against the row the call is evaluated
	/// against, exactly as applying the closure directly would. When the
	/// closure is written in the call its body is in the expression and is
	/// judged as a child; when it arrives as a value — a parameter, a field
	/// of an object — nothing in the call says what it reads, and the
	/// effect fold treats the call as running an unseen body
	/// ([`EffectSet::OPAQUE_BODY`]) and as reading the row opaquely. Only the
	/// positions named here are judged, so a value in any other position —
	/// the array `array::any` searches, the seed `array::fold` starts from —
	/// leaves the call transparent.
	///
	/// SECURITY: a closure-running builtin that omits its position here lets
	/// a body reach a field-permission layer unguarded. The
	/// `define_*_closure_function!` macros declare the position themselves,
	/// and the registry's guard test pins every declaration by name.
	fn closure_args(&self) -> &'static [usize] {
		&[]
	}

	/// Whether a second call with the same arguments answers the same and
	/// leaves nothing behind: [`EffectSet::is_deterministic`] of the declared
	/// effects.
	///
	/// Two decisions read this. The planner's literal folder evaluates a call
	/// once at plan time and substitutes the result for the whole statement, so
	/// it folds only deterministic functions; everything else is evaluated per
	/// row. A CLOCK read is not deterministic, so the folder folds none: a
	/// plan holds no instant, and `time::now()` reads the statement's clock
	/// when it runs, as an operand of an index bound
	/// (`BoundOperand::StatementClock`) or from the context everywhere else. The
	/// membership recogniser asks the same question of every call inside an
	/// expression before treating that expression as pure, which is what lets
	/// it build a subquery once for every outer row.
	fn is_deterministic(&self) -> bool {
		self.effects().is_deterministic()
	}

	/// Whether invoking this function changes state that outlives the query:
	/// [`EffectSet::mutates_state`] of the declared effects.
	///
	/// A function that only observes can be called any number of times without
	/// anything outside the expression noticing, however much its answers
	/// differ between calls — `rand::*` and `time::now` are in that class. One
	/// that advances a sequence, writes a file, sends a non-safe HTTP request,
	/// or runs a body that may write is not: how many times it runs is itself
	/// part of the answer.
	///
	/// The planner asks this before reducing a grouped projection to a single
	/// evaluation per group. Reducing a mutator would let the grouping decide
	/// how many effects the statement has, and would run one for a `GROUP ALL`
	/// that saw no rows at all, so those stay on the per-row path.
	fn mutates_state(&self) -> bool {
		self.effects().mutates_state()
	}

	/// Synchronous invocation for pure functions.
	///
	/// This is the primary entry point for pure scalar functions.
	/// The default implementation returns an error.
	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires context or async execution", self.name()))
	}

	/// The walk the executor runs this pure function as when its work grows
	/// with its input: an `async fn` over the same arguments that charges the
	/// thread's fuel a stretch at a time and, once that is spent, waits for a
	/// yield point ([`surrealdb_runtime::fuel`]). `None` for a function the
	/// executor calls through [`Self::invoke`] and charges at the call, by the
	/// size of its arguments and of its result.
	fn metered(&self) -> Option<MeteredFn> {
		None
	}

	/// Async invocation with context access.
	///
	/// This is used for context-aware or async functions.
	/// The default implementation runs `invoke()` when `invoke_async` is
	/// called, and the future it returns is already ready.
	#[allow(unused_variables)]
	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> BoxFut<'a, Result<Value>> {
		ready_boxed(self.invoke(args))
	}
}
