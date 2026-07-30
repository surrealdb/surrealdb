//! The engine's concrete implementation of the opaque builtin closure.
//!
//! `Closure::Builtin` carries an `Arc<dyn BuiltinClosureObj>` so the value
//! layer never names the execution environment. This module owns the one
//! concrete implementation of that trait: a native async function over
//! (`Stk`, `FrozenContext`, `Options`, `CursorDoc`, [`Any`]). Construction
//! sites wrap their function in [`NativeClosure`]; the evaluator downcasts
//! back to it to invoke the closure.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::Any;
use crate::val::Value;
use crate::val::closure::BuiltinClosureObj;

pub(crate) type BuiltinClosureFn = Box<
	dyn for<'a> Fn(
			&'a mut Stk,
			&'a FrozenContext,
			&'a Options,
			Option<&'a CursorDoc>,
			Any,
		) -> Pin<Box<dyn Future<Output = Result<Value>> + 'a>>
		+ Send
		+ Sync,
>;

/// A natively-implemented closure. The only [`BuiltinClosureObj`]
/// implementation in the engine.
pub(crate) struct NativeClosure(pub(crate) BuiltinClosureFn);

impl NativeClosure {
	/// Wraps a native function into the opaque payload `Closure::Builtin`
	/// carries.
	pub(crate) fn new_obj(f: BuiltinClosureFn) -> Arc<dyn BuiltinClosureObj> {
		Arc::new(NativeClosure(f))
	}
}

impl BuiltinClosureObj for NativeClosure {
	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}
