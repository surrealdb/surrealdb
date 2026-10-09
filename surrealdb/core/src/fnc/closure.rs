//! The [`ClosureEvaluator`] the closure-taking builtins call back through.

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::{BoxInvokeFut, Closure, ClosureEvaluator, Value};

/// The execution layer's [`ClosureEvaluator`], holding the environment a
/// closure body needs so that callers below this crate do not have to name it.
///
/// Borrowed rather than owned: every call site already has these in hand for
/// the duration of the function call it is serving, so constructing one costs
/// three pointer copies and no allocation.
pub(crate) struct FrameClosureEvaluator<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	doc: Option<&'a CursorDoc>,
}

impl<'a> FrameClosureEvaluator<'a> {
	pub(crate) fn new(
		ctx: &'a FrozenContext,
		opt: &'a Options,
		doc: Option<&'a CursorDoc>,
	) -> Self {
		Self {
			ctx,
			opt,
			doc,
		}
	}
}

impl ClosureEvaluator for FrameClosureEvaluator<'_> {
	fn invoke<'a>(&'a self, closure: &'a Closure, args: Vec<Value>) -> BoxInvokeFut<'a> {
		Box::pin(crate::exec::closure::invoke_in(closure, self.ctx, self.opt, self.doc, args))
	}
}
