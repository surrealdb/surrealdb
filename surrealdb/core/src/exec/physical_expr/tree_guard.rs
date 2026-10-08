//! The check that no operator-tree expression is evaluated outside the
//! oracle the VM's parity tests compare against.
//!
//! Every statement and expression runs as a VM program; an expression's
//! operator-tree compilation ([`PhysicalExpr`]) is evaluated only by the
//! test-build oracle. A build with debug assertions, outside a test build,
//! wraps every compilation [`crate::exec::planner::Planner::physical_expr`]
//! answers in [`Guarded`], which panics when it is evaluated, so the language
//! suite and the integration tests prove that nothing reaches one. With
//! `SURREAL_TREE_PROBE` set it prints the frames that reached it instead.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use super::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

/// `compiled`, guarded where the build checks that no tree is evaluated.
pub(crate) fn guarded(compiled: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
	#[cfg(all(debug_assertions, not(test)))]
	{
		Arc::new(Guarded(compiled))
	}
	#[cfg(not(all(debug_assertions, not(test))))]
	compiled
}

/// An operator-tree expression whose evaluation is a defect.
#[derive(Debug)]
pub(crate) struct Guarded(Arc<dyn PhysicalExpr>);

impl Guarded {
	fn reached(&self) {
		let trace = std::backtrace::Backtrace::force_capture().to_string();
		let frames: Vec<&str> = trace
			.lines()
			.filter(|l| l.contains("surrealdb_core::") && !l.contains("tree_guard"))
			.take(16)
			.collect();
		let message = format!(
			"an operator-tree expression was evaluated: {}\n{}",
			self.0.to_sql(),
			frames.join("\n")
		);
		if std::env::var_os("SURREAL_TREE_PROBE").is_some() {
			eprintln!("TREE_PROBE {message}");
		} else {
			panic!("{message}");
		}
	}
}

impl ToSql for Guarded {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

impl PhysicalExpr for Guarded {
	fn name(&self) -> &'static str {
		self.0.name()
	}

	fn required_context(&self) -> ContextLevel {
		self.0.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		self.reached();
		self.0.evaluate(ctx)
	}

	fn access_mode(&self) -> AccessMode {
		self.0.access_mode()
	}

	fn is_projection_function(&self) -> bool {
		self.0.is_projection_function()
	}

	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		self.reached();
		self.0.evaluate_batch(ctx, values)
	}

	fn evaluate_projection<'a>(
		&'a self,
		ctx: EvalContext<'a>,
	) -> BoxFut<'a, FlowResult<Option<Vec<(crate::expr::Idiom, Value)>>>> {
		self.reached();
		self.0.evaluate_projection(ctx)
	}

	fn expr_children(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		self.0.expr_children()
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		self.0.embedded_operators()
	}

	fn try_literal(&self) -> Option<&Value> {
		self.0.try_literal()
	}

	fn try_simple_field(&self) -> Option<&str> {
		self.0.try_simple_field()
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self.0.as_any()
	}
}
