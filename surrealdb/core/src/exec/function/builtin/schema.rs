//! Schema functions

use anyhow::Result;

use crate::exec::EvalContext;
use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::Value;

// =========================================================================
// schema::table::exists - Check if a table exists
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct SchemaTableExists;

impl ScalarFunction for SchemaTableExists {
	fn name(&self) -> &'static str {
		"schema::table::exists"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Scalar, "schema::table::exists", Fallibility::DATA)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::READS_CATALOG
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("table", Kind::String).returns(Kind::Bool)
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let args = FromArgs::from_args("schema::table::exists", args)?;
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options();
			crate::fnc::schema::table::exists((frozen, opt), args).await
		})
	}
}

/// The `schema::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&SchemaTableExists];
