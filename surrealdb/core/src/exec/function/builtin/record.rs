//! Record functions

use anyhow::Result;

use crate::exec::EvalContext;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::Value;
use crate::{define_pure_function, register_functions};

define_pure_function!(RecordId, "record::id", effects: EffectSet::PURE, (record: Any) -> Any, crate::fnc::record::id);
define_pure_function!(RecordTb, "record::tb", effects: EffectSet::PURE, (record: Any) -> String, crate::fnc::record::tb);
define_pure_function!(RecordTable, "record::table", effects: EffectSet::PURE, (record: Any) -> String, crate::fnc::record::tb);

// =========================================================================
// record::exists - Check if a record exists in the database
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct RecordExists;

impl ScalarFunction for RecordExists {
	fn name(&self) -> &'static str {
		"record::exists"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::READS_DATA
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("record", Kind::Any).returns(Kind::Bool)
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
			use crate::exe::FlowResultExt as _;
			let (rid,): (crate::val::RecordId,) = FromArgs::from_args("record::exists", args)?;
			if ctx.exec_ctx.options().is_none() {
				return Ok(Value::None);
			}
			// A record exists for the caller when reading its `id` finds one:
			// the read a path step makes, under the caller's permissions, so
			// a record the caller may not select does not exist for it.
			let id = crate::exec::vm::access::field::evaluate_field(
				&Value::RecordId(rid),
				"id",
				ctx.clone(),
			)
			.await
			.catch_return();
			match id {
				Ok(id) => Ok(Value::Bool(!id.is_none())),
				// An undefined table means the record cannot exist.
				Err(e)
					if matches!(
						e.downcast_ref(),
						Some(crate::catalog::Error::TbNotFound { .. })
					) =>
				{
					Ok(Value::Bool(false))
				}
				Err(e) => Err(e),
			}
		})
	}
}

// =========================================================================
// record::is_edge - Check if a record is an edge
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct RecordIsEdge;

impl ScalarFunction for RecordIsEdge {
	fn name(&self) -> &'static str {
		"record::is_edge"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::READS_DATA
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("record", Kind::Any).returns(Kind::Bool)
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
			use crate::doc::CursorDoc;
			let args = FromArgs::from_args("record::is_edge", args)?;
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options();
			let doc = ctx.document().within_context().map(|v| CursorDoc::new(None, v.clone()));
			crate::fnc::record::is::edge((frozen, opt, doc.as_ref()), args).await
		})
	}
}

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, RecordId, RecordTb, RecordTable);
	registry.register(RecordExists);
	registry.register(RecordIsEdge);
}
