//! Record functions

use anyhow::Result;

use crate::define_pure_function;
use crate::exec::EvalContext;
use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::Value;

define_pure_function!(RecordId, "record::id", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (record: Any) -> Any, crate::fnc::record::id);
define_pure_function!(RecordTb, "record::tb", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (record: Any) -> String, crate::fnc::record::tb);
define_pure_function!(RecordTable, "record::table", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (record: Any) -> String, crate::fnc::record::tb);

// =========================================================================
// record::exists - Check if a record exists in the database
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct RecordExists;

impl ScalarFunction for RecordExists {
	fn name(&self) -> &'static str {
		"record::exists"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Scalar,
			"record::exists",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
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
			if let crate::val::RecordIdKey::Object(key) = &rid.key
				&& let Some(id) = key.get("id")
			{
				return Ok(Value::Bool(!id.is_none()));
			}
			if ctx.exec_ctx.options().is_none() {
				return Ok(Value::None);
			}
			// A record exists for the caller when a read of it by id finds
			// one under the caller's permissions, so a record the caller may
			// not select does not exist for it. No computed field is derived.
			let exec_ctx = ctx.exec_ctx.dive()?;
			let found = crate::exec::operators::fetch::record_exists(
				&exec_ctx,
				&rid,
				!ctx.skip_fetch_perms,
			)
			.await
			.map(Value::Bool)
			.catch_return();
			match found {
				Ok(found) => Ok(found),
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

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Scalar,
			"record::is_edge",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
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

/// The `record::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] =
	&[&RecordId, &RecordTb, &RecordTable, &RecordExists, &RecordIsEdge];

#[cfg(test)]
mod tests {
	use super::{RecordExists, ScalarFunction};
	use crate::exec::EvalContext;
	use crate::exec::operators::test_util::TestDb;
	use crate::val::Value;

	#[tokio::test]
	async fn record_exists_answers_from_object_key_id_component() {
		let db = TestDb::new("").await;
		let exec = db.exec_ctx().await;
		let eval = EvalContext::from_exec_ctx(&exec);

		let present = Value::from(crate::syn::value("tb:{ id: 1 }").expect("parses"));
		let none_id = Value::from(crate::syn::value("tb:{ id: NONE }").expect("parses"));
		let missing_tb = Value::from(crate::syn::value("nowhere:{ id: 123 }").expect("parses"));

		assert_eq!(
			RecordExists.invoke_async(&eval, vec![present]).await.expect("runs"),
			Value::Bool(true)
		);
		assert_eq!(
			RecordExists.invoke_async(&eval, vec![none_id]).await.expect("runs"),
			Value::Bool(false)
		);
		assert_eq!(
			RecordExists.invoke_async(&eval, vec![missing_tb]).await.expect("runs"),
			Value::Bool(true)
		);
		exec.txn().cancel().await.expect("cancel");
	}
}
