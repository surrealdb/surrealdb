use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::ToSql;

use crate::dbs::Capabilities;
use crate::exec::context::SessionInfo;
use crate::exec::{AccessMode, ContextLevel, ExecOperator, ExecutionContext, SendSyncRequirement};
use crate::expr::FlowResult;
use crate::expr::idiom::Idiom;
use crate::kvs::Transaction;
use crate::val::Value;

mod block;
mod collections;
mod conditional;
mod control_flow;
pub(crate) mod function;
mod idiom;
mod literal;
mod matches;
mod ops;
pub(crate) mod record_id;
mod subquery;

// Re-export all expression types for external use
pub(crate) use block::BlockPhysicalExpr;
pub(crate) use collections::{ArrayLiteral, ObjectLiteral, SetLiteral};
pub(crate) use conditional::IfElseExpr;
pub(crate) use control_flow::{ControlFlowExpr, ControlFlowKind};
pub(crate) use function::{
	BuiltinFunctionExec, ClosureCallExec, ClosureExec, JsFunctionExec, ModelFunctionExec,
	ProjectionFunctionExec, SiloModuleExec, SurrealismModuleExec, UserDefinedFunctionExec,
};
pub(crate) use idiom::IdiomExpr;
pub(crate) use literal::{Literal, MockExpr, Param};
pub(crate) use matches::MatchesOp;
pub(crate) use ops::{BinaryOp, PostfixOp, UnaryOp};
pub(crate) use record_id::RecordIdExpr;
pub(crate) use subquery::ScalarSubquery;

/// Context for recursive tree-building via RepeatRecurse (@).
///
/// When a recursion path contains RepeatRecurse markers (e.g., in destructure
/// patterns like `{name, children: ->edge->table.@}`), this context is set by
/// the recursion evaluator and read by the RepeatRecurse handler to re-invoke
/// the recursion from the current value.
#[derive(Clone, Copy)]
pub struct RecursionCtx<'a> {
	/// The recursion's inner path (containing Destructure with RepeatRecurse)
	pub path: &'a [Arc<dyn PhysicalExpr>],
	/// Maximum recursion depth (None = system limit)
	pub max_depth: Option<u32>,
	/// Minimum recursion depth for path elimination.
	/// When a RepeatRecurse produces only dead-end values at a depth
	/// below this threshold, the entire sub-tree is eliminated (returns
	/// `Value::None` so that the parent's `clean_iteration` can filter it).
	pub min_depth: u32,
	/// Current recursion depth (incremented at each RepeatRecurse call)
	pub depth: u32,
}

/// Evaluation context - what's available during expression evaluation.
///
/// This is a borrowed view into the execution context for expression evaluation.
/// It provides access to parameters, namespace/database names, and the current row
/// (for per-row expressions like filters and projections).
#[derive(Clone)]
pub struct EvalContext<'a> {
	pub exec_ctx: &'a ExecutionContext,

	/// Current row for per-row expressions (projections, filters).
	/// None when evaluating in "scalar context" (USE, LIMIT, TIMEOUT, etc.)
	pub current_value: Option<&'a Value>,

	/// Block-local parameters (LET bindings within current block scope).
	/// These shadow global parameters with the same name.
	pub local_params: Option<&'a HashMap<String, Value>>,

	/// Active recursion context for RepeatRecurse evaluation.
	/// Set by evaluate_recurse_* when the inner path contains .@ markers.
	pub recursion_ctx: Option<RecursionCtx<'a>>,

	/// Original document root for the current row.  Needed by IndexPart to
	/// evaluate dynamic key expressions (`[field]`, `[$param]`) against the
	/// document rather than the chain's current position.
	pub document_root: Option<&'a Value>,
}

impl<'a> EvalContext<'a> {
	/// Convert from ExecutionContext enum for expression evaluation.
	///
	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		Self {
			exec_ctx,
			current_value: None,
			local_params: None,
			recursion_ctx: None,
			document_root: None,
		}
	}

	/// For per-row evaluation (projections, filters)
	pub fn with_value(&self, value: &'a Value) -> Self {
		Self {
			current_value: Some(value),
			..*self
		}
	}

	/// For per-row evaluation that also sets the document root
	/// (used at the top level of idiom evaluation to provide the
	/// original document for dynamic index expressions).
	pub fn with_value_and_doc(&self, value: &'a Value) -> Self {
		Self {
			current_value: Some(value),
			document_root: Some(value),
			..*self
		}
	}

	/// Set the recursion context for RepeatRecurse evaluation.
	pub fn with_recursion_ctx(&self, ctx: RecursionCtx<'a>) -> Self {
		Self {
			recursion_ctx: Some(ctx),
			..*self
		}
	}

	// =========================================================================
	// Session accessors
	// =========================================================================

	/// Get the session information (if available).
	pub fn session(&self) -> Option<&SessionInfo> {
		self.exec_ctx.session()
	}

	// =========================================================================
	// Context accessors (shortcuts)
	// =========================================================================

	/// Get the transaction (delegates to FrozenContext).
	pub fn txn(&self) -> Arc<Transaction> {
		self.exec_ctx.txn()
	}

	/// Get the capabilities as an Arc.
	pub fn capabilities(&self) -> Arc<Capabilities> {
		self.exec_ctx.capabilities()
	}

	// =========================================================================
	// Capability checks
	// =========================================================================

	/// Check if a network target is allowed.
	///
	/// Returns an error if the URL is not allowed by the capabilities.
	#[cfg(feature = "http")]
	pub async fn check_allowed_net(&self, url: &url::Url) -> anyhow::Result<()> {
		use crate::dbs::capabilities::NetTarget;
		use crate::err::Error;

		let capabilities = self.capabilities();

		// Check if the URL host is allowed
		let host = url.host().ok_or_else(|| Error::InvalidUrl(url.to_string()))?;

		let target = NetTarget::Host(host.to_owned(), url.port_or_known_default());

		// Check the domain name (if any) matches the allow list
		if !capabilities.matches_any_allow_net(&target) {
			return Err(Error::NetTargetNotAllowed(target.to_string()).into());
		}

		// Check against the deny list by hostname
		if capabilities.matches_any_deny_net(&target) {
			return Err(Error::NetTargetNotAllowed(target.to_string()).into());
		}

		// Resolve the domain name to IP addresses and check each against the deny list
		#[cfg(not(target_family = "wasm"))]
		let resolved = target.resolve().await?;
		#[cfg(target_family = "wasm")]
		let resolved = target.resolve()?;

		for t in &resolved {
			if capabilities.matches_any_deny_net(t) {
				return Err(Error::NetTargetNotAllowed(t.to_string()).into());
			}
		}

		Ok(())
	}

	/// Check if a function is allowed by capabilities.
	///
	/// Returns an error if the function is not allowed.
	pub fn check_allowed_function(&self, name: &str) -> anyhow::Result<()> {
		use crate::err::Error;

		if !self.capabilities().allows_function_name(name) {
			return Err(Error::FunctionNotAllowed(name.to_string()).into());
		}
		Ok(())
	}

	/// Get the capabilities as an Arc (cloned from FrozenContext).
	pub fn get_capabilities(&self) -> Arc<Capabilities> {
		self.exec_ctx.ctx().get_capabilities()
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait PhysicalExpr: ToSql + SendSyncRequirement + Debug {
	fn name(&self) -> &'static str;

	/// The minimum context level required to evaluate this expression.
	///
	/// Used for pre-flight validation: the executor checks that the current session
	/// has at least this context level before calling `evaluate()`.
	fn required_context(&self) -> ContextLevel;

	/// Evaluate this expression to a value.
	///
	/// Returns `FlowResult<Value>` to support control flow signals (BREAK, CONTINUE,
	/// RETURN) propagating through the physical expression layer. Regular errors
	/// are wrapped in `ControlFlow::Err`. The `?` operator works on `anyhow::Result`
	/// via the existing `From<anyhow::Error> for ControlFlow` impl.
	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value>;

	/// Does this expression reference the current row?
	/// If false, can be evaluated in scalar context.
	fn references_current_value(&self) -> bool;

	/// Returns the access mode for this expression.
	///
	/// This is critical for plan-based mutability analysis:
	/// - If an expression contains a mutation subquery, it must return `ReadWrite`
	/// - Example: `(UPSERT person)` in a SELECT must propagate `ReadWrite` upward
	fn access_mode(&self) -> AccessMode;

	/// Whether this is a projection function expression.
	///
	/// Projection functions (like `type::field` and `type::fields`) produce field
	/// bindings rather than single values, affecting how projections build output objects.
	fn is_projection_function(&self) -> bool {
		false
	}

	/// Evaluate this expression against a batch of row values.
	///
	/// `ctx` should be the base context (without `current_value` set).
	/// Each element of `values` becomes the `current_value` for one evaluation.
	///
	/// The default implementation evaluates sequentially. Override for expressions
	/// that can benefit from batching I/O (e.g., parallel record fetches, subqueries).
	///
	/// Control flow: uses `?` propagation, so the first ControlFlow signal
	/// (BREAK/CONTINUE/RETURN/Err) aborts the batch -- matching current operator behavior.
	async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		let mut results = Vec::with_capacity(values.len());
		for value in values {
			results.push(self.evaluate(ctx.with_value(value)).await?);
		}
		Ok(results)
	}

	/// Evaluate this expression as a projection function, returning field bindings.
	///
	/// Only meaningful for expressions where `is_projection_function()` returns true.
	/// Returns a list of (Idiom, Value) pairs that become fields in the output object.
	///
	/// The default implementation returns None, indicating this is not a projection function.
	async fn evaluate_projection(
		&self,
		ctx: EvalContext<'_>,
	) -> FlowResult<Option<Vec<(Idiom, Value)>>> {
		let _ = ctx; // silence unused warning
		Ok(None)
	}

	/// Returns references to child physical expressions for tree traversal.
	///
	/// Used by `EXPLAIN` / `EXPLAIN ANALYZE` to display expression trees
	/// beneath each operator. Each element is `(role, expr)` where `role`
	/// describes the relationship (e.g. "left", "right", "operand").
	#[allow(dead_code)]
	fn expr_children(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![]
	}

	/// Returns embedded operator sub-trees owned by this expression.
	///
	/// Some expressions (e.g. `ScalarSubquery`, `LookupPart`) wrap entire
	/// execution plan trees. This method exposes those trees so that
	/// `EXPLAIN` / `EXPLAIN ANALYZE` can recursively format them.
	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		vec![]
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{Array, Number, Object};

	// Note: Field access tests (test_evaluate_field_on_object, test_evaluate_field_on_array)
	// were removed because evaluate_field is async and requires an EvalContext.
	// This functionality is covered by language tests in tests/language/statements/select/*.surql

	// =========================================================================
	// Index Access Tests
	// =========================================================================

	#[test]
	fn test_evaluate_index_on_array() {
		use crate::exec::parts::index::evaluate_index;

		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(0))).unwrap();
		assert_eq!(result, Value::Number(Number::Int(1)));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(2))).unwrap();
		assert_eq!(result, Value::Number(Number::Int(3)));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(5))).unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn test_evaluate_index_on_object() {
		use crate::exec::parts::index::evaluate_index;

		let obj = Value::Object(Object::from_iter([(
			"key1".to_string(),
			Value::String("value1".to_string()),
		)]));

		let result = evaluate_index(&obj, &Value::String("key1".to_string())).unwrap();
		assert_eq!(result, Value::String("value1".to_string()));
	}

	// =========================================================================
	// Array Operation Tests
	// =========================================================================

	// Note: test_evaluate_all was removed because evaluate_all is now async
	// and requires an EvalContext for RecordId fetching. This functionality
	// is covered by language tests in tests/language/statements/select/*.surql

	#[test]
	fn test_evaluate_flatten() {
		use crate::exec::parts::array_ops::evaluate_flatten;

		let nested = Value::Array(Array::from(vec![
			Value::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
			])),
			Value::Array(Array::from(vec![Value::Number(Number::Int(3))])),
		]));

		let result = evaluate_flatten(&nested).unwrap();
		assert_eq!(
			result,
			Value::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
				Value::Number(Number::Int(3)),
			]))
		);
	}

	#[test]
	fn test_evaluate_first_and_last() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		// Test first element
		let first = match &arr {
			Value::Array(a) => a.first().cloned().unwrap_or(Value::None),
			_ => arr.clone(),
		};
		assert_eq!(first, Value::Number(Number::Int(1)));

		// Test last element
		let last = match &arr {
			Value::Array(a) => a.last().cloned().unwrap_or(Value::None),
			_ => arr.clone(),
		};
		assert_eq!(last, Value::Number(Number::Int(3)));

		// Empty array
		let empty = Value::Array(Array::from(Vec::<Value>::new()));
		let first_empty = match &empty {
			Value::Array(a) => a.first().cloned().unwrap_or(Value::None),
			_ => empty.clone(),
		};
		let last_empty = match &empty {
			Value::Array(a) => a.last().cloned().unwrap_or(Value::None),
			_ => empty.clone(),
		};
		assert_eq!(first_empty, Value::None);
		assert_eq!(last_empty, Value::None);
	}

	// =========================================================================
	// IdiomExpr Tests
	// =========================================================================

	#[test]
	fn test_idiom_expr_simple_identifier() {
		use crate::exec::parts::FieldPart;

		let parts: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(FieldPart {
			name: "test".to_string(),
		})];
		let expr = IdiomExpr::new("test".to_string(), None, parts);

		assert!(expr.is_simple_identifier());
	}

	// =========================================================================
	// Value Hash Tests
	// =========================================================================

	#[test]
	fn test_value_hash_consistency() {
		use crate::exec::parts::recurse::value_hash;

		let v1 = Value::Number(Number::Int(42));
		let v2 = Value::Number(Number::Int(42));
		let v3 = Value::Number(Number::Int(43));

		assert_eq!(value_hash(&v1), value_hash(&v2));
		assert_ne!(value_hash(&v1), value_hash(&v3));
	}
}
