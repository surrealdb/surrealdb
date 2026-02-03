use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::ToSql;

use crate::dbs::Capabilities;
use crate::exec::context::{Parameters, SessionInfo};
use crate::exec::{AccessMode, ContextLevel, ExecutionContext};
use crate::expr::idiom::Idiom;
use crate::iam::Auth;
use crate::kvs::Transaction;
use crate::val::Value;

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
}

impl<'a> EvalContext<'a> {
	/// Convert from ExecutionContext enum for expression evaluation.
	///
	/// This extracts the appropriate fields based on the context level:
	/// - Root: params only, no ns/db/txn
	/// - Namespace: params, ns, txn
	/// - Database: params, ns, db, txn
	// pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
	// 	match exec_ctx {
	// 		ExecutionContext::Root(r) => Self::scalar(&r.params, None, None, None),
	// 		ExecutionContext::Namespace(n) => {
	// 			Self::scalar(&n.root.params, Some(&n.ns), None, Some(&n.txn))
	// 		}
	// 		ExecutionContext::Database(d) => Self::scalar(
	// 			&d.ns_ctx.root.params,
	// 			Some(&d.ns_ctx.ns),
	// 			Some(&d.db),
	// 			Some(&d.ns_ctx.txn),
	// 		),
	// 	}
	// }

	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		Self {
			exec_ctx,
			current_value: None,
			local_params: None,
		}
	}

	/// For per-row evaluation (projections, filters)
	pub fn with_value(&self, value: &'a Value) -> Self {
		Self {
			current_value: Some(value),
			..*self
		}
	}

	/// Create a new context with block-local parameters.
	///
	/// Local parameters shadow global parameters with the same name.
	pub fn with_local_params(&self, params: &'a HashMap<String, Value>) -> Self {
		Self {
			local_params: Some(params),
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

	/// Get the session namespace (if available).
	pub fn session_ns(&self) -> Option<&str> {
		self.session().and_then(|s| s.ns.as_deref())
	}

	/// Get the session database (if available).
	pub fn session_db(&self) -> Option<&str> {
		self.session().and_then(|s| s.db.as_deref())
	}

	// =========================================================================
	// Context accessors (shortcuts)
	// =========================================================================

	/// Get the transaction.
	pub fn txn(&self) -> &Arc<Transaction> {
		self.exec_ctx.txn()
	}

	/// Get the parameters.
	pub fn params(&self) -> &Parameters {
		self.exec_ctx.params()
	}

	/// Get the authentication context.
	pub fn auth(&self) -> &Auth {
		self.exec_ctx.auth()
	}

	/// Get the capabilities (if available).
	pub fn capabilities(&self) -> Option<&Capabilities> {
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
		use std::str::FromStr;

		use crate::dbs::capabilities::NetTarget;
		use crate::err::Error;

		let capabilities = match self.capabilities() {
			Some(cap) => cap,
			None => {
				// No capabilities means no restrictions
				return Ok(());
			}
		};

		// Check if the URL host is allowed
		let host = url.host_str().ok_or_else(|| Error::InvalidUrl(url.to_string()))?;

		let target = NetTarget::from_str(host)
			.map_err(|_| Error::InvalidUrl(format!("Invalid host: {}", host)))?;

		if !capabilities.matches_any_allow_net(&target)
			|| capabilities.matches_any_deny_net(&target)
		{
			return Err(Error::NetTargetNotAllowed(url.to_string()).into());
		}

		Ok(())
	}

	/// Get a clone of the capabilities as an Arc.
	///
	/// Returns a default capabilities if none are available.
	pub fn get_capabilities(&self) -> Arc<Capabilities> {
		self.exec_ctx
			.root()
			.capabilities
			.clone()
			.unwrap_or_else(|| Arc::new(Capabilities::default()))
	}
}

#[async_trait]
pub trait PhysicalExpr: ToSql + Send + Sync + Debug {
	fn name(&self) -> &'static str;

	/// The minimum context level required to evaluate this expression.
	///
	/// Used for pre-flight validation: the executor checks that the current session
	/// has at least this context level before calling `evaluate()`.
	fn required_context(&self) -> ContextLevel;

	/// Evaluate this expression to a value.
	///
	/// May execute subqueries internally, hence async.
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value>;

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

	/// Evaluate this expression as a projection function, returning field bindings.
	///
	/// Only meaningful for expressions where `is_projection_function()` returns true.
	/// Returns a list of (Idiom, Value) pairs that become fields in the output object.
	///
	/// The default implementation returns None, indicating this is not a projection function.
	async fn evaluate_projection(
		&self,
		ctx: EvalContext<'_>,
	) -> anyhow::Result<Option<Vec<(Idiom, Value)>>> {
		let _ = ctx; // silence unused warning
		Ok(None)
	}
}

// Submodules
mod block;
mod collections;
mod conditional;
mod function;
mod idiom;
mod literal;
mod lookup;
mod ops;
mod recurse;
mod subquery;

// Re-export all expression types for external use
pub(crate) use block::{BlockPhysicalExpr, BreakControlFlow, ContinueControlFlow, ReturnValue};
pub(crate) use collections::{ArrayLiteral, ObjectLiteral, SetLiteral};
pub(crate) use conditional::IfElseExpr;
pub(crate) use function::{
	BuiltinFunctionExec, ClosureExec, JsFunctionExec, ModelFunctionExec, ProjectionFunctionExec,
	SiloModuleExec, SurrealismModuleExec, UserDefinedFunctionExec,
};
pub(crate) use idiom::IdiomExpr;
pub(crate) use literal::{Literal, Param};
pub(crate) use ops::{BinaryOp, PostfixOp, UnaryOp};
pub(crate) use subquery::ScalarSubquery;

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
		use crate::exec::physical_expr::idiom::evaluate_index;

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
		use crate::exec::physical_expr::idiom::evaluate_index;

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
		use crate::exec::physical_expr::idiom::evaluate_flatten;

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
		use crate::exec::physical_expr::idiom::{evaluate_first, evaluate_last};

		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		let first = evaluate_first(&arr).unwrap();
		assert_eq!(first, Value::Number(Number::Int(1)));

		let last = evaluate_last(&arr).unwrap();
		assert_eq!(last, Value::Number(Number::Int(3)));

		// Empty array
		let empty = Value::Array(Array::from(Vec::<Value>::new()));
		assert_eq!(evaluate_first(&empty).unwrap(), Value::None);
		assert_eq!(evaluate_last(&empty).unwrap(), Value::None);
	}

	// =========================================================================
	// PhysicalPart Tests
	// =========================================================================

	#[test]
	fn test_physical_part_is_simple() {
		use crate::exec::physical_part::PhysicalPart;

		assert!(PhysicalPart::Field("test".to_string()).is_simple());
		assert!(PhysicalPart::All.is_simple());
		assert!(PhysicalPart::First.is_simple());
		assert!(PhysicalPart::Last.is_simple());
		assert!(PhysicalPart::Flatten.is_simple());
		assert!(PhysicalPart::Optional.is_simple());
	}

	// =========================================================================
	// IdiomExpr Tests
	// =========================================================================

	#[test]
	fn test_idiom_expr_is_simple() {
		use crate::exec::physical_part::PhysicalPart;
		use crate::expr::Idiom;
		use crate::expr::part::Part;

		let idiom = Idiom(vec![Part::Field("test".to_string())]);
		let parts = vec![PhysicalPart::Field("test".to_string())];
		let expr = IdiomExpr::new(idiom, parts);

		assert!(expr.is_simple());
	}

	// =========================================================================
	// Value Hash Tests
	// =========================================================================

	#[test]
	fn test_value_hash_consistency() {
		use crate::exec::physical_expr::recurse::value_hash;

		let v1 = Value::Number(Number::Int(42));
		let v2 = Value::Number(Number::Int(42));
		let v3 = Value::Number(Number::Int(43));

		assert_eq!(value_hash(&v1), value_hash(&v2));
		assert_ne!(value_hash(&v1), value_hash(&v3));
	}
}
