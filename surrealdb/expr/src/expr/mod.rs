//! The type definitions for the computation format of the surreaql executor.

use anyhow::Result;

use crate::val::Value;

pub mod access;
pub mod access_type;
pub mod algorithm;
pub mod base;
pub mod block;
pub mod bytesize;
pub mod changefeed;
pub mod computed_deps;
pub mod cond;
pub mod constant;
pub mod convert;
pub mod data;
pub mod dir;
pub mod error;
pub mod explain;
pub mod expression;
pub mod fetch;
pub mod field;
pub mod filter;
pub mod function;
pub mod graphql_config;
pub mod group;
pub mod idiom;
pub mod index_kind;
pub mod kind;
pub mod language;
pub mod limit;
pub mod literal;
pub mod lookup;
#[cfg(feature = "gql")]
pub mod match_plan;
pub mod mock;
pub mod model;
pub mod mutation;
pub mod operation;
pub mod operator;
pub mod order;
pub mod output;
pub mod param;
pub mod part;
pub mod paths;
pub mod permission;
pub mod plan;
pub mod record_id;
pub mod reference;
pub mod script;
pub mod split;
pub mod start;
pub mod table_type;
pub mod tokenizer;
pub mod user;
pub mod variables;
pub mod view;
pub mod with;

pub mod decimal;
pub mod module;

pub mod closure;
pub mod statements;
pub mod visit;

pub use self::access_type::{AccessType, JwtAccess, RecordAccess};
pub use self::algorithm::Algorithm;
pub use self::base::Base;
pub use self::block::Block;
pub use self::bytesize::Bytesize;
pub use self::changefeed::ChangeFeed;
pub use self::closure::ClosureExpr;
pub use self::cond::Cond;
pub use self::constant::Constant;
pub use self::data::Data;
pub use self::dir::Dir;
pub use self::error::Error;
pub use self::explain::Explain;
pub use self::expression::{ExplainFormat, Expr};
pub use self::fetch::{Fetch, Fetchs};
pub use self::field::{Field, Fields};
pub use self::filter::Filter;
pub use self::function::{Function, FunctionCall};
pub use self::group::{Group, Groups};
pub use self::idiom::Idiom;
pub use self::kind::{Kind, KindLiteral};
pub use self::limit::Limit;
pub use self::literal::{Literal, ObjectEntry};
pub use self::lookup::Lookup;
// Re-exported for the GQL lowering and the streaming planner (PR-A landing
// piecemeal); not yet referenced through this alias in this crate.
#[cfg(feature = "gql")]
#[allow(unused_imports)]
pub use self::match_plan::MatchPlan;
pub use self::mock::Mock;
pub use self::model::Model;
pub use self::module::{ModuleExecutable, SiloExecutable, SurrealismExecutable};
pub use self::operation::Operation;
pub use self::operator::{AssignOperator, BinaryOperator, PostfixOperator, PrefixOperator};
pub use self::order::Order;
pub use self::output::Output;
pub use self::param::Param;
pub use self::part::Part;
pub use self::plan::{LogicalPlan, TopLevelExpr};
pub use self::record_id::{RecordIdKeyGen, RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit};
pub use self::script::Script;
pub use self::split::{Split, Splits};
pub use self::start::Start;
pub use self::statements::{DefineAnalyzerStatement, SelectStatement, SleepStatement};
pub use self::tokenizer::Tokenizer;
pub use self::view::View;
pub use self::with::With;

/// Result of functions which can impact the controlflow of query execution.
pub type FlowResult<T> = Result<T, ControlFlow>;

/// An enum carrying control flow information.
///
/// Returned by compute functions which can impact control flow.
#[derive(Debug)]
pub enum ControlFlow {
	Break,
	Continue,
	Return(Value),
	Err(anyhow::Error),
}

impl std::fmt::Display for ControlFlow {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ControlFlow::Break => write!(f, "BREAK"),
			ControlFlow::Continue => write!(f, "CONTINUE"),
			ControlFlow::Return(v) => write!(f, "RETURN {:?}", v),
			ControlFlow::Err(e) => write!(f, "{}", e),
		}
	}
}

impl From<anyhow::Error> for ControlFlow {
	fn from(error: anyhow::Error) -> Self {
		ControlFlow::Err(error)
	}
}

impl From<Error> for ControlFlow {
	/// Boxes the value-algebra failure directly, so it stays the concrete type
	/// in the `anyhow` slot and [`ControlFlow::is_ignorable`] keeps matching it.
	fn from(error: Error) -> Self {
		ControlFlow::Err(anyhow::Error::new(error))
	}
}

impl ControlFlow {
	/// Returns true if this represents a data-shape error that can safely
	/// be treated as `Value::None` (e.g., type mismatches, coercion failures).
	///
	/// Returns false for system errors (storage, timeout, permissions),
	/// control flow signals (Break/Continue), and Return values.
	pub fn is_ignorable(&self) -> bool {
		match self {
			// Control flow signals are never ignorable
			ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => false,
			ControlFlow::Err(e) => match e.downcast_ref::<Error>() {
				Some(err) => err.is_ignorable(),
				// Unknown error types are not ignorable
				None => false,
			},
		}
	}
}

/// Extension trait for wrapping errors with context and converting them into
/// [`ControlFlow`].
///
/// Modelled on [`anyhow::Context`], this provides `.context()` and
/// `.with_context()` methods that map any compatible error into
/// `ControlFlow::Err` with the given message.
///
/// # Implemented for
///
/// * `Result<T, E>` where `E: Into<anyhow::Error>` – wraps the error with context and converts it
///   to `ControlFlow::Err`.
/// * `Option<T>` – produces `ControlFlow::Err` when `None`.
///
/// # Examples
///
/// ```ignore
/// use crate::expr::ControlFlowExt;
///
/// // Convert a Result<T, E> into FlowResult<T> with context:
/// let key = encode_key(data).context("Failed to encode scan key")?;
///
/// // Unwrap an Option<T> into FlowResult<T> with context:
/// let table = opt_table.context("Referencing table is required")?;
/// ```
pub trait ControlFlowExt<T> {
	/// Wrap the error value with additional context, converting it into a
	/// `ControlFlow::Err`.
	fn context<C: std::fmt::Display + Send + Sync + 'static>(
		self,
		context: C,
	) -> std::result::Result<T, ControlFlow>;

	/// Wrap the error value with lazily-evaluated context, converting it into
	/// a `ControlFlow::Err`.
	#[allow(dead_code)] // Part of the public API; matches anyhow::Context.
	fn with_context<C, F>(self, f: F) -> std::result::Result<T, ControlFlow>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C;
}

impl<T, E: Into<anyhow::Error>> ControlFlowExt<T> for std::result::Result<T, E> {
	fn context<C: std::fmt::Display + Send + Sync + 'static>(
		self,
		context: C,
	) -> std::result::Result<T, ControlFlow> {
		self.map_err(|e| ControlFlow::Err(e.into().context(context)))
	}

	fn with_context<C, F>(self, f: F) -> std::result::Result<T, ControlFlow>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C,
	{
		self.map_err(|e| ControlFlow::Err(e.into().context(f())))
	}
}

impl<T> ControlFlowExt<T> for Option<T> {
	fn context<C: std::fmt::Display + Send + Sync + 'static>(
		self,
		context: C,
	) -> std::result::Result<T, ControlFlow> {
		self.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("{}", context)))
	}

	fn with_context<C, F>(self, f: F) -> std::result::Result<T, ControlFlow>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C,
	{
		self.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("{}", f())))
	}
}
