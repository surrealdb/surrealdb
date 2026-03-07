//! The type definitions for the computation format of the surreaql executor.

use anyhow::Result;

use crate::err::Error;
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
pub mod data;
pub mod dir;
pub mod explain;
pub mod expression;
pub mod fetch;
pub mod field;
pub mod filter;
pub mod function;
pub mod group;
pub mod idiom;
pub mod join;
pub mod kind;
pub mod language;
pub mod limit;
pub mod literal;
pub mod lookup;
pub mod mock;
pub mod model;
pub mod operation;
pub mod operator;
pub mod order;
pub mod output;
pub mod param;
pub mod parameterize;
pub mod part;
pub mod paths;
pub mod plan;
pub mod record_id;
pub mod reference;
pub mod script;
pub mod split;
pub mod start;
pub mod tokenizer;
pub mod user;
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
pub use self::explain::Explain;
pub use self::expression::{ExplainFormat, Expr};
pub use self::fetch::{Fetch, Fetchs};
pub use self::field::{Field, Fields};
pub use self::filter::Filter;
pub use self::function::{Function, FunctionCall};
pub use self::group::{Group, Groups};
pub use self::idiom::Idiom;
pub use self::join::{JoinExpr, JoinKind};
pub use self::kind::{Kind, KindLiteral};
pub use self::limit::Limit;
pub use self::literal::{Literal, ObjectEntry};
pub use self::lookup::Lookup;
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

impl From<crate::err::Error> for ControlFlow {
	fn from(error: crate::err::Error) -> Self {
		ControlFlow::Err(error.into())
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
			ControlFlow::Err(e) => {
				// Check if the inner error is a known ignorable type
				if let Some(err) = e.downcast_ref::<crate::err::Error>() {
					err.is_ignorable()
				} else {
					false // Unknown error types are not ignorable
				}
			}
		}
	}
}

/// Helper trait to catch controlflow return unwinding.
pub trait FlowResultExt {
	/// Function which catches `ControlFlow::Return(x)` and turns it into
	/// `Ok(x)`.
	///
	/// If the error value is either `ControlFlow::Break` or
	/// `ControlFlow::Continue` it will instead create an error that
	/// break/continue was used within an invalid location.
	fn catch_return(self) -> Result<Value, anyhow::Error>;

	/// Falls back to `Value::None` for ignorable errors (type mismatches, etc.),
	/// but propagates consequential errors (storage, timeout, permissions).
	fn or_none(self) -> FlowResult<Value>;
}

impl FlowResultExt for FlowResult<Value> {
	fn catch_return(self) -> Result<Value, anyhow::Error> {
		match self {
			Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
				Err(anyhow::Error::new(Error::InvalidControlFlow))
			}
			Err(ControlFlow::Return(x)) => Ok(x),
			Err(ControlFlow::Err(e)) => Err(e),
			Ok(x) => Ok(x),
		}
	}

	fn or_none(self) -> FlowResult<Value> {
		match self {
			Ok(v) => Ok(v),
			Err(cf) if cf.is_ignorable() => Ok(Value::None),
			Err(cf) => Err(cf),
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
