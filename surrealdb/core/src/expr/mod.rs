//! The type definitions for the computation format of the surreaql executor.

use anyhow::Result;

use crate::err::Error;
use crate::val::Value;

pub(crate) mod access;
pub(crate) mod access_type;
pub(crate) mod algorithm;
pub(crate) mod base;
pub(crate) mod block;
pub(crate) mod bytesize;
pub(crate) mod changefeed;
pub mod computed_deps;
pub(crate) mod cond;
pub(crate) mod constant;
pub(crate) mod data;
pub(crate) mod dir;
pub(crate) mod explain;
pub(crate) mod expression;
pub(crate) mod fetch;
pub(crate) mod field;
pub(crate) mod filter;
pub(crate) mod function;
pub(crate) mod group;
pub(crate) mod idiom;
pub(crate) mod kind;
pub(crate) mod language;
pub(crate) mod limit;
pub(crate) mod literal;
pub(crate) mod lookup;
pub(crate) mod mock;
pub(crate) mod model;
pub(crate) mod operation;
pub(crate) mod operator;
pub(crate) mod order;
pub(crate) mod output;
pub(crate) mod param;
pub(crate) mod parameterize;
pub(crate) mod part;
pub(crate) mod paths;
pub(crate) mod plan;
pub(crate) mod record_id;
pub(crate) mod reference;
pub(crate) mod script;
pub(crate) mod split;
pub(crate) mod start;
pub(crate) mod tokenizer;
pub(crate) mod user;
pub(crate) mod view;
pub(crate) mod with;

pub(crate) mod decimal;
pub(crate) mod module;

mod closure;
pub(crate) mod statements;
pub mod visit;

pub(crate) use self::access_type::{AccessType, JwtAccess, RecordAccess};
pub(crate) use self::algorithm::Algorithm;
pub(crate) use self::base::Base;
pub(crate) use self::block::Block;
pub(crate) use self::bytesize::Bytesize;
pub(crate) use self::changefeed::ChangeFeed;
pub(crate) use self::closure::ClosureExpr;
pub(crate) use self::cond::Cond;
pub(crate) use self::constant::Constant;
pub(crate) use self::data::Data;
pub(crate) use self::dir::Dir;
pub(crate) use self::explain::Explain;
pub(crate) use self::expression::{ExplainFormat, Expr};
pub(crate) use self::fetch::{Fetch, Fetchs};
pub(crate) use self::field::{Field, Fields};
pub(crate) use self::filter::Filter;
pub(crate) use self::function::{Function, FunctionCall};
pub(crate) use self::group::{Group, Groups};
pub(crate) use self::idiom::Idiom;
pub(crate) use self::kind::{Kind, KindLiteral};
pub(crate) use self::limit::Limit;
pub(crate) use self::literal::{Literal, ObjectEntry};
pub(crate) use self::lookup::Lookup;
pub(crate) use self::mock::Mock;
pub(crate) use self::model::Model;
pub(crate) use self::module::{ModuleExecutable, SiloExecutable, SurrealismExecutable};
pub(crate) use self::operation::Operation;
pub(crate) use self::operator::{AssignOperator, BinaryOperator, PostfixOperator, PrefixOperator};
pub(crate) use self::order::Order;
pub(crate) use self::output::Output;
pub(crate) use self::param::Param;
pub(crate) use self::part::Part;
pub(crate) use self::plan::{LogicalPlan, TopLevelExpr};
pub(crate) use self::record_id::{
	RecordIdKeyGen, RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit,
};
pub(crate) use self::script::Script;
pub(crate) use self::split::{Split, Splits};
pub(crate) use self::start::Start;
pub(crate) use self::statements::{DefineAnalyzerStatement, SelectStatement, SleepStatement};
pub(crate) use self::tokenizer::Tokenizer;
pub(crate) use self::view::View;
pub(crate) use self::with::With;

/// Result of functions which can impact the controlflow of query execution.
pub(crate) type FlowResult<T> = Result<T, ControlFlow>;

/// An enum carrying control flow information.
///
/// Returned by compute functions which can impact control flow.
#[derive(Debug)]
pub(crate) enum ControlFlow {
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
pub(crate) trait FlowResultExt {
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
pub(crate) trait ControlFlowExt<T> {
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
