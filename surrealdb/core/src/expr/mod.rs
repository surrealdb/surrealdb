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
pub(crate) mod timeout;
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
pub(crate) use self::expression::Expr;
pub(crate) use self::fetch::{Fetch, Fetchs};
pub(crate) use self::field::{Field, Fields};
pub(crate) use self::filter::Filter;
pub(crate) use self::function::{Function, FunctionCall};
pub(crate) use self::group::{Group, Groups};
pub(crate) use self::idiom::{Idiom, Idioms};
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
pub(crate) use self::timeout::Timeout;
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

impl From<anyhow::Error> for ControlFlow {
	fn from(error: anyhow::Error) -> Self {
		ControlFlow::Err(error)
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
}
