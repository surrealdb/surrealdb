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
pub(crate) mod change_feed_include;
pub(crate) mod changefeed;
pub(crate) mod cond;
pub(crate) mod constant;
pub(crate) mod data;
pub(crate) mod dir;
pub(crate) mod escape;
pub(crate) mod explain;
pub(crate) mod expression;
pub(crate) mod fetch;
pub(crate) mod field;
pub(crate) mod filter;
pub(crate) mod fmt;
pub(crate) mod function;
pub(crate) mod group;
pub(crate) mod ident;
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

pub mod statements;

pub use self::access_type::{AccessType, JwtAccess, RecordAccess};
pub use self::algorithm::Algorithm;
pub use self::base::Base;
pub use self::block::Block;
pub use self::bytesize::Bytesize;
pub use self::changefeed::ChangeFeed;
pub use self::cond::Cond;
pub use self::constant::Constant;
pub use self::data::Data;
pub use self::dir::Dir;
//pub use self::edges::Edges;
pub use self::explain::Explain;
pub use self::expression::Expr;
pub use self::fetch::{Fetch, Fetchs};
pub use self::field::{Field, Fields};
pub use self::filter::Filter;
pub use self::function::{Function, FunctionCall};
pub use self::group::{Group, Groups};
pub use self::ident::Ident;
pub use self::idiom::{Idiom, Idioms};
pub use self::kind::{GeometryKind, Kind, KindLiteral};
pub use self::limit::Limit;
pub use self::literal::Literal;
pub use self::lookup::Lookup;
pub use self::mock::Mock;
pub use self::model::{Model, get_model_path};
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
pub use self::statements::{
	AccessGrant, AccessStatement, AlterStatement, AlterTableStatement, AnalyzeStatement,
	CreateStatement, DefineAccessStatement, DefineAnalyzerStatement, DefineApiStatement,
	DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
	DefineIndexStatement, DefineModelStatement, DefineNamespaceStatement, DefineParamStatement,
	DefineStatement, DefineTableStatement, DefineUserStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, KillStatement, LiveStatement, OptionStatement,
	OutputStatement, RebuildStatement, RelateStatement, RemoveAccessStatement,
	RemoveAnalyzerStatement, RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveIndexStatement, RemoveModelStatement, RemoveNamespaceStatement,
	RemoveParamStatement, RemoveStatement, RemoveTableStatement, RemoveUserStatement,
	SelectStatement, SetStatement, ShowStatement, SleepStatement, UpdateStatement, UpsertStatement,
	UseStatement,
};
pub use self::timeout::Timeout;
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

impl From<anyhow::Error> for ControlFlow {
	fn from(error: anyhow::Error) -> Self {
		ControlFlow::Err(error)
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
