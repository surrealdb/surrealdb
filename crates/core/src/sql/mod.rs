//! The full type definitions for the SurrealQL query language

pub(crate) mod access;
pub(crate) mod access_type;
pub(crate) mod algorithm;
pub(crate) mod ast;
pub(crate) mod base;
pub(crate) mod block;
pub(crate) mod changefeed;
pub(crate) mod closure;
pub(crate) mod cond;
pub(crate) mod constant;
pub(crate) mod data;
pub(crate) mod dir;
pub(crate) mod escape;
pub(crate) mod explain;
pub(crate) mod expression;
pub(crate) mod fetch;
pub(crate) mod field;
pub(crate) mod file;
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
pub(crate) mod operator;
pub(crate) mod order;
pub(crate) mod output;
pub(crate) mod param;
pub(crate) mod part;
pub(crate) mod permission;
pub(crate) mod record_id;
pub(crate) mod reference;
pub(crate) mod scoring;
pub(crate) mod script;
pub(crate) mod split;
pub(crate) mod start;
pub(crate) mod table_type;
pub(crate) mod timeout;
pub(crate) mod tokenizer;
pub(crate) mod user;
pub(crate) mod view;
pub(crate) mod with;

pub mod index;
pub mod statements;

#[cfg(feature = "arbitrary")]
pub(crate) mod arbitrary;

use std::fmt::Display;

pub use self::access::{Access, Accesses};
pub use self::access_type::{AccessType, JwtAccess, RecordAccess};
pub use self::algorithm::Algorithm;
pub use self::ast::{Ast, TopLevelExpr};
pub use self::base::Base;
pub use self::block::Block;
pub use self::changefeed::ChangeFeed;
pub use self::closure::Closure;
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
pub use self::index::Index;
pub use self::kind::{Kind, KindLiteral};
pub use self::limit::Limit;
pub use self::literal::Literal;
pub use self::lookup::Lookup;
pub use self::mock::Mock;
pub use self::model::Model;
pub use self::operator::{AssignOperator, BinaryOperator, PostfixOperator, PrefixOperator};
pub use self::order::Order;
pub use self::output::Output;
pub use self::param::Param;
pub use self::part::Part;
pub use self::permission::{Permission, Permissions};
pub use self::record_id::{RecordIdKeyGen, RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit};
pub use self::scoring::Scoring;
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
pub use self::table_type::{Relation, TableType};
pub use self::timeout::Timeout;
pub use self::tokenizer::Tokenizer;
pub use self::view::View;
pub use self::with::With;

/// Trait for types that can be converted to SQL representation
pub trait ToSql {
	fn to_sql(&self) -> String;
}

impl<T> ToSql for T
where
	T: Display,
{
	fn to_sql(&self) -> String {
		self.to_string()
	}
}
