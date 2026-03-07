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
pub(crate) mod explain;
pub(crate) mod expression;
pub(crate) mod fetch;
pub(crate) mod field;
pub(crate) mod file;
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
pub(crate) mod module;
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
#[cfg(test)]
mod test_to_sql;
pub(crate) mod tokenizer;
pub(crate) mod user;
pub(crate) mod view;
pub(crate) mod with;

pub mod index;
pub mod statements;

#[cfg(feature = "arbitrary")]
pub(crate) mod arbitrary;

pub(crate) use self::access_type::AccessType;
pub(crate) use self::algorithm::Algorithm;
#[cfg(not(feature = "arbitrary"))]
pub(crate) use self::ast::Ast;
#[cfg(feature = "arbitrary")]
pub use self::ast::Ast;
pub(crate) use self::ast::{ExplainFormat, TopLevelExpr};
pub(crate) use self::base::Base;
pub(crate) use self::block::Block;
pub(crate) use self::changefeed::ChangeFeed;
pub(crate) use self::closure::Closure;
pub(crate) use self::cond::Cond;
pub(crate) use self::constant::Constant;
pub(crate) use self::data::Data;
pub(crate) use self::dir::Dir;
pub(crate) use self::explain::Explain;
pub(crate) use self::expression::Expr;
pub(crate) use self::fetch::{Fetch, Fetchs};
pub(crate) use self::field::{Field, Fields};
pub(crate) use self::function::{Function, FunctionCall};
pub(crate) use self::group::{Group, Groups};
pub(crate) use self::idiom::Idiom;
pub(crate) use self::index::Index;
pub(crate) use self::kind::Kind;
pub(crate) use self::limit::Limit;
pub(crate) use self::literal::Literal;
pub(crate) use self::lookup::Lookup;
pub(crate) use self::mock::Mock;
pub(crate) use self::model::Model;
#[cfg_attr(not(feature = "surrealism"), allow(unused_imports))]
pub(crate) use self::module::{ModuleExecutable, ModuleName, SiloExecutable, SurrealismExecutable};
pub(crate) use self::operator::{AssignOperator, BinaryOperator, PostfixOperator, PrefixOperator};
pub(crate) use self::order::Order;
pub(crate) use self::output::Output;
pub(crate) use self::param::Param;
pub(crate) use self::part::Part;
pub(crate) use self::permission::{Permission, Permissions};
pub(crate) use self::record_id::{
	RecordIdKeyGen, RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit,
};
pub(crate) use self::scoring::Scoring;
pub(crate) use self::script::Script;
pub(crate) use self::split::{Split, Splits};
pub(crate) use self::start::Start;
pub(crate) use self::statements::{
	CreateStatement, DefineAgentStatement, DefineEventStatement, DefineFieldStatement,
	DefineFunctionStatement, DefineIndexStatement, DefineModelStatement, DefineModuleStatement,
	DeleteStatement, InsertStatement, KillStatement, LiveStatement, RelateStatement,
	SelectStatement, UpdateStatement, UpsertStatement,
};
pub(crate) use self::table_type::TableType;
pub(crate) use self::view::View;
pub(crate) use self::with::With;
