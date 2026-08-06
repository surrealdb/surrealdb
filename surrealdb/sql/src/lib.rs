//! The abstract syntax tree for SurrealQL.
//!
//! This is the shape a query has immediately after parsing, before any
//! lowering, name resolution, or planning. [`Ast`] is the whole of a parsed
//! query; [`Expr`] is a single expression within one.
//!
//! # Layer
//!
//! The AST is the bottom of the language stack. `surrealdb-syn` builds it and
//! `surrealdb-core` lowers it into the expression layer, so everything this
//! crate names sits below it: `surrealdb-strand`, `surrealdb-common`,
//! `surrealdb-types`, `surrealdb-iam`, and the upstream scalar crates
//! (`chrono`, `uuid`, `rust_decimal`, `regex`, `bytes`, `geo-types`). Nothing
//! here reaches up into the parser, the expression layer, the catalog, or the
//! engine.
//!
//! Lowering therefore lives with the consumer: core owns the `sql -> expr`
//! conversions in its `expr::convert` module. The conversions this crate does
//! own are the ones the orphan rule pins here, between an AST node and its
//! `surrealdb-types` counterpart.
//!
//! # Scope
//!
//! The AST is parse-only. No node is persisted, so nothing here carries a
//! storage-format guarantee and this crate does not depend on `revision`.
//! Where a vocabulary type also has to be stored, the persisted form is a
//! separate revisioned twin at the catalog layer and the two are bridged by
//! `From` in both directions, so a change to how something parses or renders
//! cannot move the bytes on disk.
//!
//! # Stability
//!
//! This crate is an internal implementation detail of SurrealDB with no
//! stability guarantee. It is scheduled for deletion once the greenfield
//! parser reaches parity and takes over the same layer, so its API surface
//! should not grow. Depend on `surrealdb` or `surrealdb-core` instead.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

pub mod access;
pub mod access_type;
pub mod algorithm;
pub mod analyzer_function;
pub mod api_method;
pub mod ast;
pub mod base;
pub mod block;
pub mod builtin_paths;
pub mod changefeed;
pub mod closure;
pub mod cond;
pub mod constant;
pub mod cover;
pub mod data;
pub mod dir;
pub mod event_kind;
pub mod explain;
pub mod expression;
pub mod fetch;
pub mod field;
pub mod file;
pub mod filter;
pub mod function;
pub mod group;
pub mod ident;
pub mod idiom;
pub mod kind;
pub mod language;
pub mod limit;
pub mod literal;
pub mod lookup;
pub mod mock;
pub mod model;
pub mod module;
pub mod operator;
pub mod order;
pub mod output;
pub mod param;
pub mod part;
pub mod permission;
pub mod record_id;
pub mod reference;
pub mod scoring;
pub mod script;
pub mod split;
pub mod start;
pub mod table_name;
pub mod table_type;
pub mod tokenizer;
pub mod user;
pub mod view;
pub mod with;

pub mod index;
pub mod statements;

#[cfg(feature = "arbitrary")]
pub mod arbitrary;

pub use self::access_type::AccessType;
pub use self::algorithm::Algorithm;
pub use self::api_method::ApiMethod;
// `Ast` is public so embedders (e.g. the server's Postgres listener) can parse
// a query once and cache it for repeated execution via `Datastore::process*`.
pub use self::ast::Ast;
pub use self::ast::{ExplainFormat, TopLevelExpr};
pub use self::base::Base;
pub use self::block::Block;
pub use self::changefeed::ChangeFeed;
pub use self::closure::Closure;
pub use self::cond::Cond;
pub use self::constant::Constant;
pub use self::cover::CoverStmts;
pub use self::data::Data;
pub use self::dir::Dir;
pub use self::event_kind::EventKind;
pub use self::explain::Explain;
pub use self::expression::Expr;
pub use self::fetch::{Fetch, Fetchs};
pub use self::field::{Field, Fields};
pub use self::function::{Function, FunctionCall};
pub use self::group::{Group, Groups};
pub use self::ident::Ident;
pub use self::idiom::Idiom;
pub use self::index::Index;
pub use self::kind::Kind;
pub use self::limit::Limit;
pub use self::literal::Literal;
pub use self::lookup::Lookup;
pub use self::mock::Mock;
pub use self::model::Model;
pub use self::module::{ModuleExecutable, ModuleName, SiloExecutable, SurrealismExecutable};
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
	CreateStatement, DefineFunctionStatement, DefineIndexStatement, DefineModelStatement,
	DefineModuleStatement, DeleteStatement, InsertStatement, KillStatement, LiveStatement,
	RelateStatement, SelectStatement, UpdateStatement, UpsertStatement,
};
pub use self::table_name::TableName;
pub use self::table_type::TableType;
pub use self::view::View;
pub use self::with::With;
