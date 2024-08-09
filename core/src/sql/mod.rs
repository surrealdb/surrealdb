//! The full type definitions for the SurrealQL query language

pub(crate) mod access;
pub(crate) mod access_type;
pub(crate) mod algorithm;
#[cfg(feature = "arbitrary")]
pub(crate) mod arbitrary;
pub(crate) mod array;
pub(crate) mod base;
pub(crate) mod block;
pub(crate) mod bytes;
pub(crate) mod cast;
pub(crate) mod change_feed_include;
pub(crate) mod changefeed;
pub(crate) mod closure;
pub(crate) mod cond;
pub(crate) mod constant;
pub(crate) mod data;
pub(crate) mod datetime;
pub(crate) mod dir;
pub(crate) mod duration;
pub(crate) mod edges;
pub(crate) mod escape;
pub(crate) mod explain;
pub(crate) mod expression;
pub(crate) mod fetch;
pub(crate) mod field;
pub(crate) mod filter;
pub(crate) mod fmt;
pub(crate) mod function;
pub(crate) mod future;
pub(crate) mod geometry;
pub(crate) mod graph;
pub(crate) mod group;
pub(crate) mod id;
pub(crate) mod ident;
pub(crate) mod idiom;
pub(crate) mod kind;
pub(crate) mod language;
pub(crate) mod limit;
pub(crate) mod mock;
pub(crate) mod model;
pub(crate) mod number;
pub(crate) mod object;
pub(crate) mod operation;
pub(crate) mod operator;
pub(crate) mod order;
pub(crate) mod output;
pub(crate) mod param;
pub(crate) mod part;
pub(crate) mod paths;
pub(crate) mod permission;
pub(crate) mod query;
pub(crate) mod range;
pub(crate) mod regex;
pub(crate) mod scoring;
pub(crate) mod script;
pub(crate) mod split;
pub(crate) mod start;
pub(crate) mod statement;
pub(crate) mod strand;
pub(crate) mod subquery;
pub(crate) mod table;
pub(crate) mod table_type;
pub(crate) mod thing;
pub(crate) mod timeout;
pub(crate) mod tokenizer;
pub(crate) mod user;
pub(crate) mod uuid;
pub(crate) mod value;
pub(crate) mod version;
pub(crate) mod view;
pub(crate) mod with;

#[doc(hidden)]
pub mod index;

pub mod serde;
pub mod statements;

pub use self::access::Access;
pub use self::access::Accesses;
pub use self::access_type::{AccessType, JwtAccess, RecordAccess};
pub use self::algorithm::Algorithm;
pub use self::array::Array;
pub use self::base::Base;
pub use self::block::Block;
pub use self::block::Entry;
pub use self::bytes::Bytes;
pub use self::cast::Cast;
pub use self::changefeed::ChangeFeed;
pub use self::closure::Closure;
pub use self::cond::Cond;
pub use self::constant::Constant;
pub use self::data::Data;
pub use self::datetime::Datetime;
pub use self::dir::Dir;
pub use self::duration::Duration;
pub use self::edges::Edges;
pub use self::explain::Explain;
pub use self::expression::Expression;
pub use self::fetch::Fetch;
pub use self::fetch::Fetchs;
pub use self::field::Field;
pub use self::field::Fields;
pub use self::filter::Filter;
pub use self::function::Function;
pub use self::future::Future;
pub use self::geometry::Geometry;
pub use self::graph::Graph;
pub use self::group::Group;
pub use self::group::Groups;
pub use self::id::Id;
pub use self::ident::Ident;
pub use self::idiom::Idiom;
pub use self::idiom::Idioms;
pub use self::index::Index;
pub use self::kind::Kind;
pub use self::limit::Limit;
pub use self::mock::Mock;
pub use self::model::Model;
pub use self::number::Number;
pub use self::object::Object;
pub use self::operation::Operation;
pub use self::operator::Operator;
pub use self::order::Order;
pub use self::order::Orders;
pub use self::output::Output;
pub use self::param::Param;
pub use self::part::Part;
pub use self::permission::Permission;
pub use self::permission::Permissions;
pub use self::query::Query;
pub use self::range::Range;
pub use self::regex::Regex;
pub use self::scoring::Scoring;
pub use self::script::Script;
pub use self::split::Split;
pub use self::split::Splits;
pub use self::start::Start;
pub use self::statement::Statement;
pub use self::statement::Statements;
pub use self::strand::Strand;
pub use self::subquery::Subquery;
pub use self::table::Table;
pub use self::table::Tables;
pub use self::table_type::{Relation, TableType};
pub use self::thing::Thing;
pub use self::timeout::Timeout;
pub use self::tokenizer::Tokenizer;
pub use self::uuid::Uuid;
pub use self::value::serde::to_value;
#[doc(hidden)]
pub use self::value::serde::{from_value, FromValueError};
pub use self::value::Value;
pub use self::value::Values;
pub use self::version::Version;
pub use self::view::View;
pub use self::with::With;

// module reexporting parsing function to prevent a breaking change.
#[doc(hidden)]
mod parser {
	pub use crate::syn::*;
}

pub use self::parser::{idiom, json, parse, subquery, thing, value};
