//! The release each piece of SurrealQL syntax and each builtin first shipped in.
//!
//! A catalog entry that holds SurrealQL text, or a stored value, needs a reader that knows every
//! builtin function, method, operator, kind and clause the text uses. [`Arrival`] reports the
//! newest release among them, so the encoder can record it as the entry's read floor.
//!
//! # Where each release is declared
//!
//! - A builtin function or constant is a row of [`builtin_paths::PATHS`], and every row names a
//!   release, so a row cannot be added without one.
//! - A method is resolved by the runtime's method registry, which this crate cannot see. Its
//!   release is read from [`LATER_METHODS`]; a method the table does not list arrived in the first
//!   release.
//! - Every variant of the enums matched here, and every field of the statement and clause structs
//!   destructured here, is matched or bound by name. A variant or field added later fails to
//!   compile until the arm or binding states the release it arrived in.
//!
//! [`LATEST_ARRIVAL`] is at least every release any arm, field, row or table entry names, so a
//! caller that holds a floor at `LATEST_ARRIVAL` has no reason to walk anything.

#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Ordering as CmpOrdering;
use std::convert::Infallible;
use std::ops::Bound;

pub use surrealdb_kvs::release::Release;

use crate::expr::access_type::{BearerAccess, JwtAccess, RecordAccess};
use crate::expr::changefeed::ChangeFeed;
use crate::expr::cond::Cond;
use crate::expr::data::Assignment;
use crate::expr::explain::Explain;
use crate::expr::fetch::{Fetch, Fetchs};
use crate::expr::field::Selector;
use crate::expr::graphql_config::GraphQLConfig;
use crate::expr::group::{Group, Groups};
use crate::expr::index::Distance;
use crate::expr::limit::Limit;
use crate::expr::lookup::Lookup;
use crate::expr::operator::{BinaryOperator, NearestNeighbor, PostfixOperator, PrefixOperator};
use crate::expr::permission::Permissions;
use crate::expr::reference::Reference;
use crate::expr::split::{Split, Splits};
use crate::expr::start::Start;
use crate::expr::statements::access::{
	AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke, AccessStatementShow,
};
use crate::expr::statements::alter::{
	AlterAccessStatement, AlterAnalyzerStatement, AlterApiStatement, AlterBucketStatement,
	AlterConfigStatement, AlterDatabaseStatement, AlterEventStatement, AlterFieldStatement,
	AlterFunctionStatement, AlterIndexStatement, AlterKind, AlterModuleStatement,
	AlterNamespaceStatement, AlterParamStatement, AlterSequenceStatement, AlterSystemStatement,
	AlterTableStatement, AlterUserStatement,
};
use crate::expr::statements::define::config::api::ApiConfig;
use crate::expr::statements::define::config::defaults::DefaultConfig;
use crate::expr::statements::define::{
	ApiAction, DefineBucketStatement, DefineConfigStatement, DefineSequenceStatement,
};
use crate::expr::statements::rebuild::RebuildIndexStatement;
use crate::expr::statements::remove::{
	RemoveApiStatement, RemoveBucketStatement, RemoveSequenceStatement,
};
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineAccessStatement, DefineAnalyzerStatement,
	DefineApiStatement, DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement,
	DefineFunctionStatement, DefineIndexStatement, DefineModelStatement, DefineModuleStatement,
	DefineNamespaceStatement, DefineParamStatement, DefineStatement, DefineTableStatement,
	DefineUserStatement, DeleteStatement, ForeachStatement, IfelseStatement, InsertStatement,
	KillStatement, LiveStatement, OptionStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveAccessStatement, RemoveAnalyzerStatement, RemoveConfigStatement,
	RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement, RemoveFunctionStatement,
	RemoveIndexStatement, RemoveModelStatement, RemoveModuleStatement, RemoveNamespaceStatement,
	RemoveParamStatement, RemoveStatement, RemoveTableStatement, RemoveUserStatement,
	SelectStatement, SetStatement, ShowStatement, SleepStatement, UpdateStatement, UpsertStatement,
};
use crate::expr::table_type::Relation;
use crate::expr::view::View;
use crate::expr::visit::{Visit, Visitor};
use crate::expr::with::With;
use crate::expr::{
	Block, ClosureExpr, Expr, Fields, Function, FunctionCall, Idiom, Kind, KindLiteral, Literal,
	Model, Part, builtin_paths,
};
use crate::val::{Closure, RecordId, RecordIdKey, Value};

/// The newest release any arm, field, [`builtin_paths::PATHS`] row or [`LATER_METHODS`] entry
/// names.
pub const LATEST_ARRIVAL: Release = Release::FIRST_STORED;

/// Methods that first shipped after [`Release::FIRST_STORED`], by lowercase name in ascending
/// order.
pub const LATER_METHODS: &[(&str, Release)] = &[];

/// The release of everything that has been in SurrealQL since the first release with a stored form.
const FIRST: Release = Release::FIRST_STORED;

/// The release a builtin function or constant `path` arrived in.
///
/// A path that is not a builtin (a custom `fn::` or `ml::` call, a module or a silo function) is
/// resolved against the catalog, never against this crate, and counts as the first release.
pub fn builtin_release(path: &str) -> Release {
	builtin_paths::release(path).unwrap_or(FIRST)
}

/// The release a method named `name` arrived in, compared ASCII case insensitively.
pub fn method_release(name: &str) -> Release {
	match LATER_METHODS.binary_search_by(|(known, _)| cmp_ignore_ascii_case(known, name)) {
		Ok(at) => LATER_METHODS.get(at).map_or(FIRST, |(_, release)| *release),
		Err(_) => FIRST,
	}
}

fn cmp_ignore_ascii_case(a: &str, b: &str) -> CmpOrdering {
	a.bytes().map(|c| c.to_ascii_lowercase()).cmp(b.bytes().map(|c| c.to_ascii_lowercase()))
}

/// The newest release among everything a piece of syntax uses.
pub trait Arrival {
	fn arrival(&self) -> Release;
}

macro_rules! arrival_by_visitor {
	($($ty:ty => $hook:ident),* $(,)?) => {
		$(
			impl Arrival for $ty {
				fn arrival(&self) -> Release {
					let mut visitor = ArrivalVisitor::new();
					let Ok(()) = visitor.$hook(self);
					visitor.floor
				}
			}
		)*
	};
}

arrival_by_visitor! {
	Expr => visit_expr,
	Kind => visit_kind,
	Idiom => visit_idiom,
	Fields => visit_fields,
	Block => visit_block,
}

impl Arrival for Cond {
	fn arrival(&self) -> Release {
		let Cond(expr) = self;
		expr.arrival()
	}
}

/// The newest release a stored value uses.
///
/// Every variant is matched explicitly, and the containers a value can hold (arrays, sets, objects,
/// ranges and the keys of record ids) are walked iteratively, so a deeply nested value costs heap
/// and not stack. A closure holds an expression, which is walked by the visitor.
pub fn value_arrival(value: &Value) -> Release {
	// No value kind, closure body or builtin postdates the first release, so the walk has nothing
	// to find.
	if LATEST_ARRIVAL <= FIRST {
		return FIRST;
	}
	walk_value(value)
}

/// The walk of [`value_arrival`], without its early return.
fn walk_value(value: &Value) -> Release {
	let mut floor = FIRST;
	let mut pending: Vec<&Value> = Vec::new();
	let mut next = Some(value);
	while let Some(value) = next.take().or_else(|| pending.pop()) {
		let release = match value {
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Uuid(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Regex(_) => FIRST,
			Value::Array(array) => {
				pending.extend(array.0.iter());
				FIRST
			}
			Value::Set(set) => {
				pending.extend(set.0.iter());
				FIRST
			}
			Value::Object(object) => {
				pending.extend(object.0.values());
				FIRST
			}
			Value::Range(range) => {
				for bound in [&range.start, &range.end] {
					match bound {
						Bound::Included(v) | Bound::Excluded(v) => pending.push(v),
						Bound::Unbounded => {}
					}
				}
				FIRST
			}
			Value::RecordId(id) => {
				push_key_values(&id.key, &mut pending);
				FIRST
			}
			Value::Closure(closure) => match closure.as_ref() {
				Closure::Expr {
					args,
					returns,
					body,
					captures: _,
				} => {
					let mut release = body.arrival();
					for (_, kind) in args {
						release = release.max(kind.arrival());
					}
					if let Some(kind) = returns {
						release = release.max(kind.arrival());
					}
					release
				}
				Closure::Builtin(_) => FIRST,
			},
		};
		floor = floor.max(release);
	}
	floor
}

/// Pushes every value a record id key holds onto `pending`: the elements of an array key, the
/// values of an object key, and those of every key a range of keys bounds.
fn push_key_values<'a>(key: &'a RecordIdKey, pending: &mut Vec<&'a Value>) {
	let mut keys = vec![key];
	while let Some(key) = keys.pop() {
		match key {
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => {}
			RecordIdKey::Array(array) => pending.extend(array.0.iter()),
			RecordIdKey::Object(object) => pending.extend(object.0.values()),
			RecordIdKey::Range(range) => {
				// A range of keys holds keys, not values, so its bounds go back onto the key
				// stack.
				for bound in [&range.start, &range.end] {
					match bound {
						Bound::Included(k) | Bound::Excluded(k) => keys.push(k),
						Bound::Unbounded => {}
					}
				}
			}
		}
	}
}

/// The newest release a record id uses: the newest among the values its key holds.
pub fn record_id_arrival(id: &RecordId) -> Release {
	if LATEST_ARRIVAL <= FIRST {
		return FIRST;
	}
	let mut pending = Vec::new();
	push_key_values(&id.key, &mut pending);
	pending.into_iter().map(value_arrival).fold(FIRST, Release::max)
}

/// The newest release among the values of an object.
pub fn object_arrival<'a>(values: impl IntoIterator<Item = &'a Value>) -> Release {
	if LATEST_ARRIVAL <= FIRST {
		return FIRST;
	}
	values.into_iter().map(value_arrival).fold(FIRST, Release::max)
}

/// The release floor of everything a visitor meets.
struct ArrivalVisitor {
	floor: Release,
}

impl ArrivalVisitor {
	fn new() -> Self {
		Self {
			floor: FIRST,
		}
	}

	fn raise(&mut self, release: Release) {
		self.floor = self.floor.max(release);
	}
}

/// A clause that no visitor hook receives. Each implementation binds every field of its clause, so
/// a new field fails to compile until it states its release.
trait Clause {
	fn release(&self) -> Release;
}

impl<T: Clause> Clause for Option<T> {
	fn release(&self) -> Release {
		match self {
			Some(clause) => clause.release(),
			None => FIRST,
		}
	}
}

impl<T: Clause> Clause for AlterKind<T> {
	fn release(&self) -> Release {
		match self {
			AlterKind::Set(clause) => clause.release(),
			AlterKind::None | AlterKind::Drop => FIRST,
		}
	}
}

impl Clause for Cond {
	fn release(&self) -> Release {
		let Cond(_) = self;
		FIRST
	}
}

impl Clause for Limit {
	fn release(&self) -> Release {
		let Limit(_) = self;
		FIRST
	}
}

impl Clause for Start {
	fn release(&self) -> Release {
		let Start(_) = self;
		FIRST
	}
}

impl Clause for Explain {
	fn release(&self) -> Release {
		let Explain(_) = self;
		FIRST
	}
}

impl Clause for Fetch {
	fn release(&self) -> Release {
		let Fetch(_) = self;
		FIRST
	}
}

impl Clause for Fetchs {
	fn release(&self) -> Release {
		self.iter().fold(FIRST, |release, fetch| release.max(fetch.release()))
	}
}

impl Clause for Split {
	fn release(&self) -> Release {
		let Split(_) = self;
		FIRST
	}
}

impl Clause for Splits {
	fn release(&self) -> Release {
		let Splits(splits) = self;
		splits.iter().fold(FIRST, |release, split| release.max(split.release()))
	}
}

impl Clause for Group {
	fn release(&self) -> Release {
		let Group(_) = self;
		FIRST
	}
}

impl Clause for Groups {
	fn release(&self) -> Release {
		let Groups(groups) = self;
		groups.iter().fold(FIRST, |release, group| release.max(group.release()))
	}
}

impl Clause for With {
	fn release(&self) -> Release {
		match self {
			With::NoIndex | With::Index(_) => FIRST,
		}
	}
}

impl Clause for ChangeFeed {
	fn release(&self) -> Release {
		let ChangeFeed {
			expiry: _,
			store_diff: _,
		} = self;
		FIRST
	}
}

fn expr_release(expr: &Expr) -> Release {
	match expr {
		Expr::Literal(_)
		| Expr::Param(_)
		| Expr::Idiom(_)
		| Expr::Table(_)
		| Expr::Mock(_)
		| Expr::Block(_)
		| Expr::FunctionCall(_)
		| Expr::Closure(_)
		| Expr::Break
		| Expr::Continue
		| Expr::Return(_)
		| Expr::Throw(_)
		| Expr::IfElse(_)
		| Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_)
		| Expr::Explain {
			..
		} => FIRST,
		Expr::Constant(constant) => constant_release(constant),
		Expr::Prefix {
			op,
			expr: _,
		} => prefix_release(op),
		Expr::Postfix {
			op,
			expr: _,
		} => postfix_release(op),
		Expr::Binary {
			op,
			left: _,
			right: _,
		} => binary_release(op),
	}
}

fn constant_release(constant: &crate::expr::Constant) -> Release {
	use crate::expr::Constant;
	match constant {
		Constant::MathE
		| Constant::MathFrac1Pi
		| Constant::MathFrac1Sqrt2
		| Constant::MathFrac2Pi
		| Constant::MathFrac2SqrtPi
		| Constant::MathFracPi2
		| Constant::MathFracPi3
		| Constant::MathFracPi4
		| Constant::MathFracPi6
		| Constant::MathFracPi8
		| Constant::MathInfinity
		| Constant::MathLn10
		| Constant::MathLn2
		| Constant::MathLog102
		| Constant::MathLog10E
		| Constant::MathLog210
		| Constant::MathLog2E
		| Constant::MathNegInfinity
		| Constant::MathPi
		| Constant::MathSqrt2
		| Constant::MathTau
		| Constant::TimeEpoch
		| Constant::TimeMin
		| Constant::TimeMax
		| Constant::DurationMax => FIRST,
	}
}

fn prefix_release(op: &PrefixOperator) -> Release {
	match op {
		PrefixOperator::Not
		| PrefixOperator::Positive
		| PrefixOperator::Negate
		| PrefixOperator::Range
		| PrefixOperator::RangeInclusive
		| PrefixOperator::Cast(_) => FIRST,
	}
}

fn postfix_release(op: &PostfixOperator) -> Release {
	match op {
		PostfixOperator::Range | PostfixOperator::RangeSkip | PostfixOperator::Call(_) => FIRST,
		PostfixOperator::MethodCall(name, _) => method_release(name),
	}
}

fn binary_release(op: &BinaryOperator) -> Release {
	match op {
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual
		| BinaryOperator::Contain
		| BinaryOperator::NotContain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::ContainNone
		| BinaryOperator::Inside
		| BinaryOperator::NotInside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => FIRST,
		BinaryOperator::Matches(matches) => {
			let crate::expr::operator::MatchesOperator {
				rf: _,
				operator: _,
			} = matches;
			FIRST
		}
		BinaryOperator::NearestNeighbor(neighbor) => match neighbor.as_ref() {
			NearestNeighbor::K(_, distance) => distance_release(distance),
			NearestNeighbor::KTree(_) | NearestNeighbor::Approximate(_, _) => FIRST,
		},
	}
}

fn distance_release(distance: &Distance) -> Release {
	match distance {
		Distance::Chebyshev
		| Distance::Cosine
		| Distance::Euclidean
		| Distance::Hamming
		| Distance::Jaccard
		| Distance::Manhattan
		| Distance::Minkowski(..)
		| Distance::Pearson
		| Distance::CosineNormalized
		| Distance::InnerProduct => FIRST,
	}
}

fn literal_release(literal: &Literal) -> Release {
	match literal {
		Literal::None
		| Literal::Null
		| Literal::UnboundedRange
		| Literal::Bool(..)
		| Literal::Float(..)
		| Literal::Integer(..)
		| Literal::Decimal(..)
		| Literal::String(..)
		| Literal::Bytes(..)
		| Literal::Regex(..)
		| Literal::RecordId(..)
		| Literal::Array(..)
		| Literal::Set(..)
		| Literal::Object(..)
		| Literal::Duration(..)
		| Literal::Datetime(..)
		| Literal::Uuid(..)
		| Literal::Geometry(..)
		| Literal::File(..) => FIRST,
	}
}

fn function_release(function: &Function) -> Release {
	match function {
		Function::Normal(path) => builtin_release(path),
		Function::Custom(_)
		| Function::Script(_)
		| Function::Model(_)
		| Function::Module(..)
		| Function::Silo {
			..
		} => FIRST,
	}
}

fn part_release(part: &Part) -> Release {
	match part {
		Part::Method(name, _) => method_release(name),
		Part::All
		| Part::Flatten
		| Part::Last
		| Part::First
		| Part::Field(_)
		| Part::Where(_)
		| Part::Lookup(_)
		| Part::Value(_)
		| Part::Start(_)
		| Part::Destructure(_)
		| Part::Optional
		| Part::Recurse(_, _, _)
		| Part::Doc
		| Part::RepeatRecurse => FIRST,
	}
}

fn kind_release(kind: &Kind) -> Release {
	match kind {
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(..)
		| Kind::Record(..)
		| Kind::Geometry(..)
		| Kind::Either(..)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(..)
		| Kind::File(..) => FIRST,
	}
}

fn kind_literal_release(literal: &KindLiteral) -> Release {
	match literal {
		KindLiteral::String(..)
		| KindLiteral::Integer(..)
		| KindLiteral::Float(..)
		| KindLiteral::Decimal(..)
		| KindLiteral::Duration(..)
		| KindLiteral::Array(..)
		| KindLiteral::Object(..)
		| KindLiteral::Bool(..) => FIRST,
	}
}

fn define_release(statement: &DefineStatement) -> Release {
	match statement {
		DefineStatement::Namespace(..)
		| DefineStatement::Database(..)
		| DefineStatement::Function(..)
		| DefineStatement::Analyzer(..)
		| DefineStatement::Param(..)
		| DefineStatement::Table(..)
		| DefineStatement::Event(..)
		| DefineStatement::Field(..)
		| DefineStatement::Index(..)
		| DefineStatement::User(..)
		| DefineStatement::Model(..)
		| DefineStatement::Access(..)
		| DefineStatement::Config(..)
		| DefineStatement::Api(..)
		| DefineStatement::Bucket(..)
		| DefineStatement::Sequence(..)
		| DefineStatement::Module(..) => FIRST,
	}
}

fn alter_release(statement: &AlterStatement) -> Release {
	match statement {
		AlterStatement::System(..)
		| AlterStatement::Namespace(..)
		| AlterStatement::Database(..)
		| AlterStatement::Table(..)
		| AlterStatement::Api(..)
		| AlterStatement::Event(..)
		| AlterStatement::Index(..)
		| AlterStatement::Sequence(..)
		| AlterStatement::Field(..)
		| AlterStatement::Param(..)
		| AlterStatement::Bucket(..)
		| AlterStatement::Config(..)
		| AlterStatement::Analyzer(..)
		| AlterStatement::Function(..)
		| AlterStatement::User(..)
		| AlterStatement::Access(..)
		| AlterStatement::Module(..) => FIRST,
	}
}

fn remove_release(statement: &RemoveStatement) -> Release {
	match statement {
		RemoveStatement::Namespace(..)
		| RemoveStatement::Database(..)
		| RemoveStatement::Function(..)
		| RemoveStatement::Analyzer(..)
		| RemoveStatement::Access(..)
		| RemoveStatement::Param(..)
		| RemoveStatement::Table(..)
		| RemoveStatement::Event(..)
		| RemoveStatement::Field(..)
		| RemoveStatement::Index(..)
		| RemoveStatement::User(..)
		| RemoveStatement::Model(..)
		| RemoveStatement::Api(..)
		| RemoveStatement::Bucket(..)
		| RemoveStatement::Sequence(..)
		| RemoveStatement::Module(..)
		| RemoveStatement::Config(..) => FIRST,
	}
}

/// Overrides the hook of a statement or clause struct: every field is bound by name, none with
/// `..`, the clauses that no hook receives are released, and the default walk follows.
macro_rules! struct_hooks {
	($($hook:ident($ty:ident) { $($fields:tt)* } [$($clause:ident),*])*) => {
		$(
			fn $hook(&mut self, node: &$ty) -> Result<(), Infallible> {
				let $ty { $($fields)* } = node;
				$(self.raise(Clause::release($clause));)*
				Visit::visit(node, self)
			}
		)*
	};
}

impl Visitor for ArrivalVisitor {
	type Error = Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Infallible> {
		self.raise(expr_release(expr));
		if let Expr::Prefix {
			op: PrefixOperator::Cast(kind),
			..
		} = expr
		{
			self.visit_kind(kind)?;
		}
		Visit::visit(expr, self)
	}

	fn visit_literal(&mut self, literal: &Literal) -> Result<(), Infallible> {
		self.raise(literal_release(literal));
		Visit::visit(literal, self)
	}

	fn visit_function(&mut self, function: &Function) -> Result<(), Infallible> {
		self.raise(function_release(function));
		Visit::visit(function, self)
	}

	fn visit_part(&mut self, part: &Part) -> Result<(), Infallible> {
		self.raise(part_release(part));
		Visit::visit(part, self)
	}

	fn visit_kind(&mut self, kind: &Kind) -> Result<(), Infallible> {
		self.raise(kind_release(kind));
		Visit::visit(kind, self)
	}

	fn visit_kind_literal(&mut self, literal: &KindLiteral) -> Result<(), Infallible> {
		self.raise(kind_literal_release(literal));
		Visit::visit(literal, self)
	}

	fn visit_define(&mut self, statement: &DefineStatement) -> Result<(), Infallible> {
		self.raise(define_release(statement));
		Visit::visit(statement, self)
	}

	fn visit_alter(&mut self, statement: &AlterStatement) -> Result<(), Infallible> {
		self.raise(alter_release(statement));
		Visit::visit(statement, self)
	}

	fn visit_remove(&mut self, statement: &RemoveStatement) -> Result<(), Infallible> {
		self.raise(remove_release(statement));
		Visit::visit(statement, self)
	}

	fn visit_rebuild(&mut self, statement: &RebuildStatement) -> Result<(), Infallible> {
		let RebuildStatement::Index(RebuildIndexStatement {
			name: _,
			table: _,
			if_exists: _,
			concurrently: _,
		}) = statement;
		Visit::visit(statement, self)
	}

	struct_hooks! {
		visit_access_grant(AccessStatementGrant) { ac: _, base: _, subject: _ } []
		visit_access_show(AccessStatementShow) { ac: _, base: _, gr: _, cond } [cond]
		visit_access_revoke(AccessStatementRevoke) { ac: _, base: _, gr: _, cond } [cond]
		visit_access_purge(AccessStatementPurge) { ac: _, base: _, kind: _, grace: _ } []
		visit_kill(KillStatement) { id: _ } []
		visit_live(LiveStatement) { id: _, node: _, fields: _, what: _, cond, fetch } [cond, fetch]
		visit_option(OptionStatement) { name: _, what: _ } []
		visit_show(ShowStatement) { table: _, since: _, limit: _ } []
		visit_alter_module(AlterModuleStatement) { name: _, if_exists: _, comment: _, permissions: _ } []
		visit_alter_user(AlterUserStatement) {
			name: _,
			base: _,
			if_exists: _,
			pending_password: _,
			hash: _,
			scram: _,
			roles: _,
			token_duration: _,
			session_duration: _,
			comment: _,
		} []
		visit_alter_api(AlterApiStatement) { path: _, if_exists: _, clauses: _, comment: _ } []
		visit_alter_access(AlterAccessStatement) {
			name: _,
			base: _,
			if_exists: _,
			authenticate: _,
			context: _,
			grant_duration: _,
			token_duration: _,
			session_duration: _,
			comment: _,
		} []
		visit_alter_system(AlterSystemStatement) { query_timeout: _, compact: _ } []
		visit_alter_namespace(AlterNamespaceStatement) { compact: _ } []
		visit_alter_database(AlterDatabaseStatement) { compact: _ } []
		visit_alter_table(AlterTableStatement) {
			name: _,
			if_exists: _,
			schemafull: _,
			permissions: _,
			changefeed,
			comment: _,
			compact: _,
			kind: _,
			inline_edges_cap: _,
			inline_refs_cap: _,
		} [changefeed]
		visit_alter_event(AlterEventStatement) {
			name: _,
			what: _,
			if_exists: _,
			when: _,
			then: _,
			comment: _,
			kind: _,
		} []
		visit_alter_index(AlterIndexStatement) {
			name: _,
			table: _,
			if_exists: _,
			prepare_remove: _,
			comment: _,
		} []
		visit_alter_sequence(AlterSequenceStatement) { name: _, if_exists: _, timeout: _ } []
		visit_alter_param(AlterParamStatement) {
			name: _,
			if_exists: _,
			value: _,
			comment: _,
			permissions: _,
		} []
		visit_alter_bucket(AlterBucketStatement) {
			name: _,
			if_exists: _,
			backend: _,
			permissions: _,
			readonly: _,
			comment: _,
		} []
		visit_alter_config(AlterConfigStatement) { if_exists: _, inner: _, comment: _ } []
		visit_alter_analyzer(AlterAnalyzerStatement) {
			name: _,
			if_exists: _,
			function: _,
			tokenizers: _,
			filters: _,
			comment: _,
		} []
		visit_alter_function(AlterFunctionStatement) {
			name: _,
			if_exists: _,
			args: _,
			block: _,
			comment: _,
			permissions: _,
			returns: _,
		} []
		visit_alter_field(AlterFieldStatement) {
			name: _,
			what: _,
			if_exists: _,
			kind: _,
			flexible: _,
			readonly: _,
			inline: _,
			value: _,
			assert: _,
			default: _,
			permissions: _,
			comment: _,
			reference: _,
			key_names: _,
		} []
		visit_upsert(UpsertStatement) {
			only: _,
			what: _,
			with,
			data: _,
			cond,
			output: _,
			timeout: _,
			explain,
		} [with, cond, explain]
		visit_update(UpdateStatement) {
			only: _,
			what: _,
			with,
			data: _,
			cond,
			output: _,
			timeout: _,
			explain,
		} [with, cond, explain]
		visit_sleep(SleepStatement) { duration: _ } []
		visit_set(SetStatement) { name: _, what: _, kind: _ } []
		visit_select(SelectStatement) {
			fields: _,
			omit: _,
			only: _,
			what: _,
			with,
			cond,
			split,
			group,
			order: _,
			limit,
			start,
			fetch,
			version: _,
			timeout: _,
			explain,
			tempfiles: _,
			for_update: _,
		} [with, cond, split, group, limit, start, fetch, explain]
		visit_remove_namespace(RemoveNamespaceStatement) { name: _, if_exists: _, expunge: _ } []
		visit_remove_database(RemoveDatabaseStatement) { name: _, if_exists: _, expunge: _ } []
		visit_remove_function(RemoveFunctionStatement) { name: _, if_exists: _ } []
		visit_remove_module(RemoveModuleStatement) { name: _, if_exists: _ } []
		visit_remove_config(RemoveConfigStatement) { kind: _, if_exists: _ } []
		visit_remove_analyzer(RemoveAnalyzerStatement) { name: _, if_exists: _ } []
		visit_remove_access(RemoveAccessStatement) { name: _, base: _, if_exists: _ } []
		visit_remove_param(RemoveParamStatement) { name: _, if_exists: _ } []
		visit_remove_table(RemoveTableStatement) { name: _, if_exists: _, expunge: _ } []
		visit_remove_event(RemoveEventStatement) { name: _, table_name: _, if_exists: _ } []
		visit_remove_field(RemoveFieldStatement) { name: _, table_name: _, if_exists: _ } []
		visit_remove_index(RemoveIndexStatement) { name: _, what: _, if_exists: _ } []
		visit_remove_user(RemoveUserStatement) { name: _, base: _, if_exists: _ } []
		visit_remove_model(RemoveModelStatement) { name: _, version: _, if_exists: _ } []
		visit_remove_api(RemoveApiStatement) { name: _, if_exists: _ } []
		visit_remove_bucket(RemoveBucketStatement) { name: _, if_exists: _ } []
		visit_remove_sequence(RemoveSequenceStatement) { name: _, if_exists: _ } []
		visit_relate(RelateStatement) {
			only: _,
			or_update: _,
			through: _,
			from: _,
			to: _,
			data: _,
			output: _,
			timeout: _,
		} []
		visit_output_stmt(OutputStatement) { what: _, fetch } [fetch]
		visit_insert(InsertStatement) {
			into: _,
			data: _,
			ignore: _,
			update: _,
			output: _,
			timeout: _,
			relation: _,
		} []
		visit_if_else(IfelseStatement) { exprs: _, close: _ } []
		visit_foreach(ForeachStatement) { param: _, range: _, block: _ } []
		visit_delete(DeleteStatement) {
			only: _,
			what: _,
			with,
			cond,
			output: _,
			timeout: _,
			explain,
		} [with, cond, explain]
		visit_define_sequence(DefineSequenceStatement) {
			kind: _,
			name: _,
			batch: _,
			start: _,
			timeout: _,
		} []
		visit_define_bucket(DefineBucketStatement) {
			kind: _,
			name: _,
			backend: _,
			permissions: _,
			readonly: _,
			comment: _,
		} []
		visit_define_api(DefineApiStatement) {
			kind: _,
			path: _,
			actions: _,
			fallback: _,
			config: _,
			comment: _,
		} []
		visit_api_action(ApiAction) { methods: _, action: _, config: _ } []
		visit_define_config(DefineConfigStatement) { kind: _, inner: _ } []
		visit_api_config(ApiConfig) { middleware: _, permissions: _ } []
		visit_default_config(DefaultConfig) { namespace: _, database: _ } []
		visit_define_access(DefineAccessStatement) {
			kind: _,
			name: _,
			base: _,
			access_type: _,
			authenticate: _,
			context: _,
			duration: _,
			comment: _,
		} []
		visit_define_model(DefineModelStatement) {
			kind: _,
			hash: _,
			name: _,
			version: _,
			comment: _,
			permissions: _,
		} []
		visit_define_module(DefineModuleStatement) {
			kind: _,
			name: _,
			executable: _,
			unsigned: _,
			comment: _,
			permissions: _,
		} []
		visit_define_user(DefineUserStatement) {
			kind: _,
			name: _,
			base: _,
			pending_password: _,
			hash: _,
			hash_is_set: _,
			code: _,
			roles: _,
			duration: _,
			comment: _,
			scram: _,
		} []
		visit_define_index(DefineIndexStatement) {
			kind: _,
			name: _,
			what: _,
			cols: _,
			index: _,
			comment: _,
			concurrently: _,
		} []
		visit_define_field(DefineFieldStatement) {
			kind: _,
			name: _,
			what: _,
			field_kind: _,
			flexible: _,
			readonly: _,
			inline: _,
			value: _,
			assert: _,
			computed: _,
			default: _,
			permissions: _,
			comment: _,
			reference: _,
			graphql_alias: _,
			graphql_deprecated: _,
			key_names: _,
		} []
		visit_reference(Reference) { on_delete: _ } []
		visit_define_event(DefineEventStatement) {
			kind: _,
			name: _,
			target_table: _,
			when: _,
			then: _,
			comment: _,
			event_kind: _,
		} []
		visit_define_table(DefineTableStatement) {
			kind: _,
			id: _,
			name: _,
			drop: _,
			full: _,
			view: _,
			permissions: _,
			changefeed,
			comment: _,
			table_type: _,
			graphql_alias: _,
			graphql_deprecated: _,
			inline_edges_cap: _,
			inline_refs_cap: _,
		} [changefeed]
		visit_permissions(Permissions) { select: _, create: _, update: _, delete: _ } []
		visit_view(View) { expr: _, what: _, cond, group } [cond, group]
		visit_define_param(DefineParamStatement) {
			kind: _,
			name: _,
			value: _,
			comment: _,
			permissions: _,
		} []
		visit_define_analyzer(DefineAnalyzerStatement) {
			kind: _,
			name: _,
			function: _,
			tokenizers: _,
			filters: _,
			comment: _,
		} []
		visit_define_function(DefineFunctionStatement) {
			kind: _,
			name: _,
			args: _,
			block: _,
			comment: _,
			permissions: _,
			returns: _,
			graphql_alias: _,
			graphql_deprecated: _,
		} []
		visit_define_database(DefineDatabaseStatement) {
			kind: _,
			id: _,
			name: _,
			strict: _,
			comment: _,
			changefeed,
		} [changefeed]
		visit_define_namespace(DefineNamespaceStatement) { kind: _, id: _, name: _, comment: _ } []
		visit_create(CreateStatement) { only: _, what: _, data: _, output: _, timeout: _ } []
		visit_lookup(Lookup) {
			kind: _,
			expr: _,
			only: _,
			what: _,
			cond,
			split,
			group,
			order: _,
			limit,
			start,
			alias: _,
		} [cond, split, group, limit, start]
		visit_record_access(RecordAccess) { signup: _, signin: _, jwt: _, bearer: _ } []
		visit_jwt_access(JwtAccess) { verify: _, issue: _, audience: _ } []
		visit_bearer_access(BearerAccess) { kind: _, subject: _, jwt: _ } []
		visit_relation(Relation) { from: _, to: _, enforced: _, lightweight: _ } []
		visit_graphql_config(GraphQLConfig) {
			tables: _,
			functions: _,
			depth_limit: _,
			complexity_limit: _,
			introspection: _,
		} []
		visit_model(Model) { name: _, version: _ } []
		visit_function_call(FunctionCall) { receiver: _, arguments: _ } []
		visit_assignment(Assignment) { place: _, operator: _, value: _ } []
		visit_selector(Selector) { expr: _, alias: _ } []
		visit_closure(ClosureExpr) { args: _, returns: _, body: _ } []
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::expr::Param;
	use crate::expr::variables::Variables;
	use crate::val::{Array, Object, Range, RecordIdKeyRange, Set, TableName};

	/// This file without its test module, and the builtin path table.
	fn sources() -> [(&'static str, &'static str); 2] {
		let this = include_str!("arrival.rs");
		let this = this.split("#[cfg(test)]\nmod tests").next().unwrap_or(this);
		[("arrival.rs", this), ("builtin_paths.rs", include_str!("builtin_paths.rs"))]
	}

	/// Every release `source` names as a literal `Release::new(major, minor)`.
	fn literal_releases(source: &str) -> Vec<Release> {
		let needle = concat!("Release::", "new(");
		source
			.match_indices(needle)
			.map(|(at, _)| {
				let rest = &source[at + needle.len()..];
				let args = &rest[..rest.find(')').expect("a closing parenthesis")];
				let (major, minor) = args.split_once(',').expect("two arguments");
				let text = format!("{}.{}", major.trim(), minor.trim());
				Release::parse(&text)
					.unwrap_or_else(|| panic!("`{needle}{args})` is not a literal"))
			})
			.collect()
	}

	#[test]
	fn latest_arrival_is_at_least_every_release_the_tables_name() {
		let paths = builtin_paths::PATHS.values().map(|(_, _, release)| *release);
		let methods = LATER_METHODS.iter().map(|(_, release)| *release);
		for release in paths.chain(methods) {
			assert!(
				release <= LATEST_ARRIVAL,
				"a builtin or method arrived in {release}, after LATEST_ARRIVAL {LATEST_ARRIVAL}"
			);
		}
	}

	#[test]
	fn latest_arrival_is_at_least_every_release_the_source_names() {
		for (file, source) in sources() {
			for release in literal_releases(source) {
				assert!(
					release <= LATEST_ARRIVAL,
					"{file} names release {release}, after LATEST_ARRIVAL {LATEST_ARRIVAL}"
				);
			}
			// A release built any other way cannot be read off the source.
			assert!(
				!source.contains(concat!("from_", "u32(")),
				"{file} builds a release from an integer"
			);
		}
	}

	#[test]
	fn latest_arrival_lies_between_the_first_stored_release_and_this_build() {
		assert!(LATEST_ARRIVAL >= Release::FIRST_STORED);
		assert!(LATEST_ARRIVAL <= Release::CURRENT);
	}

	#[test]
	fn later_methods_are_lowercase_ascending_and_later_than_the_first_release() {
		for (name, release) in LATER_METHODS {
			assert_eq!(*name, name.to_ascii_lowercase(), "{name} is not lowercase");
			assert!(*release > FIRST, "{name} is listed with the first release {release}");
		}
		for pair in LATER_METHODS.windows(2) {
			if let [(a, _), (b, _)] = pair {
				assert_eq!(cmp_ignore_ascii_case(a, b), CmpOrdering::Less, "{a} is not before {b}");
			}
		}
	}

	#[test]
	fn a_method_or_path_no_table_lists_arrived_in_the_first_release() {
		assert_eq!(method_release("len"), FIRST);
		assert_eq!(method_release("no_such_method"), FIRST);
		assert_eq!(builtin_release("fn::custom"), FIRST);
		assert_eq!(builtin_release("no::such::path"), FIRST);
	}

	#[test]
	fn a_builtin_path_has_its_row_release_whatever_its_case() {
		for (path, (_, _, release)) in builtin_paths::PATHS.entries() {
			assert_eq!(builtin_release(path), *release, "{path}");
			assert_eq!(builtin_release(&path.to_ascii_uppercase()), *release, "{path}");
		}
	}

	#[test]
	fn a_listed_method_has_its_release_whatever_its_case() {
		for (name, release) in LATER_METHODS {
			assert_eq!(method_release(name), *release, "{name}");
			assert_eq!(method_release(&name.to_ascii_uppercase()), *release, "{name}");
		}
	}

	#[test]
	fn cmp_ignore_ascii_case_orders_as_lowercase() {
		assert_eq!(cmp_ignore_ascii_case("ABC", "abc"), CmpOrdering::Equal);
		assert_eq!(cmp_ignore_ascii_case("abc", "ABD"), CmpOrdering::Less);
		assert_eq!(cmp_ignore_ascii_case("ab", "AB_"), CmpOrdering::Less);
		assert_eq!(cmp_ignore_ascii_case("b", "A"), CmpOrdering::Greater);
	}

	fn string(text: &str) -> Value {
		Value::String(Strand::new(text))
	}

	/// An expression that calls a builtin function and a method.
	fn call() -> Expr {
		Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("array::add".to_owned()),
			arguments: vec![Expr::Idiom(Idiom(vec![
				Part::Field(Strand::new("a")),
				Part::Method(Strand::new("len"), Vec::new()),
			]))],
		}))
	}

	/// A value that holds every container the walk descends into, and a closure.
	fn every_container() -> Value {
		let closure = Value::Closure(Box::new(Closure::Expr {
			args: vec![(Param::from("x".to_owned()), Kind::Int)],
			returns: Some(Kind::Array(Box::new(Kind::Any), None)),
			body: call(),
			captures: Variables::default(),
		}));
		let key_range = RecordIdKey::Range(Box::new(RecordIdKeyRange {
			start: Bound::Included(RecordIdKey::Array(Array(vec![string("lo")]))),
			end: Bound::Excluded(RecordIdKey::Object(Object::from(BTreeMap::from([(
				"k",
				string("hi"),
			)])))),
		}));
		let id = Value::RecordId(RecordId {
			table: TableName::new("t"),
			key: key_range,
		});
		let range = Value::Range(Box::new(Range {
			start: Bound::Included(Value::Set(Set::from(vec![Value::Bool(true)]))),
			end: Bound::Unbounded,
		}));
		Value::Array(Array(vec![
			Value::Object(Object::from(BTreeMap::from([("closure", closure), ("id", id)]))),
			range,
			Value::None,
		]))
	}

	#[test]
	fn the_walk_finds_nothing_after_latest_arrival() {
		// The gates return the first release without walking, which is sound only while nothing
		// a value can hold arrived later.
		let value = every_container();
		assert!(walk_value(&value) <= LATEST_ARRIVAL);
		assert_eq!(value_arrival(&value), walk_value(&value));
	}

	#[test]
	fn the_walk_of_a_deep_value_uses_the_heap() {
		let mut value = Value::None;
		for _ in 0..100_000 {
			value = Value::Array(Array(vec![value]));
		}
		assert_eq!(walk_value(&value), FIRST);
		// Unnest before dropping: the drop's recursion is not under test.
		let mut next = Some(value);
		while let Some(Value::Array(mut array)) = next.take() {
			next = array.0.pop();
		}
	}

	#[test]
	fn a_record_id_and_an_object_answer_the_releases_of_their_values() {
		let id = RecordId {
			table: TableName::new("t"),
			key: RecordIdKey::Array(Array(vec![every_container()])),
		};
		assert_eq!(record_id_arrival(&id), FIRST);
		assert_eq!(object_arrival([&every_container(), &Value::Null]), FIRST);
	}

	#[test]
	fn syntax_using_only_first_release_builtins_arrives_in_the_first_release() {
		assert_eq!(call().arrival(), FIRST);
		assert_eq!(Kind::Either(vec![Kind::None, Kind::Int]).arrival(), FIRST);
	}
}
