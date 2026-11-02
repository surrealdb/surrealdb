#![deny(clippy::wildcard_enum_match_arm)]

use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{RecordId, SqlFormat, ToSql, write_sql};

use super::SleepStatement;
use crate::expr::closure::ClosureExpr;
use crate::expr::lookup::LookupKind;
use crate::expr::operator::BindingPower;
use crate::expr::statements::info::InfoStructure;
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, UpdateStatement,
	UpsertStatement,
};
use crate::expr::{
	BinaryOperator, Block, Constant, CoverStmts, Dir, FunctionCall, Idiom, Literal, Mock,
	ObjectEntry, Param, Part, PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use crate::types::PublicValue;
use crate::val::table_name_public::IntoTableName;
use crate::val::{TableName, Value};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, Default)]
pub enum ExplainFormat {
	#[default]
	Text,
	Json,
}

/// `PartialEq` compares the same fields in the same order as `Hash` hashes them.
#[derive(Debug, Eq, surrealdb_stored_derive::DynHash)]
pub enum Expr {
	Literal(Literal),
	Param(Param),
	Idiom(Idiom),
	// Maybe move into Literal?
	Table(TableName),
	// This type can probably be removed in favour of range expressions.
	Mock(Mock),
	Block(Box<Block>),
	Constant(Constant),
	Prefix {
		op: PrefixOperator,
		expr: Box<Expr>,
	},
	Postfix {
		expr: Box<Expr>,
		op: PostfixOperator,
	},
	Binary {
		left: Box<Expr>,
		op: BinaryOperator,
		right: Box<Expr>,
	},
	// TODO: Factor out the call from the function expression.
	FunctionCall(Box<FunctionCall>),

	Closure(Box<ClosureExpr>),

	Break,
	Continue,
	Return(Box<OutputStatement>),
	Throw(Box<Expr>),

	IfElse(Box<IfelseStatement>),
	Select(Box<SelectStatement>),
	Create(Box<CreateStatement>),
	Update(Box<UpdateStatement>),
	Upsert(Box<UpsertStatement>),
	Delete(Box<DeleteStatement>),
	Relate(Box<RelateStatement>),
	Insert(Box<InsertStatement>),
	Define(Box<DefineStatement>),
	Remove(Box<RemoveStatement>),
	Rebuild(Box<RebuildStatement>),
	Alter(Box<AlterStatement>),
	Info(Box<InfoStatement>),
	Foreach(Box<ForeachStatement>),
	Let(Box<SetStatement>),
	Sleep(Box<SleepStatement>),
	Explain {
		format: ExplainFormat,
		analyze: bool,
		statement: Box<Expr>,
	},
}

/// Written out and kept out of line so one copy serves every crate that clones an `Expr`.
impl Clone for Expr {
	#[inline(never)]
	fn clone(&self) -> Self {
		match self {
			Self::Literal(v) => Self::Literal(v.clone()),
			Self::Param(v) => Self::Param(v.clone()),
			Self::Idiom(v) => Self::Idiom(v.clone()),
			Self::Table(v) => Self::Table(v.clone()),
			Self::Mock(v) => Self::Mock(v.clone()),
			Self::Block(v) => Self::Block(v.clone()),
			Self::Constant(v) => Self::Constant(v.clone()),
			Self::FunctionCall(v) => Self::FunctionCall(v.clone()),
			Self::Closure(v) => Self::Closure(v.clone()),
			Self::Return(v) => Self::Return(v.clone()),
			Self::Throw(v) => Self::Throw(v.clone()),
			Self::IfElse(v) => Self::IfElse(v.clone()),
			Self::Select(v) => Self::Select(v.clone()),
			Self::Create(v) => Self::Create(v.clone()),
			Self::Update(v) => Self::Update(v.clone()),
			Self::Upsert(v) => Self::Upsert(v.clone()),
			Self::Delete(v) => Self::Delete(v.clone()),
			Self::Relate(v) => Self::Relate(v.clone()),
			Self::Insert(v) => Self::Insert(v.clone()),
			Self::Define(v) => Self::Define(v.clone()),
			Self::Remove(v) => Self::Remove(v.clone()),
			Self::Rebuild(v) => Self::Rebuild(v.clone()),
			Self::Alter(v) => Self::Alter(v.clone()),
			Self::Info(v) => Self::Info(v.clone()),
			Self::Foreach(v) => Self::Foreach(v.clone()),
			Self::Let(v) => Self::Let(v.clone()),
			Self::Sleep(v) => Self::Sleep(v.clone()),
			Self::Break => Self::Break,
			Self::Continue => Self::Continue,
			Self::Prefix {
				op,
				expr,
			} => Self::Prefix {
				op: op.clone(),
				expr: expr.clone(),
			},
			Self::Postfix {
				expr,
				op,
			} => Self::Postfix {
				expr: expr.clone(),
				op: op.clone(),
			},
			Self::Binary {
				left,
				op,
				right,
			} => Self::Binary {
				left: left.clone(),
				op: op.clone(),
				right: right.clone(),
			},
			Self::Explain {
				format,
				analyze,
				statement,
			} => Self::Explain {
				format: *format,
				analyze: *analyze,
				statement: statement.clone(),
			},
		}
	}
}

/// Written out and kept out of line so one copy serves every crate that compares an `Expr`.
impl PartialEq for Expr {
	#[inline(never)]
	fn eq(&self, other: &Self) -> bool {
		match self {
			Self::Literal(a) => matches!(other, Self::Literal(b) if a == b),
			Self::Param(a) => matches!(other, Self::Param(b) if a == b),
			Self::Idiom(a) => matches!(other, Self::Idiom(b) if a == b),
			Self::Table(a) => matches!(other, Self::Table(b) if a == b),
			Self::Mock(a) => matches!(other, Self::Mock(b) if a == b),
			Self::Block(a) => matches!(other, Self::Block(b) if a == b),
			Self::Constant(a) => matches!(other, Self::Constant(b) if a == b),
			Self::FunctionCall(a) => matches!(other, Self::FunctionCall(b) if a == b),
			Self::Closure(a) => matches!(other, Self::Closure(b) if a == b),
			Self::Return(a) => matches!(other, Self::Return(b) if a == b),
			Self::Throw(a) => matches!(other, Self::Throw(b) if a == b),
			Self::IfElse(a) => matches!(other, Self::IfElse(b) if a == b),
			Self::Select(a) => matches!(other, Self::Select(b) if a == b),
			Self::Create(a) => matches!(other, Self::Create(b) if a == b),
			Self::Update(a) => matches!(other, Self::Update(b) if a == b),
			Self::Upsert(a) => matches!(other, Self::Upsert(b) if a == b),
			Self::Delete(a) => matches!(other, Self::Delete(b) if a == b),
			Self::Relate(a) => matches!(other, Self::Relate(b) if a == b),
			Self::Insert(a) => matches!(other, Self::Insert(b) if a == b),
			Self::Define(a) => matches!(other, Self::Define(b) if a == b),
			Self::Remove(a) => matches!(other, Self::Remove(b) if a == b),
			Self::Rebuild(a) => matches!(other, Self::Rebuild(b) if a == b),
			Self::Alter(a) => matches!(other, Self::Alter(b) if a == b),
			Self::Info(a) => matches!(other, Self::Info(b) if a == b),
			Self::Foreach(a) => matches!(other, Self::Foreach(b) if a == b),
			Self::Let(a) => matches!(other, Self::Let(b) if a == b),
			Self::Sleep(a) => matches!(other, Self::Sleep(b) if a == b),
			Self::Break => matches!(other, Self::Break),
			Self::Continue => matches!(other, Self::Continue),
			Self::Prefix {
				op,
				expr,
			} => {
				matches!(other, Self::Prefix { op: op2, expr: expr2 } if op == op2 && expr == expr2)
			}
			Self::Postfix {
				expr,
				op,
			} => {
				matches!(other, Self::Postfix { expr: expr2, op: op2 } if expr == expr2 && op == op2)
			}
			Self::Binary {
				left,
				op,
				right,
			} => {
				matches!(other, Self::Binary { left: left2, op: op2, right: right2 } if left == left2 && op == op2 && right == right2)
			}
			Self::Explain {
				format,
				analyze,
				statement,
			} => {
				matches!(other, Self::Explain { format: format2, analyze: analyze2, statement: statement2 } if format == format2 && analyze == analyze2 && statement == statement2)
			}
		}
	}
}

impl Expr {
	/// Prepare reached credentials and report whether a cached lowering needs
	/// the resulting payload. Already-prepared expressions return `false`.
	pub fn prepare_credentials_changed(&mut self) -> bool {
		super::credentials::prepare_changed(self)
	}

	/// Prepare credentials once, when a statement is admitted for execution.
	pub fn prepare_credentials(&mut self) {
		super::credentials::prepare(self);
	}

	/// Render statement text with credential material removed.
	pub fn to_redacted_sql(&self) -> String {
		let mut value = self.clone();
		super::credentials::redact(&mut value);
		value.to_sql()
	}

	/// Check if this expression does only reads.
	pub fn read_only(&self) -> bool {
		match self {
			Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Sleep(_) => true,

			Expr::Info(info) => info.read_only(),

			// Composite literals evaluate their element expressions in place.
			Expr::Literal(l) => l.read_only(),

			Expr::Idiom(x) => x.read_only(),
			Expr::Block(block) => block.read_only(),
			Expr::Prefix {
				expr,
				..
			} => expr.read_only(),
			Expr::Postfix {
				expr,
				op,
			} => {
				match op {
					// Calling a closure executes a body this expression can only
					// see when the target is a closure literal; anything else
					// (a param, a field) must over-approximate to writable.
					PostfixOperator::Call(args) => {
						matches!(&**expr, Expr::Closure(_))
							&& expr.read_only() && args.iter().all(Expr::read_only)
					}
					// A registered method dispatches to a builtin, so only the
					// receiver and arguments carry user expressions. Any other
					// name invokes a closure stored on the receiver — a runtime
					// value whose body is invisible here — and must
					// over-approximate to writable, mirroring `Part::Method`.
					PostfixOperator::MethodCall(name, args) => {
						crate::expr::method::is_builtin_method(name)
							&& expr.read_only() && args.iter().all(Expr::read_only)
					}
					PostfixOperator::Range | PostfixOperator::RangeSkip => expr.read_only(),
				}
			}
			Expr::Binary {
				left,
				right,
				..
			} => left.read_only() && right.read_only(),
			Expr::FunctionCall(function) => function.read_only(),
			Expr::Return(expr) => expr.read_only(),
			Expr::Throw(expr) => expr.read_only(),
			Expr::IfElse(s) => s.read_only(),
			Expr::Select(s) => s.read_only(),
			Expr::Let(s) => s.read_only(),
			Expr::Foreach(s) => s.read_only(),
			Expr::Explain {
				statement,
				..
			} => statement.read_only(),
			// A closure literal is inert as a value, but wherever it can flow
			// it can also be invoked (call operator, closure-taking builtins),
			// so a writing body makes the expression writable.
			Expr::Closure(c) => c.body.read_only(),
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_) => false,
		}
	}

	pub fn from_public_value(value: PublicValue) -> Self {
		match value {
			surrealdb_types::Value::None => Expr::Literal(Literal::None),
			surrealdb_types::Value::Null => Expr::Literal(Literal::Null),
			surrealdb_types::Value::Bool(b) => Expr::Literal(Literal::Bool(b)),
			surrealdb_types::Value::Number(n) => match n {
				surrealdb_types::Number::Int(i) => Expr::Literal(Literal::Integer(i)),
				surrealdb_types::Number::Float(f) => Expr::Literal(Literal::Float(f)),
				surrealdb_types::Number::Decimal(d) => Expr::Literal(Literal::Decimal(d)),
				surrealdb_types::Number::BigDecimal(b) => Expr::Literal(Literal::BigDecimal(b)),
				surrealdb_types::Number::I8(i) => Expr::Literal(Literal::I8(i)),
				surrealdb_types::Number::U8(u) => Expr::Literal(Literal::U8(u)),
				surrealdb_types::Number::I16(i) => Expr::Literal(Literal::I16(i)),
				surrealdb_types::Number::U16(u) => Expr::Literal(Literal::U16(u)),
				surrealdb_types::Number::I32(i) => Expr::Literal(Literal::I32(i)),
				surrealdb_types::Number::U32(u) => Expr::Literal(Literal::U32(u)),
				surrealdb_types::Number::U64(u) => Expr::Literal(Literal::U64(u)),
				surrealdb_types::Number::F16(f) => Expr::Literal(Literal::F16(f)),
				surrealdb_types::Number::F32(f) => Expr::Literal(Literal::F32(f)),
			},
			surrealdb_types::Value::String(s) => Expr::Literal(Literal::String(s.into())),
			surrealdb_types::Value::Bytes(b) => {
				Expr::Literal(Literal::Bytes(crate::val::Bytes(b.into_inner())))
			}
			surrealdb_types::Value::Duration(d) => {
				Expr::Literal(Literal::Duration(crate::val::Duration(d.into_inner())))
			}
			surrealdb_types::Value::Datetime(d) => {
				Expr::Literal(Literal::Datetime(crate::val::Datetime(d.into_inner())))
			}
			surrealdb_types::Value::Date(v) => Expr::Literal(Literal::Date(v.into())),
			surrealdb_types::Value::Time(v) => Expr::Literal(Literal::Time(v.into())),
			surrealdb_types::Value::Timestamp(v) => Expr::Literal(Literal::Timestamp(v.into())),
			surrealdb_types::Value::Interval(v) => Expr::Literal(Literal::Interval(v.into())),
			surrealdb_types::Value::Uuid(u) => {
				Expr::Literal(Literal::Uuid(crate::val::Uuid(u.into_inner())))
			}
			surrealdb_types::Value::Array(a) => {
				Expr::Literal(Literal::Array(a.into_iter().map(Expr::from_public_value).collect()))
			}
			surrealdb_types::Value::Set(s) => {
				Expr::Literal(Literal::Array(s.into_iter().map(Expr::from_public_value).collect()))
			}
			surrealdb_types::Value::Object(o) => Expr::Literal(Literal::Object(
				o.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k.into(),
						value: Expr::from_public_value(v),
					})
					.collect(),
			)),
			surrealdb_types::Value::Table(t) => Expr::Table(t.into_table_name()),
			surrealdb_types::Value::RecordId(RecordId {
				table,
				key,
			}) => Expr::Literal(Literal::RecordId(RecordIdLit {
				table: table.into_table_name(),
				key: Self::public_record_id_key(key),
			})),
			surrealdb_types::Value::Geometry(g) => Expr::Literal(Literal::Geometry(g.into())),
			surrealdb_types::Value::File(f) => {
				Expr::Literal(Literal::File(crate::val::File::new(f.bucket, f.key)))
			}
			surrealdb_types::Value::Range(r) => Expr::from(*r),
			surrealdb_types::Value::Regex(r) => {
				Expr::Literal(Literal::Regex(crate::val::Regex(r.into_inner())))
			}
		}
	}

	fn public_record_id_key(key: surrealdb_types::RecordIdKey) -> RecordIdKeyLit {
		match key {
			surrealdb_types::RecordIdKey::Number(n) => RecordIdKeyLit::Number(n),
			surrealdb_types::RecordIdKey::String(s) => RecordIdKeyLit::String(s.into()),
			surrealdb_types::RecordIdKey::Uuid(u) => {
				RecordIdKeyLit::Uuid(crate::val::Uuid(u.into_inner()))
			}
			surrealdb_types::RecordIdKey::Array(a) => {
				RecordIdKeyLit::Array(a.into_iter().map(Expr::from_public_value).collect())
			}
			surrealdb_types::RecordIdKey::Object(o) => RecordIdKeyLit::Object(
				o.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k.into(),
						value: Expr::from_public_value(v),
					})
					.collect(),
			),
			surrealdb_types::RecordIdKey::Range(range) => {
				RecordIdKeyLit::Range(Box::new(super::RecordIdKeyRangeLit {
					start: range.start.map(Self::public_record_id_key),
					end: range.end.map(Self::public_record_id_key),
				}))
			}
		}
	}
}

impl From<surrealdb_types::Range> for Expr {
	fn from(r: surrealdb_types::Range) -> Self {
		use std::ops::Bound;
		match r.into_inner() {
			// Unbounded range: ..
			(Bound::Unbounded, Bound::Unbounded) => Expr::Literal(Literal::UnboundedRange),
			// Prefix ranges: ..end or ..=end
			(Bound::Unbounded, Bound::Excluded(end)) => Expr::Prefix {
				op: PrefixOperator::Range,
				expr: Box::new(Expr::from_public_value(end)),
			},
			(Bound::Unbounded, Bound::Included(end)) => Expr::Prefix {
				op: PrefixOperator::RangeInclusive,
				expr: Box::new(Expr::from_public_value(end)),
			},
			// Binary ranges with inclusive start
			(Bound::Included(start), Bound::Excluded(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::Range,
				right: Box::new(Expr::from_public_value(end)),
			},
			(Bound::Included(start), Bound::Included(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::RangeInclusive,
				right: Box::new(Expr::from_public_value(end)),
			},
			// Binary ranges with excluded start (skip)
			(Bound::Excluded(start), Bound::Excluded(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::RangeSkip,
				right: Box::new(Expr::from_public_value(end)),
			},
			(Bound::Excluded(start), Bound::Included(end)) => Expr::Binary {
				left: Box::new(Expr::from_public_value(start)),
				op: BinaryOperator::RangeSkipInclusive,
				right: Box::new(Expr::from_public_value(end)),
			},
			// Postfix ranges: start.. or start>..
			(Bound::Included(start), Bound::Unbounded) => Expr::Postfix {
				expr: Box::new(Expr::from_public_value(start)),
				op: PostfixOperator::Range,
			},
			(Bound::Excluded(start), Bound::Unbounded) => Expr::Postfix {
				expr: Box::new(Expr::from_public_value(start)),
				op: PostfixOperator::RangeSkip,
			},
		}
	}
}

impl Expr {
	/// The value this expression denotes, when it denotes one without being
	/// evaluated.
	///
	/// Only a literal can answer this; every other form either reads the
	/// environment or needs the evaluator, so it answers `None`. See
	/// [`Literal::as_static_value`] for the contract the result upholds.
	///
	/// Narrower than [`Self::is_static`], which is also true of expressions
	/// that are computable without the environment but still need evaluating
	/// (arithmetic on literals, say).
	pub fn as_static_value(&self) -> Option<crate::val::Value> {
		if let Expr::Literal(literal) = self {
			literal.as_static_value()
		} else {
			None
		}
	}

	/// Checks if a expression is 'pure' i.e. does not rely on the environment.
	pub fn is_static(&self) -> bool {
		match self {
			Expr::Literal(literal) => literal.is_static(),
			Expr::Constant(_) => true,
			Expr::Prefix {
				expr,
				..
			} => expr.is_static(),
			Expr::Postfix {
				expr,
				..
			} => expr.is_static(),
			Expr::Binary {
				left,
				right,
				..
			} => left.is_static() && right.is_static(),
			Expr::FunctionCall(x) => {
				// This is not correct as functions like http::get are not 'pure' but this is
				// replicating previous behavior.
				//
				// FIXME: Fix this discrepency and weird static/non-static behavior.
				x.arguments.iter().all(|x| x.is_static())
			}
			Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => false,
		}
	}

	pub fn to_idiom(&self) -> Idiom {
		match self {
			Expr::Idiom(i) => i.simplify(),
			Expr::Param(i) => Idiom::field(i.clone().into_strand()),
			Expr::FunctionCall(x) => x.receiver.to_idiom(),
			Expr::Literal(l) => match l {
				Literal::String(s) => Idiom::field(s.clone()),
				Literal::Datetime(d) => Idiom::field(d.to_string()),
				Literal::Date(d) => Idiom::field(d.to_string()),
				Literal::Time(d) => Idiom::field(d.to_string()),
				Literal::Timestamp(d) => Idiom::field(d.to_string()),
				Literal::Interval(d) => Idiom::field(d.to_string()),
				Literal::None
				| Literal::Null
				| Literal::UnboundedRange
				| Literal::Bool(_)
				| Literal::Float(_)
				| Literal::Integer(_)
				| Literal::Decimal(_)
				| Literal::BigDecimal(_)
				| Literal::I8(_)
				| Literal::U8(_)
				| Literal::I16(_)
				| Literal::U16(_)
				| Literal::I32(_)
				| Literal::U32(_)
				| Literal::U64(_)
				| Literal::F16(_)
				| Literal::F32(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::RecordId(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Duration(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_) => Idiom::field(l.to_sql()),
			},
			Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
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
			} => Idiom::field(self.to_sql()),
		}
	}

	pub fn to_raw_string(&self) -> String {
		if let Expr::Idiom(idiom) = self {
			idiom.to_raw_string()
		} else if let Expr::Table(ident) = self {
			ident.as_str().to_string()
		} else {
			self.to_sql()
		}
	}

	/// Returns if this expression needs to be parenthesized when inside another expression.
	pub fn needs_parentheses(&self) -> bool {
		match self {
			Expr::Literal(Literal::UnboundedRange | Literal::RecordId(_))
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_)
			| Expr::Return(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => true,

			Expr::Postfix {
				op,
				..
			} => matches!(
				op,
				PostfixOperator::Range
					| PostfixOperator::RangeSkip
					| PostfixOperator::MethodCall(_, _)
					| PostfixOperator::Call(_)
			),

			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_) => false,
		}
	}

	/// Returns true if there is a `NONE` or `NULL` value in the left most spot when formatting.
	/// returns true for `NONE + 1`, `NULL()`, `NONE`, `NULL..` etc.
	///
	/// Required for proper formatting when `NONE` can conflict with a clause.
	pub fn has_left_none_null(&self) -> bool {
		match self {
			Expr::Literal(Literal::None | Literal::Null) => true,
			Expr::Binary {
				left: expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			} => expr.has_left_none_null(),
			Expr::Idiom(x) => {
				if let Some(Part::Start(x)) = x.0.first() {
					x.has_left_none_null()
				} else {
					false
				}
			}
			Expr::Literal(
				Literal::Bool(_)
				| Literal::Float(_)
				| Literal::Integer(_)
				| Literal::Decimal(_)
				| Literal::BigDecimal(_)
				| Literal::I8(_)
				| Literal::U8(_)
				| Literal::I16(_)
				| Literal::U16(_)
				| Literal::I32(_)
				| Literal::U32(_)
				| Literal::U64(_)
				| Literal::F16(_)
				| Literal::F32(_)
				| Literal::String(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::RecordId(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Duration(_)
				| Literal::Datetime(_)
				| Literal::Date(_)
				| Literal::Time(_)
				| Literal::Timestamp(_)
				| Literal::Interval(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_)
				| Literal::UnboundedRange,
			)
			| Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_)
			| Expr::Return(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => false,
		}
	}

	/// Whether this is an unsigned sized integer literal (`1u8` ... `1u64`).
	pub fn is_unsigned_literal(&self) -> bool {
		matches!(
			self,
			Expr::Literal(Literal::U8(_) | Literal::U16(_) | Literal::U32(_) | Literal::U64(_))
		)
	}

	pub fn has_left_minus(&self) -> bool {
		match self {
			Expr::Prefix {
				op: PrefixOperator::Negate,
				..
			} => true,
			Expr::Postfix {
				expr,
				..
			}
			| Expr::Binary {
				left: expr,
				..
			} => expr.has_left_minus(),
			Expr::Literal(Literal::Integer(x)) => x.is_negative(),
			Expr::Literal(Literal::Float(x)) => x.is_sign_negative(),
			Expr::Literal(Literal::Decimal(x)) => x.is_sign_negative(),
			Expr::Literal(Literal::BigDecimal(x)) => x.signum() < 0,
			Expr::Literal(Literal::I8(x)) => x.is_negative(),
			Expr::Literal(Literal::I16(x)) => x.is_negative(),
			Expr::Literal(Literal::I32(x)) => x.is_negative(),
			Expr::Literal(Literal::F16(x)) => x.is_sign_negative(),
			Expr::Literal(Literal::F32(x)) => x.is_sign_negative(),
			Expr::Idiom(x) => {
				if let Some(x) = x.0.first()
					&& let Part::Lookup(lookup) = x
					&& let LookupKind::Graph(Dir::Out) = lookup.kind
				{
					return true;
				}
				false
			}
			Expr::Literal(
				Literal::None
				| Literal::Null
				| Literal::Bool(_)
				| Literal::U8(_)
				| Literal::U16(_)
				| Literal::U32(_)
				| Literal::U64(_)
				| Literal::String(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::RecordId(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Duration(_)
				| Literal::Datetime(_)
				| Literal::Date(_)
				| Literal::Time(_)
				| Literal::Timestamp(_)
				| Literal::Interval(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_)
				| Literal::UnboundedRange,
			)
			| Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				op:
					PrefixOperator::Not
					| PrefixOperator::Positive
					| PrefixOperator::Range
					| PrefixOperator::RangeInclusive
					| PrefixOperator::Cast(_),
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_)
			| Expr::Return(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => false,
		}
	}

	pub fn has_left_idiom(&self) -> bool {
		match self {
			Expr::Idiom(_) => true,

			Expr::Postfix {
				expr,
				..
			}
			| Expr::Binary {
				left: expr,
				..
			} => expr.has_left_idiom(),
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_)
			| Expr::Return(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => false,
		}
	}
}

impl ToSql for Expr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Expr::Literal(literal) => literal.fmt_sql(f, fmt),
			Expr::Param(param) => param.fmt_sql(f, fmt),
			Expr::Idiom(idiom) => idiom.fmt_sql(f, fmt),
			Expr::Table(ident) => {
				write_sql!(f, fmt, "{}", common::fmt::EscapeIdent(ident.as_str()))
			}
			Expr::Mock(mock) => mock.fmt_sql(f, fmt),
			Expr::Block(block) => block.fmt_sql(f, fmt),
			Expr::Constant(constant) => constant.fmt_sql(f, fmt),
			Expr::Prefix {
				op,
				expr,
			} => {
				let expr_bp = BindingPower::for_expr(expr);
				let op_bp = BindingPower::for_prefix_operator(op);
				if expr.needs_parentheses()
					|| expr_bp < op_bp
					|| expr_bp == op_bp && matches!(expr_bp, BindingPower::Range)
					// We need to avoid `--` from showing up so we need to cover if the expression
					// has a left minus
					|| *op == PrefixOperator::Negate && expr.has_left_minus()
					// `-1u8` lexes as one (negative, out of range) literal, so a negated
					// unsigned literal keeps its parentheses
					|| *op == PrefixOperator::Negate && expr.is_unsigned_literal()
				{
					write_sql!(f, fmt, "{op}({expr})");
				} else {
					write_sql!(f, fmt, "{op}{expr}");
				}
			}
			Expr::Postfix {
				expr,
				op,
			} => {
				let expr_bp = BindingPower::for_expr(expr);
				let op_bp = BindingPower::for_postfix_operator(op);
				if expr.needs_parentheses()
					|| expr_bp < op_bp
					|| expr_bp == op_bp && matches!(expr_bp, BindingPower::Range)
					|| matches!(op, PostfixOperator::Call(_))
				{
					write_sql!(f, fmt, "({expr}){op}");
				} else {
					write_sql!(f, fmt, "{expr}{op}");
				}
			}
			Expr::Binary {
				left,
				op,
				right,
			} => {
				let op_bp = BindingPower::for_binary_operator(op);
				let left_bp = BindingPower::for_expr(left);
				let right_bp = BindingPower::for_expr(right);

				// A left-associative operator's default nesting already puts a
				// same-power child on the left (`a - b + c` parses as
				// `(a - b) + c`), so equal power there needs no parentheses.
				// `Power` is right-associative, so the opposite holds: a
				// same-power left child only exists because it was explicitly
				// parenthesised (`(a ** b) ** c`), and dropping the parens
				// would reparse it right-associatively instead.
				if left.needs_parentheses()
					|| left_bp < op_bp
					|| left_bp == op_bp
						&& matches!(
							left_bp,
							BindingPower::Range
								| BindingPower::Relation | BindingPower::Equality
								| BindingPower::Power
						) || matches!(op, BinaryOperator::Matches(_))
					&& left_bp == BindingPower::Relation
				{
					write_sql!(f, fmt, "({left})");
				} else {
					write_sql!(f, fmt, "{left}");
				}

				if matches!(
					op,
					BinaryOperator::Range
						| BinaryOperator::RangeSkip
						| BinaryOperator::RangeInclusive
						| BinaryOperator::RangeSkipInclusive
				) {
					op.fmt_sql(f, fmt);
				} else {
					f.push(' ');
					op.fmt_sql(f, fmt);
					f.push(' ');
				}

				// Mirror image of the left operand: a same-power right child
				// under a left-associative operator only exists because it
				// was explicitly parenthesised (`a - (b + c)`, `a / (b * c)`),
				// since default left-associative nesting never produces one.
				// Dropping the parentheses would reparse it left-associatively
				// instead, changing the value for any non-associative pairing
				// within the group (`-` vs `+`, `/` vs `*`, `??` vs `?:`).
				// `Power` is right-associative, so its default nesting already
				// matches and needs no parentheses here.
				if right.needs_parentheses()
					|| right_bp < op_bp
					|| right_bp == op_bp
						&& matches!(
							right_bp,
							BindingPower::Nullish
								| BindingPower::Range | BindingPower::Relation
								| BindingPower::Equality | BindingPower::AddSub
								| BindingPower::MulDiv
						) || matches!(op, BinaryOperator::Matches(_))
					&& right_bp == BindingPower::Relation
				{
					write_sql!(f, fmt, "({right})");
				} else {
					write_sql!(f, fmt, "{right}");
				}
			}
			Expr::FunctionCall(function_call) => function_call.fmt_sql(f, fmt),
			Expr::Closure(closure) => closure.fmt_sql(f, fmt),
			Expr::Break => f.push_str("BREAK"),
			Expr::Continue => f.push_str("CONTINUE"),
			Expr::Return(x) => x.fmt_sql(f, fmt),
			Expr::Throw(expr) => write_sql!(f, fmt, "THROW {}", CoverStmts(expr.as_ref())),
			Expr::IfElse(s) => s.fmt_sql(f, fmt),
			Expr::Select(s) => s.fmt_sql(f, fmt),
			Expr::Create(s) => s.fmt_sql(f, fmt),
			Expr::Update(s) => s.fmt_sql(f, fmt),
			Expr::Delete(s) => s.fmt_sql(f, fmt),
			Expr::Relate(s) => s.fmt_sql(f, fmt),
			Expr::Insert(s) => s.fmt_sql(f, fmt),
			Expr::Define(s) => s.fmt_sql(f, fmt),
			Expr::Remove(s) => s.fmt_sql(f, fmt),
			Expr::Rebuild(s) => s.fmt_sql(f, fmt),
			Expr::Upsert(s) => s.fmt_sql(f, fmt),
			Expr::Alter(s) => s.fmt_sql(f, fmt),
			Expr::Info(s) => s.fmt_sql(f, fmt),
			Expr::Foreach(s) => s.fmt_sql(f, fmt),
			Expr::Let(s) => s.fmt_sql(f, fmt),
			Expr::Sleep(s) => s.fmt_sql(f, fmt),
			Expr::Explain {
				format: explain_format,
				analyze,
				statement,
			} => {
				f.push_str("EXPLAIN");
				if *analyze {
					f.push_str(" ANALYZE");
				}
				match explain_format {
					ExplainFormat::Text => f.push_str(" FORMAT TEXT"),
					ExplainFormat::Json => f.push_str(" FORMAT JSON"),
				}
				f.push(' ');
				statement.fmt_sql(f, fmt);
			}
		}
	}
}

impl Expr {
	/// Renders canonical catalog text, wrapping statement-shaped subexpressions
	/// for use after a clause keyword and deriving any unprepared credentials.
	pub fn to_stored_sql(&self) -> String {
		crate::expr::CoverStmts(&super::credentials::for_storage(self)).to_sql()
	}
}

impl InfoStructure for Expr {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl Revisioned for Expr {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Expr {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(
			&super::credentials::for_storage(self).to_sql(),
			writer,
		)
	}
}

impl DeserializeRevisioned for Expr {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let query: String = DeserializeRevisioned::deserialize_revisioned(reader)?;

		// The wire format is engine-rendered SurrealQL text, parsed under the
		// stored-text profile and consumed whole, for the reasons
		// `parser::expr_for_definition` documents.
		let expr = crate::parser::expr_for_definition(&query)
			.map_err(|err| revision::Error::Conversion(err.to_string()))?;
		Ok(expr)
	}
}

impl revision::SkipRevisioned for Expr {
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		// Wire format is the SurrealQL source string. Skip its bytes without
		// re-parsing into an `Expr`.
		<String as revision::SkipRevisioned>::skip_revisioned(reader)
	}

	#[inline]
	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> Result<(), revision::Error> {
		<String as revision::SkipRevisioned>::skip_revisioned_slice(reader)
	}
}

impl revision::WalkRevisioned for Expr {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Expr, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for Expr {}

#[cfg(test)]
mod length_prefixed_bytes_tests {
	use revision::{SerializeRevisioned, WalkRevisioned};
	use surrealdb_types::ToSql;

	use crate::expr::Expr;
	use crate::expr::literal::Literal;

	#[test]
	fn expr_with_bytes_matches_serialize() {
		let expr = Expr::Literal(Literal::Integer(42));
		let mut bytes = Vec::new();
		expr.serialize_revisioned(&mut bytes).unwrap();
		let wire_text = expr.to_sql();
		let mut r = bytes.as_slice();
		let walker = Expr::walk_revisioned(&mut r).unwrap();
		let observed = walker.with_bytes(|raw| raw.to_vec()).unwrap();
		assert_eq!(observed.as_slice(), wire_text.as_bytes());
		assert!(r.is_empty());
	}
}
