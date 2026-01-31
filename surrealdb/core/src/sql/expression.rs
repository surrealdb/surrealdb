use std::ops::Bound;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmts, EscapeIdent};
use crate::sql::ast::ExplainFormat;
use crate::sql::literal::ObjectEntry;
use crate::sql::lookup::LookupKind;
use crate::sql::operator::BindingPower;
use crate::sql::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, SleepStatement,
	UpdateStatement, UpsertStatement,
};
use crate::sql::{
	BinaryOperator, Block, Closure, Constant, Dir, FunctionCall, Idiom, Literal, Mock, Param, Part,
	PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use crate::types::{PublicFile, PublicNumber, PublicRecordId, PublicValue};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Expr {
	Literal(Literal),

	Param(Param),
	Idiom(Idiom),
	Table(String),
	Mock(Mock),
	// TODO(3.0) maybe unbox? check size.
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
	Closure(Box<Closure>),

	Break,
	Continue,
	Throw(Box<Expr>),

	Return(Box<OutputStatement>),
	IfElse(Box<IfelseStatement>),
	Select(Box<SelectStatement>),
	Create(Box<CreateStatement>),
	Update(Box<UpdateStatement>),
	Delete(Box<DeleteStatement>),
	Relate(Box<RelateStatement>),
	Insert(Box<InsertStatement>),
	Define(Box<DefineStatement>),
	Remove(Box<RemoveStatement>),
	Rebuild(Box<RebuildStatement>),
	Upsert(Box<UpsertStatement>),
	Alter(Box<AlterStatement>),
	Info(Box<InfoStatement>),
	Foreach(Box<ForeachStatement>),
	Let(Box<SetStatement>),
	Sleep(Box<SleepStatement>),
	Explain {
		format: ExplainFormat,
		statement: Box<Expr>,
	},
}

impl Expr {
	pub(crate) fn to_idiom(&self) -> Idiom {
		match self {
			Expr::Idiom(i) => i.simplify(),
			Expr::Param(i) => Idiom::field(i.clone().into_string()),
			Expr::FunctionCall(x) => x.receiver.to_idiom(),
			Expr::Literal(l) => match l {
				Literal::String(s) => Idiom::field(s.clone()),
				Literal::Datetime(d) => Idiom::field(d.to_string()),
				x => Idiom::field(x.to_sql()),
			},
			x => Idiom::field(x.to_sql()),
		}
	}

	pub(crate) fn from_public_value(value: PublicValue) -> Self {
		match value {
			PublicValue::None => Expr::Literal(Literal::None),
			PublicValue::Null => Expr::Literal(Literal::Null),
			PublicValue::Bool(x) => Expr::Literal(Literal::Bool(x)),
			PublicValue::Number(PublicNumber::Float(x)) => Expr::Literal(Literal::Float(x)),
			PublicValue::Number(PublicNumber::Int(x)) => Expr::Literal(Literal::Integer(x)),
			PublicValue::Number(PublicNumber::Decimal(x)) => Expr::Literal(Literal::Decimal(x)),
			PublicValue::String(x) => Expr::Literal(Literal::String(x)),
			PublicValue::Bytes(x) => Expr::Literal(Literal::Bytes(x)),
			PublicValue::Regex(x) => Expr::Literal(Literal::Regex(x)),
			PublicValue::Table(x) => Expr::Table(x.into_string()),
			PublicValue::RecordId(PublicRecordId {
				table,
				key,
			}) => Expr::Literal(Literal::RecordId(RecordIdLit {
				table: table.into_string(),
				key: RecordIdKeyLit::from_record_id_key(key),
			})),
			PublicValue::Array(x) => {
				Expr::Literal(Literal::Array(x.into_iter().map(Expr::from_public_value).collect()))
			}
			PublicValue::Set(x) => {
				// Convert set to array for literal representation since there's no set literal
				// syntax
				Expr::Literal(Literal::Array(x.into_iter().map(Expr::from_public_value).collect()))
			}
			PublicValue::Object(x) => Expr::Literal(Literal::Object(
				x.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k,
						value: Expr::from_public_value(v),
					})
					.collect(),
			)),
			PublicValue::Duration(x) => Expr::Literal(Literal::Duration(x)),
			PublicValue::Datetime(x) => Expr::Literal(Literal::Datetime(x)),
			PublicValue::Uuid(x) => Expr::Literal(Literal::Uuid(x)),
			PublicValue::Geometry(x) => Expr::Literal(Literal::Geometry(x)),
			PublicValue::File(x) => Expr::Literal(Literal::File(PublicFile::new(x.bucket, x.key))),
			PublicValue::Range(x) => convert_public_range_to_literal(*x),
		}
	}

	// NOTE: Changes to this function also likely require changes to
	// crate::expr::Expr::needs_parentheses
	/// Returns if this expression needs to be parenthesized when inside another expression.
	pub(crate) fn needs_parentheses(&self) -> bool {
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
			Expr::Literal(Literal::None) | Expr::Literal(Literal::Null) => true,
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
			_ => false,
		}
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
			Expr::Idiom(x) => {
				if let Some(x) = x.0.first()
					&& let Part::Graph(lookup) = x
					&& let LookupKind::Graph(Dir::Out) = lookup.kind
				{
					return true;
				}
				false
			}
			_ => false,
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
			_ => false,
		}
	}
}

fn convert_public_geometry_to_internal(geom: surrealdb_types::Geometry) -> crate::val::Geometry {
	match geom {
		surrealdb_types::Geometry::Point(p) => crate::val::Geometry::Point(p),
		surrealdb_types::Geometry::Line(l) => crate::val::Geometry::Line(l),
		surrealdb_types::Geometry::Polygon(p) => crate::val::Geometry::Polygon(p),
		surrealdb_types::Geometry::MultiPoint(mp) => crate::val::Geometry::MultiPoint(mp),
		surrealdb_types::Geometry::MultiLine(ml) => crate::val::Geometry::MultiLine(ml),
		surrealdb_types::Geometry::MultiPolygon(mp) => crate::val::Geometry::MultiPolygon(mp),
		surrealdb_types::Geometry::Collection(c) => crate::val::Geometry::Collection(
			c.into_iter().map(convert_public_geometry_to_internal).collect(),
		),
	}
}

fn convert_public_range_to_literal(range: surrealdb_types::Range) -> Expr {
	use crate::sql::literal::Literal;
	use crate::sql::operator::BinaryOperator;

	let range = range.into_inner();

	// Determine the operator first before moving the values
	let op = match (&range.0, &range.1) {
		(std::ops::Bound::Included(_), std::ops::Bound::Included(_)) => {
			BinaryOperator::RangeInclusive
		}
		_ => BinaryOperator::Range,
	};

	let start_expr = match range.0 {
		std::ops::Bound::Included(v) => Expr::from_public_value(v),
		std::ops::Bound::Excluded(v) => Expr::from_public_value(v),
		std::ops::Bound::Unbounded => Expr::Literal(Literal::None),
	};

	let end_expr = match range.1 {
		std::ops::Bound::Included(v) => Expr::from_public_value(v),
		std::ops::Bound::Excluded(v) => Expr::from_public_value(v),
		std::ops::Bound::Unbounded => Expr::Literal(Literal::None),
	};

	Expr::Binary {
		left: Box::new(start_expr),
		op,
		right: Box::new(end_expr),
	}
}

pub(crate) fn convert_public_value_to_internal(value: surrealdb_types::Value) -> crate::val::Value {
	match value {
		surrealdb_types::Value::None => crate::val::Value::None,
		surrealdb_types::Value::Null => crate::val::Value::Null,
		surrealdb_types::Value::Bool(b) => crate::val::Value::Bool(b),
		surrealdb_types::Value::Number(n) => match n {
			surrealdb_types::Number::Int(i) => {
				crate::val::Value::Number(crate::val::Number::Int(i))
			}
			surrealdb_types::Number::Float(f) => {
				crate::val::Value::Number(crate::val::Number::Float(f))
			}
			surrealdb_types::Number::Decimal(d) => {
				crate::val::Value::Number(crate::val::Number::Decimal(d))
			}
		},
		surrealdb_types::Value::String(s) => crate::val::Value::String(s),
		surrealdb_types::Value::Duration(d) => {
			crate::val::Value::Duration(crate::val::Duration(d.into_inner()))
		}
		surrealdb_types::Value::Datetime(dt) => {
			crate::val::Value::Datetime(crate::val::Datetime(dt.into_inner()))
		}
		surrealdb_types::Value::Uuid(u) => {
			crate::val::Value::Uuid(crate::val::Uuid(u.into_inner()))
		}
		surrealdb_types::Value::Array(a) => crate::val::Value::Array(crate::val::Array::from(
			a.into_iter().map(convert_public_value_to_internal).collect::<Vec<_>>(),
		)),
		surrealdb_types::Value::Set(s) => crate::val::Value::Set(crate::val::Set::from(
			s.into_iter()
				.map(convert_public_value_to_internal)
				.collect::<std::collections::BTreeSet<_>>(),
		)),
		surrealdb_types::Value::Object(o) => crate::val::Value::Object(crate::val::Object::from(
			o.into_iter()
				.map(|(k, v)| (k, convert_public_value_to_internal(v)))
				.collect::<std::collections::BTreeMap<_, _>>(),
		)),
		surrealdb_types::Value::Geometry(g) => {
			crate::val::Value::Geometry(convert_public_geometry_to_internal(g))
		}
		surrealdb_types::Value::Bytes(b) => {
			crate::val::Value::Bytes(crate::val::Bytes(b.into_inner()))
		}
		surrealdb_types::Value::Table(t) => crate::val::Value::Table(t.into()),
		surrealdb_types::Value::RecordId(PublicRecordId {
			table,
			key,
		}) => {
			let key = convert_public_record_id_key_to_internal(key);
			crate::val::Value::RecordId(crate::val::RecordId {
				table: table.into(),
				key,
			})
		}
		surrealdb_types::Value::File(f) => crate::val::Value::File(crate::val::File {
			bucket: f.bucket,
			key: f.key,
		}),
		surrealdb_types::Value::Range(r) => crate::val::Value::Range(Box::new(crate::val::Range {
			start: match r.start {
				Bound::Included(v) => Bound::Included(convert_public_value_to_internal(v)),
				Bound::Excluded(v) => Bound::Excluded(convert_public_value_to_internal(v)),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match r.end {
				Bound::Included(v) => Bound::Included(convert_public_value_to_internal(v)),
				Bound::Excluded(v) => Bound::Excluded(convert_public_value_to_internal(v)),
				Bound::Unbounded => Bound::Unbounded,
			},
		})),
		surrealdb_types::Value::Regex(r) => {
			crate::val::Value::Regex(crate::val::Regex(r.into_inner()))
		}
	}
}

fn convert_public_record_id_key_to_internal(
	key: surrealdb_types::RecordIdKey,
) -> crate::val::RecordIdKey {
	match key {
		surrealdb_types::RecordIdKey::Number(n) => crate::val::RecordIdKey::Number(n),
		surrealdb_types::RecordIdKey::String(s) => crate::val::RecordIdKey::String(s),
		surrealdb_types::RecordIdKey::Uuid(u) => {
			crate::val::RecordIdKey::Uuid(crate::val::Uuid(u.into_inner()))
		}
		surrealdb_types::RecordIdKey::Array(a) => crate::val::RecordIdKey::Array(
			crate::val::Array(a.into_iter().map(convert_public_value_to_internal).collect()),
		),
		surrealdb_types::RecordIdKey::Object(o) => {
			crate::val::RecordIdKey::Object(crate::val::Object(
				o.into_iter().map(|(k, v)| (k, convert_public_value_to_internal(v))).collect(),
			))
		}
		surrealdb_types::RecordIdKey::Range(r) => {
			crate::val::RecordIdKey::Range(Box::new(crate::val::RecordIdKeyRange {
				start: match r.start {
					Bound::Included(k) => {
						Bound::Included(convert_public_record_id_key_to_internal(k))
					}
					Bound::Excluded(k) => {
						Bound::Excluded(convert_public_record_id_key_to_internal(k))
					}
					Bound::Unbounded => Bound::Unbounded,
				},
				end: match r.end {
					Bound::Included(k) => {
						Bound::Included(convert_public_record_id_key_to_internal(k))
					}
					Bound::Excluded(k) => {
						Bound::Excluded(convert_public_record_id_key_to_internal(k))
					}
					Bound::Unbounded => Bound::Unbounded,
				},
			}))
		}
	}
}

impl ToSql for Expr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Expr::Literal(literal) => literal.fmt_sql(f, fmt),
			Expr::Param(param) => param.fmt_sql(f, fmt),
			Expr::Idiom(idiom) => idiom.fmt_sql(f, fmt),
			Expr::Table(ident) => write_sql!(f, fmt, "{}", EscapeIdent(ident)),
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

				if left.needs_parentheses()
					|| left_bp < op_bp
					|| left_bp == op_bp
						&& matches!(
							left_bp,
							BindingPower::Range | BindingPower::Relation | BindingPower::Equality
						) {
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

				if right.needs_parentheses()
					|| right_bp < op_bp
					|| right_bp == op_bp
						&& matches!(
							right_bp,
							BindingPower::Range | BindingPower::Relation | BindingPower::Equality
						) {
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
				statement,
			} => {
				f.push_str("EXPLAIN");
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

impl From<Expr> for crate::expr::Expr {
	fn from(v: Expr) -> Self {
		match v {
			Expr::Literal(l) => crate::expr::Expr::Literal(l.into()),
			Expr::Param(p) => crate::expr::Expr::Param(p.into()),
			Expr::Idiom(i) => crate::expr::Expr::Idiom(i.into()),
			Expr::Table(t) => crate::expr::Expr::Table(t.into()),
			Expr::Mock(m) => crate::expr::Expr::Mock(m.into()),
			Expr::Block(b) => crate::expr::Expr::Block(Box::new((*b).into())),
			Expr::Constant(c) => crate::expr::Expr::Constant(c.into()),
			Expr::Prefix {
				op,
				expr,
			} => crate::expr::Expr::Prefix {
				op: op.into(),
				expr: Box::new((*expr).into()),
			},
			Expr::Postfix {
				op,
				expr,
			} => crate::expr::Expr::Postfix {
				op: op.into(),
				expr: Box::new((*expr).into()),
			},

			Expr::Binary {
				left,
				op,
				right,
			} => crate::expr::Expr::Binary {
				left: Box::new((*left).into()),
				op: op.into(),
				right: Box::new((*right).into()),
			},
			Expr::FunctionCall(f) => crate::expr::Expr::FunctionCall(Box::new((*f).into())),
			Expr::Closure(s) => crate::expr::Expr::Closure(Box::new((*s).into())),
			Expr::Break => crate::expr::Expr::Break,
			Expr::Continue => crate::expr::Expr::Continue,
			Expr::Return(e) => crate::expr::Expr::Return(Box::new((*e).into())),
			Expr::Throw(e) => crate::expr::Expr::Throw(Box::new((*e).into())),
			Expr::IfElse(s) => crate::expr::Expr::IfElse(Box::new((*s).into())),
			Expr::Select(s) => crate::expr::Expr::Select(Box::new((*s).into())),
			Expr::Create(s) => crate::expr::Expr::Create(Box::new((*s).into())),
			Expr::Update(s) => crate::expr::Expr::Update(Box::new((*s).into())),
			Expr::Delete(s) => crate::expr::Expr::Delete(Box::new((*s).into())),
			Expr::Relate(s) => crate::expr::Expr::Relate(Box::new((*s).into())),
			Expr::Insert(s) => crate::expr::Expr::Insert(Box::new((*s).into())),
			Expr::Define(s) => crate::expr::Expr::Define(Box::new((*s).into())),
			Expr::Remove(s) => crate::expr::Expr::Remove(Box::new((*s).into())),
			Expr::Rebuild(s) => crate::expr::Expr::Rebuild(Box::new((*s).into())),
			Expr::Upsert(s) => crate::expr::Expr::Upsert(Box::new((*s).into())),
			Expr::Alter(s) => crate::expr::Expr::Alter(Box::new((*s).into())),
			Expr::Info(s) => crate::expr::Expr::Info(Box::new((*s).into())),
			Expr::Foreach(s) => crate::expr::Expr::Foreach(Box::new((*s).into())),
			Expr::Let(s) => crate::expr::Expr::Let(Box::new((*s).into())),
			Expr::Sleep(s) => crate::expr::Expr::Sleep(Box::new((*s).into())),
			Expr::Explain {
				format,
				statement,
			} => crate::expr::Expr::Explain {
				format: format.into(),
				statement: Box::new((*statement).into()),
			},
		}
	}
}

impl From<crate::expr::Expr> for Expr {
	fn from(v: crate::expr::Expr) -> Self {
		match v {
			crate::expr::Expr::Literal(l) => Expr::Literal(l.into()),
			crate::expr::Expr::Param(p) => Expr::Param(p.into()),
			crate::expr::Expr::Idiom(i) => Expr::Idiom(i.into()),
			crate::expr::Expr::Table(t) => Expr::Table(t.into_string()),
			crate::expr::Expr::Mock(m) => Expr::Mock(m.into()),
			crate::expr::Expr::Block(b) => Expr::Block(Box::new((*b).into())),
			crate::expr::Expr::Constant(c) => Expr::Constant(c.into()),
			crate::expr::Expr::Prefix {
				op,
				expr,
			} => Expr::Prefix {
				op: op.into(),
				expr: Box::new((*expr).into()),
			},
			crate::expr::Expr::Postfix {
				expr,
				op,
			} => Expr::Postfix {
				expr: Box::new((*expr).into()),
				op: op.into(),
			},

			crate::expr::Expr::Binary {
				left,
				op,
				right,
			} => Expr::Binary {
				left: Box::new((*left).into()),
				op: op.into(),
				right: Box::new((*right).into()),
			},
			crate::expr::Expr::FunctionCall(f) => Expr::FunctionCall(Box::new((*f).into())),
			crate::expr::Expr::Closure(s) => Expr::Closure(Box::new((*s).into())),
			crate::expr::Expr::Break => Expr::Break,
			crate::expr::Expr::Continue => Expr::Continue,
			crate::expr::Expr::Return(e) => Expr::Return(Box::new((*e).into())),
			crate::expr::Expr::Throw(e) => Expr::Throw(Box::new((*e).into())),
			crate::expr::Expr::IfElse(s) => Expr::IfElse(Box::new((*s).into())),
			crate::expr::Expr::Select(s) => Expr::Select(Box::new((*s).into())),
			crate::expr::Expr::Create(s) => Expr::Create(Box::new((*s).into())),
			crate::expr::Expr::Update(s) => Expr::Update(Box::new((*s).into())),
			crate::expr::Expr::Delete(s) => Expr::Delete(Box::new((*s).into())),
			crate::expr::Expr::Relate(s) => Expr::Relate(Box::new((*s).into())),
			crate::expr::Expr::Insert(s) => Expr::Insert(Box::new((*s).into())),
			crate::expr::Expr::Define(s) => Expr::Define(Box::new((*s).into())),
			crate::expr::Expr::Remove(s) => Expr::Remove(Box::new((*s).into())),
			crate::expr::Expr::Rebuild(s) => Expr::Rebuild(Box::new((*s).into())),
			crate::expr::Expr::Upsert(s) => Expr::Upsert(Box::new((*s).into())),
			crate::expr::Expr::Alter(s) => Expr::Alter(Box::new((*s).into())),
			crate::expr::Expr::Info(s) => Expr::Info(Box::new((*s).into())),
			crate::expr::Expr::Foreach(s) => Expr::Foreach(Box::new((*s).into())),
			crate::expr::Expr::Let(s) => Expr::Let(Box::new((*s).into())),
			crate::expr::Expr::Sleep(s) => Expr::Sleep(Box::new((*s).into())),
			crate::expr::Expr::Explain {
				format,
				statement,
			} => Expr::Explain {
				format: format.into(),
				statement: Box::new((*statement).into()),
			},
		}
	}
}
