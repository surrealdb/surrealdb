use surrealdb_types::{
	Number as PublicNumber, RecordId as PublicRecordId, SqlFormat, ToSql, Value as PublicValue,
	write_sql,
};

use crate::ast::ExplainFormat;
use crate::literal::ObjectEntry;
use crate::lookup::LookupKind;
use crate::operator::BindingPower;
use crate::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, SleepStatement,
	UpdateStatement, UpsertStatement,
};
use crate::{
	BinaryOperator, Block, Closure, Constant, CoverStmts, Dir, FunctionCall, Idiom, Literal, Mock,
	Param, Part, PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit, TableName,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Expr {
	Literal(Literal),

	Param(Param),
	Idiom(Idiom),
	Table(TableName),
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
		analyze: bool,
		statement: Box<Expr>,
	},
}

impl Expr {
	pub fn to_idiom(&self) -> Idiom {
		match self {
			Expr::Idiom(i) => i.simplify(),
			Expr::Param(i) => Idiom::field(i.clone().into_strand()),
			Expr::FunctionCall(x) => x.receiver.to_idiom(),
			Expr::Literal(l) => match l {
				Literal::String(s) => Idiom::field(s.clone()),
				Literal::Datetime(d) => Idiom::field(surrealdb_types::fmt_datetime_sql(*d)),
				x => Idiom::field(x.to_sql()),
			},
			x => Idiom::field(x.to_sql()),
		}
	}

	pub fn from_public_value(value: PublicValue) -> Self {
		match value {
			PublicValue::None => Expr::Literal(Literal::None),
			PublicValue::Null => Expr::Literal(Literal::Null),
			PublicValue::Bool(x) => Expr::Literal(Literal::Bool(x)),
			PublicValue::Number(PublicNumber::Float(x)) => Expr::Literal(Literal::Float(x)),
			PublicValue::Number(PublicNumber::Int(x)) => Expr::Literal(Literal::Integer(x)),
			PublicValue::Number(PublicNumber::Decimal(x)) => Expr::Literal(Literal::Decimal(x)),
			PublicValue::String(x) => Expr::Literal(Literal::String(x.into())),
			PublicValue::Bytes(x) => Expr::Literal(Literal::Bytes(x.into_inner())),
			PublicValue::Regex(x) => Expr::Literal(Literal::Regex(x.into_inner())),
			PublicValue::Table(x) => Expr::Table(TableName::new(x.into_string())),
			PublicValue::RecordId(PublicRecordId {
				table,
				key,
			}) => Expr::Literal(Literal::RecordId(RecordIdLit {
				table: TableName::new(table.into_string()),
				key: RecordIdKeyLit::from_record_id_key(key),
			})),
			PublicValue::Array(x) => {
				Expr::Literal(Literal::Array(x.into_iter().map(Expr::from_public_value).collect()))
			}
			PublicValue::Set(x) => {
				Expr::Literal(Literal::Set(x.into_iter().map(Expr::from_public_value).collect()))
			}
			PublicValue::Object(x) => Expr::Literal(Literal::Object(
				x.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k.into(),
						value: Expr::from_public_value(v),
					})
					.collect(),
			)),
			PublicValue::Duration(x) => Expr::Literal(Literal::Duration(x.into_inner())),
			PublicValue::Datetime(x) => Expr::Literal(Literal::Datetime(x.into_inner())),
			PublicValue::Uuid(x) => Expr::Literal(Literal::Uuid(x.into_inner())),
			PublicValue::Geometry(x) => Expr::Literal(Literal::Geometry(geo::Geometry::from(x))),
			PublicValue::File(x) => Expr::Literal(Literal::File(x.into())),
			PublicValue::Range(x) => convert_public_range_to_literal(*x),
		}
	}

	// NOTE: Changes to this function also likely require changes to
	// `surrealdb_expr::expr::Expr::needs_parentheses`.
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

fn convert_public_range_to_literal(range: surrealdb_types::Range) -> Expr {
	use crate::literal::Literal;
	use crate::operator::BinaryOperator;

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

impl ToSql for Expr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Expr::Literal(literal) => literal.fmt_sql(f, fmt),
			Expr::Param(param) => param.fmt_sql(f, fmt),
			Expr::Idiom(idiom) => idiom.fmt_sql(f, fmt),
			Expr::Table(ident) => write_sql!(f, fmt, "{ident}"),
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
