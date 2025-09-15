use std::fmt;

use crate::sql::fmt::Pretty;
use crate::sql::literal::ObjectEntry;
use crate::sql::operator::BindingPower;
use crate::sql::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, SleepStatement,
	UpdateStatement, UpsertStatement,
};
use crate::sql::{
	BinaryOperator, Block, Closure, Constant, FunctionCall, Ident, Idiom, Literal, Mock, Param,
	PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use crate::val::{Number, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Expr {
	Literal(Literal),

	Param(Param),
	Idiom(Idiom),
	Table(Ident),
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
	If(Box<IfelseStatement>),
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
}

impl Expr {
	pub(crate) fn to_idiom(&self) -> Idiom {
		match self {
			Expr::Idiom(i) => i.simplify(),
			Expr::Param(i) => Idiom::field(i.clone().ident()),
			Expr::FunctionCall(x) => x.receiver.to_idiom(),
			Expr::Literal(l) => match l {
				Literal::Strand(s) => Idiom::field(Ident::from_strand(s.clone())),
				// TODO: Null byte validity
				Literal::Datetime(d) => Idiom::field(Ident::new(d.into_raw_string()).unwrap()),
				x => Idiom::field(Ident::new(x.to_string()).unwrap()),
			},
			x => Idiom::field(Ident::new(x.to_string()).unwrap()),
		}
	}

	pub(crate) fn from_value(value: Value) -> Self {
		match value {
			Value::None => Expr::Literal(Literal::None),
			Value::Null => Expr::Literal(Literal::Null),
			Value::Bool(x) => Expr::Literal(Literal::Bool(x)),
			Value::Number(Number::Float(x)) => Expr::Literal(Literal::Float(x)),
			Value::Number(Number::Int(x)) => Expr::Literal(Literal::Integer(x)),
			Value::Number(Number::Decimal(x)) => Expr::Literal(Literal::Decimal(x)),
			Value::Strand(x) => Expr::Literal(Literal::Strand(x)),
			Value::Bytes(x) => Expr::Literal(Literal::Bytes(x)),
			Value::Regex(x) => Expr::Literal(Literal::Regex(x)),
			Value::RecordId(x) => Expr::Literal(Literal::RecordId(RecordIdLit {
				table: x.table.clone(),
				key: RecordIdKeyLit::from_record_id_key(x.key),
			})),
			Value::Array(x) => {
				Expr::Literal(Literal::Array(x.into_iter().map(Expr::from_value).collect()))
			}
			Value::Object(x) => Expr::Literal(Literal::Object(
				x.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k,
						value: Expr::from_value(v),
					})
					.collect(),
			)),
			Value::Duration(x) => Expr::Literal(Literal::Duration(x)),
			Value::Datetime(x) => Expr::Literal(Literal::Datetime(x)),
			Value::Uuid(x) => Expr::Literal(Literal::Uuid(x)),
			Value::Geometry(x) => Expr::Literal(Literal::Geometry(x)),
			Value::File(x) => Expr::Literal(Literal::File(x)),
			Value::Closure(x) => Expr::Literal(Literal::Closure(Box::new(Closure {
				args: x.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
				returns: x.returns.map(|k| k.into()),
				body: x.body.into(),
			}))),
			Value::Table(x) => Expr::Table(unsafe { Ident::new_unchecked(x.into_string()) }),
			Value::Range(x) => x.into_literal().into(),
		}
	}
}

impl fmt::Display for Expr {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use std::fmt::Write;
		let mut f = Pretty::from(f);
		match self {
			Expr::Literal(literal) => write!(f, "{literal}"),
			Expr::Param(param) => write!(f, "{param}"),
			Expr::Idiom(idiom) => write!(f, "{idiom}"),
			Expr::Table(ident) => write!(f, "{ident}"),
			Expr::Mock(mock) => write!(f, "{mock}"),
			Expr::Block(block) => write!(f, "{block}"),
			Expr::Constant(constant) => write!(f, "{constant}"),
			Expr::Prefix {
				op,
				expr,
			} => {
				let expr_bp = BindingPower::for_expr(expr);
				let op_bp = BindingPower::for_prefix_operator(op);
				if expr_bp < op_bp || expr_bp == op_bp && matches!(expr_bp, BindingPower::Range) {
					write!(f, "{op}({expr})")
				} else {
					write!(f, "{op}{expr}")
				}
			}
			Expr::Postfix {
				expr,
				op,
			} => {
				let expr_bp = BindingPower::for_expr(expr);
				let op_bp = BindingPower::for_postfix_operator(op);
				if expr_bp < op_bp || expr_bp == op_bp && matches!(expr_bp, BindingPower::Range) {
					write!(f, "({expr}){op}")
				} else {
					write!(f, "{expr}{op}")
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

				if left_bp < op_bp
					|| left_bp == op_bp
						&& matches!(left_bp, BindingPower::Range | BindingPower::Relation)
				{
					write!(f, "({left})")?;
				} else {
					write!(f, "{left}")?;
				}

				if matches!(
					op,
					BinaryOperator::Range
						| BinaryOperator::RangeSkip
						| BinaryOperator::RangeInclusive
						| BinaryOperator::RangeSkipInclusive
				) {
					write!(f, "{op}")?;
				} else {
					write!(f, " {op} ")?;
				}

				if right_bp < op_bp
					|| right_bp == op_bp
						&& matches!(right_bp, BindingPower::Range | BindingPower::Relation)
				{
					write!(f, "({right})")
				} else {
					write!(f, "{right}")
				}
			}
			Expr::FunctionCall(function_call) => write!(f, "{function_call}"),
			Expr::Closure(closure) => write!(f, "{closure}"),
			Expr::Break => write!(f, "BREAK"),
			Expr::Continue => write!(f, "CONTINUE"),
			Expr::Return(x) => write!(f, "{x}"),
			Expr::Throw(expr) => write!(f, "THROW {expr}"),
			Expr::If(s) => write!(f, "{s}"),
			Expr::Select(s) => write!(f, "{s}"),
			Expr::Create(s) => write!(f, "{s}"),
			Expr::Update(s) => write!(f, "{s}"),
			Expr::Delete(s) => write!(f, "{s}"),
			Expr::Relate(s) => write!(f, "{s}"),
			Expr::Insert(s) => write!(f, "{s}"),
			Expr::Define(s) => write!(f, "{s}"),
			Expr::Remove(s) => write!(f, "{s}"),
			Expr::Rebuild(s) => write!(f, "{s}"),
			Expr::Upsert(s) => write!(f, "{s}"),
			Expr::Alter(s) => write!(f, "{s}"),
			Expr::Info(s) => write!(f, "{s}"),
			Expr::Foreach(s) => write!(f, "{s}"),
			Expr::Let(s) => write!(f, "{s}"),
			Expr::Sleep(s) => write!(f, "{s}"),
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
			Expr::If(s) => crate::expr::Expr::IfElse(Box::new((*s).into())),
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
		}
	}
}

impl From<crate::expr::Expr> for Expr {
	fn from(v: crate::expr::Expr) -> Self {
		match v {
			crate::expr::Expr::Literal(l) => Expr::Literal(l.into()),
			crate::expr::Expr::Param(p) => Expr::Param(p.into()),
			crate::expr::Expr::Idiom(i) => Expr::Idiom(i.into()),
			crate::expr::Expr::Table(t) => Expr::Table(t.into()),
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
			crate::expr::Expr::IfElse(s) => Expr::If(Box::new((*s).into())),
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
		}
	}
}
