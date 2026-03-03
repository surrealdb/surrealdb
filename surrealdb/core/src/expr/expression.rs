use std::ops::Bound;

use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{RecordId, SqlFormat, ToSql};

use super::SleepStatement;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::closure::ClosureExpr;
use crate::expr::statements::info::InfoStructure;
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, UpdateStatement,
	UpsertStatement,
};
use crate::expr::{
	BinaryOperator, Block, Constant, ControlFlow, FlowResult, FunctionCall, Idiom, Literal, Mock,
	ObjectEntry, Param, PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use crate::fnc;
use crate::types::PublicValue;
use crate::val::{Array, Range, TableName, Value};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, Default)]
pub(crate) enum ExplainFormat {
	#[default]
	Text,
	Json,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Expr {
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

impl Expr {
	/// Check if this expression does only reads.
	pub(crate) fn read_only(&self) -> bool {
		match self {
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Info(_)
			| Expr::Sleep(_) => true,

			Expr::Idiom(x) => x.read_only(),
			Expr::Block(block) => block.read_only(),
			Expr::Prefix {
				expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			} => expr.read_only(),
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
			Expr::Closure(_) => true,
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
			},
			surrealdb_types::Value::String(s) => Expr::Literal(Literal::String(s)),
			surrealdb_types::Value::Bytes(b) => {
				Expr::Literal(Literal::Bytes(crate::val::Bytes(b.into_inner())))
			}
			surrealdb_types::Value::Duration(d) => {
				Expr::Literal(Literal::Duration(crate::val::Duration(d.into_inner())))
			}
			surrealdb_types::Value::Datetime(d) => {
				Expr::Literal(Literal::Datetime(crate::val::Datetime(d.into_inner())))
			}
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
						key: k,
						value: Expr::from_public_value(v),
					})
					.collect(),
			)),
			surrealdb_types::Value::Table(t) => Expr::Table(t.into()),
			surrealdb_types::Value::RecordId(RecordId {
				table,
				key,
			}) => {
				let key_lit = match key {
					surrealdb_types::RecordIdKey::Number(n) => RecordIdKeyLit::Number(n),
					surrealdb_types::RecordIdKey::String(s) => RecordIdKeyLit::String(s),
					surrealdb_types::RecordIdKey::Uuid(u) => {
						RecordIdKeyLit::Uuid(crate::val::Uuid(u.into_inner()))
					}
					surrealdb_types::RecordIdKey::Array(a) => {
						RecordIdKeyLit::Array(a.into_iter().map(Expr::from_public_value).collect())
					}
					surrealdb_types::RecordIdKey::Object(o) => RecordIdKeyLit::Object(
						o.into_iter()
							.map(|(k, v)| ObjectEntry {
								key: k,
								value: Expr::from_public_value(v),
							})
							.collect(),
					),
					_ => return Expr::Literal(Literal::None), // For unsupported key types
				};
				Expr::Literal(Literal::RecordId(RecordIdLit {
					table: table.into(),
					key: key_lit,
				}))
			}
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
			// Invalid ranges with unbounded start but bounded in a way we can't represent
			// start>.. (excluded start with no end) - not valid in SurrealQL
			(Bound::Excluded(_), Bound::Unbounded) | (Bound::Included(_), Bound::Unbounded) => {
				Expr::Literal(Literal::None)
			}
		}
	}
}

impl Expr {
	/// Checks if a expression is 'pure' i.e. does not rely on the environment.
	pub(crate) fn is_static(&self) -> bool {
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

	pub(crate) fn to_idiom(&self) -> Idiom {
		match self {
			Expr::Idiom(i) => i.simplify(),
			Expr::Param(i) => Idiom::field(i.as_str().to_owned()),
			Expr::FunctionCall(x) => x.receiver.to_idiom(),
			Expr::Literal(l) => match l {
				Literal::String(s) => Idiom::field(s.clone()),
				Literal::Datetime(d) => Idiom::field(d.to_string()),
				x => Idiom::field(x.to_sql()),
			},
			x => Idiom::field(x.to_sql()),
		}
	}

	/// Process this type returning a computed simple Value
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Expr::compute", skip_all)
	)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		let opt = opt.dive(1).map_err(anyhow::Error::new)?;

		match self {
			Expr::Literal(literal) => literal.compute(stk, ctx, &opt, doc).await,
			Expr::Param(param) => {
				param.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Idiom(idiom) => idiom.compute(stk, ctx, &opt, doc).await,
			Expr::Table(ident) => Ok(Value::Table(ident.clone())),
			Expr::Mock(mock) => {
				// NOTE(value pr): This is a breaking change but makes the most sense without
				// having mock be part of the Value type.
				// Mock is mostly used within `CREATE |thing:20|` to create a bunch of entries
				// at one. Here it behaves similar to `CREATE
				// ([thing:1,thing:2,thing:3...])` so when we encounted mock outside of
				// create we return the array here instead.
				//

				let gen_limit = ctx.config().limits.generation_allocation_limit;
				let iter = mock.clone().into_iter();
				if iter
					.size_hint()
					.1
					.map(|x| x.saturating_mul(std::mem::size_of::<Value>()) > gen_limit)
					.unwrap_or(true)
				{
					return Err(ControlFlow::Err(anyhow::Error::msg(
						"Mock range exceeds allocation limit",
					)));
				}
				let record_ids = iter.map(Value::RecordId).collect();
				Ok(Value::Array(Array(record_ids)))
			}
			Expr::Block(block) => block.compute(stk, ctx, &opt, doc).await,
			Expr::Constant(constant) => Ok(constant.compute()),
			Expr::Prefix {
				op,
				expr,
			} => Self::compute_prefix(stk, ctx, &opt, doc, op, expr).await,
			Expr::Postfix {
				expr,
				op,
			} => Self::compute_postfix(stk, ctx, &opt, doc, expr, op).await,
			Expr::Binary {
				..
			} => Self::compute_binary(stk, ctx, &opt, doc, self).await,
			Expr::FunctionCall(function_call) => function_call.compute(stk, ctx, &opt, doc).await,
			Expr::Closure(closure) => Ok(closure.compute(ctx).await?),
			Expr::Break => Err(ControlFlow::Break),
			Expr::Continue => Err(ControlFlow::Continue),
			Expr::Return(output_statement) => output_statement.compute(stk, ctx, &opt, doc).await,
			Expr::Throw(expr) => {
				let res = stk.run(|stk| expr.compute(stk, ctx, &opt, doc)).await?;
				Err(ControlFlow::Err(anyhow::Error::new(Error::Thrown(res.to_raw_string()))))
			}
			Expr::IfElse(ifelse_statement) => ifelse_statement.compute(stk, ctx, &opt, doc).await,
			Expr::Select(select_statement) => {
				select_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Create(create_statement) => {
				create_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Update(update_statement) => {
				update_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Delete(delete_statement) => {
				delete_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Relate(relate_statement) => {
				relate_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Insert(insert_statement) => {
				insert_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Define(define_statement) => {
				define_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Remove(remove_statement) => {
				remove_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Rebuild(rebuild_statement) => {
				rebuild_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Upsert(upsert_statement) => {
				upsert_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Alter(alter_statement) => {
				alter_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Info(info_statement) => {
				info_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Foreach(foreach_statement) => {
				foreach_statement.compute(stk, ctx, &opt, doc).await
			}
			Expr::Let(_) => {
				//TODO: This error needs to be improved or it needs to be rejected in the
				// parser.
				Err(ControlFlow::Err(anyhow::Error::new(Error::unreachable(
					"Set statement outside of block",
				))))
			}
			Expr::Sleep(sleep_statement) => {
				sleep_statement.compute(ctx, &opt, doc).await.map_err(ControlFlow::Err)
			}
			Expr::Explain {
				..
			} => Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatement(
				"EXPLAIN is only supported with the new execution model".to_string(),
			)))),
		}
	}

	async fn compute_prefix(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		op: &PrefixOperator,
		expr: &Expr,
	) -> FlowResult<Value> {
		let res = stk.run(|stk| expr.compute(stk, ctx, opt, doc)).await?;

		match op {
			PrefixOperator::Not => fnc::operate::not(res).map_err(ControlFlow::Err),
			PrefixOperator::Positive => Ok(res),
			PrefixOperator::Negate => fnc::operate::neg(res).map_err(ControlFlow::Err),
			PrefixOperator::Range => Ok(Value::Range(Box::new(Range {
				start: Bound::Unbounded,
				end: Bound::Excluded(res),
			}))),
			PrefixOperator::RangeInclusive => Ok(Value::Range(Box::new(Range {
				start: Bound::Unbounded,
				end: Bound::Included(res),
			}))),
			PrefixOperator::Cast(kind) => res
				.cast_to_kind(kind)
				.map_err(Error::from)
				.map_err(anyhow::Error::new)
				.map_err(ControlFlow::Err),
		}
	}

	async fn compute_postfix(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		expr: &Expr,
		op: &PostfixOperator,
	) -> FlowResult<Value> {
		let res = stk.run(|stk| expr.compute(stk, ctx, opt, doc)).await?;
		match op {
			PostfixOperator::Range => Ok(Value::Range(Box::new(Range {
				start: Bound::Included(res),
				end: Bound::Unbounded,
			}))),
			PostfixOperator::RangeSkip => Ok(Value::Range(Box::new(Range {
				start: Bound::Excluded(res),
				end: Bound::Unbounded,
			}))),
			PostfixOperator::MethodCall(name, exprs) => {
				let mut args = Vec::new();

				for e in exprs.iter() {
					args.push(stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?);
				}

				if let Value::Object(ref x) = res
					&& let Some(Value::Closure(x)) = x.get(name.as_str())
				{
					return x.invoke(stk, ctx, opt, doc, args).await.map_err(ControlFlow::Err);
				};
				fnc::idiom(stk, ctx, opt, doc, res, name, args).await.map_err(ControlFlow::Err)
			}
			PostfixOperator::Call(exprs) => {
				let mut args = Vec::new();

				for e in exprs.iter() {
					args.push(stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?);
				}

				if let Value::Closure(x) = res {
					x.invoke(stk, ctx, opt, doc, args).await.map_err(ControlFlow::Err)
				} else {
					Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidFunction {
						name: "ANONYMOUS".to_string(),
						message: format!("'{}' is not a function", res.kind_of()),
					})))
				}
			}
		}
	}

	async fn compute_binary(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		expr: &Expr,
	) -> FlowResult<Value> {
		// NOTE: The structure here is somewhat convoluted, because knn needs to have
		// access to the expression itself instead of just the op and left/right
		// expressions we need to pass in the parent expression when encountering a
		// binary expression and then match again here. Ideally knn should be able to
		// be called more naturally.
		let Expr::Binary {
			left,
			op,
			right,
		} = expr
		else {
			unreachable!()
		};

		if let BinaryOperator::NearestNeighbor(_) = op {
			return fnc::operate::knn(stk, ctx, opt, doc, expr).await.map_err(ControlFlow::Err);
		}

		let left = stk.run(|stk| left.compute(stk, ctx, opt, doc)).await?;

		let res = match op {
			BinaryOperator::Subtract => {
				fnc::operate::sub(left, stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::Add => {
				fnc::operate::add(left, stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::Multiply => {
				fnc::operate::mul(left, stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::Divide => {
				fnc::operate::div(left, stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::Remainder => {
				fnc::operate::rem(left, stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::Power => {
				fnc::operate::pow(left, stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::Equal => {
				fnc::operate::equal(&left, &stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::ExactEqual => {
				fnc::operate::exact(&left, &stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?)
			}
			BinaryOperator::NotEqual => fnc::operate::not_equal(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::AllEqual => fnc::operate::all_equal(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::AnyEqual => fnc::operate::any_equal(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::Or | BinaryOperator::TenaryCondition => {
				if left.is_truthy() {
					return Ok(left);
				}
				return stk.run(|stk| right.compute(stk, ctx, opt, doc)).await;
			}
			BinaryOperator::And => {
				if !left.is_truthy() {
					return Ok(left);
				}
				return stk.run(|stk| right.compute(stk, ctx, opt, doc)).await;
			}
			BinaryOperator::NullCoalescing => {
				if !left.is_nullish() {
					return Ok(left);
				}
				return stk.run(|stk| right.compute(stk, ctx, opt, doc)).await;
			}
			BinaryOperator::LessThan => fnc::operate::less_than(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::LessThanEqual => fnc::operate::less_than_or_equal(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::MoreThan => fnc::operate::more_than(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::MoreThanEqual => fnc::operate::more_than_or_equal(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::Contain => fnc::operate::contain(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::NotContain => fnc::operate::not_contain(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::ContainAll => fnc::operate::contain_all(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::ContainAny => fnc::operate::contain_any(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::ContainNone => fnc::operate::contain_none(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::Inside => fnc::operate::inside(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::NotInside => fnc::operate::not_inside(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::AllInside => fnc::operate::inside_all(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::AnyInside => fnc::operate::inside_any(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::NoneInside => fnc::operate::inside_none(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::Outside => fnc::operate::outside(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::Intersects => fnc::operate::intersects(
				&left,
				&stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?,
			),
			BinaryOperator::Matches(_) => {
				let right = stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?;
				fnc::operate::matches(stk, ctx, opt, doc, expr, left, right).await
			}
			BinaryOperator::NearestNeighbor(_) => unreachable!(),
			BinaryOperator::Range => {
				let right = stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?;
				Ok(Value::Range(Box::new(Range {
					start: Bound::Included(left),
					end: Bound::Excluded(right),
				})))
			}
			BinaryOperator::RangeInclusive => {
				let right = stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?;
				Ok(Value::Range(Box::new(Range {
					start: Bound::Included(left),
					end: Bound::Included(right),
				})))
			}
			BinaryOperator::RangeSkip => {
				let right = stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?;
				Ok(Value::Range(Box::new(Range {
					start: Bound::Excluded(left),
					end: Bound::Excluded(right),
				})))
			}
			BinaryOperator::RangeSkipInclusive => {
				let right = stk.run(|stk| right.compute(stk, ctx, opt, doc)).await?;
				Ok(Value::Range(Box::new(Range {
					start: Bound::Excluded(left),
					end: Bound::Included(right),
				})))
			}
		};

		res.map_err(ControlFlow::Err)
	}

	pub(crate) fn to_raw_string(&self) -> String {
		match self {
			Expr::Idiom(idiom) => idiom.to_raw_string(),
			Expr::Table(ident) => ident.clone().into_string(),
			_ => self.to_sql(),
		}
	}

	// NOTE: Changes to this function also likely require changes to
	// crate::sql::Expr::needs_parentheses
	/// Returns if this expression needs to be parenthesized when inside another expression.
	#[allow(dead_code)]
	fn needs_parentheses(&self) -> bool {
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
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_) => false,
		}
	}
}

impl ToSql for Expr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_expr: crate::sql::Expr = self.clone().into();
		sql_expr.fmt_sql(f, fmt);
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
		SerializeRevisioned::serialize_revisioned(&self.to_sql(), writer)
	}
}

impl DeserializeRevisioned for Expr {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let query: String = DeserializeRevisioned::deserialize_revisioned(reader)?;

		let expr = crate::syn::parse_with_settings(
			query.as_bytes(),
			crate::syn::parser::ParserSettings {
				files_enabled: true,
				surrealism_enabled: true,
				..Default::default()
			},
			async |p, stk| p.parse_expr(stk).await,
		)
		.map_err(|err| revision::Error::Conversion(err.to_string()))?;
		Ok(expr.into())
	}
}
