use std::fmt;
use std::ops::Bound;

use reblessive::Stack;
use reblessive::tree::Stk;
use revision::Revisioned;

use super::SleepStatement;
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::Pretty;
use crate::expr::operator::BindingPower;
use crate::expr::statements::info::InfoStructure;
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, UpdateStatement,
	UpsertStatement,
};
use crate::expr::{
	BinaryOperator, Block, Constant, ControlFlow, FlowResult, FunctionCall, Ident, Idiom, Literal,
	Mock, Param, PostfixOperator, PrefixOperator,
};
use crate::fnc;
use crate::val::{Array, Closure, Range, Strand, Value};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Expr {
	Literal(Literal),
	Param(Param),
	Idiom(Idiom),
	// Maybe move into Literal?
	Table(Ident),
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

	Closure(Box<Closure>),

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
				// TODO: Fix this discrepency and weird static/non-static behavior.
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
			| Expr::Sleep(_) => false,
		}
	}

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

	/// Updates the `"parent"` doc field for statements with a meaning full
	/// document.
	async fn update_parent_doc<F, R>(ctx: &Context, doc: Option<&CursorDoc>, f: F) -> R
	where
		F: AsyncFnOnce(&Context, Option<&CursorDoc>) -> R,
	{
		let mut ctx_store = None;
		let ctx = if let Some(doc) = doc {
			let mut new_ctx = MutableContext::new(ctx);
			new_ctx.add_value("parent", doc.doc.as_ref().clone().into());
			ctx_store.insert(new_ctx.freeze())
		} else {
			ctx
		};

		f(ctx, doc).await
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
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
			Expr::Table(ident) => Ok(Value::Table(ident.clone().into())),
			Expr::Mock(mock) => {
				// NOTE(value pr): This is a breaking change but makes the most sense without
				// having mock be part of the Value type.
				// Mock is mostly used within `CREATE |thing:20|` to create a bunch of entries
				// at one. Here it behaves similar to `CREATE
				// ([thing:1,thing:2,thing:3...])` so when we encounted mock outside of
				// create we return the array here instead.
				//
				let record_ids = mock.clone().into_iter().map(Value::RecordId).collect();
				Ok(Value::Array(Array(record_ids)))
			}
			Expr::Block(block) => block.compute(stk, ctx, &opt, doc).await,
			Expr::Constant(constant) => constant.compute().map_err(ControlFlow::Err),
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
			Expr::Closure(closure) => Ok(Value::Closure(closure.clone())),
			Expr::Break => Err(ControlFlow::Break),
			Expr::Continue => Err(ControlFlow::Continue),
			Expr::Return(output_statement) => output_statement.compute(stk, ctx, &opt, doc).await,
			Expr::Throw(expr) => {
				let res = stk.run(|stk| expr.compute(stk, ctx, &opt, doc)).await?;
				Err(ControlFlow::Err(anyhow::Error::new(Error::Thrown(res.to_raw_string()))))
			}
			Expr::IfElse(ifelse_statement) => ifelse_statement.compute(stk, ctx, &opt, doc).await,
			Expr::Select(select_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					select_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Create(create_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					create_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Update(update_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					update_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Delete(delete_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					delete_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Relate(relate_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					relate_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Insert(insert_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					insert_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Define(define_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					define_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Remove(remove_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					remove_statement.compute(ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Rebuild(rebuild_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					rebuild_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Upsert(upsert_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					upsert_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Alter(alter_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					alter_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
			}
			Expr::Info(info_statement) => {
				Self::update_parent_doc(ctx, doc, async |ctx, doc| {
					info_statement.compute(stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
				})
				.await
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
		}
	}

	async fn compute_prefix(
		stk: &mut Stk,
		ctx: &Context,
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
		ctx: &Context,
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

				if let Value::Object(ref x) = res {
					if let Some(Value::Closure(x)) = x.get(name.as_str()) {
						return x.compute(stk, ctx, opt, doc, args).await.map_err(ControlFlow::Err);
					}
				};
				fnc::idiom(stk, ctx, opt, doc, res, name, args).await.map_err(ControlFlow::Err)
			}
			PostfixOperator::Call(exprs) => {
				let mut args = Vec::new();

				for e in exprs.iter() {
					args.push(stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?);
				}

				if let Value::Closure(x) = res {
					x.compute(stk, ctx, opt, doc, args).await.map_err(ControlFlow::Err)
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
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		expr: &Expr,
	) -> FlowResult<Value> {
		// TODO: The structure here is somewhat convoluted, because knn needs to have
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
			Expr::Table(ident) => ident.to_raw_string(),
			_ => self.to_string(),
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
					|| left_bp == right_bp
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
					|| left_bp == right_bp
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
			Expr::IfElse(s) => write!(f, "{s}"),
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

impl InfoStructure for Expr {
	fn structure(self) -> Value {
		// TODO: null byte validity
		Strand::new(self.to_string()).unwrap().into()
	}
}

impl Revisioned for Expr {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		self.to_string().serialize_revisioned(writer)?;
		Ok(())
	}

	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let query: String = Revisioned::deserialize_revisioned(reader)?;

		let mut stack = Stack::new();
		let mut parser = crate::syn::parser::Parser::new_with_experimental(query.as_bytes(), true);
		let expr = stack
			.enter(|stk| parser.parse_expr(stk))
			.finish()
			.map_err(|err| revision::Error::Conversion(format!("{err:?}")))?;
		Ok(expr.into())
	}
}
