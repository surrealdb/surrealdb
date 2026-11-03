//! A scalar function call, as the expressions of a write evaluate it.
//!
//! A read lowers a call to a [`LogicalExpr`] over bound arguments, which is the one place that
//! knows what each registered function answers in each dialect. A write holds [`Expr`]s, so a call
//! is lowered by the read lowering and converted: each argument that is not a literal is a
//! placeholder parameter in the lowering, and the conversion puts the argument's expression in its
//! place. A read and a write therefore answer a function through one lowering.

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalIf};
use surrealdb_expr::expr::statements::{IfelseStatement, SetStatement};
use surrealdb_expr::expr::{
	Block, ClosureExpr, Expr, FunctionCall, Idiom, Kind, Literal, ObjectEntry, Param, Part,
	PrefixOperator,
};
use surrealdb_strand::Strand;

use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{BoundExpr, BoundLiteral};
use crate::lower::expr::lower_bound_expr;

/// The name of the parameter that stands for the argument at an index while a call is lowered.
const ARGUMENT: &str = "__sql_argument_";

/// The prefix of the names of the parameters a conversion binds.
const BINDING: &str = "__sql_binding_";

/// The call of the function `name` over `args` in `dialect`, as an expression a write evaluates.
pub(crate) fn lower_scalar_call(
	dialect: SqlDialect,
	name: &str,
	args: &[Expr],
) -> Result<Expr, PlanError> {
	let policy = match dialect {
		SqlDialect::Postgres => DialectPolicy::postgres(),
		SqlDialect::MySql => DialectPolicy::mysql(),
	};
	let call = BoundExpr::FunctionCall {
		name: name.to_owned(),
		args: args.iter().enumerate().map(|(index, arg)| bound_argument(index, arg)).collect(),
		distinct: false,
	};
	let logical = lower_bound_expr(&call, &policy)?;
	let mut conversion = Conversion {
		args,
		bindings: 0,
	};
	conversion.convert(&logical, None)
}

/// A literal argument as the literal, so that a lowering that reads its argument at plan time
/// does; any other argument as a placeholder.
fn bound_argument(index: usize, arg: &Expr) -> BoundExpr {
	match literal_argument(arg) {
		Some(literal) => BoundExpr::Literal(literal),
		None => BoundExpr::Param {
			index,
			name: format!("{ARGUMENT}{index}"),
		},
	}
}

fn literal_argument(arg: &Expr) -> Option<BoundLiteral> {
	let (negated, inner) = if let Expr::Prefix {
		op: PrefixOperator::Negate,
		expr,
	} = arg
	{
		(true, &**expr)
	} else {
		(false, arg)
	};
	let Expr::Literal(literal) = inner else {
		return None;
	};
	if let Literal::Integer(value) = literal {
		return if negated {
			value.checked_neg().map(BoundLiteral::Integer)
		} else {
			Some(BoundLiteral::Integer(*value))
		};
	}
	if let Literal::Float(value) = literal {
		return Some(BoundLiteral::Float(if negated {
			-value
		} else {
			*value
		}));
	}
	if let Literal::Decimal(value) = literal {
		return Some(BoundLiteral::Numeric(if negated {
			(-*value).to_string()
		} else {
			value.to_string()
		}));
	}
	if negated {
		return None;
	}
	if let Literal::String(value) = literal {
		return Some(BoundLiteral::String(value.to_string()));
	}
	if let Literal::Bool(value) = literal {
		return Some(BoundLiteral::Boolean(*value));
	}
	if matches!(literal, Literal::Null) {
		return Some(BoundLiteral::Null);
	}
	None
}

struct Conversion<'a> {
	args: &'a [Expr],
	bindings: usize,
}

impl Conversion<'_> {
	fn unsupported(what: &str) -> PlanError {
		PlanError::UnsupportedExpression(format!("{what} in a function lowered for a write"))
	}

	fn fresh(&mut self) -> Param {
		self.bindings += 1;
		Param::from(format!("{BINDING}{}", self.bindings))
	}

	/// `expr` as an expression a write evaluates, where `current` is the expression that stands
	/// for [`LogicalExpr::Current`].
	fn convert(&mut self, expr: &LogicalExpr, current: Option<&Expr>) -> Result<Expr, PlanError> {
		let all = |this: &mut Self, exprs: &[LogicalExpr]| -> Result<Vec<Expr>, PlanError> {
			exprs.iter().map(|expr| this.convert(expr, current)).collect()
		};
		Ok(match expr {
			LogicalExpr::Literal(literal) => Expr::Literal(literal.to_expr_literal()),
			LogicalExpr::Array(items) => Expr::Literal(Literal::Array(all(self, items)?)),
			LogicalExpr::Object(entries) => {
				let mut converted = Vec::with_capacity(entries.len());
				for (key, value) in entries {
					converted.push(ObjectEntry {
						key: Strand::from(key.as_str()),
						value: self.convert(value, current)?,
					});
				}
				Expr::Literal(Literal::Object(converted))
			}
			LogicalExpr::Param(param) => {
				match param.strip_prefix(ARGUMENT).and_then(|index| index.parse::<usize>().ok()) {
					Some(index) => self.args.get(index).cloned().ok_or_else(|| {
						PlanError::UnsupportedExpression(format!(
							"argument index {index} out of bounds"
						))
					})?,
					None => Expr::Param(param.clone()),
				}
			}
			LogicalExpr::Constant(constant) => Expr::Constant(constant.clone()),
			LogicalExpr::Field(name) => Expr::Idiom(Idiom(vec![Part::Field(name.clone())])),
			LogicalExpr::Current => {
				current.cloned().ok_or_else(|| Self::unsupported("a reference to the value"))?
			}
			LogicalExpr::Access {
				base,
				field,
			} => extend(self.convert(base, current)?, Part::Field(field.clone())),
			LogicalExpr::Index {
				base,
				index,
			} => {
				let base = self.convert(base, current)?;
				extend(base, Part::Value(self.convert(index, current)?))
			}
			LogicalExpr::Prefix {
				op,
				expr,
			} => Expr::Prefix {
				op: op.clone(),
				expr: Box::new(self.convert(expr, current)?),
			},
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => Expr::Binary {
				left: Box::new(self.convert(left, current)?),
				op: op.clone(),
				right: Box::new(self.convert(right, current)?),
			},
			LogicalExpr::Call(call) => Expr::FunctionCall(Box::new(FunctionCall {
				receiver: call.receiver.clone(),
				arguments: all(self, &call.args)?,
			})),
			LogicalExpr::If(branches) => self.convert_if(branches, current)?,
			LogicalExpr::Throw(message) => Expr::Throw(Box::new(self.convert(message, current)?)),
			LogicalExpr::Optional {
				base,
				then,
			} => self.convert_optional(base, then, current)?,
			LogicalExpr::Filter {
				base,
				predicate,
			} => {
				let base = self.convert(base, current)?;
				let element = self.fresh();
				let body = self.convert(predicate, Some(&Expr::Param(element.clone())))?;
				call("array::filter", vec![base, closure(element, body)])
			}
			LogicalExpr::Broadcast {
				base,
				body,
				flatten: false,
			} => {
				let base = self.convert(base, current)?;
				let element = self.fresh();
				let body = self.convert(body, Some(&Expr::Param(element.clone())))?;
				call("array::map", vec![base, closure(element, body)])
			}
			LogicalExpr::Set(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Doc
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Closure(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => {
				return Err(Self::unsupported("an expression with no write form"));
			}
		})
	}

	fn convert_if(
		&mut self,
		branches: &LogicalIf,
		current: Option<&Expr>,
	) -> Result<Expr, PlanError> {
		let mut exprs = Vec::with_capacity(branches.branches.len());
		for (condition, value) in &branches.branches {
			exprs.push((self.convert(condition, current)?, self.convert(value, current)?));
		}
		let close = match &branches.otherwise {
			Some(otherwise) => Some(self.convert(otherwise, current)?),
			None => None,
		};
		Ok(Expr::IfElse(Box::new(IfelseStatement {
			exprs,
			close,
		})))
	}

	/// `base`, evaluated once, whose value `then` reads as the current value; NONE or NULL when
	/// `base` is.
	fn convert_optional(
		&mut self,
		base: &LogicalExpr,
		then: &LogicalExpr,
		current: Option<&Expr>,
	) -> Result<Expr, PlanError> {
		let value = self.convert(base, current)?;
		let name = self.fresh();
		let bound = Expr::Param(name.clone());
		let body = self.convert(then, Some(&bound))?;
		let body = if matches!(base, LogicalExpr::Array(_) | LogicalExpr::Literal(_))
			&& !matches!(base, LogicalExpr::Literal(l) if matches!(l.to_expr_literal(), Literal::None | Literal::Null))
		{
			body
		} else {
			Expr::IfElse(Box::new(IfelseStatement {
				exprs: vec![
					(call("type::is_none", vec![bound.clone()]), Expr::Literal(Literal::None)),
					(call("type::is_null", vec![bound]), Expr::Literal(Literal::Null)),
				],
				close: Some(body),
			}))
		};
		Ok(Expr::Block(Box::new(Block(vec![
			Expr::Let(Box::new(SetStatement {
				name: Strand::from(name.as_str()),
				what: value,
				kind: None,
			})),
			body,
		]))))
	}
}

fn call(name: &str, arguments: Vec<Expr>) -> Expr {
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: surrealdb_expr::expr::Function::Normal(name.to_owned()),
		arguments,
	}))
}

fn closure(param: Param, body: Expr) -> Expr {
	Expr::Closure(Box::new(ClosureExpr {
		args: vec![(param, Kind::Any)],
		returns: None,
		body,
	}))
}

/// `base` followed by the path step `part`.
fn extend(mut base: Expr, part: Part) -> Expr {
	if let Expr::Idiom(Idiom(parts)) = &mut base {
		parts.push(part);
		return base;
	}
	Expr::Idiom(Idiom(vec![Part::Start(base), part]))
}
