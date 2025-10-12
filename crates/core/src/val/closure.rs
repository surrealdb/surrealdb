use std::cmp::Ordering;
use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use storekey::{BorrowDecode, Encode};

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Variables};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::{Expr, FlowResultExt, Kind, Param};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Closure {
	pub args: Vec<(Param, Kind)>,
	pub returns: Option<Kind>,
	pub body: Expr,
	pub vars: Variables,
}

impl PartialOrd for Closure {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl Ord for Closure {
	fn cmp(&self, _: &Self) -> Ordering {
		Ordering::Equal
	}
}

impl Closure {
	pub(crate) async fn compute(&self, ctx: &Context) -> Result<Value> {
		let mut closure = self.clone();
		closure.vars.extend(Variables::from_expr(&self.body, ctx));
		Ok(Value::Closure(Box::new(closure)))
	}

	pub(crate) async fn invoke(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
	) -> Result<Value> {
		let mut ctx = MutableContext::new_isolated(ctx);
		ctx.attach_variables(self.vars.clone())?;

		// check for missing arguments.
		if self.args.len() > args.len() {
			if let Some(x) = self.args[args.len()..].iter().find(|x| !x.1.can_be_none()) {
				bail!(Error::InvalidArguments {
					name: "ANONYMOUS".to_string(),
					message: format!("Expected a value for {}", x.0),
				})
			}
		}

		for ((name, kind), val) in self.args.iter().zip(args.into_iter()) {
			if let Ok(val) = val.coerce_to_kind(kind) {
				ctx.add_value(name.clone().into_string(), val.into());
			} else {
				bail!(Error::InvalidArguments {
					name: "ANONYMOUS".to_string(),
					message: format!("Expected a value of type '{kind}' for argument {name}"),
				});
			}
		}

		let ctx = ctx.freeze();
		let result = stk.run(|stk| self.body.compute(stk, &ctx, opt, doc)).await.catch_return()?;
		if let Some(returns) = &self.returns {
			result
				.coerce_to_kind(returns)
				.map_err(|e| Error::ReturnCoerce {
					name: "ANONYMOUS".to_string(),
					error: Box::new(e),
				})
				.map_err(anyhow::Error::new)
		} else {
			Ok(result)
		}
	}
}

impl VisitExpression for Closure {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.body.visit(visitor)
	}
}

impl fmt::Display for Closure {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("|")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "{name}: ")?;
			match kind {
				k @ Kind::Either(_) => write!(f, "<{k}>")?,
				k => write!(f, "{k}")?,
			}
		}
		f.write_str("|")?;
		if let Some(returns) = &self.returns {
			write!(f, " -> {returns}")?;
		}
		write!(f, " {}", self.body)
	}
}

impl<F> Encode<F> for Closure {
	fn encode<W: std::io::Write>(
		&self,
		_: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		Err(storekey::EncodeError::message("Closure cannot be encoded"))
	}
}

impl<'de, F> BorrowDecode<'de, F> for Closure {
	fn borrow_decode(_: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		Err(storekey::DecodeError::message("Closure cannot be decoded"))
	}
}

impl Revisioned for Closure {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Closure {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		_writer: &mut W,
	) -> Result<(), revision::Error> {
		Err(revision::Error::Conversion("Closures cannot be stored on disk".to_string()))
	}
}

impl DeserializeRevisioned for Closure {
	fn deserialize_revisioned<R: std::io::Read>(_reader: &mut R) -> Result<Self, revision::Error> {
		Err(revision::Error::Conversion("Closures cannot be deserialized from disk".to_string()))
	}
}
