use crate::{ctx::Context, dbs::Options, doc::CursorDoc, err::Error, sql::value::Value};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::{Ident, Kind};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Closure";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Closure {
	pub args: Vec<(Ident, Kind)>,
	pub returns: Option<Kind>,
	pub body: Value,
}

impl Closure {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
		args: Vec<Value>,
	) -> Result<Value, Error> {
		let mut ctx = Context::new(ctx);
		for (i, (name, kind)) in (&self.args).iter().enumerate() {
			match (kind, args.get(i)) {
				(Kind::Option(_), None) => continue,
				(_, None) => {
					return Err(Error::InvalidArguments {
						name: "ANONYMOUS".to_string(),
						message: format!("Expected a value for ${}", name),
					})
				}
				(kind, Some(val)) => {
					if let Ok(val) = val.to_owned().coerce_to(kind) {
						ctx.add_value(name.to_string(), val);
					} else {
						return Err(Error::InvalidArguments {
							name: "ANONYMOUS".to_string(),
							message: format!("Expected a value of type {kind} for ${}", name),
						});
					}
				}
			}
		}

		self.body.compute(stk, &ctx, opt, doc).await
	}
}

impl fmt::Display for Closure {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("|")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${name}: {kind}")?;
		}
		f.write_str("|")?;
		if let Some(returns) = &self.returns {
			write!(f, " -> {returns}")?;
		}
		write!(f, "{}", self.body)
	}
}
