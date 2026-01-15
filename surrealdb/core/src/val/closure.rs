use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Variables};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{ClosureExpr, Expr, FlowResultExt, Kind, Param};
use crate::fnc::args::Any;
use crate::val::Value;

pub(crate) type BuiltinClosure = Arc<
	dyn for<'a> Fn(
			&'a mut Stk,
			&'a FrozenContext,
			&'a Options,
			Option<&'a CursorDoc>,
			Any,
		) -> Pin<Box<dyn Future<Output = Result<Value>> + 'a>>
		+ Send
		+ Sync,
>;

#[derive(Clone)]
pub(crate) enum Closure {
	Expr {
		args: Vec<(Param, Kind)>,
		returns: Option<Kind>,
		body: Expr,
		captures: Variables,
	},
	Builtin(BuiltinClosure),
}

impl std::fmt::Debug for Closure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Closure::Expr {
				args,
				returns,
				body,
				captures,
			} => f
				.debug_struct("Closure::Expr")
				.field("args", args)
				.field("returns", returns)
				.field("body", body)
				.field("captures", captures)
				.finish(),
			Closure::Builtin(_) => {
				f.debug_tuple("Closure::Builtin")
					// .field("_")
					.finish()
			}
		}
	}
}

impl PartialEq for Closure {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(
				Closure::Expr {
					args: args1,
					returns: returns1,
					body: body1,
					captures: captures1,
				},
				Closure::Expr {
					args: args2,
					returns: returns2,
					body: body2,
					captures: captures2,
				},
			) => args1 == args2 && returns1 == returns2 && body1 == body2 && captures1 == captures2,
			(
				Closure::Builtin {
					..
				},
				Closure::Builtin {
					..
				},
			) => {
				// Builtin closures cannot be meaningfully compared
				false
			}
			_ => false,
		}
	}
}

impl Eq for Closure {}

impl std::hash::Hash for Closure {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		match self {
			Closure::Expr {
				args,
				returns,
				body,
				captures,
			} => {
				0u8.hash(state); // discriminant
				args.hash(state);
				returns.hash(state);
				body.hash(state);
				captures.hash(state);
			}
			Closure::Builtin(_) => {
				1u8.hash(state); // discriminant
				// Note: logic is not hashed as function pointers cannot be hashed
			}
		}
	}
}

impl PartialOrd for Closure {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Closure {
	fn cmp(&self, _other: &Self) -> Ordering {
		// Builtin closures cannot be meaningfully compared
		Ordering::Equal
	}
}

impl Closure {
	pub(crate) async fn invoke(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
	) -> Result<Value> {
		match self {
			Closure::Expr {
				args: arg_spec,
				returns,
				body,
				captures,
			} => {
				let mut ctx = Context::new_isolated(ctx);
				ctx.attach_variables(captures.clone())?;

				// check for missing arguments.
				if arg_spec.len() > args.len()
					&& let Some(x) = arg_spec[args.len()..].iter().find(|x| !x.1.can_be_none())
				{
					bail!(Error::InvalidArguments {
						name: "ANONYMOUS".to_string(),
						message: format!("Expected a value for {}", x.0.to_sql()),
					})
				}

				for ((name, kind), val) in arg_spec.iter().zip(args.into_iter()) {
					if let Ok(val) = val.coerce_to_kind(kind) {
						ctx.add_value(name.clone().into_string(), val.into());
					} else {
						bail!(Error::InvalidArguments {
							name: "ANONYMOUS".to_string(),
							message: format!(
								"Expected a value of type '{}' for argument {}",
								kind.to_sql(),
								name.to_sql()
							),
						});
					}
				}

				let ctx = ctx.freeze();
				let result =
					stk.run(|stk| body.compute(stk, &ctx, opt, doc)).await.catch_return()?;
				if let Some(returns) = &returns {
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
			Closure::Builtin(logic) => {
				// Wrap args in Any - the builtin function will handle
				// argument validation and conversion using FromArgs::from_args
				let args = Any(args);

				// Call the builtin function
				logic(stk, ctx, opt, doc, args).await
			}
		}
	}

	pub(crate) fn into_expr(self) -> Expr {
		match self {
			Closure::Expr {
				args,
				returns,
				body,
				..
			} => Expr::Closure(Box::new(ClosureExpr {
				args,
				returns,
				body,
			})),
			Closure::Builtin {
				..
			} => Expr::Closure(Box::new(ClosureExpr {
				args: vec![],
				returns: None,
				body: Expr::Block(Box::default()),
			})),
		}
	}
}

impl ToSql for Closure {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Closure::Expr {
				args,
				returns,
				body,
				..
			} => {
				write_sql!(f, sql_fmt, "|");
				for (i, (name, kind)) in args.iter().enumerate() {
					if i > 0 {
						write_sql!(f, sql_fmt, ", ");
					}
					write_sql!(f, sql_fmt, "{name}: ");
					match kind {
						k @ Kind::Either(_) => write_sql!(f, sql_fmt, "<{k}>"),
						k => write_sql!(f, sql_fmt, "{k}"),
					}
				}
				write_sql!(f, sql_fmt, "|");
				if let Some(returns) = &returns {
					write_sql!(f, sql_fmt, " -> {returns}");
				}
				write_sql!(f, sql_fmt, " {}", body);
			}
			Closure::Builtin {
				..
			} => {
				write_sql!(f, sql_fmt, r#"|| THROW "builtin""#);
			}
		}
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
