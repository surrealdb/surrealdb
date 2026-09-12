use std::cmp::Ordering;
use std::sync::Arc;

use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::variables::Variables;
use crate::expr::{ClosureExpr, Expr, Kind, Param};

/// An engine-provided closure implementation, opaque to the value layer.
///
/// The value layer holds and clones it but cannot invoke, compare, render,
/// or persist it. The execution layer downcasts via [`Self::as_any`] to the
/// concrete type it constructed in order to invoke the closure.
pub trait BuiltinClosureObj: Send + Sync + 'static {
	fn as_any(&self) -> &dyn std::any::Any;

	/// Render the closure as SurrealQL. An implementation that keeps its
	/// source renders that; the default is an opaque placeholder for a
	/// closure that has none.
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(r#"|| THROW "builtin""#);
	}

	/// Whether `other` denotes the same closure as this one.
	///
	/// Consulted by [`Closure`]'s equality once identity has been ruled out:
	/// an implementation that keeps its source compares it (and whatever it
	/// captured) so two separately created values of the same closure are
	/// equal, as two `Closure::Expr` values are. The default holds that two
	/// distinct objects are distinct closures.
	fn dyn_eq(&self, _other: &dyn BuiltinClosureObj) -> bool {
		false
	}
}

#[derive(Clone)]
pub enum Closure {
	Expr {
		args: Vec<(Param, Kind)>,
		returns: Option<Kind>,
		body: Expr,
		captures: Variables,
	},
	Builtin(Arc<dyn BuiltinClosureObj>),
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
			// The same object is the same closure; otherwise the implementation
			// decides, so a closure value is at least equal to itself and a
			// compiled closure compares structurally.
			(Closure::Builtin(a), Closure::Builtin(b)) => Arc::ptr_eq(a, b) || a.dyn_eq(b.as_ref()),
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
	pub fn into_expr(self) -> Expr {
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
			Closure::Builtin(obj) => obj.fmt_sql(f, sql_fmt),
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

impl revision::SkipRevisioned for Closure {
	fn skip_revisioned<R: std::io::Read>(_reader: &mut R) -> Result<(), revision::Error> {
		Err(revision::Error::Conversion("Closures cannot be skipped on the wire".to_string()))
	}
}

impl revision::WalkRevisioned for Closure {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Closure, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		// Closures never appear on the wire; expose a leaf walker whose
		// `decode`/`skip` invariably error so callers walking from outer
		// types fail fast rather than silently advancing.
		Ok(revision::LeafWalker::new(reader))
	}
}
