#![deny(clippy::wildcard_enum_match_arm)]

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

	/// The values this closure captured by value, exposed so the value-depth
	/// guard can walk them. A closure that captures a value deep-clones it, so a
	/// closure capturing a closure nests exactly as a `Closure::Expr` does and
	/// must be counted the same way, or an accumulator that wraps itself in a
	/// closure each turn escapes the cap and overflows the stack on its own
	/// recursive clone or drop. A closure that captures nothing by value (it
	/// wraps a function pointer, say) returns an empty list, the default.
	fn captured_values(&self) -> Vec<&crate::val::Value> {
		Vec::new()
	}

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
	fn cmp(&self, other: &Self) -> Ordering {
		if self == other {
			return Ordering::Equal;
		}
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
			) => self
				.to_sql()
				.cmp(&other.to_sql())
				.then_with(|| captures1.0.cmp(&captures2.0))
				.then_with(|| args1.len().cmp(&args2.len()))
				.then_with(|| {
					for ((p1, k1), (p2, k2)) in args1.iter().zip(args2.iter()) {
						let ord = p1
							.as_str()
							.cmp(p2.as_str())
							.then_with(|| k1.to_sql().cmp(&k2.to_sql()));
						if ord != Ordering::Equal {
							return ord;
						}
					}
					Ordering::Equal
				})
				.then_with(|| {
					returns1.as_ref().map(ToSql::to_sql).cmp(&returns2.as_ref().map(ToSql::to_sql))
				})
				.then_with(|| body1.to_sql().cmp(&body2.to_sql()))
				.then_with(|| format!("{self:?}").cmp(&format!("{other:?}"))),
			(
				Closure::Expr {
					..
				},
				Closure::Builtin(_),
			) => Ordering::Less,
			(
				Closure::Builtin(_),
				Closure::Expr {
					..
				},
			) => Ordering::Greater,
			(Closure::Builtin(a), Closure::Builtin(b)) => self
				.to_sql()
				.cmp(&other.to_sql())
				.then_with(|| a.captured_values().cmp(&b.captured_values()))
				.then_with(|| Arc::as_ptr(a).cast::<()>().cmp(&Arc::as_ptr(b).cast::<()>())),
		}
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
					if matches!(kind, Kind::Either(_)) {
						write_sql!(f, sql_fmt, "<{kind}>");
					} else {
						write_sql!(f, sql_fmt, "{kind}");
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

std::thread_local! {
	static SPILL_SERIALIZATION_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

const MAX_SPILLED_CLOSURES: usize = 65_536;

struct SpillClosureTable {
	next_id: u64,
	entries: std::collections::VecDeque<(u64, Closure)>,
}

static SPILL_CLOSURES: std::sync::LazyLock<std::sync::Mutex<SpillClosureTable>> =
	std::sync::LazyLock::new(|| {
		std::sync::Mutex::new(SpillClosureTable {
			next_id: 1,
			entries: std::collections::VecDeque::new(),
		})
	});

/// Run `f` with temporary spill serialization enabled for [`Closure`] values on
/// the current thread.
///
/// Outside this scope, [`Closure`]'s `SerializeRevisioned` and
/// `DeserializeRevisioned` implementations refuse to encode or decode closures
/// so they can never be persisted to the datastore.
pub fn with_spill_serialization<R>(f: impl FnOnce() -> R) -> R {
	struct Guard;
	impl Drop for Guard {
		fn drop(&mut self) {
			SPILL_SERIALIZATION_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
		}
	}
	SPILL_SERIALIZATION_DEPTH.with(|d| d.set(d.get() + 1));
	let _guard = Guard;
	f()
}

fn is_spill_serialization_active() -> bool {
	SPILL_SERIALIZATION_DEPTH.with(std::cell::Cell::get) > 0
}

fn stash_spilled_closure(closure: &Closure) -> u64 {
	let mut table = SPILL_CLOSURES.lock().unwrap_or_else(|e| e.into_inner());
	if let Some((id, existing)) = table.entries.back()
		&& existing == closure
	{
		return *id;
	}
	let id = table.next_id;
	table.next_id = table.next_id.wrapping_add(1);
	if table.entries.len() >= MAX_SPILLED_CLOSURES {
		table.entries.pop_front();
	}
	table.entries.push_back((id, closure.clone()));
	id
}

fn take_spilled_closure(id: u64) -> Option<Closure> {
	let table = SPILL_CLOSURES.lock().unwrap_or_else(|e| e.into_inner());
	let first = table.entries.front()?.0;
	let offset = usize::try_from(id.checked_sub(first)?).ok()?;
	let (found_id, closure) = table.entries.get(offset)?;
	(*found_id == id).then(|| closure.clone())
}

impl Revisioned for Closure {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Closure {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		if is_spill_serialization_active() {
			let id = stash_spilled_closure(self);
			return id.serialize_revisioned(writer);
		}
		Err(revision::Error::Conversion("Closures cannot be stored on disk".to_string()))
	}
}

impl DeserializeRevisioned for Closure {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		if is_spill_serialization_active() {
			let id = u64::deserialize_revisioned(reader)?;
			return take_spilled_closure(id).ok_or_else(|| {
				revision::Error::Conversion("Spilled closure handle not found".to_string())
			});
		}
		Err(revision::Error::Conversion("Closures cannot be deserialized from disk".to_string()))
	}
}

impl revision::SkipRevisioned for Closure {
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		if is_spill_serialization_active() {
			return u64::skip_revisioned(reader);
		}
		Err(revision::Error::Conversion("Closures cannot be skipped on the wire".to_string()))
	}

	#[inline]
	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> Result<(), revision::Error> {
		if is_spill_serialization_active() {
			return u64::skip_revisioned_slice(reader);
		}
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use super::*;
	use crate::expr::Literal;
	use crate::val::Value;

	fn expr_closure(param: &str, lit: i64) -> Closure {
		Closure::Expr {
			args: vec![(Param::from(param.to_string()), Kind::Any)],
			returns: None,
			body: Expr::Literal(Literal::Integer(lit)),
			captures: Variables::new(),
		}
	}

	#[test]
	fn closure_ord_is_consistent_with_eq() {
		let c1 = expr_closure("x", 1);
		let c1_dup = expr_closure("x", 1);
		let c2 = expr_closure("x", 2);
		let c3 = expr_closure("y", 1);

		assert_eq!(c1, c1_dup);
		assert_eq!(c1.cmp(&c1_dup), Ordering::Equal);

		assert_ne!(c1, c2);
		assert_ne!(c1.cmp(&c2), Ordering::Equal);
		assert_eq!(c1.cmp(&c2), c2.cmp(&c1).reverse());

		assert_ne!(c1, c3);
		assert_ne!(c1.cmp(&c3), Ordering::Equal);
		assert_eq!(c1.cmp(&c3), c3.cmp(&c1).reverse());

		#[expect(clippy::mutable_key_type, reason = "a closure value is ordered by its definition")]
		let mut set = BTreeSet::new();
		set.insert(Value::Closure(Box::new(c1)));
		set.insert(Value::Closure(Box::new(c1_dup)));
		set.insert(Value::Closure(Box::new(c2)));
		set.insert(Value::Closure(Box::new(c3)));
		assert_eq!(set.len(), 3);
	}

	#[test]
	fn closure_serialization_refused_outside_spill_and_roundtrips_inside_spill() {
		let c = expr_closure("x", 42);
		let mut buf = Vec::new();
		assert!(c.serialize_revisioned(&mut buf).is_err());

		with_spill_serialization(|| {
			c.serialize_revisioned(&mut buf).expect("spill serialization succeeds");
			let mut slice = buf.as_slice();
			let decoded = Closure::deserialize_revisioned(&mut slice)
				.expect("spill deserialization succeeds");
			assert_eq!(decoded, c);
		});

		let mut slice = buf.as_slice();
		assert!(Closure::deserialize_revisioned(&mut slice).is_err());
	}
}
