//! A `DEFINE FUNCTION` definition compiled once per node and shared across
//! transactions.
//!
//! Every transaction reads the definitions of the functions its statements
//! call through its own snapshot, and each such read yields a runtime
//! definition whose body is compiled to its logical form on first use. That
//! compile answers the same way for every transaction that reads the same
//! stored definition, so it is paid once per node and per definition here,
//! and the facts a planner asks of a body — its shape, what it reads, what it
//! calls — are computed beside it, once.
//!
//! The unit of sharing is one function under one content hash. A definition
//! is a single key, so instead of a version stamp beside it the entry is
//! content-addressed: the key is the hash of the stored definition as read
//! through the requesting transaction. A transaction reading a redefined
//! function hashes to a fresh key and misses; one reading its own uncommitted
//! redefinition does the same, and the entry it publishes is served only to
//! a transaction that reads the identical definition, which is the one
//! transaction's own or a later committed one with the same text. A rolled
//! back redefinition is never read again and its entry ages out unread. A
//! `VERSION`ed read has no shared form and goes to the transaction.

use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use anyhow::Result;
use surrealdb_catalog::providers::DatabaseProvider;
use surrealdb_catalog::{DatabaseId, Error as CatalogError, FunctionDefinition, NamespaceId};
use surrealdb_expr::expr::Function;
use surrealdb_expr::expr::logical::{
	LogicalEval, LogicalExpr, LogicalOp, LogicalReturn, LogicalSequence, LogicalSource, SourceRef,
};

use super::{DatastoreCache, Entry, Lookup};
use crate::tx::Transaction;

/// The parameters a body reads the calling row through, whichever statement
/// it is called from.
const ROW_PARAMS: &[&str] = &["this", "self", "parent"];

/// One function's compiled definition, its lowered body and the facts a
/// planner reads off it, as every transaction that reads the same stored
/// definition sees them.
#[derive(Debug)]
pub struct FunctionSnapshot {
	definition: Arc<FunctionDefinition>,
	facts: FunctionFacts,
	hash: u64,
}

/// What a planner asks of a function body without walking it again. The
/// declared facts — permission, `AUTH LIMIT`, argument kinds, return kind —
/// are read from the definition itself.
#[derive(Debug)]
pub struct FunctionFacts {
	/// The expression a body of the form `{ RETURN e }` or `{ e }` yields, in
	/// terms of the function's `$arguments`; `None` for any other body, a
	/// body with no logical form included.
	pub return_expr: Option<LogicalExpr>,
	/// Whether some expression of the body reads the calling row other than
	/// through the arguments: a field by name, `@`, a row parameter, a graph
	/// lookup, a nested statement that supplies or reads rows, or a call whose
	/// body runs against the row (a user-defined, script, module or silo
	/// function). A builtin call is taken as reading only its arguments; the
	/// planner's function registry is what names the builtins that read the
	/// row by a name their arguments evaluate to. `true` for a body with no
	/// logical form.
	pub reads_row: bool,
	/// The user-defined functions the body calls, at any depth, each named
	/// once and without its `fn::` prefix. Empty for a body with no logical
	/// form.
	pub calls: Vec<String>,
}

impl FunctionSnapshot {
	/// The snapshot of `fn::name` as `txn` sees it, or `None` when no such
	/// function is defined.
	///
	/// One read of the definition through `txn`, one cache lookup under the
	/// definition's content hash, and a compile on a miss, published under
	/// the hash for the next transaction on the node. Without a cache the
	/// definition is compiled and nothing is shared. Any other failure to
	/// read the definition is the error it is.
	pub async fn for_function(
		cache: Option<&DatastoreCache>,
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		name: &str,
	) -> Result<Option<Arc<Self>>> {
		let definition = match txn.get_db_function(ns, db, name, None).await {
			Ok(definition) => definition,
			Err(e) => {
				if matches!(e.downcast_ref::<CatalogError>(), Some(CatalogError::FcNotFound { .. }))
				{
					return Ok(None);
				}
				return Err(e);
			}
		};
		let hash = content_hash(&definition);
		if let Some(cache) = cache {
			let lookup = Lookup::Function(ns, db, name, hash);
			if let Some(entry) = cache.get(&lookup).map(Entry::try_into_function).transpose()? {
				return Ok(Some(entry));
			}
		}
		let snapshot = Arc::new(Self::compile(definition, hash));
		if let Some(cache) = cache {
			cache.insert(
				Lookup::Function(ns, db, name, hash),
				Entry::Function(Arc::clone(&snapshot)),
			);
		}
		Ok(Some(snapshot))
	}

	/// Compiles `definition`'s body and reads the facts off it.
	fn compile(definition: Arc<FunctionDefinition>, hash: u64) -> Self {
		let facts = match definition.logical_body() {
			Ok(body) => FunctionFacts {
				return_expr: return_expr(body).cloned(),
				reads_row: reads_row(body),
				calls: calls(body),
			},
			Err(_) => FunctionFacts {
				return_expr: None,
				reads_row: true,
				calls: Vec::new(),
			},
		};
		Self {
			definition,
			facts,
			hash,
		}
	}

	/// The compiled definition, its body compiled to logical form once for
	/// the life of the snapshot ([`FunctionDefinition::logical_body`]).
	pub fn definition(&self) -> &Arc<FunctionDefinition> {
		&self.definition
	}

	/// The body in logical form; `Err` for a body with no logical form.
	pub fn body(&self) -> Result<&LogicalOp> {
		self.definition.logical_body()
	}

	/// The facts read off the body.
	pub fn facts(&self) -> &FunctionFacts {
		&self.facts
	}

	/// The hash of the stored definition this snapshot was compiled from; two
	/// snapshots of identical definitions share it.
	pub fn content_hash(&self) -> u64 {
		self.hash
	}

	/// The hash a snapshot of `definition` carries, so a plan compiled from
	/// one snapshot can tell whether a definition read later is the same one.
	pub fn hash_of(definition: &FunctionDefinition) -> u64 {
		content_hash(definition)
	}
}

/// The hash of `definition` as stored: its name, arguments, body text,
/// return kind, permissions and `AUTH LIMIT`. Identity is the text, so a
/// definition hashes the same whether or not its body has been compiled.
fn content_hash(definition: &FunctionDefinition) -> u64 {
	let mut hasher = DefaultHasher::new();
	definition.hash(&mut hasher);
	hasher.finish()
}

/// The expression a `{ RETURN e }` or `{ e }` body yields; see
/// [`FunctionFacts::return_expr`].
fn return_expr(body: &LogicalOp) -> Option<&LogicalExpr> {
	let LogicalOp::Sequence(LogicalSequence {
		stmts,
	}) = body
	else {
		return None;
	};
	match stmts.as_slice() {
		[
			LogicalOp::Return(LogicalReturn {
				what,
				fetch,
			}),
		] if fetch.is_empty() => match what.as_ref() {
			LogicalOp::Eval(LogicalEval {
				expr,
			}) => Some(expr),
			_ => None,
		},
		[
			LogicalOp::Eval(LogicalEval {
				expr,
			}),
		] => Some(expr),
		_ => None,
	}
}

/// Whether some scalar of `body` reads the calling row; see
/// [`FunctionFacts::reads_row`].
fn reads_row(body: &LogicalOp) -> bool {
	let mut found = false;
	visit_scalars_deep(body, &mut |scalar| {
		if found {
			return;
		}
		found = scalar
			.visit(&mut |e| {
				let reads = match e {
					LogicalExpr::Field(_)
					| LogicalExpr::Doc
					| LogicalExpr::Current
					| LogicalExpr::Lookup {
						..
					}
					| LogicalExpr::Subquery(_) => true,
					LogicalExpr::Param(param) => ROW_PARAMS.contains(&param.as_str()),
					LogicalExpr::Call(call) => match &call.receiver {
						Function::Normal(_) | Function::Model(_) => false,
						Function::Custom(_)
						| Function::Script(_)
						| Function::Module(..)
						| Function::Silo {
							..
						} => true,
					},
					LogicalExpr::Literal(_)
					| LogicalExpr::Array(_)
					| LogicalExpr::Set(_)
					| LogicalExpr::Object(_)
					| LogicalExpr::RecordId(_)
					| LogicalExpr::Table(_)
					| LogicalExpr::Constant(_)
					| LogicalExpr::Access {
						..
					}
					| LogicalExpr::Index {
						..
					}
					| LogicalExpr::First(_)
					| LogicalExpr::Last(_)
					| LogicalExpr::Each(_)
					| LogicalExpr::Flatten {
						..
					}
					| LogicalExpr::Filter {
						..
					}
					| LogicalExpr::Method {
						..
					}
					| LogicalExpr::Destructure {
						..
					}
					| LogicalExpr::Optional {
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
					| LogicalExpr::Prefix {
						..
					}
					| LogicalExpr::Postfix {
						..
					}
					| LogicalExpr::Invoke {
						..
					}
					| LogicalExpr::Binary {
						..
					}
					| LogicalExpr::Closure(_)
					| LogicalExpr::If(_)
					| LogicalExpr::Throw(_)
					| LogicalExpr::Break
					| LogicalExpr::Continue => false,
				};
				if reads {
					std::ops::ControlFlow::Break(())
				} else {
					std::ops::ControlFlow::Continue(())
				}
			})
			.is_break();
	});
	found
}

/// The user-defined functions `body` calls, at any depth; see
/// [`FunctionFacts::calls`].
fn calls(body: &LogicalOp) -> Vec<String> {
	let mut seen = HashSet::new();
	let mut out = Vec::new();
	visit_scalars_deep(body, &mut |scalar| {
		let _ = scalar.visit_deep(&mut |e| {
			if let LogicalExpr::Call(call) = e
				&& let Function::Custom(name) = &call.receiver
				&& seen.insert(name.clone())
			{
				out.push(name.clone());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
	});
	out
}

/// Applies `f` to every scalar of `op`, the scalars of every plan nested in a
/// scalar subquery at any depth included, and to every `FROM $param` source
/// as the parameter it reads.
fn visit_scalars_deep(op: &LogicalOp, f: &mut impl FnMut(&LogicalExpr)) {
	let Ok(()) = op.visit_scalars(&mut |scalar| {
		f(scalar);
		let _ = scalar.visit(&mut |e| {
			if let LogicalExpr::Subquery(plan) = e {
				visit_scalars_deep(plan, f);
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		Ok::<_, std::convert::Infallible>(())
	});
	if let LogicalOp::Source(LogicalSource {
		source: SourceRef::Param(param),
		..
	}) = op
	{
		f(&LogicalExpr::Param(param.clone()));
	}
	for child in op.children() {
		visit_scalars_deep(child, f);
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_catalog::{sql, syn};
	use surrealdb_expr::expr::logical::lower;

	use super::*;

	/// The logical form of a `{ … }` block.
	fn block(text: &str) -> LogicalOp {
		let ast = syn::parse(text).expect("block should parse");
		let sql::TopLevelExpr::Expr(expr) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected an expression");
		};
		lower(expr).expect("block should lower")
	}

	#[test]
	fn a_single_return_or_bare_expression_is_the_bodys_expression() {
		assert_eq!(
			return_expr(&block("{ RETURN $a + 1 }")).map(ToString::to_string).as_deref(),
			Some("$a + 1")
		);
		assert_eq!(
			return_expr(&block("{ $a + 1 }")).map(ToString::to_string).as_deref(),
			Some("$a + 1")
		);
		assert!(return_expr(&block("{ LET $x = 1; RETURN $x }")).is_none());
		assert!(return_expr(&block("{ 1; 2 }")).is_none());
	}

	#[test]
	fn row_reads_and_calls_are_read_off_the_body() {
		assert!(!reads_row(&block("{ RETURN $a + 1 }")));
		assert!(!reads_row(&block("{ RETURN math::abs($a) }")));
		assert!(reads_row(&block("{ RETURN c }")));
		assert!(reads_row(&block("{ RETURN $this }")));
		assert!(reads_row(&block("{ RETURN (SELECT VALUE x FROM t) }")));
		assert!(reads_row(&block("{ RETURN fn::other($a) }")));
		assert_eq!(calls(&block("{ RETURN fn::a($x) + fn::b(fn::a(1)) }")), ["a", "b"]);
		assert!(calls(&block("{ RETURN $a }")).is_empty());
	}
}
