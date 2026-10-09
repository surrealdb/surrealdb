//! What one statement owns across every table and every nesting level it
//! writes.
//!
//! A [`WriteCtx`] is what a statement resolves once per table; this is what it
//! resolves once per *statement*, and it is the only thing a nested write can
//! be reached through. Three kinds of state live here:
//!
//! - the resolved write contexts, keyed by table **and** purpose, so a table written directly and
//!   the same table written as a view target do not share a clause set;
//! - the compiled plans of the statements a write performs on its own behalf — the referential
//!   cascades a purge runs — so a delete cascading over ten thousand references compiles once;
//! - the frame each of those runs in, derived from the statement's.
//!
//! # Reach
//!
//! [`WriteCtx`] holds a [`Weak`] back to the scope that resolved it, rather
//! than the stage signature carrying it: a fourth argument on
//! [`WriteStage::apply`](super::stage::WriteStage::apply) would touch every
//! stage body and every direct-call test to serve the three that need it. The
//! reference is weak because the scope owns the contexts; the upgrade cannot
//! fail while a stage runs, since the operator's stream holds the `Arc` for its
//! whole life, so [`WriteCtx::scope`] reports a broken invariant rather than a
//! condition a caller handles.
//!
//! # Re-entrancy
//!
//! Both caches are behind a mutex a nested write may ask for while an outer
//! one is resolving: a cascade resolves its own context, and the record it
//! deletes may cascade again. Neither lock is therefore held across the
//! resolution it guards — the entry is looked up, the lock dropped, the work
//! done, and the lock retaken to publish — so the second asker finds the
//! entry and no frame waits on a lock the frame beneath it holds.

use std::sync::Arc;

use tokio::sync::Mutex;

use super::ctx::{TableExistence, WriteCtx};
use crate::exec::ExecutionContext;
use crate::exec::permission::view_frame;
use crate::expr::ControlFlow;
use crate::val::TableName;

/// Whose write a resolved context is.
///
/// Part of the cache key, and what selects the clauses, the output, the table's
/// existence requirement and the frame. Keying by table alone would be silently
/// wrong: an `INSERT` whose rows name their own tables can write directly to a
/// table that is also the view target of another table the same statement
/// writes, and whichever of the two resolved first would hand the other its
/// clause set. It also keeps [`TableViews`](crate::exec::table_view::TableViews)'
/// rule that one resolver must not be shared across contexts that differ in
/// permission frame true by construction rather than by accident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WritePurpose {
	/// The statement's own write: its clauses, its `RETURN`, its frame, and the
	/// existence requirement its form carries.
	Statement,
	/// A row this statement synthesised on a view table. It has no clauses and
	/// no `RETURN` of its own, the table is defined on demand as
	/// `get_or_add_tb` defines it, and it runs in the view frame.
	ViewMaintenance,
}

/// The write contexts a scope has resolved: one per table per purpose.
///
/// A `Vec` rather than a map because a statement writes to a handful of tables
/// at most, and the key is compared rather than hashed.
type ResolvedContexts = Vec<((TableName, WritePurpose), Arc<WriteCtx>)>;

/// The write contexts [`KeptContexts`] keeps, by table, purpose and the
/// version they were read at.
type KeptWriteContexts = Vec<((TableName, WritePurpose, Option<u64>), Arc<WriteCtx>)>;

/// What a statement run again and again compiled on earlier runs, while the
/// catalog it read holds: a block statement's across its reaches in one
/// epoch.
#[derive(Debug, Default)]
pub(crate) struct KeptContexts {
	/// The write contexts. A later run takes their catalog reads rather than reading again,
	/// and compiles its own expressions.
	contexts: std::sync::Mutex<KeptWriteContexts>,
}

/// What one statement owns across every table and nesting level it writes.
#[derive(Debug)]
pub(crate) struct WriteScope {
	/// The context the statement itself runs in, and the frame every derived
	/// one is derived from.
	exec: ExecutionContext,
	/// Whether the statement's own form requires its table to exist.
	existence: TableExistence,
	/// The write contexts resolved so far.
	contexts: Mutex<ResolvedContexts>,
	/// The contexts earlier runs of the same statement resolved, whose
	/// catalog reads this one takes; `None` for a statement run once.
	kept: Option<Arc<KeptContexts>>,
	/// Whether a context resolved earlier is checked against the catalog
	/// before it is taken again, and resolved anew where its table's catalog
	/// changed: the scope of a late-bound write, whose tables' catalog may
	/// change while it runs.
	revalidates: bool,
}

impl WriteScope {
	/// Open the scope for one statement.
	pub(crate) fn new(exec: ExecutionContext, existence: TableExistence) -> Arc<Self> {
		Arc::new(Self {
			exec,
			existence,
			contexts: Mutex::new(Vec::new()),
			kept: None,
			revalidates: false,
		})
	}

	/// [`Self::new`] for a late-bound write: each context it resolves is
	/// checked against the catalog before it is taken again
	/// ([`WriteCtx::stands`]).
	pub(crate) fn revalidating(exec: ExecutionContext, existence: TableExistence) -> Arc<Self> {
		let mut scope = Self::new(exec, existence);
		Arc::get_mut(&mut scope).expect("a scope just made has no other owner").revalidates = true;
		scope
	}

	/// Open the scope for one run of a statement run again and again, taking
	/// the catalog reads of the contexts earlier runs left in `kept` and
	/// leaving its own there.
	///
	/// The caller guarantees the catalog has not changed since `kept` was
	/// first filled.
	pub(crate) fn kept(
		exec: ExecutionContext,
		existence: TableExistence,
		kept: Arc<KeptContexts>,
	) -> Arc<Self> {
		let mut scope = Self::new(exec, existence);
		Arc::get_mut(&mut scope).expect("a scope just opened is not shared").kept = Some(kept);
		scope
	}

	/// The context the statement itself runs in.
	pub(crate) fn exec(&self) -> &ExecutionContext {
		&self.exec
	}

	/// The write context for one table and one purpose, resolved on first use.
	///
	/// Resolution reads the catalog and compiles expressions, which must not
	/// happen per record; a statement that writes to more than one table — an
	/// `INSERT` whose rows name their own — holds one of these per table.
	pub(crate) async fn write_ctx_for(
		self: &Arc<Self>,
		table: &TableName,
		purpose: WritePurpose,
	) -> Result<Arc<WriteCtx>, ControlFlow> {
		if let Some(resolved) = self.cached_ctx(table, purpose).await {
			if !self.revalidates || resolved.stands().await? {
				return Ok(resolved);
			}
			self.contexts.lock().await.retain(|((t, p), _)| !(t == table && *p == purpose));
		}
		let (exec, existence) = match purpose {
			WritePurpose::Statement => (self.exec.clone(), self.existence),
			WritePurpose::ViewMaintenance => (
				view_frame(&self.exec).dive().map_err(ControlFlow::Err)?,
				TableExistence::DefinedOnDemand,
			),
		};
		let key = (table.clone(), purpose, exec.version_stamp());
		let earlier = self.kept.as_ref().and_then(|kept| {
			let kept = kept.contexts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
			kept.iter().find(|(k, _)| *k == key).map(|(_, resolved)| Arc::clone(resolved))
		});
		let same_database = |earlier: &WriteCtx| {
			exec.database().is_ok_and(|db| {
				db.ns_ctx.ns.namespace_id == earlier.ns.namespace_id
					&& db.db.database_id == earlier.db.database_id
			})
		};
		let mut resolved = match earlier {
			Some(earlier) if same_database(&earlier) => earlier.rebound(&exec),
			_ => {
				let resolved = crate::exec::vm::descriptor::write_unit(WriteCtx::resolve(
					&exec, table, existence,
				))
				.await?;
				if let Some(kept) = &self.kept {
					let mut kept =
						kept.contexts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
					kept.retain(|(k, _)| *k != key);
					kept.push((key, Arc::new(resolved.rebound(&exec))));
				}
				resolved
			}
		};
		resolved.scope = Arc::downgrade(self);
		let resolved = Arc::new(resolved);
		let mut contexts = self.contexts.lock().await;
		// Resolution awaits, so a nested write may have published the same
		// entry meanwhile. Its context is as good as this one and is the one
		// already in use, so it wins and this one is dropped.
		if let Some((_, published)) =
			contexts.iter().find(|((t, p), _)| t == table && *p == purpose)
		{
			return Ok(Arc::clone(published));
		}
		contexts.push(((table.clone(), purpose), Arc::clone(&resolved)));
		Ok(resolved)
	}

	async fn cached_ctx(&self, table: &TableName, purpose: WritePurpose) -> Option<Arc<WriteCtx>> {
		let contexts = self.contexts.lock().await;
		contexts
			.iter()
			.find(|((t, p), _)| t == table && *p == purpose)
			.map(|(_, resolved)| Arc::clone(resolved))
	}
}

#[cfg(test)]
thread_local! {
	/// How many times this thread asked for the plan of a cascade's statement,
	/// for tests that must know a write run as bytecode asked for none.
	pub(crate) static CASCADE_PLANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many nested statements this thread has lowered to the VM, for
	/// tests that must know a cascade or a view's re-read ran as bytecode.
	pub(crate) static NESTED_ON_VM: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
