//! The values of the table migration job's queue: the requests statements make, and the job's
//! progress through a table.
//!
//! A statement that gives a table work for the job — rows stamped before a migration step it
//! minted, a field whose stored values must be verified, rows to check for keys no definition
//! declares, a verification to start over, a check to drop, or the removal of the table — writes a
//! request
//! ([`TableMigrationRequest`]) at `/!tq{ns}{db}{tb}{uid}`, under a fresh version-7 id. The request
//! is written in the transaction that makes the change, so it exists exactly when that transaction
//! commits. No other transaction writes the key, so a request never conflicts with the job or with
//! another statement.
//!
//! The job keeps its progress through a table ([`TableMigrationState`]) at `/!tm{ns}{db}{tb}`,
//! which only the job writes. A page that finds requests folds them into the progress in the order
//! their ids sort ([`TableMigrationRequest::apply`]) and deletes them, committing nothing else.
//! Every other page commits its rewrites, or its verification counts, together with the advanced
//! progress by compare-and-set on the progress it read. A request committed after a page's
//! snapshot is not one the page deletes, so the next page folds it: a step minted while a lap
//! ends starts the next lap however the two transactions' commits are ordered.
//!
//! What the job will do for a table is its progress with every request it has not yet folded
//! applied in order. Both keys live at the root level, so they survive the removal of the table
//! they name: the job deletes the progress and the requests of a table, database or namespace that
//! is gone. No statement deletes either key, so a removal never conflicts with a page of the job
//! through them. `REMOVE TABLE` writes a request ([`TableMigrationRequest::Discard`]) instead, so a
//! table defined again under the name before the job visits it inherits nothing; a namespace or
//! database defined again is given a new id, under which nothing is queued.
//!
//! `REMOVE TABLE` also deletes the table's rows key by key, so a page that rewrote rows and
//! committed after the removal's snapshot fails the removal's commit. Before it reads the table, a
//! removal of a table with work queued therefore records a hold ([`RemovalHold`]) in the progress,
//! in a transaction of its own that commits at once, and takes it out again once the removal has
//! ended. A page reads the progress and writes it by compare-and-set, so a page whose snapshot
//! predates the hold fails its commit, and a page that sees a hold leaves the table's rows alone.
//! The removing transaction itself never writes the progress, so the hold's commits do not
//! conflict with it.

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_kvs::impl_kv_value_revisioned;

/// How far the table migration job has got with one table.
#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableMigrationState {
	/// The lap under way, from 1. A lap reads the whole table once; the job starts another while
	/// the table's newest migration generation is newer than the one the lap began under.
	pub lap: u64,
	/// The newest generation carrying a migration step when the lap began, read on the lap's
	/// first page: rows stamped before it are rewritten at the table's head. `0` when no
	/// generation carried a step then, and before the lap's first page.
	pub target: u16,
	/// The last record key the lap has passed; `None` before its first page.
	pub cursor: Option<Vec<u8>>,
	/// The records the lap has read.
	pub scanned: u64,
	/// The records the lap has rewritten.
	pub rewritten: u64,
	/// The values the lap replaced with a step's `FALLBACK`.
	pub fallbacks: u64,
	/// Whether the table has rows to rewrite through its migration steps.
	pub rewrite: bool,
	/// The fields whose stored values are being verified.
	pub verify: Vec<VerifiedField>,
	/// Whether every transaction that could still commit a row the verification cannot judge has
	/// ended, so the lap's counts decide promotion. Set before the verifying lap reads its first
	/// row.
	pub settled: bool,
	/// The check for rows holding a top-level key no definition of the table declares; `None`
	/// when none is queued. An entry of revision 1 holds none.
	#[revision(start = 2)]
	pub undeclared: Option<UndeclaredKeys>,
	/// The removals of the table under way, each of which keeps the job off the table's rows
	/// until it ends. An entry of revision 1 or 2 holds none.
	#[revision(start = 3)]
	pub holds: Vec<RemovalHold>,
}

/// A removal of a table under way in a transaction on some node, which keeps the table migration
/// job off the table's rows until that transaction ends.
///
/// The removing node takes the hold out once its transaction has committed or rolled back. A hold
/// whose node is gone, or that has stood longer than the job allows a removal, is one whose
/// release was lost, and the job takes it out itself.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemovalHold {
	/// The hold's own id, by which the removal takes it out.
	pub id: uuid::Uuid,
	/// The node running the removal.
	pub node: uuid::Uuid,
}

/// A field a verification pass is checking.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct VerifiedField {
	/// The field's name, as its definition renders it raw.
	pub name: String,
	/// The digest of the stored definition the pass started under
	/// ([`surrealdb_catalog::definition_digest`]). The field is promoted only if its definition
	/// still has this digest.
	pub definition: i64,
	/// The rows the lap found whose value does not conform.
	pub failures: u64,
	/// Whether the pass over this field has ended without promoting it: `failures` rows did not
	/// conform, or the store gives no way to know that no older transaction can still commit a
	/// row the pass missed.
	pub finished: bool,
}

/// What a statement asks the table migration job to do for one table.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TableMigrationRequest {
	/// Rewrite the rows stamped before the table's newest migration step.
	Rewrite,
	/// Verify the stored values of a field, from the first row, once every transaction older than
	/// the request has ended.
	Verify(VerifiedField),
	/// Start a verification under way over, for a write it could not judge.
	Restart,
	/// Check every row for a top-level key none of these names (sorted) names, in place of any
	/// check queued before, from the first row, once every transaction older than the request has
	/// ended.
	Undeclared(Vec<String>),
	/// Drop the check for undeclared keys: a `SCHEMALESS` table refuses no key, so the check
	/// reports nothing a write refuses.
	DropUndeclared,
	/// Drop the progress and every request made before this one: the table was removed, so
	/// nothing queued for it applies to a table defined again under its name.
	#[revision(start = 2)]
	Discard,
}

impl TableMigrationRequest {
	/// The progress `state` becomes once this request is applied to it; `None` for a table with
	/// no progress that the request gives none.
	///
	/// - [`Self::Rewrite`] asks for a rewrite, starting at lap 1 a table with no lap begun.
	/// - [`Self::Verify`] puts the field in place of any pass over a field of its name, and starts
	///   the lap over, unsettled, so it reads every row from the start after every transaction
	///   older than the request has ended.
	/// - [`Self::Restart`] starts the lap over, unsettled, when a verification of the progress or
	///   its check for undeclared keys is under way, and changes nothing otherwise.
	/// - [`Self::Undeclared`] puts a check against its names in place of any check, and starts the
	///   lap over, unsettled, as [`Self::Verify`] does.
	/// - [`Self::DropUndeclared`] drops the check, and the progress with it when nothing else is
	///   left in it ([`TableMigrationState::is_spent`]).
	/// - [`Self::Discard`] leaves no progress, save the holds of removals still under way.
	pub fn apply(&self, state: Option<TableMigrationState>) -> Option<TableMigrationState> {
		match self {
			Self::Rewrite => {
				let mut state = state.unwrap_or_default();
				state.lap = state.lap.max(1);
				state.rewrite = true;
				Some(state)
			}
			Self::Verify(field) => {
				let mut state = state.unwrap_or_default();
				state.verify.retain(|f| f.name != field.name);
				state.verify.push(field.clone());
				state.settled = false;
				state.restart();
				Some(state)
			}
			Self::Restart => {
				let mut state = state?;
				if state.verifying() {
					state.settled = false;
					state.restart();
				}
				Some(state)
			}
			Self::Undeclared(declared) => {
				let mut state = state.unwrap_or_default();
				state.undeclared = Some(UndeclaredKeys {
					declared: declared.clone(),
					failures: 0,
					finished: false,
				});
				state.settled = false;
				state.restart();
				Some(state)
			}
			Self::DropUndeclared => {
				let mut state = state?;
				if state.undeclared.take().is_none() {
					return Some(state);
				}
				(!state.is_spent()).then_some(state)
			}
			Self::Discard => {
				// A hold belongs to a removal still under way, of a table that may be defined
				// again under the name: it outlives what was queued before it.
				let holds = state.map(|state| state.holds).unwrap_or_default();
				(!holds.is_empty()).then(|| TableMigrationState {
					holds,
					..Default::default()
				})
			}
		}
	}
}

/// The check, made by a verification pass, for rows holding a top-level key no definition of the
/// table declares. A `SCHEMAFULL` table refuses every write of such a row, so the check reports
/// the rows a later write refuses. It states no claim.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UndeclaredKeys {
	/// The names the table's definitions declare at the top level, sorted: the first part of each
	/// definition's path. The lap judges rows against these; a lap that has passed rows under
	/// other names starts over.
	pub declared: Vec<String>,
	/// The rows the lap found holding a top-level key other than `id`, `in` and `out` that
	/// `declared` does not name.
	pub failures: u64,
	/// Whether the check has ended with something to report: `failures` rows hold such a key, or
	/// the store gives no way to know that no older transaction can still commit a row the pass
	/// missed.
	pub finished: bool,
}

impl TableMigrationState {
	/// Start the lap over: from the first record, with no counts. A finished verification is not
	/// judged by a lap, so it keeps the count its pass ended with.
	pub fn restart(&mut self) {
		self.lap += 1;
		self.target = 0;
		self.cursor = None;
		self.scanned = 0;
		self.rewritten = 0;
		self.fallbacks = 0;
		for field in self.verify.iter_mut().filter(|field| !field.finished) {
			field.failures = 0;
		}
		if let Some(check) = self.undeclared.as_mut().filter(|check| !check.finished) {
			check.failures = 0;
		}
	}

	/// Whether a lap still has rows to judge: a field under verification, or the check for
	/// undeclared keys, has not finished.
	pub fn verifying(&self) -> bool {
		self.verify.iter().any(|f| !f.finished)
			|| self.undeclared.as_ref().is_some_and(|check| !check.finished)
	}

	/// Whether the entry still asks the job for a lap.
	pub fn has_work(&self) -> bool {
		self.rewrite || self.verifying()
	}

	/// Whether nothing is left in the entry: it asks for no lap, reports no finished
	/// verification or check for undeclared keys, and holds no removal, so it is deleted.
	pub fn is_spent(&self) -> bool {
		!self.has_work()
			&& self.verify.is_empty()
			&& self.undeclared.is_none()
			&& self.holds.is_empty()
	}

	/// Whether a removal of the table holds the job off its rows.
	pub fn held(&self) -> bool {
		!self.holds.is_empty()
	}
}

impl_kv_value_revisioned!(TableMigrationState);
impl_kv_value_revisioned!(TableMigrationRequest);

#[cfg(test)]
mod tests {
	use super::*;

	fn field(name: &str, definition: i64) -> VerifiedField {
		VerifiedField {
			name: name.to_owned(),
			definition,
			failures: 0,
			finished: false,
		}
	}

	fn fold(requests: &[TableMigrationRequest]) -> Option<TableMigrationState> {
		requests.iter().fold(None, |state, request| request.apply(state))
	}

	#[test]
	fn a_rewrite_starts_the_first_lap_and_a_verification_restarts_it() {
		let state = fold(&[TableMigrationRequest::Rewrite]).unwrap();
		assert_eq!((state.lap, state.rewrite), (1, true));
		let state = TableMigrationRequest::Verify(field("a", 1)).apply(Some(state)).unwrap();
		assert_eq!((state.lap, state.rewrite, state.settled), (2, true, false));
		assert_eq!(state.verify, vec![field("a", 1)]);
	}

	#[test]
	fn a_later_verification_of_a_field_replaces_the_earlier_one() {
		let state = fold(&[
			TableMigrationRequest::Verify(field("a", 1)),
			TableMigrationRequest::Verify(field("b", 2)),
			TableMigrationRequest::Verify(field("a", 3)),
		])
		.unwrap();
		assert_eq!(state.verify, vec![field("b", 2), field("a", 3)]);
		assert_eq!(state.lap, 3);
	}

	#[test]
	fn a_restart_moves_only_a_verification_under_way() {
		assert_eq!(fold(&[TableMigrationRequest::Restart]), None);
		let rewriting = fold(&[TableMigrationRequest::Rewrite]);
		assert_eq!(TableMigrationRequest::Restart.apply(rewriting.clone()), rewriting);
		let mut verifying = fold(&[TableMigrationRequest::Verify(field("a", 1))]).unwrap();
		verifying.settled = true;
		verifying.cursor = Some(b"passed".to_vec());
		verifying.verify[0].failures = 2;
		let restarted = TableMigrationRequest::Restart.apply(Some(verifying.clone())).unwrap();
		assert_eq!(restarted.lap, verifying.lap + 1);
		assert_eq!(
			(restarted.settled, restarted.cursor, restarted.verify[0].failures),
			(false, None, 0)
		);
		let mut finished = verifying;
		finished.verify[0].finished = true;
		assert_eq!(TableMigrationRequest::Restart.apply(Some(finished.clone())), Some(finished));
	}

	#[test]
	fn a_check_for_undeclared_keys_restarts_the_lap_and_a_drop_removes_it() {
		let names = vec!["a".to_owned()];
		let state = fold(&[
			TableMigrationRequest::Rewrite,
			TableMigrationRequest::Undeclared(names.clone()),
		])
		.unwrap();
		assert_eq!((state.lap, state.rewrite, state.settled), (2, true, false));
		assert_eq!(
			state.undeclared,
			Some(UndeclaredKeys {
				declared: names.clone(),
				failures: 0,
				finished: false,
			})
		);
		// A drop keeps progress that still has a rewrite to do, and leaves none when nothing else
		// is in it.
		let kept = TableMigrationRequest::DropUndeclared.apply(Some(state)).unwrap();
		assert_eq!((kept.rewrite, kept.undeclared), (true, None));
		assert_eq!(
			fold(&[
				TableMigrationRequest::Undeclared(names),
				TableMigrationRequest::DropUndeclared
			]),
			None
		);
		assert_eq!(fold(&[TableMigrationRequest::DropUndeclared]), None);
	}

	#[test]
	fn a_discard_drops_what_was_queued_before_it_and_nothing_after() {
		let names = vec!["a".to_owned()];
		assert_eq!(
			fold(&[
				TableMigrationRequest::Rewrite,
				TableMigrationRequest::Verify(field("a", 1)),
				TableMigrationRequest::Undeclared(names),
				TableMigrationRequest::Discard,
			]),
			None
		);
		let after = fold(&[
			TableMigrationRequest::Verify(field("a", 1)),
			TableMigrationRequest::Discard,
			TableMigrationRequest::Rewrite,
		])
		.unwrap();
		assert_eq!((after.lap, after.rewrite, after.verify), (1, true, vec![]));
	}

	#[test]
	fn a_discard_keeps_the_holds_of_removals_under_way() {
		let hold = RemovalHold {
			id: uuid::Uuid::from_u128(1),
			node: uuid::Uuid::from_u128(2),
		};
		let mut held = fold(&[TableMigrationRequest::Rewrite]).unwrap();
		held.holds.push(hold.clone());
		let discarded = TableMigrationRequest::Discard.apply(Some(held)).unwrap();
		assert_eq!(
			discarded,
			TableMigrationState {
				holds: vec![hold.clone()],
				..Default::default()
			}
		);
		assert!(!discarded.has_work() && !discarded.is_spent());
		// A rewrite queued after the discard starts at lap 1 beneath the hold.
		let rewritten = TableMigrationRequest::Rewrite.apply(Some(discarded)).unwrap();
		assert_eq!((rewritten.lap, rewritten.rewrite, rewritten.holds), (1, true, vec![hold]));
	}

	#[test]
	fn a_restart_moves_a_check_for_undeclared_keys_under_way() {
		let mut checking =
			fold(&[TableMigrationRequest::Undeclared(vec!["a".to_owned()])]).unwrap();
		checking.settled = true;
		checking.cursor = Some(b"passed".to_vec());
		if let Some(check) = checking.undeclared.as_mut() {
			check.failures = 2;
		}
		let restarted = TableMigrationRequest::Restart.apply(Some(checking.clone())).unwrap();
		assert_eq!(restarted.lap, checking.lap + 1);
		assert_eq!(
			(restarted.settled, restarted.cursor, restarted.undeclared.map(|c| c.failures)),
			(false, None, Some(0))
		);
	}
}
