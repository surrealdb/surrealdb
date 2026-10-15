//! The value of a table migration queue entry.
//!
//! A table that has work for the table migration job — rows stamped before its newest migration
//! step, or fields whose stored values must be verified — has one entry at
//! `/!tm{ns}{db}{tb}`. The entry is written in the transaction that queues the work, so it exists
//! exactly when that transaction commits, and lives at the root level so it survives the removal
//! of the table it names: the job deletes an entry whose table is gone.
//!
//! Each page of the job commits its rewrites, or its verification counts, together with the
//! advanced entry by compare-and-set on the entry it read, so a page whose entry was replaced or
//! deleted meanwhile commits nothing.

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_kvs::impl_kv_value_revisioned;

/// How far the table migration job has got with one table.
#[revisioned(revision = 1)]
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
	}

	/// Whether the entry still asks the job for a lap.
	pub fn has_work(&self) -> bool {
		self.rewrite || self.verify.iter().any(|f| !f.finished)
	}
}

impl_kv_value_revisioned!(TableMigrationState);
