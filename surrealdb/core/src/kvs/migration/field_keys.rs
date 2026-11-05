//! Re-keys field definitions stored under a name this build does not derive.
//!
//! A field definition lives at `/*{ns}*{db}*{tb}!fd{name}`, where `{name}` is
//! not read from the value but derived from the definition's path by
//! [`FieldDefinition::storage_name`]. Every point operation on a field derives
//! that name and addresses the one key it spells: `DEFINE FIELD` looking for
//! the definition it overwrites or refuses to duplicate, `ALTER FIELD`,
//! `REMOVE FIELD`, and the removal of the fields nested in a removed one. The
//! listings — `INFO`, the schema every write is checked against, export — scan
//! the table's band instead and read each path from its value.
//!
//! Releases up to and including 3.3 wrote the definitions a kind implies for
//! the elements of an array or set, such as `select.*` for `TYPE
//! array<string>`, under the path's escaped rendering, which quotes a leading
//! reserved keyword: `` `select`.* ``, where an explicit definition of the
//! same path is stored under `select.*`. For a field named by a keyword the two
//! differ. Such a definition lists like any other while no statement can
//! address it: `REMOVE FIELD` on the path `INFO` prints answers that no such
//! field exists, and `DEFINE FIELD ... OVERWRITE` writes a second definition
//! of the path beside it, which every write to the table is then checked
//! against too.
//!
//! This migration walks the field band of every table and moves each row whose
//! key disagrees with the storage name its own value derives.
//!
//! # Why the predicate is the derivation, not the escaped spelling
//!
//! Every writer of the band keys a definition by
//! [`FieldDefinition::storage_name`], so a key that disagrees with its value is
//! never one this build meant to write. Matching on the disagreement repairs
//! that drift wherever it came from, without this code carrying the spellings
//! past releases used.
//!
//! # A duplicate resolves in favour of the addressable row
//!
//! A table that took an explicit `DEFINE FIELD` of an implied path holds both
//! rows. The one under the storage name is the one every point operation has
//! been reading and writing, so it stays as it is, and the unreachable row is
//! dropped with a warning naming the field. Each row was checked on every
//! write to the table, so a record those writes left satisfies the one kept.
//!
//! # Which nodes read what it writes
//!
//! A row is moved byte for byte, so its value is as legible after the move as
//! before. A node on 3.3, where a rolling upgrade into this release starts,
//! addresses a field by the same storage name and lists fields by scanning the
//! band, so it reads a moved row as it reads an explicit definition. It still
//! writes an implied definition under the escaped key, and one it writes after
//! this migration has run stays there: a cluster upgraded node by node should
//! not define a keyword-named array or set field until every node runs this
//! release.
//!
//! # Re-entrancy
//!
//! One transaction per row, which re-reads the row before moving it: a row a
//! peer has already moved is gone, and the move ends there. A peer moving the
//! same row at once makes one of the two commits conflict, and the loser runs
//! the move again, after a jittered backoff, against what the winner left. The
//! ledger entry is written after the walk, so an interrupted walk resumes on
//! the next start, finding every row it had not moved still under its old key.

use std::borrow::Cow;
use std::time::Duration;

use anyhow::Result;
use tracing::{debug, warn};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseId, FieldDefinition, NamespaceId, StoredFieldDefinition};
use crate::key::schema::{FieldKey, FieldPrefix};
use crate::key::{KVKey, KVValue, Key};
use crate::kvs::TransactionType;
use crate::kvs::ds::Datastore;
use crate::kvs::version::ledger::{LedgerClass, LedgerId};
use crate::val::TableName;

const TARGET: &str = "surrealdb::core::kvs::migration";

/// The ledger entry this migration is recorded under, at `/!mg{id}`.
///
/// Compatible, as every startup migration a 4.x build adds is: a build that does not know the
/// entry reads a moved row as it reads any other, so it may open the store read-write.
pub(super) const LEDGER_ID: LedgerId = LedgerId::new(LedgerClass::Compatible, 9);

/// How many times a move that lost its commit to a peer is run before the
/// conflict fails the migration.
const MOVE_ATTEMPTS: usize = 3;

/// Delay before the first re-run of a move; doubled on each attempt, plus
/// jitter.
const RETRY_BACKOFF: Duration = Duration::from_millis(10);

/// Moves every field definition whose key disagrees with the storage name its
/// value derives to the key that name spells.
pub(super) async fn rekey_definitions_to_their_storage_name(ds: &Datastore) -> Result<()> {
	// The driver renews the lease only between migrations, so this one renews
	// it itself: after listing each database's tables, after every move and
	// after every table. The longest stretch without a renewal is one
	// database's listing, one table's scan or one move. A renewal that is not
	// yet due costs nothing, and a lost one is not fatal: the walk is safe to
	// repeat on a node that takes the lease over.
	let lease = super::lease(ds)?;
	let txn = ds.transaction(TransactionType::Read).await?;
	let mut tables = Vec::new();
	for ns in catch!(txn, txn.all_ns(None).await).iter() {
		for db in catch!(txn, txn.all_db(ns.namespace_id, None).await).iter() {
			for tb in catch!(txn, txn.all_tb(ns.namespace_id, db.database_id, None).await).iter() {
				tables.push((ns.namespace_id, db.database_id, tb.name.clone()));
			}
			let _ = lease.try_maintain_lease().await;
		}
	}
	txn.cancel().await?;

	let mut moved = 0usize;
	for (ns, db, tb) in tables {
		for row in collect_drifted(ds, ns, db, &tb).await? {
			rekey(ds, ns, db, &tb, &row).await?;
			moved += 1;
			let _ = lease.try_maintain_lease().await;
		}
		let _ = lease.try_maintain_lease().await;
	}
	if moved > 0 {
		debug!(target: TARGET, count = moved, "Re-keyed field definitions to their storage name");
	}
	Ok(())
}

/// One definition found under a key this build does not derive for it.
struct Drifted {
	/// The key the row was read from, exactly as the scan returned it. The
	/// move deletes these bytes rather than a re-encoded name, so it cannot
	/// remove a key other than the one that was read.
	stored_at: Key<'static>,
	/// The key the definition's storage name spells, where the row belongs.
	belongs_at: Key<'static>,
	/// The path as the definition stores it, for the log.
	name: String,
}

/// Every drifted definition of one table.
///
/// A row that does not decode, or whose stored path does not compile, is left
/// in place and logged rather than failing the migration: neither is a row
/// this migration can place, and moving bytes it cannot identify would be the
/// worse outcome.
async fn collect_drifted(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<Vec<Drifted>> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let range = catch!(
		txn,
		FieldPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()
	);
	// Read raw, and decode each value on its own, so one undecodable row does
	// not take the rest of the table's fields down with it. The band is sized
	// by the number of fields a table defines, which is why this is one scan
	// rather than a paged walk.
	let rows = catch!(txn, txn.getr_raw(range, None).await);
	txn.cancel().await?;

	let mut drifted = Vec::new();
	for (key, value) in rows {
		let stored = match StoredFieldDefinition::kv_decode_value(&value, ()) {
			Ok(stored) => stored,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					table = %tb,
					"Skipping a key in the field band that does not decode as a field definition"
				);
				continue;
			}
		};
		let path = match stored.name.compile() {
			Ok(path) => path,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					table = %tb,
					"Skipping a field definition whose stored path does not compile"
				);
				continue;
			}
		};
		let storage_name = FieldDefinition::storage_name(&path);
		let belongs_at =
			FieldKey::new(ns, db, Cow::Borrowed(tb), Cow::Borrowed(storage_name.as_str()))
				.encode_key()?;
		if belongs_at.as_slice() == key.as_slice() {
			continue;
		}
		drifted.push(Drifted {
			stored_at: Key::from(key),
			belongs_at,
			name: stored.name.as_str().to_owned(),
		});
	}
	Ok(drifted)
}

/// Moves one drifted row to the key its storage name spells, running the move
/// again when it loses its commit to a peer.
async fn rekey(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	row: &Drifted,
) -> Result<()> {
	let mut attempt = 1;
	loop {
		match move_row(ds, ns, db, tb, row).await {
			Err(e)
				if attempt < MOVE_ATTEMPTS && crate::kvs::is_retryable_transaction_conflict(&e) =>
			{
				// The jitter keeps two nodes that lost the same commit to each
				// other from re-running their moves in step.
				let base = RETRY_BACKOFF * (1 << (attempt - 1));
				let jitter = Duration::from_millis(rand::random::<u64>() % 20);
				crate::common::time::sleep(base + jitter).await;
				attempt += 1;
			}
			done => return done,
		}
	}
}

/// Moves one drifted row in one transaction: the row as it now stands is
/// written at its storage name unless a definition is already there, the key
/// it was read from is deleted, and the table's catalog version is stamped, as
/// every other write of a field definition stamps it.
async fn move_row(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	row: &Drifted,
) -> Result<()> {
	let txn = ds.transaction(TransactionType::Write).await?;
	// The scan's snapshot is closed: a peer may have moved the row since, or a
	// node on an earlier release rewritten it.
	let Some(value) = catch!(txn, txn.get(row.stored_at.as_borrowed(), None).await) else {
		txn.cancel().await?;
		return Ok(());
	};
	if catch!(txn, txn.get(row.belongs_at.as_borrowed(), None).await).is_some() {
		warn!(
			target: TARGET,
			field = %row.name,
			table = %tb,
			"A field definition already exists under the storage name; dropping the \
			 unreachable copy stored under another spelling"
		);
	} else {
		catch!(txn, txn.set(row.belongs_at.as_borrowed(), value).await);
	}
	catch!(txn, txn.del(row.stored_at.as_borrowed()).await);
	let stamped = txn.bump_catalog_version(ns, db, tb).await;
	run!(txn, stamped)
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use surrealdb_types::ToSql as _;

	use super::*;
	use crate::expr::Idiom;

	/// The element definition `TYPE array<string>` implies for a field named
	/// `select`, as the releases this migration repairs keyed it.
	const LEGACY_ELEMENT: &str = "`select`.*";
	/// The storage name of the same path.
	const STORAGE_ELEMENT: &str = "select.*";

	/// The bytes this module addresses are the bytes 3.0 through 3.3 wrote.
	///
	/// The literal is the key those releases wrote for the implied `select.*`
	/// of table `t` in ns 1 / db 2: the field band is `*{tb}\0!fd{name}\0`
	/// there as it is here, and the name is the escaped rendering of the path.
	/// Every other test in this file builds its keys through `FieldKey`, so if
	/// either half were wrong they would agree with each other and be wrong
	/// together. This is the one that says otherwise.
	#[test]
	fn the_legacy_layout_matches_what_3_3_wrote() {
		let tb = TableName::from("t");
		let key = FieldKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(LEGACY_ELEMENT),
		)
		.encode_key()
		.unwrap();
		assert_eq!(key.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*t\0!fd`select`.*\0");
	}

	/// The drift this migration repairs, stated as the derivation itself: the
	/// storage name of the path is not the escaped rendering 3.3 keyed its
	/// implied definition by.
	#[test]
	fn the_storage_name_of_a_keyword_path_is_not_the_one_3_3_wrote() {
		let path = Idiom::from_str(LEGACY_ELEMENT).unwrap();
		assert_eq!(FieldDefinition::storage_name(&path), STORAGE_ELEMENT);
		assert_eq!(path.to_sql(), LEGACY_ELEMENT);
	}

	/// A path that leads with no keyword renders the same either way, which is
	/// what keeps this migration off every other row.
	#[test]
	fn a_path_without_a_keyword_derives_the_name_it_was_written_under() {
		let path = Idiom::from_str("tags.*").unwrap();
		assert_eq!(FieldDefinition::storage_name(&path), "tags.*");
		assert_eq!(path.to_sql(), "tags.*");
	}

	#[cfg(feature = "kv-mem")]
	mod against_a_datastore {
		use std::sync::Arc;

		use super::*;
		use crate::dbs::Session;
		use crate::key::KVKeyDecode as _;

		/// Runs `sql` as the owner in namespace and database `test`, which it
		/// defines first: a table defined without them leaves no namespace or
		/// database row for the migration's walk to find.
		async fn execute(ds: &Datastore, sql: &str) {
			let session = Session::owner().with_ns("test").with_db("test");
			let sql = format!(
				"DEFINE NAMESPACE IF NOT EXISTS test; USE NS test; \
				 DEFINE DATABASE IF NOT EXISTS test; USE DB test; {sql}"
			);
			for response in ds.execute(&sql, &session, None).await.unwrap() {
				response.result.unwrap();
			}
		}

		/// A datastore holding the tables `t` and `u` in namespace and database
		/// `test`, returning their ids.
		async fn fixture() -> (Arc<Datastore>, NamespaceId, DatabaseId) {
			let ds = Datastore::new("memory").await.unwrap();
			execute(&ds, "DEFINE TABLE t SCHEMAFULL; DEFINE TABLE u SCHEMAFULL;").await;
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let database = txn.expect_db_by_name("test", "test").await.unwrap();
			txn.cancel().await.unwrap();
			(ds, database.namespace_id, database.database_id)
		}

		/// The encoded definition of `path` on table `tb` with `TYPE string`. Its
		/// stored path is the raw rendering, as the releases this migration
		/// repairs stored it too.
		fn element(tb: &str, path: &str) -> Vec<u8> {
			FieldDefinition {
				name: Idiom::from_str(path).unwrap(),
				table: TableName::from(tb),
				field_kind: Some(crate::expr::Kind::String),
				..Default::default()
			}
			.to_stored()
			.kv_encode_value()
			.unwrap()
		}

		fn key(ns: NamespaceId, db: DatabaseId, tb: &str, name: &str) -> Key<'static> {
			FieldKey::new(ns, db, Cow::Owned(TableName::from(tb)), Cow::Borrowed(name))
				.encode_key()
				.unwrap()
		}

		/// Writes `value` under the field key `name` of table `tb`, around the
		/// catalog, which is how a release with another derivation put it
		/// there.
		async fn write_at(
			ds: &Datastore,
			(ns, db): (NamespaceId, DatabaseId),
			tb: &str,
			name: &str,
			value: Vec<u8>,
		) {
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set(key(ns, db, tb, name), value).await.unwrap();
			txn.commit().await.unwrap();
		}

		/// Every key in table `tb`'s field band, paired with the value under it.
		async fn band(
			ds: &Datastore,
			(ns, db): (NamespaceId, DatabaseId),
			tb: &str,
		) -> Vec<(Vec<u8>, Vec<u8>)> {
			let tb = TableName::from(tb);
			let range = FieldPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&tb),
			}
			.range()
			.unwrap();
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.getr_raw(range, None).await.unwrap();
			txn.cancel().await.unwrap();
			found
		}

		/// The names table `tb`'s field band is keyed by, in key order.
		async fn names(ds: &Datastore, scope: (NamespaceId, DatabaseId), tb: &str) -> Vec<String> {
			band(ds, scope, tb)
				.await
				.into_iter()
				.map(|(k, _)| FieldKey::decode_key(&k).unwrap().fd.into_owned())
				.collect()
		}

		/// Reads a field the way every statement does: by the storage name of
		/// its path, through the catalog rather than the raw band.
		async fn addressed(
			ds: &Datastore,
			(ns, db): (NamespaceId, DatabaseId),
			tb: &str,
			name: &str,
		) -> Option<Arc<FieldDefinition>> {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.get_tb_field(ns, db, &TableName::from(tb), name, None).await.unwrap();
			txn.cancel().await.unwrap();
			found
		}

		/// The case the migration exists for: an element definition written
		/// under the escaped rendering of its path ends up under its storage
		/// name, with its value unchanged, and is reachable by name.
		#[tokio::test]
		async fn a_legacy_element_definition_becomes_addressable() {
			let (ds, ns, db) = fixture().await;
			let value = element("t", STORAGE_ELEMENT);
			write_at(&ds, (ns, db), "t", LEGACY_ELEMENT, value.clone()).await;
			assert!(addressed(&ds, (ns, db), "t", STORAGE_ELEMENT).await.is_none());

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			assert_eq!(names(&ds, (ns, db), "t").await, vec![STORAGE_ELEMENT]);
			assert_eq!(band(&ds, (ns, db), "t").await[0].1, value);
			assert!(addressed(&ds, (ns, db), "t", STORAGE_ELEMENT).await.is_some());
		}

		/// A move stamps the table's catalog version, as every other write of a
		/// field definition does, so nothing compiled against the table before
		/// the move is taken to describe it after.
		#[tokio::test]
		async fn a_move_stamps_the_table_version() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, (ns, db), "t", LEGACY_ELEMENT, element("t", STORAGE_ELEMENT)).await;
			let version = async || {
				let txn = ds.transaction(TransactionType::Read).await.unwrap();
				let version = txn.catalog_version(ns, db, &TableName::from("t")).await.unwrap();
				txn.cancel().await.unwrap();
				version
			};
			let before = version().await;

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			let after = version().await;
			assert!(after.is_some());
			assert_ne!(after, before);
		}

		/// A definition already under its storage name is not rewritten, so the
		/// migration costs a scan and nothing else on a store with no drift.
		#[tokio::test]
		async fn a_definition_at_its_storage_name_is_untouched() {
			let (ds, ns, db) = fixture().await;
			execute(&ds, "DEFINE FIELD `select` ON t TYPE array<string>;").await;
			let before = band(&ds, (ns, db), "t").await;
			assert_eq!(names(&ds, (ns, db), "t").await, vec!["select", STORAGE_ELEMENT]);

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			assert_eq!(band(&ds, (ns, db), "t").await, before);
		}

		/// The state an explicit definition of an implied path leaves: one row
		/// under each key. The row under the storage name is the one every
		/// statement addressed, and survives untouched; the unreachable one
		/// goes.
		#[tokio::test]
		async fn a_duplicate_resolves_in_favour_of_the_addressable_row() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, (ns, db), "t", LEGACY_ELEMENT, element("t", STORAGE_ELEMENT)).await;
			let explicit = FieldDefinition {
				name: Idiom::from_str(STORAGE_ELEMENT).unwrap(),
				table: TableName::from("t"),
				field_kind: Some(crate::expr::Kind::String),
				comment: Some("written by DEFINE FIELD OVERWRITE".to_owned()),
				..Default::default()
			}
			.to_stored()
			.kv_encode_value()
			.unwrap();
			write_at(&ds, (ns, db), "t", STORAGE_ELEMENT, explicit.clone()).await;

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			assert_eq!(names(&ds, (ns, db), "t").await, vec![STORAGE_ELEMENT]);
			assert_eq!(band(&ds, (ns, db), "t").await[0].1, explicit);
		}

		/// Running it again changes nothing, which is what makes a crash between
		/// the migration and its ledger entry safe.
		#[tokio::test]
		async fn running_it_twice_converges() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, (ns, db), "t", LEGACY_ELEMENT, element("t", STORAGE_ELEMENT)).await;
			write_at(&ds, (ns, db), "t", "tags.*", element("t", "tags.*")).await;

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();
			let once = band(&ds, (ns, db), "t").await;
			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			assert_eq!(band(&ds, (ns, db), "t").await, once);
			assert_eq!(names(&ds, (ns, db), "t").await, vec![STORAGE_ELEMENT, "tags.*"]);
		}

		/// Every table of every database is walked, not just the first found.
		#[tokio::test]
		async fn each_table_is_migrated() {
			let (ds, ns, db) = fixture().await;
			execute(&ds, "DEFINE DATABASE other; USE DB other; DEFINE TABLE v;").await;
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let other = txn.expect_db_by_name("test", "other").await.unwrap().database_id;
			txn.cancel().await.unwrap();
			let tables = [(db, "t"), (db, "u"), (other, "v")];
			for (db, tb) in tables {
				write_at(&ds, (ns, db), tb, LEGACY_ELEMENT, element(tb, STORAGE_ELEMENT)).await;
			}

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			for (db, tb) in tables {
				assert_eq!(names(&ds, (ns, db), tb).await, vec![STORAGE_ELEMENT], "table {tb}");
			}
		}

		/// A row this migration cannot identify is left alone rather than
		/// failing the run: moving bytes it cannot decode would be worse than
		/// leaving them, and the rest of the band is still repaired.
		#[tokio::test]
		async fn an_undecodable_row_is_left_in_place() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, (ns, db), "t", LEGACY_ELEMENT, element("t", STORAGE_ELEMENT)).await;
			write_at(&ds, (ns, db), "t", "`junk`", b"not a field definition".to_vec()).await;

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			assert_eq!(names(&ds, (ns, db), "t").await, vec!["`junk`", STORAGE_ELEMENT]);
		}

		/// The registry entry, not just the function: a datastore that has never
		/// been stamped owes this migration, and running the shipped registry over
		/// it repairs the band. Wiring this migration into `MIGRATIONS` is what
		/// makes any of the rest of this reach a real datastore.
		#[tokio::test]
		async fn the_shipped_registry_selects_and_runs_it() {
			use crate::key::schema::MigrationKey;
			use crate::kvs::migration::{MIGRATIONS, run_with};
			use crate::kvs::version::StorageVersion;

			let (ds, ns, db) = fixture().await;
			write_at(&ds, (ns, db), "t", LEGACY_ELEMENT, element("t", STORAGE_ELEMENT)).await;

			// An unstamped datastore at the legacy level carrying data, which is
			// what an upgrade from a release that predates the stamp looks like.
			crate::kvs::migration::tests::at_the_legacy_level(&ds).await;
			run_with(&ds, MIGRATIONS, &[], &StorageVersion::current(), false).await.unwrap();

			assert_eq!(names(&ds, (ns, db), "t").await, vec![STORAGE_ELEMENT]);
			// Recorded, so a restart does not walk the bands again.
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let recorded = txn.exists_key(&MigrationKey::new(LEDGER_ID.raw()), None).await.unwrap();
			txn.cancel().await.unwrap();
			assert!(recorded);
		}

		/// The whole path, through the statement layer rather than the catalog:
		/// once the migration has run, `DEFINE FIELD ... OVERWRITE` replaces an
		/// element definition keyed as the releases up to 3.3 keyed it, and
		/// `REMOVE FIELD` removes it. This is the operator-visible shape of the
		/// repair.
		#[tokio::test]
		async fn overwrite_and_remove_reach_a_migrated_definition() {
			let (ds, ns, db) = fixture().await;
			execute(&ds, "DEFINE FIELD `select` ON t TYPE array<string>;").await;
			// Move the definition the kind implied to where 3.3 wrote it.
			let implied = band(&ds, (ns, db), "t").await.remove(1).1;
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.del(key(ns, db, "t", STORAGE_ELEMENT)).await.unwrap();
			txn.set(key(ns, db, "t", LEGACY_ELEMENT), implied).await.unwrap();
			txn.commit().await.unwrap();

			rekey_definitions_to_their_storage_name(&ds).await.unwrap();

			execute(&ds, "DEFINE FIELD OVERWRITE `select`.* ON t TYPE string ASSERT $value != '';")
				.await;
			assert_eq!(names(&ds, (ns, db), "t").await, vec!["select", STORAGE_ELEMENT]);
			let overwritten = addressed(&ds, (ns, db), "t", STORAGE_ELEMENT).await.unwrap();
			assert!(overwritten.assert.is_some());

			execute(&ds, "REMOVE FIELD `select`.* ON t;").await;
			assert_eq!(names(&ds, (ns, db), "t").await, vec!["select"]);
		}
	}
}
