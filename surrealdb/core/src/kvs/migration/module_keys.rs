//! Re-keys module definitions stored under a name this build cannot derive.
//!
//! A module definition lives at `/*{ns}*{db}!md{name}`, where `{name}` is not
//! stored in the value but *derived* from it by
//! [`StoredModuleDefinition::get_storage_name`]: `mod::{name}` for a named
//! module, and `silo::{organisation}::{package}::<{major}.{minor}.{patch}>` for
//! a silo package release. Every point operation — `REMOVE`, `ALTER`, `DEFINE
//! … OVERWRITE`, and a call to one of the module's exports — re-derives that
//! name and reads the single key it spells.
//!
//! The silo derivation gained the `::` before its version segment in 3.3, so
//! releases up to and including 3.2 wrote `silo::acme::widgets<1.2.3>` where
//! this build derives `silo::acme::widgets::<1.2.3>`. Nothing addresses the key
//! those releases wrote, and nothing reports it missing either: `INFO` lists
//! modules by scanning the band and recomputing each name from the decoded
//! value, so such a definition renders exactly as it should while `REMOVE` on
//! the name `INFO` just printed answers that no such module exists. `DEFINE …
//! OVERWRITE` then writes a *second* row, at the derived key, and the band
//! holds the same module twice.
//!
//! This migration walks the band and moves every row whose key disagrees with
//! the name its own value derives, so that each module is addressable under
//! exactly the name `INFO` reports for it.
//!
//! # Why the predicate is the derivation, not the `silo::` spelling
//!
//! [`crate::catalog::providers::DatabaseProvider::put_db_module`] is the only
//! writer of this band, and it keys every row by the same derivation the
//! readers use. A key that disagrees with its value is therefore never
//! something this engine meant to write — it is always a row whose derivation
//! has since changed. Matching on that disagreement repairs the drift exactly,
//! without this code having to carry a list of the spellings past releases
//! used.
//!
//! # Why the row moves rather than being copied
//!
//! Migration 1 copies and leaves the original, so a node still on the previous
//! release keeps resolving what it already has. That is not available here: the
//! two keys hold the *same module*, and the listing path scans the band rather
//! than addressing keys, so leaving both would make `INFO FOR DB STRUCTURE`,
//! `surreal export` and every other consumer of `all_db_modules` report the
//! module twice. Leaving the duplicate in place is the state this migration
//! exists to end, so the legacy key goes.
//!
//! An older node in a mixed-version cluster consequently stops resolving a silo
//! module once this runs. There is no arrangement of the band that avoids that:
//! the two builds parse and derive different names for the same package, so no
//! single key satisfies both. That belongs in the release notes. It costs
//! `mod::` modules nothing, whose derivation has never changed and whose rows
//! this migration therefore never touches.
//!
//! # Re-entrancy
//!
//! Two transactions per row: a create-only write at the derived key, then a
//! delete of the key it was read from. An interruption between them leaves the
//! duplicate this migration removes, and the ledger entry is not yet written,
//! so the next start finds the legacy key still there and completes the move.
//! Two nodes running this at once compute the same target from the same value;
//! whichever loses the create-only write proceeds to the delete, which is the
//! state both were driving towards.

use std::borrow::Cow;

use anyhow::Result;
use tracing::{debug, warn};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, NamespaceId, StoredModuleDefinition};
use crate::key::schema::{ModuleKey, ModulePrefix};
use crate::key::{KVKey, KVValue, Key};
use crate::kvs::TransactionType;
use crate::kvs::ds::Datastore;

const TARGET: &str = "surrealdb::core::kvs::migration";

/// Moves every module definition whose key disagrees with the name its value
/// derives to the key that name spells.
pub(super) async fn rekey_definitions_to_their_derived_name(ds: &Datastore) -> Result<()> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let databases = async {
		let namespaces = txn.all_ns(None).await?;
		let mut databases = Vec::new();
		for ns in namespaces.iter() {
			for db in txn.all_db(ns.namespace_id, None).await?.iter() {
				databases.push((ns.namespace_id, db.database_id));
			}
		}
		Ok::<_, anyhow::Error>(databases)
	}
	.await;
	txn.cancel().await?;
	let databases = databases?;

	let mut moved = 0usize;
	for (ns, db) in databases {
		moved += migrate_database(ds, ns, db).await?;
	}
	if moved > 0 {
		debug!(target: TARGET, count = moved, "Re-keyed module definitions to their derived name");
	}
	Ok(())
}

/// Re-keys one database's drifted definitions, returning how many were moved.
async fn migrate_database(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Result<usize> {
	let drifted = collect_drifted(ds, ns, db).await?;
	for row in &drifted {
		rekey(ds, ns, db, row).await?;
	}
	Ok(drifted.len())
}

/// One definition found under a key this build does not derive for it.
struct Drifted {
	/// The key the row was read from, exactly as the scan returned it. The
	/// delete addresses these bytes rather than re-encoding a name, so it
	/// cannot remove a key other than the one that was read.
	stored_at: Key<'static>,
	/// The name derived from `definition`, which is where the row belongs.
	derived: String,
	/// The row, rewritten unchanged under `derived`.
	definition: StoredModuleDefinition,
}

/// Every drifted definition in one database.
///
/// The band holds module definitions and nothing else, so unlike migration 1
/// there is no foreign key material to tell apart — but a row that does not
/// decode, or whose value derives no name at all, is left in place and logged
/// rather than failing the migration. Neither is a row this migration can
/// place, and moving bytes we cannot identify would be the worse outcome.
async fn collect_drifted(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Result<Vec<Drifted>> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let range = catch!(
		txn,
		ModulePrefix {
			ns,
			db,
		}
		.range()
	);
	// Read raw, and decode each value on its own, so one undecodable row does
	// not take the rest of the database's modules down with it. The band is
	// sized by the number of modules a database defines, which is why this is
	// one scan rather than a paged walk.
	let rows = catch!(txn, txn.getr_raw(range, None).await);
	txn.cancel().await?;

	let mut drifted = Vec::new();
	for (key, value) in rows {
		let definition = match StoredModuleDefinition::kv_decode_value(&value, ()) {
			Ok(definition) => definition,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					"Skipping a key in the module band that does not decode as a \
					 module definition"
				);
				continue;
			}
		};
		let derived = match definition.get_storage_name() {
			Ok(derived) => derived,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					"Skipping a module definition whose value derives no storage name"
				);
				continue;
			}
		};
		let belongs_at = ModuleKey::new(ns, db, Cow::Borrowed(derived.as_str())).encode_key()?;
		if belongs_at.as_ref() == key.as_slice() {
			continue;
		}
		drifted.push(Drifted {
			stored_at: Key::from(key),
			derived,
			definition,
		});
	}
	Ok(drifted)
}

/// Writes one definition at its derived key and drops the key it was read from.
async fn rekey(ds: &Datastore, ns: NamespaceId, db: DatabaseId, row: &Drifted) -> Result<()> {
	let key = ModuleKey::new(ns, db, Cow::Borrowed(row.derived.as_str()));

	// Create-only. The value was read under an earlier, already-closed
	// snapshot, so a blind write would clobber whatever reached the derived key
	// in between — a peer's copy of this same move, or the `DEFINE … OVERWRITE`
	// an operator ran to work around the definition being unreachable. Either
	// was written with fresher information than this row carries.
	let txn = ds.transaction(TransactionType::Write).await?;
	let created = txn.put_key(&key, &row.definition).await;
	match run!(txn, created) {
		Ok(()) => {}
		Err(e) if super::already_exists(&e) => {
			warn!(
				target: TARGET,
				name = %row.derived,
				"A module definition already exists at the derived name; dropping the \
				 unreachable copy stored under the older one"
			);
		}
		// Anything else propagates. A write conflict is not evidence that a peer
		// completed this move, and swallowing it would let the ledger record a
		// migration that left a definition unaddressable.
		Err(e) => return Err(e),
	}

	// The row is now readable under the name every statement derives for it, so
	// what remains at the old key is a duplicate that only the listing paths can
	// see. Deleting the bytes that were read, rather than a re-encoded name,
	// keeps this from ever addressing a different key than the scan found.
	let txn = ds.transaction(TransactionType::Write).await?;
	let deleted = txn.del(row.stored_at.clone()).await;
	run!(txn, deleted)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The two silo spellings: the one releases up to 3.2 wrote, and the one
	/// this build derives from the same value.
	const LEGACY_SILO: &str = "silo::acme::widgets<1.2.3>";
	const DERIVED_SILO: &str = "silo::acme::widgets::<1.2.3>";

	/// The bytes this module addresses are the bytes 3.0 through 3.2 actually
	/// wrote.
	///
	/// The literal is a silo module's definition key in ns 1 / db 2 as those
	/// releases spelled it: the band is `!md{name}` there as it is here, and the
	/// name is the one their derivation produced. Every other test in this file
	/// builds its keys through `ModuleKey`, so if either half were wrong they
	/// would all agree with each other and be wrong together. This is the one
	/// that says otherwise.
	#[test]
	fn the_legacy_layout_matches_what_3_2_wrote() {
		let key = ModuleKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(LEGACY_SILO))
			.encode_key()
			.unwrap();
		assert_eq!(
			key.as_ref(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mdsilo::acme::widgets<1.2.3>\0"
		);
	}

	/// The drift this migration repairs, stated as the derivation itself: the
	/// name 3.2 spelled is not the name this build derives from the same value.
	#[test]
	fn the_derived_silo_name_is_not_the_one_3_2_wrote() {
		assert_eq!(silo(1, 2, 3).get_storage_name().unwrap(), DERIVED_SILO);
		assert_ne!(DERIVED_SILO, LEGACY_SILO);
	}

	/// A named module derives `mod::{name}`, which has never changed. Pinned
	/// because it is what keeps this migration off every non-silo row.
	#[test]
	fn a_named_module_derives_the_name_it_always_has() {
		assert_eq!(surrealism("plain").get_storage_name().unwrap(), "mod::plain");
	}

	/// A silo package release, stored the way `DEFINE MODULE silo::…` stores
	/// one: coordinates in the value and no name at all.
	fn silo(major: u32, minor: u32, patch: u32) -> StoredModuleDefinition {
		use crate::catalog::{ModuleExecutable, SiloExecutable, StoredPermission};
		StoredModuleDefinition {
			name: None,
			comment: None,
			permissions: StoredPermission::Full,
			executable: ModuleExecutable::Silo(SiloExecutable {
				organisation: "acme".to_string(),
				package: "widgets".to_string(),
				major,
				minor,
				patch,
			}),
			unsigned: true,
		}
	}

	/// A named module backed by a wasm object in a bucket.
	fn surrealism(name: &str) -> StoredModuleDefinition {
		use crate::catalog::{ModuleExecutable, StoredPermission, SurrealismExecutable};
		StoredModuleDefinition {
			name: Some(name.to_string()),
			comment: None,
			permissions: StoredPermission::Full,
			executable: ModuleExecutable::Surrealism(SurrealismExecutable {
				bucket: "mods".to_string(),
				key: format!("/{name}.wasm"),
			}),
			unsigned: true,
		}
	}

	#[cfg(feature = "kv-mem")]
	mod against_a_datastore {
		use std::sync::Arc;

		use super::*;
		use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
		use crate::kvs::Datastore;

		/// A datastore with one namespace and database, returning their ids.
		async fn fixture() -> (Arc<Datastore>, NamespaceId, DatabaseId) {
			let ds = Datastore::new("memory").await.unwrap();
			let (ns, db) = (NamespaceId(1), DatabaseId(1));
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.put_ns(NamespaceDefinition {
				namespace_id: ns,
				name: "test".into(),
				comment: None,
			})
			.await
			.unwrap();
			txn.put_db(
				"test",
				DatabaseDefinition {
					namespace_id: ns,
					database_id: db,
					name: "test".into(),
					strict: false,
					comment: None,
					changefeed: None,
				},
			)
			.await
			.unwrap();
			txn.commit().await.unwrap();
			(ds, ns, db)
		}

		/// Writes a definition under an explicit name, which is how a release
		/// with a different derivation put it there.
		async fn write_at(
			ds: &Datastore,
			ns: NamespaceId,
			db: DatabaseId,
			name: &str,
			definition: &StoredModuleDefinition,
		) {
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set_key(&ModuleKey::new(ns, db, Cow::Borrowed(name)), definition).await.unwrap();
			txn.commit().await.unwrap();
		}

		/// Every key in the module band, paired with the definition under it.
		async fn band(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Vec<(Vec<u8>, Vec<u8>)> {
			let range = ModulePrefix {
				ns,
				db,
			}
			.range()
			.unwrap();
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.getr_raw(range, None).await.unwrap();
			txn.cancel().await.unwrap();
			found
		}

		/// The names the band is keyed by, in key order.
		async fn names(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Vec<String> {
			use crate::key::KVKeyDecode as _;
			band(ds, ns, db)
				.await
				.into_iter()
				.map(|(k, _)| ModuleKey::decode_key(&k).unwrap().md.into_owned())
				.collect()
		}

		/// Reads a module the way every statement does: by the name this build
		/// derives, through the catalog rather than the raw band.
		async fn addressable(ds: &Datastore, ns: NamespaceId, db: DatabaseId, name: &str) -> bool {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.get_db_module(ns, db, name, None).await.is_ok();
			txn.cancel().await.unwrap();
			found
		}

		/// The case the migration exists for: a silo definition written under
		/// the spelling 3.2 used ends up under the one this build derives, with
		/// its value intact, and is reachable by name.
		#[tokio::test]
		async fn a_legacy_silo_definition_becomes_addressable() {
			let (ds, ns, db) = fixture().await;
			let definition = silo(1, 2, 3);
			write_at(&ds, ns, db, LEGACY_SILO, &definition).await;
			assert!(!addressable(&ds, ns, db, DERIVED_SILO).await);

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(names(&ds, ns, db).await, vec![DERIVED_SILO]);
			let stored = &band(&ds, ns, db).await[0].1;
			assert_eq!(StoredModuleDefinition::kv_decode_value(stored, ()).unwrap(), definition);
			assert!(addressable(&ds, ns, db, DERIVED_SILO).await);
		}

		/// A `mod::` definition is keyed by a derivation that has never changed,
		/// so the migration must not touch it — not its key and not its bytes.
		#[tokio::test]
		async fn a_named_module_is_left_exactly_where_it_is() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, ns, db, "mod::plain", &surrealism("plain")).await;
			let before = band(&ds, ns, db).await;

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(band(&ds, ns, db).await, before);
		}

		/// A definition already sitting at its derived key is not rewritten, so
		/// the migration costs a scan and nothing else on an up-to-date store.
		#[tokio::test]
		async fn a_definition_at_its_derived_key_is_untouched() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, ns, db, DERIVED_SILO, &silo(1, 2, 3)).await;
			let before = band(&ds, ns, db).await;

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(band(&ds, ns, db).await, before);
		}

		/// The state an operator reaches by working around the break: `DEFINE
		/// MODULE OVERWRITE` on the name `INFO` reports writes a second row at
		/// the derived key, leaving the band holding the same module twice. The
		/// newer row is authoritative and must survive untouched; the
		/// unreachable one goes.
		#[tokio::test]
		async fn a_duplicate_resolves_in_favour_of_the_addressable_row() {
			let (ds, ns, db) = fixture().await;
			// The same package coordinates on both rows, because that is what
			// makes them a duplicate: they derive the same key. The comment is
			// what tells them apart, and it is the newer row's that must remain.
			write_at(&ds, ns, db, LEGACY_SILO, &silo(1, 2, 3)).await;
			let mut overwritten = silo(1, 2, 3);
			overwritten.comment = Some("written to work around the break".to_string());
			write_at(&ds, ns, db, DERIVED_SILO, &overwritten).await;

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(names(&ds, ns, db).await, vec![DERIVED_SILO]);
			let stored = &band(&ds, ns, db).await[0].1;
			assert_eq!(StoredModuleDefinition::kv_decode_value(stored, ()).unwrap(), overwritten);
		}

		/// Running it again changes nothing, which is what makes a crash between
		/// the migration and its ledger entry safe.
		#[tokio::test]
		async fn running_it_twice_converges() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, ns, db, LEGACY_SILO, &silo(1, 2, 3)).await;
			write_at(&ds, ns, db, "mod::plain", &surrealism("plain")).await;

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();
			let once = band(&ds, ns, db).await;
			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(band(&ds, ns, db).await, once);
			assert_eq!(names(&ds, ns, db).await, vec!["mod::plain", DERIVED_SILO]);
		}

		/// Every database is walked, not just the first one found.
		#[tokio::test]
		async fn each_database_is_migrated() {
			let (ds, ns, db) = fixture().await;
			let other = DatabaseId(2);
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.put_db(
				"other",
				DatabaseDefinition {
					namespace_id: ns,
					database_id: other,
					name: "other".into(),
					strict: false,
					comment: None,
					changefeed: None,
				},
			)
			.await
			.unwrap();
			txn.commit().await.unwrap();
			write_at(&ds, ns, db, LEGACY_SILO, &silo(1, 2, 3)).await;
			write_at(&ds, ns, other, LEGACY_SILO, &silo(1, 2, 3)).await;

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(names(&ds, ns, db).await, vec![DERIVED_SILO]);
			assert_eq!(names(&ds, ns, other).await, vec![DERIVED_SILO]);
		}

		/// A row this migration cannot identify is left alone rather than
		/// failing the run: moving bytes we cannot decode would be worse than
		/// leaving them, and the rest of the band still gets repaired.
		#[tokio::test]
		async fn an_undecodable_row_is_left_in_place() {
			let (ds, ns, db) = fixture().await;
			write_at(&ds, ns, db, LEGACY_SILO, &silo(1, 2, 3)).await;
			let junk = ModuleKey::new(ns, db, Cow::Borrowed("mod::junk")).encode_key().unwrap();
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set(junk.clone(), b"not a module definition".to_vec()).await.unwrap();
			txn.commit().await.unwrap();

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			let mut found = names(&ds, ns, db).await;
			found.sort();
			assert_eq!(found, vec!["mod::junk", DERIVED_SILO]);
		}

		/// The registry entry, not just the function: a datastore that has never
		/// been stamped owes migration 2, and running the shipped registry over
		/// it repairs the band. Wiring this migration into `MIGRATIONS` is what
		/// makes any of the rest of this reach a real datastore.
		#[tokio::test]
		async fn the_shipped_registry_selects_and_runs_it() {
			use crate::key::schema::MigrationKey;
			use crate::kvs::migration::{MIGRATIONS, run_with};
			use crate::kvs::version::StorageVersion;

			let (ds, ns, db) = fixture().await;
			write_at(&ds, ns, db, LEGACY_SILO, &silo(1, 2, 3)).await;

			// An unstamped datastore carrying data, which is what an upgrade
			// from a release that predates the stamp looks like.
			run_with(&ds, MIGRATIONS, &StorageVersion::current(), false).await.unwrap();

			assert_eq!(names(&ds, ns, db).await, vec![DERIVED_SILO]);
			// Recorded, so a restart does not walk the band again.
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let recorded = txn.exists_key(&MigrationKey::new(2), None).await.unwrap();
			txn.cancel().await.unwrap();
			assert!(recorded);
		}

		/// The whole path, through the statement layer rather than the catalog:
		/// a silo definition written under an older release's derivation cannot be
		/// addressed by the name `INFO` reports for it, and can be once the
		/// migration has run. This is the operator-visible shape of the repair.
		#[cfg(feature = "surrealism")]
		#[tokio::test]
		async fn remove_module_reaches_a_migrated_definition() {
			use crate::catalog::providers::CatalogProvider as _;
			use crate::dbs::Session;
			use crate::dbs::capabilities::{Capabilities, Targets};
			use crate::kvs::migration::{MIGRATIONS, run_with};
			use crate::kvs::version::StorageVersion;

			let ds = crate::kvs::Datastore::builder()
				.with_capabilities(Capabilities::all().with_experimental(Targets::All))
				.build_with_path("memory")
				.await
				.unwrap();
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			let database = txn.ensure_ns_db(None, "test", "test").await.unwrap();
			txn.commit().await.unwrap();
			let (ns, db) = (database.namespace_id, database.database_id);
			write_at(&ds, ns, db, LEGACY_SILO, &silo(1, 2, 3)).await;

			let session = Session::owner().with_ns("test").with_db("test");
			let remove = format!("REMOVE MODULE {DERIVED_SILO}");

			// Under the key an older release wrote, the definition lists but does
			// not resolve: `INFO` recomputes the name, `REMOVE` addresses the key.
			let before = ds.execute(&remove, &session, None).await.unwrap();
			let Err(e) = &before[0].result else {
				panic!(
					"a definition under the older derivation was reachable before the migration"
				);
			};
			assert!(e.to_string().contains(DERIVED_SILO), "unexpected error: {e}");

			run_with(&ds, MIGRATIONS, &StorageVersion::current(), false).await.unwrap();

			let after = ds.execute(&remove, &session, None).await.unwrap();
			after[0].result.as_ref().expect("the migrated definition is addressable by name");
			assert!(names(&ds, ns, db).await.is_empty());
		}

		/// A definition whose value derives no name — no name field and no silo
		/// coordinates — has nowhere to be moved to, so it stays where it is.
		#[tokio::test]
		async fn a_definition_with_no_derivable_name_is_left_in_place() {
			use crate::catalog::{ModuleExecutable, StoredPermission, SurrealismExecutable};
			let (ds, ns, db) = fixture().await;
			let nameless = StoredModuleDefinition {
				name: None,
				comment: None,
				permissions: StoredPermission::None,
				executable: ModuleExecutable::Surrealism(SurrealismExecutable {
					bucket: "mods".to_string(),
					key: "/anonymous.wasm".to_string(),
				}),
				unsigned: true,
			};
			assert!(nameless.get_storage_name().is_err());
			write_at(&ds, ns, db, "mod::anonymous", &nameless).await;
			let before = band(&ds, ns, db).await;

			rekey_definitions_to_their_derived_name(&ds).await.unwrap();

			assert_eq!(band(&ds, ns, db).await, before);
		}
	}
}
