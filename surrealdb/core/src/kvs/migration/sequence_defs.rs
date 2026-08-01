//! Moves sequence definitions out of the table band.
//!
//! Before 3.3 a `DEFINE SEQUENCE` definition was stored at
//! `/*{ns}*{db}*sq{name}`. At that byte position `*` introduces a *table name*,
//! so the definition of a sequence named `foo` occupied exactly the keyspace
//! root of a table named `sqfoo`, and the scan that lists definitions —
//! `[/*{ns}*{db}*sq\0, /*{ns}*{db}*sr)` — covered every key of every table whose
//! name begins with `sq`. Listing sequences on such a database decoded table
//! rows as `SequenceDefinition` and failed, which took `INFO FOR DB`, export,
//! and `REMOVE DATABASE`/`REMOVE NAMESPACE` down with it.
//!
//! Definitions now live at `/*{ns}*{db}!sd{name}`, a subspace holding nothing
//! else. This migration copies each legacy definition across and **leaves the
//! legacy key in place**.
//!
//! # Why the legacy key stays
//!
//! A migration runs as soon as the first upgraded node starts, while the rest
//! of a cluster may still be on the previous release, which resolves sequence
//! definitions at `*sq` and nowhere else. Removing the key there would make
//! every sequence vanish from those nodes mid-rollout — and an operator who
//! then re-ran `DEFINE SEQUENCE` to restore one would, on that older node, also
//! clear the allocator state under `!sq{name}`, restarting allocation at
//! `START` and re-issuing values already handed out.
//!
//! Copying leaves the older nodes working on what they already have. What they
//! cannot see is a sequence *created* after the rollout begins, because a node
//! on this release writes only to `!sd`; that is the one asymmetry the upgrade
//! window carries, and it belongs in the release notes.
//!
//! TODO(3.4): once no supported upgrade path starts below 3.3, add a second
//! migration that deletes the `*sq` keys this one leaves behind. Until then the
//! contract is that they are inert: nothing reads them, and this migration is
//! the only thing that writes to that band.
//!
//! # Telling a definition from a table's data
//!
//! Both live under the same prefix, so the range alone cannot separate them.
//! A legacy definition key is `{db root}*sq{name}\0` and *ends there*; every
//! key belonging to a table carries more after the table root — a catalog tag
//! (`!fd`, `!ix`, …), a record (`*`), an index entry (`+`), a graph edge (`~`)
//! or a reference (`&`). Nothing is ever stored at a bare table root. So a key
//! is a definition exactly when its remainder is one `storekey`-encoded string
//! and nothing follows it, which [`legacy_name`] decides. Because `storekey`
//! escapes `0x00` and `0x01` inside a string, that terminator is unambiguous
//! for every name, including names that themselves contain `*`, `!` or a null.
//!
//! # Re-entrancy
//!
//! One copy per definition, each in its own transaction. Re-running after any
//! interruption converges, because a definition already copied is written again
//! with the same bytes. Two nodes running this at once do the same work in the
//! same order and reach the same state.

use anyhow::Result;
use storekey::decode_borrow;
use tracing::{debug, warn};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, NamespaceId, SequenceDefinition};
use crate::key::schema::{DbRoot, SequenceKey};
use crate::key::{KVSubspace, KVValue, Key, KeyRange};
use crate::kvs::TransactionType;
use crate::kvs::ds::Datastore;

const TARGET: &str = "surrealdb::core::kvs::migration";

/// The legacy tag, which sat where a table name begins.
const LEGACY_TAG: &[u8] = b"*sq";

/// Copies every database's legacy sequence definitions into the `!sd`
/// subspace, leaving the legacy keys in place.
pub(super) async fn copy_definitions_out_of_the_table_band(ds: &Datastore) -> Result<()> {
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

	let mut copied = 0usize;
	for (ns, db) in databases {
		copied += migrate_database(ds, ns, db).await?;
	}
	if copied > 0 {
		debug!(target: TARGET, count = copied, "Copied sequence definitions to the !sd subspace");
	}
	Ok(())
}

/// Copies one database's legacy definitions, returning how many were copied.
async fn migrate_database(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Result<usize> {
	let legacy = collect_legacy(ds, ns, db).await?;

	for (_, definition) in &legacy {
		let txn = ds.transaction(TransactionType::Write).await?;
		// Create-only. The value was read under an earlier, already-closed
		// snapshot, so a blind write would clobber whatever reached `!sd` in
		// between — another node's copy of the same definition, or a client's
		// newer `DEFINE SEQUENCE`. If the key is there, someone with fresher
		// information wrote it and this copy has nothing to add.
		let copied = txn
			.put_key(
				&SequenceKey::new(ns, db, std::borrow::Cow::Borrowed(definition.name.as_str())),
				definition,
			)
			.await;
		match run!(txn, copied) {
			// Already present, by definition of a create-only write.
			Err(e) if super::already_exists(&e) => continue,
			// Anything else propagates. A write conflict is not evidence that a
			// peer completed this copy, and swallowing it would let the ledger
			// record a migration that left a definition behind.
			other => other?,
		}
	}
	Ok(legacy.len())
}

/// How many keys to read per scan round.
///
/// The scanned region is the legacy prefix, which a table whose name begins
/// with `sq` also occupies, so its size is bounded by that table's size rather
/// than by the number of sequences. Each page is read under its own
/// transaction, which bounds both the memory held and the lifetime of any one
/// snapshot.
const SCAN_BATCH: u32 = 1000;

/// Every legacy definition in one database, paired with the key it was read
/// from.
///
/// A value that does not decode is left in place and logged rather than failing
/// the migration: it is not a definition this code wrote, and deleting bytes we
/// cannot identify would be the worse outcome.
async fn collect_legacy(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
) -> Result<Vec<(Key<'static>, SequenceDefinition)>> {
	let root = DbRoot::new(ns, db);
	let mut prefix = root.encode_bound()?.as_ref().to_vec();
	prefix.extend_from_slice(LEGACY_TAG);
	// The definitions' own bytes plus everything beneath them. Built by hand
	// rather than from a bound, because the layout it addresses is the one the
	// schema no longer declares.
	let region = Key::from(prefix.clone()).prefix_expect();
	let region_end = region.end.clone();
	let mut next = Some(root.raw(region));

	let mut found = Vec::new();
	while let Some(range) = next {
		// One transaction per batch, not one for the whole scan. The region this
		// walks is sized by any table whose name begins with `sq`, not by the
		// number of sequences, so a single snapshot held across it would outlive
		// a distributed backend's GC horizon on exactly the databases this
		// migration exists for.
		let txn = ds.transaction(TransactionType::Read).await?;
		// Keys alone decide what is a definition, so the scan does not carry the
		// table's values back with it; the few that matter are read below.
		let batch = catch!(txn, txn.batch_keys_raw(range, SCAN_BATCH, None).await);
		// A key that is not a definition belongs to a table sharing this prefix,
		// and none of that table's other keys can be one either: a definition is
		// byte-identical to the table's root, so it sorts first and has already
		// been seen. Seeking past the subtree turns the cost of a colliding
		// table from "every key it holds" into "one round trip".
		let mut skip_to = None;
		let mut names: Vec<(Key<'static>, String)> = Vec::new();
		for key in &batch.result {
			match classify(key, &prefix) {
				Legacy::Definition(name) => names.push((Key::from(key.clone()), name)),
				Legacy::TableSubtree(root_len) => {
					skip_to = Some(Key::from(key[..root_len].to_vec()).next_neighbour_expect());
					break;
				}
				Legacy::Foreign => {}
			}
		}
		for (key, name) in names {
			let value = catch!(txn, txn.get(key.clone(), None).await);
			let Some(value) = value else {
				continue;
			};
			match SequenceDefinition::kv_decode_value(&value, ()) {
				Ok(definition) => {
					if definition.name.as_str() != name {
						warn!(
							target: TARGET,
							key_name = %name,
							value_name = %definition.name,
							"Skipping a legacy sequence definition whose stored name \
							 disagrees with its key"
						);
						continue;
					}
					found.push((key, definition));
				}
				Err(e) => warn!(
					target: TARGET,
					name = %name,
					error = %e,
					"Skipping a key under the legacy sequence prefix that does not \
					 decode as a sequence definition"
				),
			}
		}
		txn.cancel().await?;
		// What a batch leaves unread is a tail of the same region, so the bound
		// that produced the range is the one that wraps the continuation.
		next = match skip_to {
			// Resume past the skipped subtree, whether or not the batch filled.
			// The keys it left unread are the ones being skipped over, so the
			// batch's own continuation is not the one to follow.
			Some(start) if start < region_end => Some(root.raw(KeyRange {
				start,
				end: region_end.clone(),
			})),
			Some(_) => None,
			None => batch.next.map(|rng| root.raw(rng)),
		};
	}
	Ok(found)
}

/// What a key under the legacy prefix is.
#[derive(Debug, PartialEq, Eq)]
enum Legacy {
	/// A sequence definition, spelling this name.
	Definition(String),
	/// A key belonging to a table whose name begins with `sq`, whose root is
	/// this many bytes long.
	TableSubtree(usize),
	/// Not under the prefix at all.
	Foreign,
}

/// Classifies a key under the legacy prefix.
///
/// A definition is `{prefix}{name}` where `{name}` is one `storekey` string and
/// the key *ends* with it. Anything longer belongs to a table whose name starts
/// with `sq`: the extra bytes are that table's own key material.
fn classify(key: &[u8], prefix: &[u8]) -> Legacy {
	let Some(rest) = key.strip_prefix(prefix) else {
		return Legacy::Foreign;
	};
	let Some(end) = name_end(rest) else {
		// A name with no terminator is not a whole key of either shape.
		return Legacy::Foreign;
	};
	if end == rest.len() {
		return match decode_borrow::<String>(rest) {
			Ok(name) => Legacy::Definition(name),
			Err(_) => Legacy::Foreign,
		};
	}
	Legacy::TableSubtree(prefix.len() + end)
}

/// The offset just past the `storekey`-encoded name at the start of `rest`.
///
/// `storekey` escapes `0x00` and `0x01` by prefixing `0x01`, so the byte after
/// an escape belongs to the name whatever it is, and the first unescaped `0x00`
/// is the terminator.
fn name_end(rest: &[u8]) -> Option<usize> {
	let mut i = 0;
	while i < rest.len() {
		match rest[i] {
			0x01 => i += 2,
			0x00 => return Some(i + 1),
			_ => i += 1,
		}
	}
	None
}

/// The sequence name a legacy definition key spells, or `None` otherwise.
#[cfg(test)]
fn legacy_name(key: &[u8], prefix: &[u8]) -> Option<String> {
	match classify(key, prefix) {
		Legacy::Definition(name) => Some(name),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn prefix() -> Vec<u8> {
		let mut p =
			DbRoot::new(NamespaceId(1), DatabaseId(2)).encode_bound().unwrap().as_ref().to_vec();
		p.extend_from_slice(LEGACY_TAG);
		p
	}

	fn legacy_key(name: &str) -> Vec<u8> {
		let mut k = prefix();
		k.extend_from_slice(&storekey::encode_vec(&name).unwrap());
		k
	}

	/// The bytes this module addresses are the bytes 3.0 through 3.2 actually
	/// wrote.
	///
	/// The literal is the definition key for `test` in ns 1 / db 2 as those
	/// releases encoded it. Everything else here is built from [`legacy_key`],
	/// so if that helper and the real layout disagreed the rest of these tests
	/// would agree with each other and be wrong together. This is the one that
	/// says otherwise.
	#[test]
	fn the_legacy_layout_matches_what_3_2_wrote() {
		assert_eq!(legacy_key("test"), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sqtest\0");
	}

	#[test]
	fn a_definition_key_yields_its_name() {
		let prefix = prefix();
		assert_eq!(legacy_name(&legacy_key("foo"), &prefix).as_deref(), Some("foo"));
		assert_eq!(legacy_name(&legacy_key(""), &prefix).as_deref(), Some(""));
	}

	/// Names are the adversarial part: they are user-supplied and may contain
	/// the bytes the surrounding key structure uses.
	#[test]
	fn awkward_names_round_trip() {
		let prefix = prefix();
		for name in [
			"foo",
			"sq",
			"sqfoo",
			"*",
			"!fd",
			"*sq",
			"a*b!c",
			"\u{0}",
			"a\u{0}b",
			"\u{1}",
			"\u{1}\u{0}\u{1}",
			"ключ",
			"emoji-🔑",
			"a".repeat(512).as_str(),
		] {
			assert_eq!(
				legacy_name(&legacy_key(name), &prefix).as_deref(),
				Some(name),
				"name {name:?} did not round trip"
			);
		}
	}

	/// Every shape of key that lives under a table whose name begins with `sq`
	/// must be rejected, because all of them share the legacy prefix.
	#[test]
	fn keys_belonging_to_a_table_are_rejected() {
		let prefix = prefix();
		for suffix in [
			&b"!fd"[..],  // a field definition
			&b"!ix"[..],  // an index definition
			&b"!ev"[..],  // an event
			&b"*"[..],    // a record
			&b"+"[..],    // an index entry
			&b"~"[..],    // a graph edge
			&b"&"[..],    // a reference
			&b"!lq"[..],  // a live query
			&b"\x00"[..], // a bare extra terminator
		] {
			// `sqfoo`'s table root is byte-identical to sequence `foo`'s legacy
			// key, so every one of these keys starts with the legacy prefix.
			let mut key = legacy_key("foo");
			key.extend_from_slice(suffix);
			assert_eq!(
				legacy_name(&key, &prefix),
				None,
				"a table key ending {suffix:?} was mistaken for a definition"
			);
		}
	}

	/// A key that is not under the prefix at all is not a definition.
	#[test]
	fn a_foreign_key_is_rejected() {
		let prefix = prefix();
		assert_eq!(legacy_name(b"/!nstest\0", &prefix), None);
		let other_db =
			DbRoot::new(NamespaceId(9), DatabaseId(9)).encode_bound().unwrap().as_ref().to_vec();
		assert_eq!(legacy_name(&other_db, &prefix), None);
	}

	/// A truncated name has no terminator, so it cannot be read as one.
	#[test]
	fn a_truncated_key_is_rejected() {
		let prefix = prefix();
		let mut key = prefix.clone();
		key.extend_from_slice(b"foo");
		assert_eq!(legacy_name(&key, &prefix), None);
	}

	#[cfg(feature = "kv-mem")]
	mod against_a_datastore {
		use super::*;
		use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
		use crate::key::schema::SequencePrefix;
		use crate::kvs::Datastore;

		/// Creates a namespace and a database inside it with the given ids.
		async fn create_db(ds: &Datastore, ns: NamespaceId, db: DatabaseId, name: &str) {
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.put_ns(NamespaceDefinition {
				namespace_id: ns,
				name: name.into(),
				comment: None,
			})
			.await
			.unwrap();
			txn.put_db(name, database(ns, db, name)).await.unwrap();
			txn.commit().await.unwrap();
		}

		fn database(ns: NamespaceId, db: DatabaseId, name: &str) -> DatabaseDefinition {
			DatabaseDefinition {
				namespace_id: ns,
				database_id: db,
				name: name.into(),
				strict: false,
				comment: None,
				changefeed: None,
			}
		}

		/// A datastore with one namespace and database, returning their ids.
		async fn fixture() -> (Datastore, NamespaceId, DatabaseId) {
			let ds = Datastore::new("memory").await.unwrap();
			let (ns, db) = (NamespaceId(1), DatabaseId(1));
			create_db(&ds, ns, db, "test").await;
			(ds, ns, db)
		}

		fn definition(name: &str, batch: u32, start: i64) -> SequenceDefinition {
			SequenceDefinition {
				name: name.into(),
				batch,
				start,
				timeout: None,
			}
		}

		/// Writes a definition where 3.2 would have put it.
		async fn write_legacy(
			ds: &Datastore,
			ns: NamespaceId,
			db: DatabaseId,
			d: &SequenceDefinition,
		) {
			let mut key = DbRoot::new(ns, db).encode_bound().unwrap().as_ref().to_vec();
			key.extend_from_slice(LEGACY_TAG);
			key.extend_from_slice(&storekey::encode_vec(&d.name.as_str()).unwrap());
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set(Key::from(key), d.kv_encode_value().unwrap()).await.unwrap();
			txn.commit().await.unwrap();
		}

		/// Every definition the new subspace holds.
		async fn definitions(
			ds: &Datastore,
			ns: NamespaceId,
			db: DatabaseId,
		) -> Vec<SequenceDefinition> {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let range = SequencePrefix::new(ns, db).range().unwrap();
			let found = txn.getr(range, None).await.unwrap();
			txn.cancel().await.unwrap();
			found.into_iter().map(|(_, d)| d).collect()
		}

		/// Everything still sitting under the legacy prefix, with its bytes.
		async fn legacy_bytes(
			ds: &Datastore,
			ns: NamespaceId,
			db: DatabaseId,
		) -> Vec<(Vec<u8>, Vec<u8>)> {
			let root = DbRoot::new(ns, db);
			let mut prefix = root.encode_bound().unwrap().as_ref().to_vec();
			prefix.extend_from_slice(LEGACY_TAG);
			let range = root.raw(Key::from(prefix).prefix_expect());
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.getr_raw(range, None).await.unwrap();
			txn.cancel().await.unwrap();
			found
		}

		/// Everything still sitting under the legacy prefix.
		async fn legacy_keys(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Vec<Vec<u8>> {
			let root = DbRoot::new(ns, db);
			let mut prefix = root.encode_bound().unwrap().as_ref().to_vec();
			prefix.extend_from_slice(LEGACY_TAG);
			let range = root.raw(Key::from(prefix).prefix_expect());
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.getr_raw(range, None).await.unwrap();
			txn.cancel().await.unwrap();
			found.into_iter().map(|(k, _)| k).collect()
		}

		#[tokio::test]
		async fn definitions_move_with_their_values_intact() {
			let (ds, ns, db) = fixture().await;
			let a = definition("orders", 50, 7);
			let b = definition("invoices", 1, -3);
			write_legacy(&ds, ns, db, &a).await;
			write_legacy(&ds, ns, db, &b).await;

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			let mut moved = definitions(&ds, ns, db).await;
			moved.sort_by(|x, y| x.name.cmp(&y.name));
			assert_eq!(moved, vec![b, a]);
			// The legacy keys stay: a node still on the previous release resolves
			// definitions there and nowhere else.
			assert_eq!(legacy_keys(&ds, ns, db).await.len(), 2);
		}

		/// The case the move exists for: a table whose name begins with `sq`
		/// shares the legacy prefix, and none of its keys may be touched or
		/// mistaken for a definition.
		#[tokio::test]
		async fn a_colliding_table_is_left_alone() {
			let (ds, ns, db) = fixture().await;
			let sequence = definition("foo", 10, 0);
			write_legacy(&ds, ns, db, &sequence).await;

			// Table `sqfoo`'s root is byte-identical to sequence `foo`'s legacy
			// key, so each of these sorts inside the scan the migration runs.
			let mut roots = Vec::new();
			for table in ["sq", "sqfoo", "sqzzz"] {
				let mut root = DbRoot::new(ns, db).encode_bound().unwrap().as_ref().to_vec();
				root.extend_from_slice(b"*");
				root.extend_from_slice(&storekey::encode_vec(&table).unwrap());
				roots.push(root);
			}
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			let mut planted = Vec::new();
			for root in &roots {
				for suffix in
					[&b"!fda\0"[..], &b"*\x01record"[..], &b"~edge"[..], &b"+\x00\x00\x00\x01"[..]]
				{
					let mut key = root.clone();
					key.extend_from_slice(suffix);
					txn.set(Key::from(key.clone()), b"table data".to_vec()).await.unwrap();
					planted.push(key);
				}
			}
			txn.commit().await.unwrap();

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			assert_eq!(definitions(&ds, ns, db).await, vec![sequence]);
			// Every planted key survives, byte for byte.
			let remaining = legacy_keys(&ds, ns, db).await;
			for key in &planted {
				assert!(
					remaining.contains(key),
					"the migration deleted a key belonging to a table: {key:?}"
				);
			}
			// The planted table keys plus the legacy definition this migration
			// copies but deliberately leaves behind.
			assert_eq!(remaining.len(), planted.len() + 1);
		}

		/// The legacy key is left byte-for-byte intact, which is what keeps a node
		/// still on the previous release able to resolve its sequences during a
		/// rolling upgrade.
		#[tokio::test]
		async fn the_legacy_key_survives_unchanged() {
			let (ds, ns, db) = fixture().await;
			let definition = definition("orders", 50, 7);
			write_legacy(&ds, ns, db, &definition).await;
			let before = legacy_bytes(&ds, ns, db).await;

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			assert_eq!(definitions(&ds, ns, db).await, vec![definition.clone()]);
			// Same key, same value: an older node reading `*sq` sees exactly what
			// it saw before the upgrade.
			assert_eq!(legacy_bytes(&ds, ns, db).await, before);
			assert_eq!(SequenceDefinition::kv_decode_value(&before[0].1, ()).unwrap(), definition);
		}

		/// Running it again changes nothing, which is what makes a crash between
		/// the migration and its ledger entry safe.
		#[tokio::test]
		async fn running_it_twice_converges() {
			let (ds, ns, db) = fixture().await;
			write_legacy(&ds, ns, db, &definition("orders", 50, 7)).await;

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();
			let after_first = definitions(&ds, ns, db).await;
			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			assert_eq!(definitions(&ds, ns, db).await, after_first);
			assert_eq!(legacy_keys(&ds, ns, db).await.len(), 1);
		}

		/// A definition already present at `!sd` is left alone.
		///
		/// This is the create-only write that makes a second pass — another node
		/// having already copied, or a client having written a newer definition
		/// under the same name — safe. Deterministic rather than racy: an actual
		/// interleaving is not reproducible on a single-threaded runtime, and a
		/// test that cannot fail is worse than none.
		#[tokio::test]
		async fn an_existing_definition_is_never_overwritten() {
			let (ds, ns, db) = fixture().await;
			let stale = definition("orders", 1, 0);
			write_legacy(&ds, ns, db, &stale).await;

			// Stands in for whatever reached `!sd` first: another node's copy, or
			// a client's `DEFINE SEQUENCE orders BATCH 999`.
			let newer = definition("orders", 999, 42);
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set_key(&SequenceKey::new(ns, db, std::borrow::Cow::Borrowed("orders")), &newer)
				.await
				.unwrap();
			txn.commit().await.unwrap();

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			assert_eq!(
				definitions(&ds, ns, db).await,
				vec![newer],
				"the migration clobbered a definition written after it read its copy"
			);
		}

		/// Repeated interleaved passes converge, whichever order they land in.
		#[tokio::test]
		async fn repeated_passes_converge() {
			let (ds, ns, db) = fixture().await;
			for i in 0..8 {
				write_legacy(&ds, ns, db, &definition(&format!("seq{i}"), 10, i)).await;
			}

			for _ in 0..4 {
				copy_definitions_out_of_the_table_band(&ds).await.unwrap();
			}

			let mut moved = definitions(&ds, ns, db).await;
			moved.sort_by(|x, y| x.name.cmp(&y.name));
			assert_eq!(moved.len(), 8);
			for (i, d) in moved.iter().enumerate() {
				assert_eq!(d.name.as_str(), format!("seq{i}"));
				assert_eq!(d.start, i as i64);
			}
			assert_eq!(legacy_keys(&ds, ns, db).await.len(), 8);
		}

		/// Nothing to move is not an error, and costs nothing.
		#[tokio::test]
		async fn an_empty_datastore_is_a_no_op() {
			let ds = Datastore::new("memory").await.unwrap();
			copy_definitions_out_of_the_table_band(&ds).await.unwrap();
		}

		/// A value that is not a definition is left where it is rather than
		/// deleted, because the migration cannot say what it is.
		#[tokio::test]
		async fn an_undecodable_value_is_left_in_place() {
			let (ds, ns, db) = fixture().await;
			let mut key = DbRoot::new(ns, db).encode_bound().unwrap().as_ref().to_vec();
			key.extend_from_slice(LEGACY_TAG);
			key.extend_from_slice(&storekey::encode_vec(&"mystery").unwrap());
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set(Key::from(key.clone()), vec![0xff, 0xff, 0xff]).await.unwrap();
			txn.commit().await.unwrap();

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			assert!(definitions(&ds, ns, db).await.is_empty());
			assert_eq!(legacy_keys(&ds, ns, db).await, vec![key]);
		}

		/// Names carrying the bytes the key structure uses survive the move.
		#[tokio::test]
		async fn adversarial_names_survive_the_move() {
			let (ds, ns, db) = fixture().await;
			let names = ["sq", "sqfoo", "*", "!fd", "a\u{0}b", "\u{1}", "ключ", "🔑"];
			for name in names {
				write_legacy(&ds, ns, db, &definition(name, 1, 0)).await;
			}

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			let moved = definitions(&ds, ns, db).await;
			assert_eq!(moved.len(), names.len());
			for name in names {
				assert!(
					moved.iter().any(|d| d.name.as_str() == name),
					"sequence {name:?} did not survive the copy"
				);
			}
			assert_eq!(legacy_keys(&ds, ns, db).await.len(), names.len());
		}

		/// The scan pages, so a region larger than one batch must still yield
		/// every definition in it — including ones that fall after the first
		/// page boundary.
		#[tokio::test]
		async fn definitions_are_found_past_the_first_scan_batch() {
			let (ds, ns, db) = fixture().await;

			// Table `sqbulk` shares the legacy prefix, so its keys pad the scanned
			// region well past one batch. Sequence names are chosen to sort both
			// before and after that padding.
			write_legacy(&ds, ns, db, &definition("aaa-first", 1, 1)).await;
			write_legacy(&ds, ns, db, &definition("zzz-last", 1, 2)).await;

			let mut root = DbRoot::new(ns, db).encode_bound().unwrap().as_ref().to_vec();
			root.extend_from_slice(b"*");
			root.extend_from_slice(&storekey::encode_vec(&"sqbulk").unwrap());
			let padding = (SCAN_BATCH as usize) * 2 + 7;
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			for i in 0..padding {
				let mut key = root.clone();
				key.extend_from_slice(b"*");
				key.extend_from_slice(&storekey::encode_vec(&format!("{i:08}")).unwrap());
				txn.set(Key::from(key), b"record".to_vec()).await.unwrap();
			}
			txn.commit().await.unwrap();

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			let mut moved = definitions(&ds, ns, db).await;
			moved.sort_by(|x, y| x.name.cmp(&y.name));
			assert_eq!(
				moved.iter().map(|d| d.name.to_string()).collect::<Vec<_>>(),
				vec!["aaa-first".to_string(), "zzz-last".to_string()]
			);
			// The padding is untouched, and the two definitions are copied rather
			// than moved, so everything that was there is still there.
			assert_eq!(legacy_keys(&ds, ns, db).await.len(), padding + 2);
		}

		/// Every database is covered, not just the first.
		#[tokio::test]
		async fn all_databases_are_migrated() {
			let ds = Datastore::new("memory").await.unwrap();
			let places = [
				(NamespaceId(1), DatabaseId(1)),
				(NamespaceId(2), DatabaseId(1)),
				(NamespaceId(2), DatabaseId(2)),
			];
			create_db(&ds, NamespaceId(1), DatabaseId(1), "one").await;
			create_db(&ds, NamespaceId(2), DatabaseId(1), "two").await;
			// A second database inside the namespace that already exists.
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.put_db("two", database(NamespaceId(2), DatabaseId(2), "gamma")).await.unwrap();
			txn.commit().await.unwrap();
			for (ns, db) in places {
				write_legacy(&ds, ns, db, &definition("shared", 5, 1)).await;
			}

			copy_definitions_out_of_the_table_band(&ds).await.unwrap();

			for (ns, db) in places {
				assert_eq!(definitions(&ds, ns, db).await.len(), 1, "ns {ns:?} db {db:?}");
				assert_eq!(legacy_keys(&ds, ns, db).await.len(), 1, "ns {ns:?} db {db:?}");
			}
		}
	}
}
