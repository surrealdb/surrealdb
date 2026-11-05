//! Re-saves field and function definitions that store their kinds as an AST.
//!
//! Before 3.3 a field's `TYPE` and a function's argument and return types were
//! stored as the structured `Kind` enum (`StoredFieldDefinition` before
//! revision 5, `StoredFunctionDefinition` before revision 4). Later revisions
//! store the canonical SurrealQL text of the kind, and the codec converts an
//! older revision on every read by rendering the `Kind` it holds.
//!
//! This migration re-saves each such definition at its current revision, so
//! that every definition in the band stores text and no read renders a `Kind`
//! again, in the Rust codec or in the bytecode VM's.
//!
//! # Which nodes read what it writes
//!
//! The revisions written — `StoredFieldDefinition` revision 6 and
//! `StoredFunctionDefinition` revision 4 — are the ones 3.3.0 and later write
//! for every `DEFINE FIELD` and `DEFINE FUNCTION`, so every node on 3.3.0 or
//! later reads them. A node on 3.2 or earlier, which writes field revision 4
//! and function revision 3, does not. The migration therefore must not run
//! while such a node can share the store: the supported upgrade path into
//! this release starts at the latest 3.3.x, so a rolling upgrade never mixes
//! it with a 3.2 node.
//!
//! # Re-entrancy and concurrent writers
//!
//! One conditional write per definition, fenced on the bytes that were read:
//! a definition a client redefined in the meantime is left as the client wrote
//! it. Re-running converges, because a definition already at its current
//! revision is not selected.

use anyhow::Result;
use tracing::{debug, warn};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseId, NamespaceId, StoredFieldDefinition, StoredFunctionDefinition};
use crate::key::schema::{FieldKey, FieldPrefix, FunctionKey, FunctionPrefix};
use crate::key::{KVKeyDecode, KVValue};
use crate::kvs::TransactionType;
use crate::kvs::ds::Datastore;

const TARGET: &str = "surrealdb::core::kvs::migration";

/// The first field revision that stores its kind as text.
const FIELD_TEXT_REVISION: u16 = 5;
/// The first function revision that stores its kinds as text.
const FUNCTION_TEXT_REVISION: u16 = 4;

/// Re-saves every field and function definition that stores a kind as an AST.
pub(super) async fn resave_kinds_as_text(ds: &Datastore) -> Result<()> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let scopes = async {
		let mut scopes = Vec::new();
		for ns in txn.all_ns(None).await?.iter() {
			for db in txn.all_db(ns.namespace_id, None).await?.iter() {
				let tables = txn.all_tb(ns.namespace_id, db.database_id, None).await?;
				let tables = tables.iter().map(|tb| tb.name.clone()).collect::<Vec<_>>();
				scopes.push((ns.namespace_id, db.database_id, tables));
			}
		}
		Ok::<_, anyhow::Error>(scopes)
	}
	.await;
	txn.cancel().await?;

	let mut resaved = 0usize;
	for (ns, db, tables) in scopes? {
		resaved += resave_functions(ds, ns, db).await?;
		for table in &tables {
			resaved += resave_fields(ds, ns, db, table).await?;
		}
	}
	if resaved > 0 {
		debug!(target: TARGET, count = resaved, "Re-saved definitions whose kinds were an AST");
	}
	Ok(())
}

/// The revision a stored definition was written at: its leading varint.
fn stored_revision(bytes: &[u8]) -> Option<u16> {
	match bytes.first()? {
		b @ 0..=250 => Some(*b as u16),
		251 => bytes.get(1..3).map(|b| u16::from_le_bytes([b[0], b[1]])),
		_ => None,
	}
}

async fn resave_fields(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &crate::val::TableName,
) -> Result<usize> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let range = catch!(
		txn,
		FieldPrefix {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(table),
		}
		.range()
	);
	let rows = catch!(txn, txn.getr_raw(range, None).await);
	txn.cancel().await?;

	let mut resaved = 0;
	for (key, value) in rows {
		if stored_revision(&value).is_none_or(|rev| rev >= FIELD_TEXT_REVISION) {
			continue;
		}
		let definition = match StoredFieldDefinition::kv_decode_value(&value, ()) {
			Ok(definition) => definition,
			Err(e) => {
				warn!(target: TARGET, error = %e, "Skipping a field definition that does not decode");
				continue;
			}
		};
		let key = FieldKey::decode_key(&key)?;
		if resave(ds, &key, &definition).await? {
			resaved += 1;
		}
	}
	Ok(resaved)
}

async fn resave_functions(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Result<usize> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let range = catch!(
		txn,
		FunctionPrefix {
			ns,
			db,
		}
		.range()
	);
	let rows = catch!(txn, txn.getr_raw(range, None).await);
	txn.cancel().await?;

	let mut resaved = 0;
	for (key, value) in rows {
		if stored_revision(&value).is_none_or(|rev| rev >= FUNCTION_TEXT_REVISION) {
			continue;
		}
		let definition = match StoredFunctionDefinition::kv_decode_value(&value, ()) {
			Ok(definition) => definition,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					"Skipping a function definition that does not decode"
				);
				continue;
			}
		};
		let key = FunctionKey::decode_key(&key)?;
		if resave(ds, &key, &definition).await? {
			resaved += 1;
		}
	}
	Ok(resaved)
}

/// Writes `definition` at `key` at its current revision, if what is stored
/// there still decodes to it. Answers whether it wrote.
async fn resave<K>(ds: &Datastore, key: &K, definition: &K::Value) -> Result<bool>
where
	K: crate::key::KVKey + std::fmt::Debug,
{
	let txn = ds.transaction(TransactionType::Write).await?;
	let written = txn.put_compare_key(key, definition, Some(definition)).await;
	match run!(txn, written) {
		Ok(()) => Ok(true),
		// Redefined since it was read: the newer definition stands.
		Err(e) if crate::kvs::is_conditional_write_conflict(&e) => Ok(false),
		Err(e) => Err(e),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_revision_is_the_leading_varint() {
		assert_eq!(stored_revision(&[4, 9, 9]), Some(4));
		assert_eq!(stored_revision(&[251, 0x01, 0x01]), Some(257));
		assert_eq!(stored_revision(&[]), None);
	}

	#[cfg(feature = "kv-mem")]
	mod against_a_datastore {
		use std::borrow::Cow;

		use super::*;
		use crate::dbs::Session;
		use crate::key::KVKey;
		use crate::kvs::compat::v3_3_0;
		use crate::val::TableName;

		/// A datastore holding one table with one field and one function, whose
		/// definitions are then replaced by the bytes 3.3.0 wrote.
		async fn with_3_3_definitions()
		-> (std::sync::Arc<Datastore>, FieldKey<'static>, FunctionKey<'static>) {
			let ds = Datastore::new("memory").await.unwrap();
			let session = Session::owner().with_ns("test").with_db("test");
			for response in ds
				.execute(
					"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
					 DEFINE TABLE person; DEFINE FIELD name ON person TYPE string; \
					 DEFINE FUNCTION fn::f($a: int) { $a };",
					&session,
					None,
				)
				.await
				.unwrap()
			{
				response.result.unwrap();
			}
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			let (ns, db) = {
				let db = txn.expect_db_by_name("test", "test").await.unwrap();
				(db.namespace_id, db.database_id)
			};
			let table = TableName::new("person");
			let field = FieldKey::new(ns, db, Cow::Owned(table), Cow::Owned("name".to_owned()));
			let function = FunctionKey::new(ns, db, Cow::Owned("f".to_owned()));
			txn.set(field.encode_key().unwrap(), v3_3_0::FIELD_WITH_TYPE.to_vec()).await.unwrap();
			txn.set(function.encode_key().unwrap(), v3_3_0::FUNCTION_WITH_ARGS.to_vec())
				.await
				.unwrap();
			txn.commit().await.unwrap();
			(ds, field, function)
		}

		/// Whether `bytes` store a definition whose kinds are text: the stored grammar a store at
		/// a feature level writes, or a `revision` entry at or beyond `text_revision`.
		fn stores_text(bytes: &[u8], text_revision: u16) -> bool {
			bytes.first() == Some(&crate::catalog::stored::CATALOG_MARKER)
				|| stored_revision(bytes).is_some_and(|revision| revision >= text_revision)
		}

		async fn stored<K: crate::key::KVKey + std::fmt::Debug>(
			ds: &Datastore,
			key: &K,
		) -> Vec<u8> {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let bytes = txn.get_key_raw(key, None).await.unwrap().unwrap();
			txn.cancel().await.unwrap();
			bytes
		}

		#[tokio::test]
		async fn an_ast_era_definition_is_resaved_as_text() {
			let (ds, field, function) = with_3_3_definitions().await;
			resave_kinds_as_text(&ds).await.unwrap();

			let field_bytes = stored(&ds, &field).await;
			assert!(stores_text(&field_bytes, FIELD_TEXT_REVISION));
			assert_eq!(
				StoredFieldDefinition::kv_decode_value(&field_bytes, ()).unwrap(),
				StoredFieldDefinition::kv_decode_value(v3_3_0::FIELD_WITH_TYPE, ()).unwrap(),
			);
			let function_bytes = stored(&ds, &function).await;
			assert!(stores_text(&function_bytes, FUNCTION_TEXT_REVISION));
			assert_eq!(
				StoredFunctionDefinition::kv_decode_value(&function_bytes, ()).unwrap(),
				StoredFunctionDefinition::kv_decode_value(v3_3_0::FUNCTION_WITH_ARGS, ()).unwrap(),
			);
		}

		#[tokio::test]
		async fn a_text_era_definition_is_left_as_it_is() {
			let (ds, field, _) = with_3_3_definitions().await;
			resave_kinds_as_text(&ds).await.unwrap();
			let once = stored(&ds, &field).await;
			resave_kinds_as_text(&ds).await.unwrap();
			assert_eq!(stored(&ds, &field).await, once);
		}
	}
}
