//! What the change feed persists.
//!
//! A database or table with `CHANGEFEED` set records every record and table
//! definition change into the `#` section of the keyspace, timestamped by the
//! commit that made it, so a reader can replay a window of history. These are the
//! records that section holds: the per-table batch of mutations one commit
//! produced, the reader's grouping of those batches by versionstamp, and the
//! per-transaction buffer that accumulates them until commit time.
//!
//! `SetWithDiff` records its patches in reverse - the operations that turn the
//! *current* value back into the *previous* one - because `replace` and `remove`
//! cannot be resolved in the forward direction. That is a property of the stored
//! form, so the classification that upholds it travels with the shape rather than
//! staying with the document machinery that calls it.
//!
//! Reading the feed back, and garbage-collecting it by retention, are one layer
//! up: both need the transaction.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::Mutex;
use revision::revisioned;
use surrealdb_catalog::stored::EmbeddedEntry;
use surrealdb_catalog::{
	DatabaseId, FromStored, NamespaceId, Record, StoredTableDefinition, TableDefinition,
};
use surrealdb_expr::expr::Operation;
use surrealdb_expr::expr::statements::info::InfoStructure;
use surrealdb_expr::val::{Array, Object, RecordId, TableName, Value};
use surrealdb_kvs::Val;
use surrealdb_kvs::value::KVValue;

use crate::values::save_point::{fold_frames, pop_save_point, reset_frames};

// Mutation is a single mutation to a table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TableMutation {
	// Although the Value is supposed to contain a field "id" of [`RecordId`],
	// we do include it in the first field for convenience.
	Set(RecordId, Value),
	Del(RecordId),
	/// A table definition, held through [`EmbeddedEntry`] so that a definition `revision`
	/// cannot hold travels in the stored grammar.
	Def(EmbeddedEntry<StoredTableDefinition>),
	/// Includes the ID, current value (after change), changes that can be
	/// applied to get the original value
	/// Example, ("mytb:tobie", {{"note": "surreal"}}, [{"op": "add", "path":
	/// "/note", "value": "surreal"}], false) Means that we have already
	/// applied the add "/note" operation to achieve the recorded result
	SetWithDiff(RecordId, Value, Vec<Operation>),
	/// Delete a record where the ID is stored, and the now-deleted value
	DelWithOriginal(RecordId, Value),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableMutations(pub TableName, pub Vec<TableMutation>);

impl_kv_value_embedding!(TableMutations);

impl TableMutations {
	/// Create a new table mutations
	pub fn new(tb: TableName) -> Self {
		Self(tb, Vec::new())
	}
	/// Push a table change to the table mutations
	pub fn push_table_change(&mut self, dt: StoredTableDefinition) {
		// Push the table change to the entry
		self.1.push(TableMutation::Def(EmbeddedEntry::from(dt)));
	}

	/// Push a mutation to the table mutations (record change)
	pub fn push_record_change(
		&mut self,
		id: RecordId,
		previous: Arc<Record>,
		current: Arc<Record>,
		store_difference: bool,
	) {
		// Check if this is a delete operation
		if current.data.is_nullish() {
			// Push the delete mutation to the entry
			self.1.push(match store_difference {
				true => TableMutation::DelWithOriginal(id, into_owned(previous)),
				false => TableMutation::Del(id),
			});
		} else {
			// Push the set mutation to the entry
			self.1.push(match store_difference {
				true => {
					if previous.data.is_none() {
						TableMutation::Set(id, into_owned(current))
					} else {
						// We intentionally record the patches in reverse (current -> previous)
						// because we cannot otherwise resolve operations such as "replace" and
						// "remove".
						let patches_to_create_previous = current.data.diff(&previous.data);
						TableMutation::SetWithDiff(
							id,
							into_owned(current),
							patches_to_create_previous,
						)
					}
				}
				false => TableMutation::Set(id, into_owned(current)),
			});
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DatabaseMutation(pub Vec<TableMutations>);

impl DatabaseMutation {
	pub fn new() -> Self {
		Self(Vec::new())
	}
}

impl Default for DatabaseMutation {
	fn default() -> Self {
		Self::new()
	}
}

// ChangeSet is a set of mutations made to a database at a specific timestamp.
// The u128 timestamp represents the version number when these changes occurred.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ChangeSet(pub u128, pub DatabaseMutation);

impl TableMutation {
	/// Convert a stored change feed table mutation (record change) into a
	/// Value that can be used in the storage of change feeds and their
	/// transmission to consumers.
	///
	/// Fails only for `Def`, whose stored table definition is compiled so the
	/// rendering matches every other reader's; see the arm.
	pub fn into_value(self) -> Result<Value> {
		let mut h = Object::default();
		let h = match self {
			TableMutation::Set(_thing, v) => {
				h.insert("update", v);
				h
			}
			TableMutation::SetWithDiff(_thing, current, operations) => {
				h.insert("current", current);
				h.insert(
					"update",
					Value::Array(Array(
						operations.into_iter().map(|x| Value::Object(x.into_object())).collect(),
					)),
				);
				h
			}
			TableMutation::Del(t) => {
				let mut inner = Object::default();
				inner.insert("id", Value::RecordId(t));
				h.insert("delete", Value::Object(inner));
				h
			}
			TableMutation::Def(t) => {
				// Rendered through the compiled definition, not the stored one:
				// a definition's clauses are stored under weaker escaping than
				// its rendering uses (`catalog::text`), so rendering the stored
				// form directly would emit a `DEFINE TABLE` that does not read
				// back. Compiling is the funnel every other reader goes through.
				h.insert("define_table", TableDefinition::from_stored(&t)?.structure());
				h
			}
			TableMutation::DelWithOriginal(id, val) => {
				let mut inner = Object::default();
				inner.insert("id", Value::RecordId(id));
				// Surface the stored pre-image so `INCLUDE ORIGINAL` deletes are
				// readable via SHOW CHANGES. Additive: plain `Del` is unchanged,
				// and only `store_diff` deletes carry this `original` field.
				inner.insert("original", val);
				h.insert("delete", Value::Object(inner));
				h
			}
		};
		Ok(Value::Object(h))
	}
}

impl DatabaseMutation {
	pub fn into_value(self) -> Result<Value> {
		let mut changes = Vec::<Value>::new();
		for tbs in self.0 {
			for tb in tbs.1 {
				changes.push(tb.into_value()?);
			}
		}
		Ok(Value::Array(Array::from(changes)))
	}
}

impl ChangeSet {
	pub fn into_value(self) -> anyhow::Result<Value> {
		let mut m = Object::default();
		// The versionstamp is a u128; convert it losslessly (erroring rather
		// than truncating if it is ever too large to represent as a Number).
		m.insert("versionstamp", Value::try_from(self.0)?);
		m.insert("changes", self.1.into_value()?);
		Ok(Value::Object(m))
	}
}

// WriteMutationSet is a set of mutations to be to a table at the specific
// timestamp.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct WriteMutationSet(pub Vec<TableMutations>);

/// Move a record's data out of its `Arc`, cloning only when the `Arc` is shared.
///
/// The change feed is deliberately lazy about this: the plain `Del` arm converts
/// neither side, so converting at the caller instead would add a deep `Value`
/// clone to every change feed delete.
fn into_owned(record: Arc<Record>) -> Value {
	match Arc::try_unwrap(record) {
		Ok(record) => record.data,
		Err(arc) => arc.data.clone(),
	}
}

// PreparedWrite is a tuple of (namespace, database, table, serialized table mutations).
// The timestamp will be provided at commit time via Transaction::current_timestamp().
type PreparedWrite = (NamespaceId, DatabaseId, TableName, Val);

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct ChangeKey {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: TableName,
}

/// Changefeed is a per-transaction buffer of table mutations that are
/// persisted to the database at commit time.
pub struct Changefeed {
	/// The buffer of table mutations to be written to the database, as the
	/// save-point frame stack ([`crate::values::save_point`]). Every mutation is
	/// buffered into the top frame, so a rollback discards exactly the mutations
	/// the writes it undid produced — a feed reader is never shown a change the
	/// transaction took back.
	buffer: Mutex<Vec<HashMap<ChangeKey, TableMutations>>>,
}

impl Default for Changefeed {
	fn default() -> Self {
		Self::new()
	}
}

impl Changefeed {
	/// Create a new changefeed buffer
	pub fn new() -> Self {
		Self {
			buffer: Mutex::new(vec![HashMap::new()]),
		}
	}

	/// Record a table definition modification
	pub fn buffer_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		dt: &StoredTableDefinition,
	) {
		// Acquire the buffer lock
		let mut frames = self.buffer.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		// Get or create the entry for the change key and push the table change
		frame
			.entry(ChangeKey {
				ns,
				db,
				tb: tb.clone(),
			})
			.or_insert_with(|| TableMutations::new(tb.clone()))
			.push_table_change(dt.to_owned());
	}

	/// Record a record modification or deletion
	#[expect(clippy::too_many_arguments)]
	pub fn buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: RecordId,
		previous: Arc<Record>,
		current: Arc<Record>,
		store_difference: bool,
	) {
		// Acquire the buffer lock
		let mut frames = self.buffer.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		// Get or create the entry for the change key and push the record change
		frame
			.entry(ChangeKey {
				ns,
				db,
				tb: tb.clone(),
			})
			.or_insert_with(|| TableMutations::new(tb.clone()))
			.push_record_change(id, previous, current, store_difference);
	}

	// get returns all the mutations buffered for this transaction.
	// The timestamp will be provided at commit time.
	//
	// A scope still open here belongs to the transaction — the writes it
	// describes are the ones about to be committed — so the stack is folded down
	// before the mutations are read, which also restores the order they were
	// buffered in.
	pub fn changes(&self) -> Result<Vec<PreparedWrite>> {
		// Acquire the buffer lock
		let mut frames = self.buffer.lock();
		fold_frames(&mut frames, |entry: &mut TableMutations, mutations| {
			entry.1.extend(mutations.1)
		});
		let buffer = frames.first().expect("the transaction frame is always present");
		// For zero-length changes, return early
		if buffer.is_empty() {
			return Ok(Vec::new());
		}
		// Create a new change result set
		let mut res = Vec::with_capacity(buffer.len());
		// Iterate over the buffered mutations
		for (key, mutations) in buffer.iter() {
			// Encode the value
			let value = mutations.kv_encode_value()?;
			// Push the prepared write to the result (timestamp will be added at commit time)
			res.push((key.ns, key.db, key.tb.clone(), value));
		}
		// Return the prepared writes
		Ok(res)
	}

	/// Clear the buffer (used on transaction cancel/rollback).
	pub fn clear(&self) {
		reset_frames(&mut self.buffer.lock());
	}

	/// Open a save-point scope: the mutations buffered from here on are its own.
	pub fn push_save_point(&self) {
		self.buffer.lock().push(HashMap::new());
	}

	/// Fold the save point's frame into the one beneath it: its writes survived,
	/// so the mutations describing them belong to the enclosing scope, after
	/// whatever that scope had already buffered.
	pub fn release_save_point(&self) {
		let mut frames = self.buffer.lock();
		if let Some(top) = pop_save_point(&mut frames) {
			let parent = frames.last_mut().expect("popping left at least one frame");
			for (key, mutations) in top {
				match parent.get_mut(&key) {
					Some(entry) => entry.1.extend(mutations.1),
					None => {
						parent.insert(key, mutations);
					}
				}
			}
		}
	}

	/// Discard the save point's frame: the writes it described were undone, so
	/// `SHOW CHANGES` must never report them.
	pub fn rollback_save_point(&self) {
		pop_save_point(&mut self.buffer.lock());
	}
}

#[cfg(test)]
mod save_point_tests {
	use super::*;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);

	fn table() -> TableName {
		"t".into()
	}

	fn record(key: &str) -> RecordId {
		RecordId::new(table(), key.to_owned())
	}

	/// Buffer the creation of `t:key`.
	fn create(changefeed: &Changefeed, key: &str) {
		changefeed.buffer_record_change(
			NS,
			DB,
			&table(),
			record(key),
			Arc::new(Record::new(Value::None)),
			Arc::new(Record::new(Value::from(key))),
			false,
		);
	}

	/// The records the buffer would flush a mutation for, in the order it holds
	/// them — which is the order a feed reader replays them in.
	fn flushed(changefeed: &Changefeed) -> Vec<RecordId> {
		changefeed
			.changes()
			.unwrap()
			.into_iter()
			.flat_map(|(_, _, _, value)| TableMutations::kv_decode_value(&value, ()).unwrap().1)
			.map(|mutation| match mutation {
				TableMutation::Set(id, _) => id,
				other @ (TableMutation::Del(_)
				| TableMutation::Def(_)
				| TableMutation::SetWithDiff(..)
				| TableMutation::DelWithOriginal(..)) => {
					panic!("the test buffers creates, not {other:?}")
				}
			})
			.collect()
	}

	/// A rolled-back save point's writes are undone, so `SHOW CHANGES` must
	/// never report them: the transaction still commits, and the feed is the
	/// record a reader replays history from.
	#[test]
	fn a_rolled_back_save_point_reports_none_of_its_mutations() {
		let changefeed = Changefeed::new();
		create(&changefeed, "kept");
		changefeed.push_save_point();
		create(&changefeed, "undone");
		changefeed.rollback_save_point();
		assert_eq!(flushed(&changefeed), vec![record("kept")]);
	}

	/// A released save point's writes survived, so its mutations belong to the
	/// enclosing scope, after whatever that scope had already buffered.
	#[test]
	fn a_released_save_point_hands_its_mutations_up_in_order() {
		let changefeed = Changefeed::new();
		create(&changefeed, "first");
		changefeed.push_save_point();
		create(&changefeed, "second");
		changefeed.release_save_point();
		create(&changefeed, "third");
		assert_eq!(flushed(&changefeed), vec![record("first"), record("second"), record("third")]);
	}

	/// Nested, because a single scope cannot tell framing apart from one flat
	/// map: either discards the same mutations. What distinguishes them is an
	/// inner rollback under an outer scope that survives — the inner mutations
	/// have to go and the outer ones have to stay, which one map cannot do.
	#[test]
	fn nested_save_points_keep_only_the_mutations_that_survived() {
		let changefeed = Changefeed::new();
		create(&changefeed, "transaction");
		changefeed.push_save_point();
		create(&changefeed, "outer");
		changefeed.push_save_point();
		create(&changefeed, "inner");
		changefeed.rollback_save_point();
		changefeed.release_save_point();
		assert_eq!(flushed(&changefeed), vec![record("transaction"), record("outer")]);
	}

	/// A scope still open at the flush belongs to the transaction: the writes it
	/// describes are the ones about to be committed.
	#[test]
	fn an_open_save_point_is_flushed_with_the_transaction() {
		let changefeed = Changefeed::new();
		create(&changefeed, "first");
		changefeed.push_save_point();
		create(&changefeed, "second");
		assert_eq!(flushed(&changefeed), vec![record("first"), record("second")]);
	}

	/// Cancelling drops the open scopes along with their mutations, and leaves
	/// the stack at the floor depth every other operation assumes.
	#[test]
	fn clearing_drops_every_open_scope() {
		let changefeed = Changefeed::new();
		create(&changefeed, "a");
		changefeed.push_save_point();
		create(&changefeed, "b");
		changefeed.clear();
		assert_eq!(changefeed.buffer.lock().len(), 1);
		assert!(flushed(&changefeed).is_empty());
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use surrealdb_catalog::{DatabaseId, NamespaceId, TableId};
	use surrealdb_collections::map;
	use surrealdb_expr::val::convert_value_to_public_value;

	use super::*;

	#[test]
	fn serialization() {
		let cs = ChangeSet(
			65536u128,
			DatabaseMutation(vec![TableMutations(
				"mytb".into(),
				vec![
					TableMutation::Set(
						RecordId::new("mytb".into(), "tobie".to_owned()),
						Value::Object(Object::from(HashMap::from([
							("id", Value::from(RecordId::new("mytb".into(), "tobie".to_owned()))),
							("note", Value::from("surreal")),
						]))),
					),
					TableMutation::Del(RecordId::new("mytb".into(), "tobie".to_owned())),
					TableMutation::Def(EmbeddedEntry::from(StoredTableDefinition::new(
						NamespaceId(1),
						DatabaseId(2),
						TableId(3),
						"mytb".into(),
					))),
				],
			)]),
		);
		let v = convert_value_to_public_value(cs.into_value().unwrap()).unwrap().into_json_value();
		let s = serde_json::to_string(&v).unwrap();
		assert_eq!(
			s,
			r#"{"changes":[{"update":{"id":"mytb:tobie","note":"surreal"}},{"delete":{"id":"mytb:tobie"}},{"define_table":{"drop":false,"id":3,"kind":{"kind":"ANY"},"name":"mytb","permissions":{"create":false,"delete":false,"select":false,"update":false},"schemafull":false}}],"versionstamp":65536}"#
		);
	}

	#[test]
	fn serialization_rev2() {
		let cs = ChangeSet(
			65536u128,
			DatabaseMutation(vec![TableMutations(
				"mytb".into(),
				vec![
					TableMutation::SetWithDiff(
						RecordId::new("mytb".into(), "tobie".to_owned()),
						Value::Object(Object::from(HashMap::from([
							("id", Value::from(RecordId::new("mytb".into(), "tobie".to_owned()))),
							("note", Value::from("surreal")),
						]))),
						vec![Operation::Add {
							path: vec!["note".into()],
							value: Value::from("surreal"),
						}],
					),
					TableMutation::SetWithDiff(
						RecordId::new("mytb".into(), "tobie".to_owned()),
						Value::Object(Object::from(HashMap::from([
							("id", Value::from(RecordId::new("mytb".into(), "tobie2".to_owned()))),
							("note", Value::from("surreal")),
						]))),
						vec![Operation::Remove {
							path: vec!["temp".into()],
						}],
					),
					TableMutation::Del(RecordId::new("mytb".into(), "tobie".to_owned())),
					TableMutation::DelWithOriginal(
						RecordId::new("mytb".into(), "tobie".to_owned()),
						Value::Object(Object::from(map! {
								"id" => Value::from(RecordId::new("mytb".into(),"tobie".to_owned())),
								"note" => Value::from("surreal"),
						})),
					),
					TableMutation::Def(EmbeddedEntry::from(StoredTableDefinition::new(
						NamespaceId(1),
						DatabaseId(2),
						TableId(3),
						"mytb".into(),
					))),
				],
			)]),
		);
		let v = convert_value_to_public_value(cs.into_value().unwrap()).unwrap().into_json_value();
		let s = serde_json::to_string(&v).unwrap();
		assert_eq!(
			s,
			r#"{"changes":[{"current":{"id":"mytb:tobie","note":"surreal"},"update":[{"op":"add","path":"/note","value":"surreal"}]},{"current":{"id":"mytb:tobie2","note":"surreal"},"update":[{"op":"remove","path":"/temp"}]},{"delete":{"id":"mytb:tobie"}},{"delete":{"id":"mytb:tobie","original":{"id":"mytb:tobie","note":"surreal"}}},{"define_table":{"drop":false,"id":3,"kind":{"kind":"ANY"},"name":"mytb","permissions":{"create":false,"delete":false,"select":false,"update":false},"schemafull":false}}],"versionstamp":65536}"#
		);
	}
}
