use anyhow::Result;
use revision::revisioned;

use crate::catalog::{FromStored, StoredTableDefinition, TableDefinition};
use crate::doc::CursorRecord;
use crate::expr::Operation;
use crate::expr::statements::info::InfoStructure;
use crate::key::impl_kv_value_revisioned;
use crate::val::{Array, Object, RecordId, TableName, Value};

// Mutation is a single mutation to a table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TableMutation {
	// Although the Value is supposed to contain a field "id" of [`RecordId`],
	// we do include it in the first field for convenience.
	Set(RecordId, Value),
	Del(RecordId),
	Def(Box<StoredTableDefinition>),
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

impl_kv_value_revisioned!(TableMutations);

impl TableMutations {
	/// Create a new table mutations
	pub fn new(tb: TableName) -> Self {
		Self(tb, Vec::new())
	}
	/// Push a table change to the table mutations
	pub fn push_table_change(&mut self, dt: StoredTableDefinition) {
		// Push the table change to the entry
		self.1.push(TableMutation::Def(Box::new(dt)));
	}

	/// Push a mutation to the table mutations (record change)
	pub fn push_record_change(
		&mut self,
		id: RecordId,
		previous: CursorRecord,
		current: CursorRecord,
		store_difference: bool,
	) {
		// Check if this is a delete operation
		if current.as_ref().is_nullish() {
			// Push the delete mutation to the entry
			self.1.push(match store_difference {
				true => TableMutation::DelWithOriginal(id, previous.into_owned()),
				false => TableMutation::Del(id),
			});
		} else {
			// Push the set mutation to the entry
			self.1.push(match store_difference {
				true => {
					if previous.as_ref().is_none() {
						TableMutation::Set(id, current.into_owned())
					} else {
						// We intentionally record the patches in reverse (current -> previous)
						// because we cannot otherwise resolve operations such as "replace" and
						// "remove".
						let patches_to_create_previous = current.as_ref().diff(previous.as_ref());
						TableMutation::SetWithDiff(
							id,
							current.into_owned(),
							patches_to_create_previous,
						)
					}
				}
				false => TableMutation::Set(id, current.into_owned()),
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

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId, TableId};
	use crate::val::convert_value_to_public_value;

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
					TableMutation::Def(Box::new(StoredTableDefinition::new(
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
					TableMutation::Def(Box::new(StoredTableDefinition::new(
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
