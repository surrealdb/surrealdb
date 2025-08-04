use crate::expr::Operation;
use crate::expr::statements::DefineTableStatement;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Array, Object, RecordId, Value};
use crate::vs::VersionStamp;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display, Formatter};

// Mutation is a single mutation to a table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum TableMutation {
	// Although the Value is supposed to contain a field "id" of Thing,
	// we do include it in the first field for convenience.
	Set(RecordId, Value),
	Del(RecordId),
	Def(DefineTableStatement),
	/// Includes the ID, current value (after change), changes that can be applied to get the original
	/// value
	/// Example, ("mytb:tobie", {{"note": "surreal"}}, [{"op": "add", "path": "/note", "value": "surreal"}], false)
	/// Means that we have already applied the add "/note" operation to achieve the recorded result
	SetWithDiff(RecordId, Value, Vec<Operation>),
	/// Delete a record where the ID is stored, and the now-deleted value
	DelWithOriginal(RecordId, Value),
}

impl From<DefineTableStatement> for Value {
	#[inline]
	fn from(v: DefineTableStatement) -> Self {
		let mut h = HashMap::<&str, Value>::new();
		if let Some(id) = v.id {
			h.insert("id", id.into());
		}
		h.insert("name", v.name.into_strand().into());
		Value::Object(Object::from(h))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct TableMutations(pub String, pub Vec<TableMutation>);

impl_kv_value_revisioned!(TableMutations);

impl TableMutations {
	pub fn new(tb: String) -> Self {
		Self(tb, Vec::new())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

// Change is a set of mutations made to a table at the specific timestamp.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ChangeSet(pub VersionStamp, pub DatabaseMutation);

impl TableMutation {
	/// Convert a stored change feed table mutation (record change) into a
	/// Value that can be used in the storage of change feeds and their transmission to consumers
	pub fn into_value(self) -> Value {
		let mut h = BTreeMap::<String, Value>::new();
		let h = match self {
			TableMutation::Set(_thing, v) => {
				h.insert("update".to_string(), v);
				h
			}
			TableMutation::SetWithDiff(_thing, current, operations) => {
				h.insert("current".to_string(), current);
				h.insert(
					"update".to_string(),
					Value::Array(Array(
						operations
							.clone()
							.into_iter()
							.map(|x| Value::Object(x.into_object()))
							.collect(),
					)),
				);
				h
			}
			TableMutation::Del(t) => {
				h.insert(
					"delete".to_string(),
					Value::Object(Object::from(map! {
						"id".to_string() => Value::Thing(t)
					})),
				);
				h
			}
			TableMutation::Def(t) => {
				h.insert("define_table".to_string(), Value::from(t));
				h
			}
			TableMutation::DelWithOriginal(id, _val) => {
				h.insert(
					"delete".to_string(),
					Value::Object(Object::from(map! {
					"id".to_string() => Value::Thing(id),
					})),
				);
				h
			}
		};
		let o = crate::val::Object::from(h);
		Value::Object(o)
	}
}

impl DatabaseMutation {
	pub fn into_value(self) -> Value {
		let mut changes = Vec::<Value>::new();
		for tbs in self.0 {
			for tb in tbs.1 {
				changes.push(tb.into_value());
			}
		}
		Value::Array(Array::from(changes))
	}
}

impl ChangeSet {
	pub fn into_value(self) -> Value {
		let mut m = BTreeMap::<String, Value>::new();
		m.insert("versionstamp".to_string(), Value::from(self.0.into_u128()));
		m.insert("changes".to_string(), self.1.into_value());
		let so: Object = m.into();
		Value::Object(so)
	}
}

impl Display for TableMutation {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			TableMutation::Set(id, v) => write!(f, "SET {} {}", id, v),
			TableMutation::SetWithDiff(id, _previous, v) => write!(f, "SET {} {:?}", id, v),
			TableMutation::Del(id) => write!(f, "DEL {}", id),
			TableMutation::DelWithOriginal(id, _) => write!(f, "DEL {}", id),
			TableMutation::Def(t) => write!(f, "{}", t),
		}
	}
}

impl Display for TableMutations {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let tb = &self.0;
		let muts = &self.1;
		write!(f, "{}", tb)?;
		muts.iter().try_for_each(|v| write!(f, "{}", v))
	}
}

impl Display for DatabaseMutation {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let x = &self.0;

		x.iter().try_for_each(|v| write!(f, "{}", v))
	}
}

impl Display for ChangeSet {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let x = &self.1;

		write!(f, "{}", x)
	}
}

// WriteMutationSet is a set of mutations to be to a table at the specific timestamp.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct WriteMutationSet(pub Vec<TableMutations>);

impl WriteMutationSet {
	pub fn new() -> Self {
		Self(Vec::new())
	}
}

impl Default for WriteMutationSet {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use crate::expr::Ident;

	use super::*;
	use std::collections::HashMap;

	#[test]
	fn serialization() {
		let cs = ChangeSet(
			VersionStamp::from_u64(1),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![
					TableMutation::Set(
						RecordId::new("mytb".to_string(), strand!("tobie").to_owned()),
						Value::Object(Object::from(HashMap::from([
							(
								"id",
								Value::from(RecordId::new(
									"mytb".to_owned(),
									strand!("tobie").to_owned(),
								)),
							),
							("note", Value::from("surreal")),
						]))),
					),
					TableMutation::Del(RecordId::new(
						"mytb".to_owned(),
						strand!("tobie").to_owned(),
					)),
					TableMutation::Def(DefineTableStatement {
						name: Ident::new("mytb".to_owned()).unwrap(),
						..DefineTableStatement::default()
					}),
				],
			)]),
		);
		let v = cs.into_value().into_json_value().unwrap();
		let s = serde_json::to_string(&v).unwrap();
		assert_eq!(
			s,
			r#"{"changes":[{"update":{"id":"mytb:tobie","note":"surreal"}},{"delete":{"id":"mytb:tobie"}},{"define_table":{"name":"mytb"}}],"versionstamp":65536}"#
		);
	}

	#[test]
	fn serialization_rev2() {
		let cs = ChangeSet(
			VersionStamp::from_u64(1),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![
					TableMutation::SetWithDiff(
						RecordId::new("mytb".to_owned(), strand!("tobie").to_owned()),
						Value::Object(Object::from(HashMap::from([
							(
								"id",
								Value::from(RecordId::new(
									"mytb".to_owned(),
									strand!("tobie").to_owned(),
								)),
							),
							("note", Value::from("surreal")),
						]))),
						vec![Operation::Add {
							path: vec!["note".to_owned()],
							value: Value::from("surreal"),
						}],
					),
					TableMutation::SetWithDiff(
						RecordId::new("mytb".to_owned(), strand!("tobie").to_owned()),
						Value::Object(Object::from(HashMap::from([
							(
								"id",
								Value::from(RecordId::new(
									"mytb".to_owned(),
									strand!("tobie2").to_owned(),
								)),
							),
							("note", Value::from("surreal")),
						]))),
						vec![Operation::Remove {
							path: vec!["temp".to_owned()],
						}],
					),
					TableMutation::Del(RecordId::new(
						"mytb".to_owned(),
						strand!("tobie").to_owned(),
					)),
					TableMutation::DelWithOriginal(
						RecordId::new("mytb".to_owned(), strand!("tobie").to_owned()),
						Value::Object(Object::from(map! {
								"id" => Value::from(RecordId::new("mytb".to_owned(),strand!("tobie").to_owned())),
								"note" => Value::from("surreal"),
						})),
					),
					TableMutation::Def(DefineTableStatement {
						name: Ident::new("mytb".to_owned()).unwrap(),
						..DefineTableStatement::default()
					}),
				],
			)]),
		);
		let v = cs.into_value().into_json_value().unwrap();
		let s = serde_json::to_string(&v).unwrap();
		let cmp = r#"{"changes":[{"current":{"id":"mytb:tobie","note":"surreal"},"update":[{"op":"add","path":"/note","value":"surreal"}]},{"current":{"id":"mytb:tobie2","note":"surreal"},"update":[{"op":"remove","path":"/temp"}]},{"delete":{"id":"mytb:tobie"}},{"delete":{"id":"mytb:tobie"}},{"define_table":{"name":"mytb"}}],"versionstamp":65536}"#;
		println!("{s}");
		println!("{cmp}");
		assert_eq!(s, cmp);
	}
}
