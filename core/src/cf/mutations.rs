use crate::sql::array::Array;
use crate::sql::object::Object;
use crate::sql::statements::DefineTableStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::vs::to_u128_be;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display, Formatter};

// Mutation is a single mutation to a table.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub enum TableMutation {
	// Although the Value is supposed to contain a field "id" of Thing,
	// we do include it in the first field for convenience.
	Set(Thing, Value),
	Del(Thing),
	Def(DefineTableStatement),
}

impl From<DefineTableStatement> for Value {
	#[inline]
	fn from(v: DefineTableStatement) -> Self {
		let mut h = HashMap::<&str, Value>::new();
		if let Some(id) = v.id {
			h.insert("id", id.into());
		}
		h.insert("name", v.name.0.into());
		Value::Object(Object::from(h))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct TableMutations(pub String, pub Vec<TableMutation>);

impl TableMutations {
	pub fn new(tb: String) -> Self {
		Self(tb, Vec::new())
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
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
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct ChangeSet(pub [u8; 10], pub DatabaseMutation);

impl TableMutation {
	pub fn into_value(self) -> Value {
		let (k, v) = match self {
			TableMutation::Set(_t, v) => ("update".to_string(), v),
			TableMutation::Del(t) => {
				let mut h = BTreeMap::<String, Value>::new();
				h.insert("id".to_string(), Value::Thing(t));
				let o = Object::from(h);
				("delete".to_string(), Value::Object(o))
			}
			TableMutation::Def(t) => ("define_table".to_string(), Value::from(t)),
		};

		let mut h = BTreeMap::<String, Value>::new();
		h.insert(k, v);
		let o = crate::sql::object::Object::from(h);
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
		let vs = to_u128_be(self.0);
		m.insert("versionstamp".to_string(), Value::from(vs));
		m.insert("changes".to_string(), self.1.into_value());
		let so: Object = m.into();
		Value::Object(so)
	}
}

impl Display for TableMutation {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			TableMutation::Set(id, v) => write!(f, "SET {} {}", id, v),
			TableMutation::Del(id) => write!(f, "DEL {}", id),
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
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
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
	#[test]
	fn serialization() {
		use super::*;
		use std::collections::HashMap;
		let cs = ChangeSet(
			[0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![
					TableMutation::Set(
						Thing::from(("mytb".to_string(), "tobie".to_string())),
						Value::Object(Object::from(HashMap::from([
							(
								"id",
								Value::from(Thing::from(("mytb".to_string(), "tobie".to_string()))),
							),
							("note", Value::from("surreal")),
						]))),
					),
					TableMutation::Del(Thing::from(("mytb".to_string(), "tobie".to_string()))),
					TableMutation::Def(DefineTableStatement {
						name: "mytb".into(),
						..DefineTableStatement::default()
					}),
				],
			)]),
		);
		let v = cs.into_value().into_json();
		let s = serde_json::to_string(&v).unwrap();
		assert_eq!(
			s,
			r#"{"changes":[{"update":{"id":"mytb:tobie","note":"surreal"}},{"delete":{"id":"mytb:tobie"}},{"define_table":{"name":"mytb"}}],"versionstamp":1}"#
		);
	}
}
