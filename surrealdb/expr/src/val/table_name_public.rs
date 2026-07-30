//! Conversions between the public `Table` value and the engine's table name.
//!
//! Neither type is local here, so these cannot be `From` impls: the orphan rule
//! needs one side to belong to the defining crate. They are not on the types
//! themselves either — `surrealdb-strand` is a string primitive and does not
//! know the public contract, and `surrealdb-types` is the public contract and
//! does not take dependencies on internal crates. Core is where the two meet,
//! so core owns the conversion.

use surrealdb_types::Table as PublicTable;

use crate::val::TableName;

/// Convert a public table value into the engine's table name.
pub trait IntoTableName {
	fn into_table_name(self) -> TableName;
}

impl IntoTableName for PublicTable {
	fn into_table_name(self) -> TableName {
		TableName::new(self.into_string())
	}
}

/// Convert the engine's table name into a public table value.
pub trait IntoPublicTable {
	fn into_public_table(self) -> PublicTable;
}

impl IntoPublicTable for TableName {
	fn into_public_table(self) -> PublicTable {
		PublicTable::new(self.into_string())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn round_trips_without_changing_the_name() {
		let public = PublicTable::new("users".to_owned());
		let name = public.clone().into_table_name();
		assert_eq!(name.as_str(), "users");
		assert_eq!(name.into_public_table(), public);
	}

	/// The public form is not escaped, so a name that would need quoting in
	/// SurrealQL crosses unchanged. Escaping belongs to the language layer.
	#[test]
	fn does_not_escape() {
		assert_eq!(PublicTable::new("select".to_owned()).into_table_name().as_str(), "select");
	}
}
