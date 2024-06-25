use super::Value;
use surrealdb_core::sql;

impl Value {
	pub fn into_sql_value(self) -> sql::Value {
		todo!()
	}

	pub fn from_sql_value(value: sql::Value) -> Option<Self> {
		todo!()
	}
}
