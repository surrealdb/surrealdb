use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum OptionValue {
	Bool(bool),
	String(String),
}

impl Default for OptionValue {
	fn default() -> Self {
		OptionValue::Bool(true)
	}
}

impl OptionValue {
	pub fn as_bool(&self) -> Result<bool, crate::err::Error> {
		match self {
			OptionValue::Bool(b) => Ok(*b),
			OptionValue::String(s) => Err(crate::err::Error::InvalidOption {
				name: std::string::String::new(),
				message: format!("expected a boolean value, found string '{s}'"),
			}),
		}
	}

	pub fn as_str(&self) -> Result<&str, crate::err::Error> {
		match self {
			OptionValue::String(s) => Ok(s.as_str()),
			OptionValue::Bool(b) => Err(crate::err::Error::InvalidOption {
				name: std::string::String::new(),
				message: format!("expected a string value, found boolean '{b}'"),
			}),
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct OptionStatement {
	pub name: String,
	pub what: OptionValue,
}

impl ToSql for OptionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::option::OptionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
