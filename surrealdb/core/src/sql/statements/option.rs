use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeKwFreeIdent, QuoteStr};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum OptionValue {
	Bool(bool),
	String(String),
}

impl Default for OptionValue {
	fn default() -> Self {
		OptionValue::Bool(true)
	}
}

impl From<OptionValue> for crate::expr::statements::OptionValue {
	fn from(v: OptionValue) -> Self {
		match v {
			OptionValue::Bool(b) => crate::expr::statements::OptionValue::Bool(b),
			OptionValue::String(s) => crate::expr::statements::OptionValue::String(s),
		}
	}
}

impl From<crate::expr::statements::OptionValue> for OptionValue {
	fn from(v: crate::expr::statements::OptionValue) -> Self {
		match v {
			crate::expr::statements::OptionValue::Bool(b) => OptionValue::Bool(b),
			crate::expr::statements::OptionValue::String(s) => OptionValue::String(s),
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OptionStatement {
	pub name: String,
	pub what: OptionValue,
}

impl OptionStatement {
	pub(crate) fn import() -> Self {
		Self {
			name: "IMPORT".to_string(),
			what: OptionValue::Bool(true),
		}
	}
}

impl ToSql for OptionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self.what {
			OptionValue::Bool(true) => {
				write_sql!(f, fmt, "OPTION {}", EscapeKwFreeIdent(&self.name))
			}
			OptionValue::Bool(false) => {
				write_sql!(f, fmt, "OPTION {} = FALSE", EscapeKwFreeIdent(&self.name))
			}
			OptionValue::String(s) => {
				write_sql!(f, fmt, "OPTION {} = {}", EscapeKwFreeIdent(&self.name), QuoteStr(s))
			}
		}
	}
}

impl From<OptionStatement> for crate::expr::statements::OptionStatement {
	fn from(v: OptionStatement) -> Self {
		crate::expr::statements::OptionStatement {
			name: v.name,
			what: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::OptionStatement> for OptionStatement {
	fn from(v: crate::expr::statements::OptionStatement) -> Self {
		OptionStatement {
			name: v.name,
			what: v.what.into(),
		}
	}
}
