use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

/// Where `ALTER ENUM ... ADD VALUE` places a new label.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum EnumPosition {
	/// Immediately before the named label.
	Before(String),
	/// Immediately after the named label.
	After(String),
}

/// The change `ALTER ENUM` makes.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AlterEnumAction {
	/// `ADD VALUE [IF NOT EXISTS] 'x' [BEFORE 'y' | AFTER 'y']`; with no position the label is
	/// appended.
	AddValue {
		value: String,
		position: Option<EnumPosition>,
		if_not_exists: bool,
	},
	/// `RENAME VALUE 'a' TO 'b'`.
	RenameValue {
		from: String,
		to: String,
	},
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AlterEnumStatement {
	pub name: Strand,
	pub if_exists: bool,
	/// The changes in source order.
	pub actions: Vec<AlterEnumAction>,
}

impl ToSql for AlterEnumStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER ENUM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(self.name.as_str()));
		for action in &self.actions {
			match action {
				AlterEnumAction::AddValue {
					value,
					position,
					if_not_exists,
				} => {
					write_sql!(f, fmt, " ADD VALUE");
					if *if_not_exists {
						write_sql!(f, fmt, " IF NOT EXISTS");
					}
					write_sql!(f, fmt, " {}", QuoteStr(value));
					match position {
						Some(EnumPosition::Before(anchor)) => {
							write_sql!(f, fmt, " BEFORE {}", QuoteStr(anchor))
						}
						Some(EnumPosition::After(anchor)) => {
							write_sql!(f, fmt, " AFTER {}", QuoteStr(anchor))
						}
						None => {}
					}
				}
				AlterEnumAction::RenameValue {
					from,
					to,
				} => {
					write_sql!(f, fmt, " RENAME VALUE {} TO {}", QuoteStr(from), QuoteStr(to));
				}
			}
		}
	}
}
