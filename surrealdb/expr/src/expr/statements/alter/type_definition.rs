use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

/// Where `ALTER TYPE ... ADD VALUE` places a new label.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum EnumPosition {
	/// Immediately before the named label.
	Before(String),
	/// Immediately after the named label.
	After(String),
}

/// A change `ALTER TYPE` makes to the labels of a union type.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AlterTypeAction {
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

/// `ALTER TYPE [IF EXISTS] name` with its `ADD VALUE` and `RENAME VALUE` changes, which apply
/// only to a type whose body is a union of string literals.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AlterTypeStatement {
	pub name: Strand,
	pub if_exists: bool,
	/// The changes in source order.
	pub actions: Vec<AlterTypeAction>,
}

impl ToSql for AlterTypeStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER TYPE");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(self.name.as_str()));
		for action in &self.actions {
			match action {
				AlterTypeAction::AddValue {
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
				AlterTypeAction::RenameValue {
					from,
					to,
				} => {
					write_sql!(f, fmt, " RENAME VALUE {} TO {}", QuoteStr(from), QuoteStr(to));
				}
			}
		}
	}
}
