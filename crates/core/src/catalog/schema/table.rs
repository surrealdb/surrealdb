use revision::revisioned;
use uuid::Uuid;

use crate::catalog::Permissions;
use crate::expr::{Expr, Idiom, Kind};
use crate::val::Duration;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct TableDefinition {
	pub id: Option<u32>,
	pub name: String,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<String>,
	pub table_type: TableType,
	/// The last time that a DEFINE FIELD was added to this table
	pub cache_fields_ts: Uuid,
	/// The last time that a DEFINE EVENT was added to this table
	pub cache_events_ts: Uuid,
	/// The last time that a DEFINE TABLE was added to this table
	pub cache_tables_ts: Uuid,
	/// The last time that a DEFINE INDEX was added to this table
	pub cache_indexes_ts: Uuid,
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChangeFeed {
	pub expiry: Duration,
	pub store_diff: bool,
}

/// The type of records stored by a table
#[revisioned(revision = 1)]
#[derive(Debug, Default, Hash, Clone, Eq, PartialEq)]
pub enum TableType {
	#[default]
	Any,
	Normal,
	Relation(Relation),
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
	pub enforced: bool,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct View {
	pub expr: Fields,
	pub what: Vec<String>,
	pub cond: Option<Expr>,
	pub group: Option<Vec<Idiom>>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Fields {
	/// Fields had the `VALUE` clause and should only return the given selector
	///
	/// This variant should not contain Field::All
	/// TODO: Encode the above variant into the type.
	Value(Box<Field>),
	/// Normal fields where an object with the selected fields is expected
	Select(Vec<Field>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single {
		expr: Expr,
		/// The `quality` in `SELECT rating AS quality FROM ...`
		alias: Option<Idiom>,
	},
}
