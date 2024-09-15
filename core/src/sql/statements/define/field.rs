use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
use crate::sql::Part;
use crate::sql::{Base, Ident, Idiom, Kind, Permissions, Strand, Value};
use crate::sql::{Relation, TableType};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub flex: bool,
	pub kind: Option<Kind>,
	#[revision(start = 2)]
	pub readonly: bool,
	pub value: Option<Value>,
	pub assert: Option<Value>,
	pub default: Option<Value>,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
	#[revision(start = 3)]
	pub if_not_exists: bool,
	#[revision(start = 4)]
	pub overwrite: bool,
}

impl DefineFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Get the NS and DB
		let ns = opt.ns()?;
		let db = opt.db()?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the name of the field
		let fd = self.name.to_string();
		// Check if the definition exists
		if txn.get_tb_field(ns, db, &self.what, &fd).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::FdAlreadyExists {
					value: fd,
				});
			}
		}
		// Process the statement
		let key = crate::key::table::fd::new(ns, db, &self.what, &fd);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		txn.get_or_add_tb(ns, db, &self.what, opt.strict).await?;
		txn.set(
			key,
			DefineFieldStatement {
				// Don't persist the `IF NOT EXISTS` clause to schema
				if_not_exists: false,
				overwrite: false,
				..self.clone()
			},
			None,
		)
		.await?;

		// find existing field definitions.
		let fields = txn.all_tb_fields(ns, db, &self.what, None).await.ok();

		// Process possible recursive_definitions.
		if let Some(mut cur_kind) = self.kind.as_ref().and_then(|x| x.inner_kind()) {
			let mut name = self.name.clone();
			loop {
				let new_kind = cur_kind.inner_kind();
				name.0.push(Part::All);

				// Get the name of the field
				let fd = name.to_string();
				let key = crate::key::table::fd::new(ns, db, &self.what, &fd);

				// merge the new definition with possible existing definitions.
				let statement = if let Some(existing) =
					fields.as_ref().and_then(|x| x.iter().find(|x| x.name == name))
				{
					DefineFieldStatement {
						kind: Some(cur_kind),
						if_not_exists: false,
						overwrite: false,
						..existing.clone()
					}
				} else {
					DefineFieldStatement {
						name: name.clone(),
						what: self.what.clone(),
						flex: self.flex,
						kind: Some(cur_kind),
						..Default::default()
					}
				};

				txn.set(key, statement, None).await?;

				if let Some(new_kind) = new_kind {
					cur_kind = new_kind;
				} else {
					break;
				}
			}
		}
		// If this is an `in` field then check relation definitions
		if fd.as_str() == "in" {
			// Get the table definition that this field belongs to
			let tb = txn.get_tb(ns, db, &self.what).await?;
			// The table is marked as TYPE RELATION
			if let TableType::Relation(ref relation) = tb.kind {
				// Check if a field TYPE has been specified
				if let Some(kind) = self.kind.as_ref() {
					// The `in` field must be a record type
					if !kind.is_record() {
						return Err(Error::Thrown(
							"in field on a relation must be a record".into(),
						));
					}
					// Add the TYPE to the DEFINE TABLE statement
					if relation.from.as_ref() != self.kind.as_ref() {
						let key = crate::key::database::tb::new(ns, db, &self.what);
						let val = DefineTableStatement {
							kind: TableType::Relation(Relation {
								from: self.kind.to_owned(),
								..relation.to_owned()
							}),
							..tb.as_ref().to_owned()
						};
						txn.set(key, val, None).await?;
					}
				}
			}
		}
		// If this is an `out` field then check relation definitions
		if fd.as_str() == "out" {
			// Get the table definition that this field belongs to
			let tb = txn.get_tb(ns, db, &self.what).await?;
			// The table is marked as TYPE RELATION
			if let TableType::Relation(ref relation) = tb.kind {
				// Check if a field TYPE has been specified
				if let Some(kind) = self.kind.as_ref() {
					// The `out` field must be a record type
					if !kind.is_record() {
						return Err(Error::Thrown(
							"out field on a relation must be a record".into(),
						));
					}
					// Add the TYPE to the DEFINE TABLE statement
					if relation.from.as_ref() != self.kind.as_ref() {
						let key = crate::key::database::tb::new(ns, db, &self.what);
						let val = DefineTableStatement {
							kind: TableType::Relation(Relation {
								to: self.kind.to_owned(),
								..relation.to_owned()
							}),
							..tb.as_ref().to_owned()
						};
						txn.set(key, val, None).await?;
					}
				}
			}
		}
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.kind {
			write!(f, " TYPE {v}")?
		}
		if let Some(ref v) = self.default {
			write!(f, " DEFAULT {v}")?
		}
		if self.readonly {
			write!(f, " READONLY")?
		}
		if let Some(ref v) = self.value {
			write!(f, " VALUE {v}")?
		}
		if let Some(ref v) = self.assert {
			write!(f, " ASSERT {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "{}", self.permissions)?;
		Ok(())
	}
}

impl InfoStructure for DefineFieldStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => self.what.structure(),
			"flex".to_string() => self.flex.into(),
			"kind".to_string(), if let Some(v) = self.kind => v.structure(),
			"value".to_string(), if let Some(v) = self.value => v.structure(),
			"assert".to_string(), if let Some(v) = self.assert => v.structure(),
			"default".to_string(), if let Some(v) = self.default => v.structure(),
			"readonly".to_string() => self.readonly.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
