use crate::cnf::EXPERIMENTAL_RECORD_REFERENCES;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::reference::Reference;
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
use crate::sql::{Base, Ident, Idiom, Kind, Permissions, Strand, Value};
use crate::sql::{Literal, Part};
use crate::sql::{Relation, TableType};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use uuid::Uuid;

#[revisioned(revision = 5)]
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
	#[revision(start = 5)]
	pub reference: Option<Reference>,
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
		// Validate reference options
		self.validate_reference_options()?;
		// Correct reference type
		let kind = if let Some(kind) = self.correct_reference_type(ctx, opt).await? {
			Some(kind)
		} else {
			self.kind.clone()
		};
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
				kind,
				..self.clone()
			},
			None,
		)
		.await?;
		// Refresh the table cache
		let key = crate::key::database::tb::new(ns, db, &self.what);
		let tb = txn.get_tb(ns, db, &self.what).await?;
		txn.set(
			key,
			DefineTableStatement {
				cache_fields_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
			None,
		)
		.await?;
		// Clear the cache
		txn.clear();
		// Find all existing field definitions
		let fields = txn.all_tb_fields(ns, db, &self.what, None).await.ok();
		// Process possible recursive_definitions
		if let Some(mut cur_kind) = self.kind.as_ref().and_then(|x| x.inner_kind()) {
			let mut name = self.name.clone();
			loop {
				// Check if the subtype is an `any` type
				if let Kind::Any = cur_kind {
					// There is no need to add a subtype
					// field definition if the type is
					// just specified as an `array`. This
					// is because the following query:
					//  DEFINE FIELD foo ON bar TYPE array;
					// already implies that the immediate
					// subtype is an any:
					//  DEFINE FIELD foo[*] ON bar TYPE any;
					// so we skip the subtype field.
					break;
				}
				// Get the kind of this sub field
				let new_kind = cur_kind.inner_kind();
				// Add a new subtype
				name.0.push(Part::All);
				// Get the field name
				let fd = name.to_string();
				// Set the subtype `DEFINE FIELD` definition
				let key = crate::key::table::fd::new(ns, db, &self.what, &fd);
				let val = if let Some(existing) =
					fields.as_ref().and_then(|x| x.iter().find(|x| x.name == name))
				{
					DefineFieldStatement {
						kind: Some(cur_kind),
						reference: self.reference.clone(),
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
						reference: self.reference.clone(),
						..Default::default()
					}
				};
				txn.set(key, val, None).await?;
				// Process to any sub field
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
							cache_fields_ts: Uuid::now_v7(),
							kind: TableType::Relation(Relation {
								from: self.kind.to_owned(),
								..relation.to_owned()
							}),
							..tb.as_ref().to_owned()
						};
						txn.set(key, val, None).await?;
						// Clear the cache
						txn.clear();
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
							cache_fields_ts: Uuid::now_v7(),
							kind: TableType::Relation(Relation {
								to: self.kind.to_owned(),
								..relation.to_owned()
							}),
							..tb.as_ref().to_owned()
						};
						txn.set(key, val, None).await?;
						// Clear the cache
						txn.clear();
					}
				}
			}
		}
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}

	fn validate_reference_options(&self) -> Result<(), Error> {
		if !*EXPERIMENTAL_RECORD_REFERENCES {
			return Ok(());
		}

		if let Some(kind) = &self.kind {
			let kinds = match kind {
				Kind::Either(kinds) => kinds,
				kind => &vec![kind.to_owned()],
			};

			// Check if any of the kinds are references
			if kinds.iter().any(|k| matches!(k, Kind::References(_, _))) {
				// If any of the kinds are references, all of them must be
				if !kinds.iter().all(|k| matches!(k, Kind::References(_, _))) {
					return Err(Error::RefsMismatchingVariants);
				}

				// As the refs and dynrefs type essentially take over a field
				// they are not allowed to be mixed with most other clauses
				let typename = kind.to_string();

				if self.reference.is_some() {
					return Err(Error::RefsTypeConflict("REFERENCE".into(), typename));
				}

				if self.default.is_some() {
					return Err(Error::RefsTypeConflict("DEFAULT".into(), typename));
				}

				if self.value.is_some() {
					return Err(Error::RefsTypeConflict("VALUE".into(), typename));
				}

				if self.assert.is_some() {
					return Err(Error::RefsTypeConflict("ASSERT".into(), typename));
				}

				if self.flex {
					return Err(Error::RefsTypeConflict("FLEXIBLE".into(), typename));
				}

				if self.readonly {
					return Err(Error::RefsTypeConflict("READONLY".into(), typename));
				}
			}

			// If a reference is defined, the field must be a record
			if self.reference.is_some() {
				let kinds = match kind.non_optional() {
					Kind::Either(kinds) => kinds,
					Kind::Array(kind, _) | Kind::Set(kind, _) => match kind.as_ref() {
						Kind::Either(kinds) => kinds,
						kind => &vec![kind.to_owned()],
					},
					Kind::Literal(lit) => match lit {
						Literal::Array(kinds) => kinds,
						lit => &vec![Kind::Literal(lit.to_owned())],
					},
					kind => &vec![kind.to_owned()],
				};

				if !kinds.iter().all(|k| matches!(k, Kind::Record(_))) {
					return Err(Error::ReferenceTypeConflict(kind.to_string()));
				}
			}
		}

		Ok(())
	}

	async fn correct_reference_type(
		&self,
		ctx: &Context,
		opt: &Options,
	) -> Result<Option<Kind>, Error> {
		if !*EXPERIMENTAL_RECORD_REFERENCES {
			return Ok(None);
		}

		if let Some(Kind::References(Some(ft), Some(ff))) = &self.kind {
			// Obtain the field definition
			let fd = match ctx
				.tx()
				.get_tb_field(opt.ns()?, opt.db()?, &ft.to_string(), &ff.to_string())
				.await
			{
				Ok(fd) => fd,
				// If the field does not exist, there is nothing to correct
				Err(Error::FdNotFound {
					..
				}) => return Ok(None),
				Err(e) => return Err(e),
			};

			// Check if the field is an array-like value and thus "containing" references
			let is_contained = if let Some(kind) = &fd.kind {
				matches!(
					kind.non_optional(),
					Kind::Array(_, _) | Kind::Set(_, _) | Kind::Literal(Literal::Array(_))
				)
			} else {
				false
			};

			// If the field is an array-like value, add the `.*` part
			if is_contained {
				let ff = ff.clone().push(Part::All);
				return Ok(Some(Kind::References(Some(ft.clone()), Some(ff))));
			}
		}

		Ok(None)
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
		if *EXPERIMENTAL_RECORD_REFERENCES {
			if let Some(ref v) = self.reference {
				write!(f, " REFERENCE {v}")?
			}
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
		// Alternate permissions display implementation ignores delete permission
		// This display is used to show field permissions, where delete has no effect
		// Displaying the permission could mislead users into thinking it has an effect
		// Additionally, including the permission will cause a parsing error in 3.0.0
		write!(f, "{:#}", self.permissions)?;
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
			"reference".to_string(), if let Some(v) = self.reference => v.structure(),
			"readonly".to_string() => self.readonly.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
