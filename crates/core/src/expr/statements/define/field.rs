use std::fmt::{self, Display, Write};
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::{
	self, DatabaseId, FieldDefinition, NamespaceId, Permission, Permissions, Relation,
	TableDefinition, TableType,
};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::reference::Reference;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Expr, Ident, Idiom, Kind, KindLiteral, Part};
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineFieldStatement {
	pub kind: DefineKind,
	pub name: Idiom,
	pub what: Ident,
	/// Whether the field is marked as flexible.
	/// Flexible allows the field to be schemaless even if the table is marked as schemafull.
	pub flex: bool,
	pub field_kind: Option<Kind>,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
	pub reference: Option<Reference>,
}

impl DefineFieldStatement {
	pub(crate) fn to_definition(&self) -> catalog::FieldDefinition {
		fn convert_permission(permission: &Permission) -> catalog::Permission {
			match permission {
				Permission::None => catalog::Permission::None,
				Permission::Full => catalog::Permission::Full,
				Permission::Specific(expr) => catalog::Permission::Specific(expr.clone()),
			}
		}

		catalog::FieldDefinition {
			name: self.name.clone(),
			what: self.what.clone().to_raw_string(),
			flexible: self.flex,
			field_kind: self.field_kind.clone(),
			readonly: self.readonly,
			value: self.value.clone(),
			assert: self.assert.clone(),
			computed: self.computed.clone(),
			default: match &self.default {
				DefineDefault::None => catalog::DefineDefault::None,
				DefineDefault::Set(x) => catalog::DefineDefault::Set(x.clone()),
				DefineDefault::Always(x) => catalog::DefineDefault::Always(x.clone()),
			},
			select_permission: convert_permission(&self.permissions.select),
			create_permission: convert_permission(&self.permissions.create),
			update_permission: convert_permission(&self.permissions.update),
			comment: self.comment.clone().map(|x| x.into_string()),
			reference: self.reference.clone(),
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;

		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;

		// Validate computed options
		self.validate_computed_options(ns, db, ctx.tx()).await?;

		// Validate reference options
		self.validate_reference_options(ctx)?;

		// Disallow mismatched types
		self.disallow_mismatched_types(ctx, ns, db).await?;

		// Fetch the transaction
		let txn = ctx.tx();
		// Get the name of the field
		let fd = self.name.to_raw_string();
		// Check if the definition exists
		if let Some(fd) = txn.get_tb_field(ns, db, &self.what, &fd).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::FdAlreadyExists {
							name: fd.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		let tb = {
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_tb(ns, db, &self.what, opt.strict).await?
		};

		let definition = self.to_definition();

		// Process the statement
		let key = crate::key::table::fd::new(ns, db, &tb.name, &fd);
		txn.set(&key, &definition, None).await?;

		// Refresh the table cache
		let mut tb = TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			..tb.as_ref().clone()
		};

		// If this is an `in` field then check relation definitions
		if fd.as_str() == "in" {
			// The table is marked as TYPE RELATION
			if let TableType::Relation(ref relation) = tb.table_type {
				// Check if a field TYPE has been specified
				if let Some(kind) = self.field_kind.as_ref() {
					// The `in` field must be a record type
					ensure!(
						kind.is_record(),
						Error::Thrown("in field on a relation must be a record".into(),)
					);
					// Add the TYPE to the DEFINE TABLE statement
					if relation.from.as_ref() != self.field_kind.as_ref() {
						tb.table_type = TableType::Relation(Relation {
							from: self.field_kind.clone(),
							..relation.to_owned()
						});

						txn.put_tb(ns_name, db_name, &tb).await?;
						// Clear the cache
						if let Some(cache) = ctx.get_cache() {
							cache.clear_tb(ns, db, &self.what);
						}

						txn.clear_cache();
						return Ok(Value::None);
					}
				}
			}
		}

		// If this is an `out` field then check relation definitions
		if fd.as_str() == "out" {
			// The table is marked as TYPE RELATION
			if let TableType::Relation(ref relation) = tb.table_type {
				// Check if a field TYPE has been specified
				if let Some(kind) = self.field_kind.as_ref() {
					// The `out` field must be a record type
					ensure!(
						kind.is_record(),
						Error::Thrown("out field on a relation must be a record".into())
					);
					// Add the TYPE to the DEFINE TABLE statement
					if relation.from.as_ref() != self.field_kind.as_ref() {
						tb.table_type = TableType::Relation(Relation {
							to: self.field_kind.clone(),
							..relation.clone()
						});
						txn.put_tb(ns_name, db_name, &tb).await?;
						// Clear the cache
						if let Some(cache) = ctx.get_cache() {
							cache.clear_tb(ns, db, &self.what);
						}

						txn.clear_cache();
						return Ok(Value::None);
					}
				}
			}
		}

		txn.put_tb(ns_name, db_name, &tb).await?;

		// Process possible recursive defitions
		self.process_recursive_definitions(ns, db, txn.clone()).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.what);
		}

		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}

	pub(crate) async fn process_recursive_definitions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		txn: Arc<Transaction>,
	) -> Result<()> {
		// Find all existing field definitions
		let fields = txn.all_tb_fields(ns, db, &self.what, None).await.ok();
		// Process possible recursive_definitions
		if let Some(mut cur_kind) = self.field_kind.as_ref().and_then(|x| x.inner_kind()) {
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
					FieldDefinition {
						field_kind: Some(cur_kind),
						//reference: self.reference.clone(),
						..existing.clone()
					}
				} else {
					FieldDefinition {
						name: name.clone(),
						what: self.what.clone().into_string(),
						flexible: self.flex,
						field_kind: Some(cur_kind),
						reference: self.reference.clone(),
						..Default::default()
					}
				};
				txn.set(&key, &val, None).await?;
				// Process to any sub field
				if let Some(new_kind) = new_kind {
					cur_kind = new_kind;
				} else {
					break;
				}
			}
		}

		Ok(())
	}

	pub(crate) async fn validate_computed_options(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		txn: Arc<Transaction>,
	) -> Result<()> {
		// Find all existing field definitions
		let fields = txn.all_tb_fields(ns, db, &self.what, None).await?;
		if self.computed.is_some() {
			// Ensure the field is not the `id` field
			ensure!(!self.name.is_id(), Error::IdFieldKeywordConflict("COMPUTED".into()));

			// Ensure the field is top-level
			ensure!(self.name.len() == 1, Error::ComputedNestedField(self.name.to_string()));

			// Ensure there are no conflicting clauses
			ensure!(self.value.is_none(), Error::ComputedKeywordConflict("VALUE".into()));
			ensure!(self.assert.is_none(), Error::ComputedKeywordConflict("ASSERT".into()));
			ensure!(self.reference.is_none(), Error::ComputedKeywordConflict("REFERENCE".into()));
			ensure!(
				matches!(self.default, DefineDefault::None),
				Error::ComputedKeywordConflict("DEFAULT".into())
			);
			ensure!(!self.flex, Error::ComputedKeywordConflict("FLEXIBLE".into()));
			ensure!(!self.readonly, Error::ComputedKeywordConflict("READONLY".into()));

			// Ensure no nested fields exist
			for field in fields.iter() {
				if field.name.starts_with(&self.name) && field.name != self.name {
					bail!(Error::ComputedNestedFieldConflict(
						self.name.to_string(),
						field.name.to_string()
					));
				}
			}
		} else {
			// Ensure no parent fields are computed
			for field in fields.iter() {
				if field.computed.is_some()
					&& self.name.starts_with(&field.name)
					&& field.name != self.name
				{
					bail!(Error::ComputedParentFieldConflict(
						self.name.to_string(),
						field.name.to_string()
					));
				}
			}
		}

		Ok(())
	}

	pub(crate) fn validate_reference_options(&self, ctx: &Context) -> Result<()> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(());
		}

		// If a reference is defined, the field must be a record
		if self.reference.is_some() {
			let kind = match &self.field_kind {
				Some(Kind::Option(kind)) => Some(kind.as_ref()),
				x => x.as_ref(),
			};

			let is_record_id = match &kind {
				Some(Kind::Either(kinds)) => kinds.iter().all(|k| matches!(k, Kind::Record(_))),
				Some(Kind::Array(kind, _)) | Some(Kind::Set(kind, _)) => match kind.as_ref() {
					Kind::Either(kinds) => kinds.iter().all(|k| matches!(k, Kind::Record(_))),
					Kind::Record(_) => true,
					_ => false,
				},
				Some(Kind::Literal(KindLiteral::Array(kinds))) => {
					kinds.iter().all(|k| matches!(k, Kind::Record(_)))
				}
				Some(Kind::Record(_)) => true,
				_ => false,
			};

			ensure!(
				is_record_id,
				Error::ReferenceTypeConflict(
					self.field_kind.as_ref().unwrap_or(&Kind::Any).to_string()
				)
			);
		}

		Ok(())
	}

	pub(crate) async fn disallow_mismatched_types(
		&self,
		ctx: &Context,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		let fds = ctx.tx().all_tb_fields(ns, db, &self.what, None).await?;

		if let Some(self_kind) = &self.field_kind {
			for fd in fds.iter() {
				if self.name.starts_with(&fd.name) && self.name != fd.name {
					if let Some(fd_kind) = &fd.field_kind {
						let path = self.name[fd.name.len()..].to_vec();
						if !fd_kind.allows_nested_kind(&path, self_kind) {
							bail!(Error::MismatchedFieldTypes {
								name: self.name.to_string(),
								kind: self_kind.to_string(),
								existing_name: fd.name.to_string(),
								existing_kind: fd_kind.to_string(),
							});
						}
					}
				}
			}
		}

		Ok(())
	}
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.field_kind {
			write!(f, " TYPE {v}")?
		}
		match self.default {
			DefineDefault::None => {}
			DefineDefault::Always(ref expr) => write!(f, " DEFAULT ALWAYS {expr}")?,
			DefineDefault::Set(ref expr) => write!(f, " DEFAULT {expr}")?,
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
		if let Some(ref v) = self.computed {
			write!(f, " COMPUTED {v}")?
		}
		if let Some(ref v) = self.reference {
			write!(f, " REFERENCE {v}")?
		}
		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {comment}")?
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
			"kind".to_string(), if let Some(v) = self.field_kind => v.structure(),
			"value".to_string(), if let Some(v) = self.value => v.structure(),
			"assert".to_string(), if let Some(v) = self.assert => v.structure(),
			"computed".to_string(), if let Some(v) = self.computed => v.structure(),
			"default_always".to_string(), if matches!(&self.default, DefineDefault::Always(_) | DefineDefault::Set(_)) => Value::Bool(matches!(self.default,DefineDefault::Always(_))), // Only reported if DEFAULT is also enabled for this field
			"default".to_string(), if let DefineDefault::Always(v) | DefineDefault::Set(v) = self.default => v.structure(),
			"reference".to_string(), if let Some(v) = self.reference => v.structure(),
			"readonly".to_string() => self.readonly.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
