use std::fmt::{self, Display, Write};
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::catalog::{
	self, DatabaseId, FieldDefinition, NamespaceId, Permission, Permissions, Relation,
	TableDefinition, TableType,
};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::{expr_to_ident, expr_to_idiom};
use crate::expr::reference::Reference;
use crate::expr::{Base, Expr, Kind, KindLiteral, Literal, Part, RecordIdKeyLit};
use crate::fmt::{is_pretty, pretty_indent};
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineFieldStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
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
	pub comment: Option<Expr>,
	pub reference: Option<Reference>,
}

impl Default for DefineFieldStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			flex: false,
			field_kind: None,
			readonly: false,
			value: None,
			assert: None,
			computed: None,
			default: DefineDefault::None,
			permissions: Permissions::default(),
			comment: None,
			reference: None,
		}
	}
}

impl DefineFieldStatement {
	pub(crate) async fn to_definition(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<catalog::FieldDefinition> {
		fn convert_permission(permission: &Permission) -> catalog::Permission {
			match permission {
				Permission::None => catalog::Permission::None,
				Permission::Full => catalog::Permission::Full,
				Permission::Specific(expr) => catalog::Permission::Specific(expr.clone()),
			}
		}

		Ok(catalog::FieldDefinition {
			name: expr_to_idiom(stk, ctx, opt, doc, &self.name, "field name").await?,
			what: expr_to_ident(stk, ctx, opt, doc, &self.what, "table name").await?,
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
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
			reference: self.reference.clone(),
		})
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		let definition = self.to_definition(stk, ctx, opt, doc).await?;

		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;

		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;

		// Validate computed options
		self.validate_computed_options(ns, db, ctx.tx(), &definition).await?;

		// Validate reference options
		self.validate_reference_options(&definition)?;

		// Disallow mismatched types
		self.disallow_mismatched_types(ctx, ns, db, &definition).await?;

		// Validate id field restrictions
		self.validate_id_restrictions(&definition)?;

		// Fetch the transaction
		let txn = ctx.tx();
		// Get the name of the field
		let fd = self.name.to_raw_string();
		// Check if the definition exists
		if let Some(fd) = txn.get_tb_field(ns, db, &definition.what, &fd).await? {
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
			txn.get_or_add_tb(Some(ctx), ns, db, &definition.what, opt.strict).await?
		};

		// Process the statement
		txn.put_tb_field(ns, db, &tb.name, &definition).await?;

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
							cache.clear_tb(ns, db, &definition.what);
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
							cache.clear_tb(ns, db, &definition.what);
						}

						txn.clear_cache();
						return Ok(Value::None);
					}
				}
			}
		}

		txn.put_tb(ns_name, db_name, &tb).await?;

		// Process possible recursive defitions
		self.process_recursive_definitions(ns, db, txn.clone(), &definition).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &definition.what);
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
		definition: &catalog::FieldDefinition,
	) -> Result<()> {
		// Find all existing field definitions
		let fields = txn.all_tb_fields(ns, db, &definition.what, None).await.ok();
		// Process possible recursive_definitions
		if let Some(mut cur_kind) = self.field_kind.as_ref().and_then(|x| x.inner_kind()) {
			let mut name = definition.name.clone();
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
				let key = crate::key::table::fd::new(ns, db, &definition.what, &fd);
				let val = if let Some(existing) =
					fields.as_ref().and_then(|x| x.iter().find(|x| x.name == name))
				{
					FieldDefinition {
						field_kind: Some(cur_kind),
						..existing.clone()
					}
				} else {
					FieldDefinition {
						name: name.clone(),
						what: definition.what.to_string(),
						flexible: definition.flexible,
						field_kind: Some(cur_kind),
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
		definition: &catalog::FieldDefinition,
	) -> Result<()> {
		// Find all existing field definitions
		let fields = txn.all_tb_fields(ns, db, &definition.what, None).await?;
		if self.computed.is_some() {
			// Ensure the field is not the `id` field
			ensure!(!definition.name.is_id(), Error::IdFieldKeywordConflict("COMPUTED".into()));

			// Ensure the field is top-level
			ensure!(
				definition.name.len() == 1,
				Error::ComputedNestedField(definition.name.to_string())
			);

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
				if field.name.starts_with(&definition.name) && field.name != definition.name {
					bail!(Error::ComputedNestedFieldConflict(
						definition.name.to_string(),
						field.name.to_string()
					));
				}
			}
		} else {
			// Ensure no parent fields are computed
			for field in fields.iter() {
				if field.computed.is_some()
					&& definition.name.starts_with(&field.name)
					&& field.name != definition.name
				{
					bail!(Error::ComputedParentFieldConflict(
						definition.name.to_string(),
						field.name.to_string()
					));
				}
			}
		}

		Ok(())
	}

	pub(crate) fn validate_reference_options(
		&self,
		definition: &catalog::FieldDefinition,
	) -> Result<()> {
		// If a reference is defined, the field must be a record
		if self.reference.is_some() {
			ensure!(
				definition.name.len() == 1,
				Error::ReferenceNestedField(definition.name.to_string())
			);

			fn valid(kind: &Kind, outer: bool) -> bool {
				match kind {
					Kind::None | Kind::Record(_) => true,
					Kind::Array(kind, _) | Kind::Set(kind, _) => outer && valid(kind, false),
					Kind::Literal(KindLiteral::Array(kinds)) => {
						outer && kinds.iter().all(|k| valid(k, false))
					}
					_ => false,
				}
			}

			let is_record_id = match self.field_kind.as_ref() {
				Some(Kind::Either(kinds)) => kinds.iter().all(|k| valid(k, true)),
				Some(Kind::Array(kind, _)) | Some(Kind::Set(kind, _)) => match kind.as_ref() {
					Kind::Either(kinds) => kinds.iter().all(|k| valid(k, true)),
					Kind::Record(_) => true,
					_ => false,
				},
				Some(Kind::Literal(KindLiteral::Array(kinds))) => {
					kinds.iter().all(|k| valid(k, true))
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
		definition: &catalog::FieldDefinition,
	) -> Result<()> {
		let fds = ctx.tx().all_tb_fields(ns, db, &definition.what, None).await?;

		if let Some(self_kind) = &self.field_kind {
			for fd in fds.iter() {
				if definition.name.starts_with(&fd.name) && definition.name != fd.name {
					if let Some(fd_kind) = &fd.field_kind {
						let path = definition.name[fd.name.len()..].to_vec();
						if !fd_kind.allows_nested_kind(&path, self_kind) {
							bail!(Error::MismatchedFieldTypes {
								name: definition.name.to_string(),
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

	pub(crate) fn validate_id_restrictions(
		&self,
		definition: &catalog::FieldDefinition,
	) -> Result<()> {
		if definition.name.is_id() {
			// Ensure no `VALUE` clause is specified
			ensure!(self.value.is_none(), Error::IdFieldKeywordConflict("VALUE".into()));

			// Ensure no `REFERENCE` clause is specified
			ensure!(self.reference.is_none(), Error::IdFieldKeywordConflict("REFERENCE".into()));

			// Ensure no `COMPUTED` clause is specified
			ensure!(self.computed.is_none(), Error::IdFieldKeywordConflict("COMPUTED".into()));

			// Ensure no `DEFAULT` clause is specified
			ensure!(
				matches!(self.default, DefineDefault::None),
				Error::IdFieldKeywordConflict("DEFAULT".into())
			);

			// Ensure the field is not a record type
			if let Some(ref kind) = self.field_kind {
				ensure!(
					RecordIdKeyLit::kind_supported(kind),
					Error::IdFieldUnsupportedKind(kind.to_string())
				);
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
			write!(f, " COMMENT {}", comment)?
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
