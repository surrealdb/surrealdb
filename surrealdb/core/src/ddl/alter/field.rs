use std::ops::Deref;

use anyhow::{Result, ensure};
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Error as CatalogError};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exec::Error as ExecError;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::field::{AlterDefault, AlterFieldStatement};
use crate::expr::statements::define::DefineKind;
use crate::iam::AuthLimit;
use crate::val::{TableName, Value};

/// Alter the field `idiom` of table `what` by the clauses of `this`, as
/// `ALTER FIELD` does once its field and table are named.
pub(crate) async fn alter_field_statement_apply(
	this: &AlterFieldStatement,
	idiom: crate::expr::Idiom,
	what: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();
	let name = idiom.to_raw_string();
	// A component of a named record id is a derived field: it has no definition
	// to alter.
	crate::ddl::key_scan::refuse_component_name(&txn, ns, db, &what, &idiom).await?;
	// Get the table definition
	let mut df = match txn.get_tb_field(ns, db, &what, &name, None).await? {
		Some(tb) => tb.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}

			return Err(CatalogError::FdNotFound {
				name,
			}
			.into());
		}
	};

	// A lightweight relation's only fields are the auto in/out pair, whose
	// types ARE the relation's IN/OUT contract — the record-less scan and
	// the endpoint checks rest on them — so they are managed exclusively by
	// the table definition, never altered directly.
	if let Some(tb) = txn.get_tb(ns, db, &what, None).await? {
		ensure!(
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_none(),
			ExecError::Thrown(
				"a LIGHTWEIGHT relation's fields cannot be altered: its in/out contract is \
				 managed by the table definition"
					.to_owned(),
			)
		);
	}

	// Snapshot the definition before mutating it so we can tell which
	// reference target tables the change drops.
	let old_definition = df.clone();

	// The type and the names of the record id components it carries are set
	// together, so the definition's key structure always matches its type.
	match this.kind {
		AlterKind::Set(ref k) => {
			let names = match this.key_names {
				AlterKind::Set(ref names) => Some(names.clone()),
				AlterKind::Drop | AlterKind::None => None,
			};
			df.set_type(Some(k.clone()), names);
		}
		AlterKind::Drop => df.set_type(None, None),
		AlterKind::None => {}
	}
	match this.flexible {
		AlterKind::Set(_) => df.flexible = true,
		AlterKind::Drop => df.flexible = false,
		AlterKind::None => {}
	}

	match this.readonly {
		AlterKind::Set(_) => df.readonly = true,
		AlterKind::Drop => df.readonly = false,
		AlterKind::None => {}
	}

	match this.inline {
		AlterKind::Set(_) => df.inline = true,
		AlterKind::Drop => df.inline = false,
		AlterKind::None => {}
	}

	match this.value {
		AlterKind::Set(ref k) => {
			df.value = Some(k.clone());
			df.value_body = Some(catalog::LogicalBody::of(k));
		}
		AlterKind::Drop => {
			df.value = None;
			df.value_body = None;
		}
		AlterKind::None => {}
	}

	match this.assert {
		AlterKind::Set(ref k) => {
			df.assert = Some(k.clone());
			df.assert_body = Some(catalog::LogicalBody::of(k));
		}
		AlterKind::Drop => {
			df.assert = None;
			df.assert_body = None;
		}
		AlterKind::None => {}
	}

	match this.default {
		AlterDefault::None => {}
		AlterDefault::Drop => {
			df.default = catalog::DefineDefault::None;
			df.default_body = None;
		}
		AlterDefault::Always(ref expr) => {
			df.default = catalog::DefineDefault::Always(expr.clone());
			df.default_body = Some(catalog::LogicalBody::of(expr));
		}
		AlterDefault::Set(ref expr) => {
			df.default = catalog::DefineDefault::Set(expr.clone());
			df.default_body = Some(catalog::LogicalBody::of(expr));
		}
	}

	if let Some(permissions) = &this.permissions {
		df.select_permission = permissions.select.clone();
		df.select_body = catalog::LogicalPermissionBody::of(&permissions.select);
		df.create_permission = permissions.create.clone();
		df.update_permission = permissions.update.clone();
	}

	match this.comment {
		AlterKind::Set(ref k) => df.comment = Some(k.clone()),
		AlterKind::Drop => df.comment = None,
		AlterKind::None => {}
	}

	match this.reference {
		AlterKind::Set(ref k) => df.reference = Some(k.clone()),
		AlterKind::Drop => df.reference = None,
		AlterKind::None => {}
	}

	// Recompute auth_limit from the current principal to prevent privilege escalation
	df.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

	// The `id` field forbids the same clauses on ALTER as on DEFINE — VALUE,
	// REFERENCE, COMPUTED, DEFAULT ALWAYS, READONLY, FLEXIBLE, and non-key
	// TYPEs — validated against the fully-resolved definition, so a clause
	// DEFINE FIELD refuses is refused here whichever ALTER sets it.
	crate::exec::field_rules::id_field_restrictions(&df)?;

	// The record id components the merged definition names: judged as `DEFINE
	// FIELD OVERWRITE` judges them. Only a statement that sets or drops the type
	// of `id` can change them; a field under a component's name was refused
	// above.
	if df.name.is_id() && !matches!(this.kind, AlterKind::None) {
		crate::ddl::define_field_statement_validate_key_components(
			&txn,
			ns,
			db,
			&DefineKind::Overwrite,
			&df,
		)
		.await?;
	}

	// INLINE embeds the field's value into the edge's adjacency payloads;
	// the same restrictions as DEFINE FIELD, validated against the fully
	// resolved definition.
	if df.inline && !opt.import {
		let is_relation = txn
			.get_tb(ns, db, &what, None)
			.await?
			.is_some_and(|tb| matches!(tb.table_type, catalog::TableType::Relation(_)));
		ensure!(
			is_relation,
			ExecError::Thrown(
				"INLINE requires an existing TYPE RELATION table: the value is embedded into \
				 the edges' adjacency payloads"
					.to_owned(),
			)
		);
		ensure!(
			!matches!(name.as_str(), "id" | "in" | "out"),
			ExecError::Thrown(
				"the `id`, `in` and `out` fields already live in the adjacency keys and cannot \
				 be INLINE"
					.to_owned(),
			)
		);
		ensure!(
			df.name.len() == 1,
			ExecError::Thrown("only a top-level field can be INLINE".to_owned())
		);
		ensure!(
			df.computed.is_none(),
			ExecError::Thrown(
				"a COMPUTED field cannot be INLINE: its value is derived at read time, so \
				 there is nothing stored to embed"
					.to_owned(),
			)
		);
		// The payload's value count is a single byte, so a table carries at
		// most 255 INLINE fields; reject the overflowing toggle here rather
		// than failing every subsequent edge write.
		let fields = txn.all_tb_fields(ns, db, &what, None).await?;
		let others = fields.iter().filter(|f| f.inline && f.name != df.name).count();
		ensure!(
			others < u8::MAX as usize,
			ExecError::Thrown(
				"a table cannot carry more than 255 INLINE fields: the adjacency payload's \
				 value count is a single byte"
					.to_owned(),
			)
		);
	}

	if df.flexible {
		let tb = txn.get_tb(ns, db, &what, None).await?;
		crate::exec::field_rules::flexible_restrictions(&df, tb.as_deref())?;
	}

	// A record id that names its components must admit every record the table
	// holds; the scan is skipped when the stored `id` already has the same
	// structure.
	if let Some(key) = df.live_key() {
		let exec = crate::ddl::frame(ctx, opt, None).await;
		crate::ddl::key_scan::validate_record_keys(&exec, &txn, ns, db, &what, key).await?;
	}

	txn.put_tb_field(ns, db, &what, &df).await?;
	// Dropping the REFERENCE clause or narrowing/changing the record kind can
	// strand reference keys under target tables the field no longer
	// references. Purge them so the DELETE reference-purge gate stays sound.
	// Skipped during import, which restores reference keys verbatim.
	if !opt.import {
		crate::exec::reference_purge::purge_dropped_reference_keys(
			&txn,
			ns,
			db,
			&what,
			&old_definition,
			Some(&df),
		)
		.await?;
	}
	// Refresh the table cache
	let Some(tb) = txn.get_tb(ns, db, &what, None).await? else {
		return Err(CatalogError::TbNotFound {
			name: what.clone(),
		}
		.into());
	};
	// The generation identifies the payload shape: the inline field set's
	// names, order, and types. Bump it only when this ALTER changes that
	// shape — the field joins or leaves the inline set, or an inline
	// field's declared type changes — so readers fall back until a write
	// restamps the payloads. Clauses that never enter a payload
	// (PERMISSIONS, COMMENT, ASSERT, DEFAULT, VALUE) leave written
	// payloads valid: permissions are re-checked at plan time, and the
	// payload stores whatever values the record stores.
	let inline_toggled = df.inline != old_definition.inline;
	let inline_kind_changed =
		df.inline && old_definition.inline && df.field_kind != old_definition.field_kind;
	txn.replace_tb(
		ns_name,
		db_name,
		&catalog::TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			graph_inline_gen: if inline_toggled || inline_kind_changed {
				tb.graph_inline_gen.wrapping_add(1)
			} else {
				tb.graph_inline_gen
			},
			..(*tb).clone()
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
