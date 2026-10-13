//! The host's side of the store instructions: catalog and key-value reads and
//! writes on the statement's transaction, through the VM's own key and value
//! codecs, and the session services a schema statement needs.
//!
//! Reads and writes go through the transaction, so a read after a write in
//! the same transaction sees the write.

use std::borrow::Cow;

use anyhow::anyhow;
use surrealdb_datastore::LevelledMode;

use super::codec;
use super::codec::TypeId;
use super::codec::key::RouteId;
use super::program::{
	AccessCheck, ClauseConversion, ColumnSource, FieldCheck, GrantSubject, IdSequence, PathSource,
	Program, Service, StatementError, StoreInstr,
};
use super::scalar::Scalar;
use crate::catalog::stored::StoredField;
use crate::ddl::grant;
use crate::exe::FlowResultExt as _;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::kvs::sequences::CatalogSequence;
use crate::val::Value;

pub(super) fn read(regs: &[Scalar<'_>], reg: u16) -> Value {
	regs[reg as usize].to_value()
}

pub(super) fn write(regs: &mut [Scalar<'_>], reg: u16, value: Value) {
	regs[reg as usize] = Scalar::Value(Cow::Owned(value));
}

/// The sink the running statement records its session effects in, and the session's options.
///
/// Only the context of a top-level statement of the request holds a sink. A program run anywhere
/// else (a function body, a closure, an event, a subscription, `eval::*`) holds none, so an
/// instruction that writes the session raises there: such code can never switch the caller's
/// namespace or database, set its options or bind a request parameter.
fn session_sink_of<'a>(
	ctx: &'a ExecutionContext,
	statement: &str,
) -> FlowResult<(&'a crate::exec::context::SessionSink, &'a crate::dbs::Options)> {
	let options = ctx.options().ok_or_else(|| {
		ControlFlow::Err(anyhow!("a {statement} run without the session's options"))
	})?;
	let sink = ctx.session_sink().ok_or_else(|| {
		ControlFlow::Err(anyhow!("a {statement} run where the session cannot be changed"))
	})?;
	Ok((sink, options))
}

/// Take store instruction `instr`, answering the instruction the body jumps
/// to, or `None` to continue with the next.
pub(super) async fn step(
	program: &Program,
	instr: &StoreInstr,
	regs: &mut [Scalar<'_>],
	reserved_taken: &mut [bool],
	ctx: &ExecutionContext,
) -> FlowResult<Option<usize>> {
	let stepped = step_inner(program, instr, regs, reserved_taken, ctx).await;
	// A refusal the statement declares — a definition that exists or is
	// missing, a capability the caller lacks — is noted, so a speculative run
	// knows its real run raises it too.
	if stepped.is_err()
		&& matches!(
			instr,
			StoreInstr::Raise { .. }
				| StoreInstr::RefuseField { .. }
				| StoreInstr::RefuseRemovingLightweight { .. }
				| StoreInstr::RefuseRedefiningTable { .. }
				| StoreInstr::RefuseNamedIdConversion { .. }
				| StoreInstr::FieldCheck { .. }
				| StoreInstr::Allow { .. }
				| StoreInstr::RefuseNoWrite
		) && let Some(txn) = ctx.try_txn()
		&& txn.speculating()
	{
		txn.note_speculative_refusal();
	}
	stepped
}

async fn step_inner(
	program: &Program,
	instr: &StoreInstr,
	regs: &mut [Scalar<'_>],
	reserved_taken: &mut [bool],
	ctx: &ExecutionContext,
) -> FlowResult<Option<usize>> {
	// A speculative run (`vm::speculate`) takes the statement's reads and
	// writes of the catalog and nothing else: what reaches past the
	// transaction's key-value view — the node's caches and services, a build
	// or backfill, an action at commit — waits for the statement's real run.
	if speculative_skip(instr)
		&& let Some(txn) = ctx.try_txn()
		&& txn.speculating()
	{
		txn.note_speculative_skip();
		return Ok(None);
	}
	match instr {
		StoreInstr::RealRunOnly => {
			if ctx.try_txn().is_some_and(|txn| txn.speculating()) {
				return Err(super::speculate::real_run_only());
			}
		}
		StoreInstr::ShowGrants {
			ac,
			base,
			gr,
			filter,
			dst,
		} => {
			let shown = show_grants(program, ctx, ac, *base, gr.as_deref(), *filter).await?;
			write(regs, *dst, shown);
		}
		StoreInstr::IssueGrant {
			ac,
			base,
			subject,
			dst,
		} => {
			let subject = match subject {
				GrantSubject::User(name) => crate::catalog::Subject::User(name.clone()),
				GrantSubject::Record(reg) => {
					grant::record_subject(read(regs, *reg)).map_err(ControlFlow::Err)?
				}
			};
			let issued =
				grant::create_grant(ac.clone(), *base, subject, ctx.ctx(), session_options(ctx)?)
					.await
					.map_err(ControlFlow::Err)?;
			write(regs, *dst, Value::Object(grant::access_object_from_grant(&issued)));
		}
		StoreInstr::RevokeGrants {
			ac,
			base,
			gr,
			filter,
			dst,
		} => {
			let revoked = revoke_grants(program, ctx, ac, *base, gr.as_deref(), *filter).await?;
			write(regs, *dst, revoked);
		}
		StoreInstr::PurgeGrants {
			ac,
			base,
			kind,
			grace,
			dst,
		} => {
			let purged =
				grant::purge_grants(*base, ac, kind, *grace, ctx.ctx(), session_options(ctx)?)
					.await
					.map_err(ControlFlow::Err)?;
			write(regs, *dst, purged);
		}
		StoreInstr::RefuseNoWrite => {
			if let Some(frame) = ctx.root().no_write {
				return Err(ControlFlow::Err(anyhow!(frame.side_effect_error())));
			}
		}
		StoreInstr::Allow {
			action,
			resource,
			base,
		} => ctx.is_allowed(*action, resource.clone(), *base).map_err(ControlFlow::Err)?,
		StoreInstr::Get {
			route,
			key,
			ty,
			dst,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			let value = match ctx.txn().get(key.into(), None).await.map_err(ControlFlow::Err)? {
				Some(bytes) => codec::decode_id(*ty, &bytes).map_err(ControlFlow::Err)?,
				None => Value::None,
			};
			write(regs, *dst, value);
		}
		StoreInstr::StampCatalog {
			ns,
			db,
			tb,
			draw,
			reserved,
		} => {
			let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
			let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
			let tb = match read(regs, *tb) {
				Value::String(name) => crate::val::TableName::from(name.as_str()),
				Value::Table(name) => name,
				other => {
					return Err(ControlFlow::Err(anyhow!(
						"stamping the catalog version of {other:?}, which names no table"
					)));
				}
			};
			let version =
				take_reserved(program, *reserved, reserved_taken).map(uuid::Uuid::from_u128);
			let version = ctx
				.txn()
				.stamp_catalog_version(ns, db, &tb, version)
				.await
				.map_err(ControlFlow::Err)?;
			super::speculate::drawn(ctx, *draw, version.as_u128());
		}
		StoreInstr::Put {
			route,
			key,
			ty,
			value,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			put_value(ctx, key, *ty, &read(regs, *value), LevelledMode::Set).await?;
		}
		StoreInstr::Replace {
			route,
			key,
			ty,
			value,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			put_value(ctx, key, *ty, &read(regs, *value), LevelledMode::Replace).await?;
		}
		StoreInstr::GetField {
			src,
			name,
			dst,
		} => {
			let name = program.fields[*name as usize].as_str();
			let value = match read(regs, *src) {
				Value::Object(object) => object.get(name).cloned().unwrap_or(Value::None),
				other => {
					return Err(ControlFlow::Err(anyhow!(
						"reading field `{name}` of {other:?}, which is not an object"
					)));
				}
			};
			write(regs, *dst, value);
		}
		StoreInstr::NextId {
			sequence,
			ns,
			db,
			dst,
			draw,
			reserved,
		} => {
			let sequence = match sequence {
				IdSequence::Namespace => CatalogSequence::Namespaces,
				IdSequence::Database => CatalogSequence::Databases(crate::catalog::NamespaceId(
					scope(regs, *ns, "namespace")?,
				)),
				IdSequence::Table => CatalogSequence::Tables(
					crate::catalog::NamespaceId(scope(regs, *ns, "namespace")?),
					crate::catalog::DatabaseId(scope(regs, *db, "database")?),
				),
			};
			let id = catalog_id(program, *reserved, reserved_taken, ctx, sequence, *draw).await?;
			write(regs, *dst, Value::from(id as i64));
		}
		StoreInstr::SessionNs {
			dst,
		} => {
			let ns = ctx
				.options()
				.and_then(|options| options.ns.clone())
				.ok_or_else(|| ControlFlow::Err(anyhow::Error::new(crate::exec::Error::NsEmpty)))?;
			write(regs, *dst, Value::String(ns.as_ref().into()));
		}
		StoreInstr::Increment {
			route,
			key,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			let txn = ctx.txn();
			let current = match txn.get(key.clone().into(), None).await.map_err(ControlFlow::Err)? {
				Some(bytes) => u64::from_be_bytes(
					bytes
						.as_slice()
						.try_into()
						.map_err(|_| anyhow!("a counter that is not 8 bytes"))?,
				),
				None => 0,
			};
			txn.set(key.into(), current.wrapping_add(1).to_be_bytes().to_vec())
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::SessionDb {
			dst,
		} => {
			let db = ctx
				.options()
				.and_then(|options| options.db.clone())
				.ok_or_else(|| ControlFlow::Err(anyhow::Error::new(crate::exec::Error::DbEmpty)))?;
			write(regs, *dst, Value::String(db.as_ref().into()));
		}
		StoreInstr::NewUuid {
			dst,
			draw,
			reserved,
		} => {
			let uuid = match take_reserved(program, *reserved, reserved_taken) {
				Some(reserved) => uuid::Uuid::from_u128(reserved),
				None => uuid::Uuid::now_v7(),
			};
			super::speculate::drawn(ctx, *draw, uuid.as_u128());
			write(regs, *dst, Value::Uuid(crate::val::Uuid(uuid)))
		}
		StoreInstr::SessionAuthLimit {
			dst,
		} => {
			let options = ctx
				.options()
				.ok_or_else(|| ControlFlow::Err(anyhow!("a definition made with no session")))?;
			let limit: crate::catalog::auth::AuthLimit =
				crate::iam::AuthLimit::new_from_auth(options.auth.as_ref()).into();
			write(regs, *dst, limit.into_vm_value().map_err(|e| ControlFlow::Err(e.into()))?);
		}
		StoreInstr::Lock {
			route,
			key,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			ctx.txn().getu_raw(key.into()).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::Delete {
			route,
			key,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			ctx.txn().del(key.into()).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::DeleteModelArtifact {
			model,
		} => {
			if let Ok(model) = rust::<crate::catalog::StoredMlModelDefinition>(&read(regs, *model))
				&& let Some(opt) = ctx.options()
				&& let Ok((ns, db)) = opt.ns_db()
			{
				let path = crate::expr::model::get_model_path(
					ns,
					db,
					&model.name,
					&model.version,
					&model.hash,
				);
				let _ = crate::obs::del(&path).await;
			}
		}
		StoreInstr::DeleteRange {
			route,
			key,
		} => {
			let range = range(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			ctx.txn().delr(range).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::BufferTableChange {
			ns,
			db,
			tb,
			definition,
		} => {
			let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
			let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
			let tb = match read(regs, *tb) {
				Value::String(s) => crate::val::TableName::new(s.as_str()),
				other => return Err(ControlFlow::Err(anyhow!("a table name {other:?}"))),
			};
			// The changefeed buffer holds the Rust definition it writes at
			// commit.
			let stored: crate::catalog::StoredTableDefinition = rust(&read(regs, *definition))?;
			ctx.txn().changefeed_buffer_table_change(ns, db, &tb, &stored);
		}
		StoreInstr::ClearDatastoreCache => {
			if let Some(cache) = ctx.ctx().get_cache() {
				cache.clear();
			}
		}
		StoreInstr::JumpIfImport {
			target,
		} => {
			if ctx.options().is_some_and(|options| options.import) {
				return Ok(Some(*target as usize));
			}
		}
		StoreInstr::Refuse {
			refusal,
		} => return Err(ControlFlow::Err(refusal.error().into())),
		StoreInstr::RefuseRedefiningTable {
			ns,
			db,
			definition,
		} => {
			let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
			let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
			let txn = ctx.txn();
			crate::exec::vm::descriptor::schema(super::schema_host::refuse_redefining_table(
				&txn, ns, db, definition,
			))
			.await
			.map_err(ControlFlow::Err)?;
		}
		StoreInstr::RefuseNamedIdConversion {
			ns,
			db,
			table,
			to,
		} => {
			let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
			let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
			let table = table_named(&read(regs, *table))?;
			let txn = ctx.txn();
			super::schema_host::refuse_named_id_conversion(&txn, ns, db, &table, *to)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::ValidateRecordKeys {
			ns,
			db,
			table,
			key,
		} => {
			let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
			let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
			let table = table_named(&read(regs, *table))?;
			super::schema_host::validate_record_keys(ctx, ns, db, &table, key)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::RefuseRemovingLightweight {
			ns,
			db,
			tb,
		} => {
			let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
			let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
			let tb = table_named(&read(regs, *tb))?;
			let txn = ctx.txn();
			let refusal = crate::exec::vm::descriptor::schema(
				super::schema_host::refuse_removing_lightweight(&txn, ns, db, &tb),
			)
			.await
			.map_err(ControlFlow::Err)?;
			if let Some(refusal) = refusal {
				return Err(ControlFlow::Err(refusal.error()));
			}
		}
		StoreInstr::ViewSources {
			ns,
			db,
			tb,
			dst,
		} => {
			use crate::catalog::providers::TableProvider;
			let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
			let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
			let tb = table_named(&read(regs, *tb))?;
			let txn = ctx.txn();
			let definition = crate::exec::vm::descriptor::schema(txn.get_tb(ns, db, &tb, None))
				.await
				.map_err(ControlFlow::Err)?;
			let sources = definition
				.and_then(|definition| {
					definition.view.as_ref().map(|view| {
						view.source_tables()
							.iter()
							.map(|table| Value::String(table.as_str().into()))
							.collect::<Vec<_>>()
					})
				})
				.unwrap_or_default();
			write(regs, *dst, Value::Array(crate::val::Array(sources)));
		}
		StoreInstr::Raise {
			error,
			arg,
		} => {
			match error {
				StatementError::TbHasViews => {
					return Err(ControlFlow::Err(views_error(&read(regs, *arg))));
				}
				StatementError::ComputedFieldCannotBeIndexed => {
					let detail = read(regs, *arg);
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::exec::Error::ComputedFieldCannotBeIndexed {
							field: string_field(&detail, "field"),
							index: string_field(&detail, "index"),
						},
					)));
				}
				StatementError::AzInUse
				| StatementError::ApMethodDuplicate
				| StatementError::AccessRootNotFound
				| StatementError::AccessNsNotFound
				| StatementError::AccessDbNotFound
				| StatementError::ApNotFound
				| StatementError::UserRootNotFound
				| StatementError::UserNsNotFound
				| StatementError::UserDbNotFound
				| StatementError::UserRootAlreadyExists
				| StatementError::UserNsAlreadyExists
				| StatementError::UserDbAlreadyExists
				| StatementError::AccessRootAlreadyExists
				| StatementError::AccessNsAlreadyExists
				| StatementError::AccessDbAlreadyExists => {
					let detail = read(regs, *arg);
					let field = |name: &str| string_field(&detail, name);
					let err = match error {
						StatementError::AzInUse => crate::catalog::Error::AzInUse {
							name: field("name"),
							table: field("table"),
							index: field("index"),
						},
						StatementError::ApMethodDuplicate => {
							crate::catalog::Error::ApMethodDuplicate {
								value: field("value"),
								method: field("method"),
							}
						}
						StatementError::AccessRootNotFound => {
							crate::catalog::Error::AccessRootNotFound {
								ac: field("ac"),
							}
						}
						StatementError::AccessNsNotFound => {
							crate::catalog::Error::AccessNsNotFound {
								ac: field("ac"),
								ns: field("ns"),
							}
						}
						StatementError::AccessDbNotFound => {
							crate::catalog::Error::AccessDbNotFound {
								ac: field("ac"),
								ns: field("ns"),
								db: field("db"),
							}
						}
						StatementError::UserRootNotFound => {
							crate::catalog::Error::UserRootNotFound {
								name: field("name"),
							}
						}
						StatementError::UserNsNotFound => crate::catalog::Error::UserNsNotFound {
							name: field("name"),
							ns: field("ns"),
						},
						StatementError::UserDbNotFound => crate::catalog::Error::UserDbNotFound {
							name: field("name"),
							ns: field("ns"),
							db: field("db"),
						},
						StatementError::UserRootAlreadyExists => {
							crate::catalog::Error::UserRootAlreadyExists {
								name: field("name"),
							}
						}
						StatementError::UserNsAlreadyExists => {
							crate::catalog::Error::UserNsAlreadyExists {
								name: field("name"),
								ns: field("ns"),
							}
						}
						StatementError::UserDbAlreadyExists => {
							crate::catalog::Error::UserDbAlreadyExists {
								name: field("name"),
								ns: field("ns"),
								db: field("db"),
							}
						}
						StatementError::AccessRootAlreadyExists => {
							crate::catalog::Error::AccessRootAlreadyExists {
								ac: field("ac"),
							}
						}
						StatementError::AccessNsAlreadyExists => {
							crate::catalog::Error::AccessNsAlreadyExists {
								ac: field("ac"),
								ns: field("ns"),
							}
						}
						StatementError::AccessDbAlreadyExists => {
							crate::catalog::Error::AccessDbAlreadyExists {
								ac: field("ac"),
								ns: field("ns"),
								db: field("db"),
							}
						}
						_ => crate::catalog::Error::ApNotFound {
							value: field("value"),
						},
					};
					return Err(ControlFlow::Err(anyhow::Error::new(err)));
				}
				_ => {}
			}
			let name = match read(regs, *arg) {
				Value::String(s) => s.as_str().to_owned(),
				other => surrealdb_types::ToSql::to_sql(&other),
			};
			let err = match error {
				StatementError::NsAlreadyExists => crate::catalog::Error::NsAlreadyExists {
					name,
				},
				StatementError::DbAlreadyExists => crate::catalog::Error::DbAlreadyExists {
					name,
				},
				StatementError::TbAlreadyExists => crate::catalog::Error::TbAlreadyExists {
					name,
				},
				StatementError::NsNotFound => crate::catalog::Error::NsNotFound {
					name,
				},
				StatementError::DbNotFound => crate::catalog::Error::DbNotFound {
					name,
				},
				StatementError::TbNotFound => crate::catalog::Error::TbNotFound {
					name: crate::val::TableName::new(name),
				},
				StatementError::FdAlreadyExists => crate::catalog::Error::FdAlreadyExists {
					name,
				},
				StatementError::PaAlreadyExists => crate::catalog::Error::PaAlreadyExists {
					name,
				},
				StatementError::EvAlreadyExists => crate::catalog::Error::EvAlreadyExists {
					name,
				},
				StatementError::FcAlreadyExists => crate::catalog::Error::FcAlreadyExists {
					name,
				},
				StatementError::AzAlreadyExists => crate::catalog::Error::AzAlreadyExists {
					name,
				},
				StatementError::SeqAlreadyExists => crate::catalog::Error::SeqAlreadyExists {
					name,
				},
				StatementError::ApAlreadyExists => crate::catalog::Error::ApAlreadyExists {
					value: name,
				},
				StatementError::CgAlreadyExists => crate::catalog::Error::CgAlreadyExists {
					name,
				},
				StatementError::BuAlreadyExists => crate::catalog::Error::BuAlreadyExists {
					value: name,
				},
				StatementError::MdAlreadyExists => crate::catalog::Error::MdAlreadyExists {
					name,
				},
				StatementError::MlAlreadyExists => crate::catalog::Error::MlAlreadyExists {
					name,
				},
				StatementError::CgNotFound => crate::catalog::Error::CgNotFound {
					name,
				},
				StatementError::EvNotFound => crate::catalog::Error::EvNotFound {
					name,
				},
				StatementError::FdNotFound => crate::catalog::Error::FdNotFound {
					name,
				},
				StatementError::IxAlreadyExists => crate::catalog::Error::IxAlreadyExists {
					name,
				},
				StatementError::IxNotFound => crate::catalog::Error::IxNotFound {
					name,
				},
				StatementError::PaNotFound => crate::catalog::Error::PaNotFound {
					name,
				},
				StatementError::FcNotFound => crate::catalog::Error::FcNotFound {
					name,
				},
				StatementError::SeqNotFound => crate::catalog::Error::SeqNotFound {
					name,
				},
				StatementError::AzNotFound => crate::catalog::Error::AzNotFound {
					name,
				},
				StatementError::BuNotFound => crate::catalog::Error::BuNotFound {
					name,
				},
				StatementError::MdNotFound => crate::catalog::Error::MdNotFound {
					name,
				},
				StatementError::MlNotFound => crate::catalog::Error::MlNotFound {
					name,
				},
				StatementError::TbHasViews
				| StatementError::ComputedFieldCannotBeIndexed
				| StatementError::ApMethodDuplicate
				| StatementError::AzInUse
				| StatementError::AccessRootNotFound
				| StatementError::AccessNsNotFound
				| StatementError::AccessDbNotFound
				| StatementError::ApNotFound
				| StatementError::UserRootNotFound
				| StatementError::UserNsNotFound
				| StatementError::UserDbNotFound
				| StatementError::UserRootAlreadyExists
				| StatementError::UserNsAlreadyExists
				| StatementError::UserDbAlreadyExists
				| StatementError::AccessRootAlreadyExists
				| StatementError::AccessNsAlreadyExists
				| StatementError::AccessDbAlreadyExists => unreachable!("raised above"),
			};
			return Err(ControlFlow::Err(anyhow::Error::new(err)));
		}
		StoreInstr::ClearCache => ctx.txn().clear_cache(),
		StoreInstr::Scan {
			route,
			key,
			ty,
			dst,
			unread,
		} => {
			let scanned = scan(*route, &read(regs, *key), *ty, ctx).await;
			match (scanned, unread) {
				(Ok(values), _) => write(regs, *dst, Value::Array(values.into())),
				// An entry this build refuses is not an entry the statement can do
				// without: it fails with the refusal, as a read of the entry does.
				(Err(e), Some(_)) if crate::err::Error::entry_refusal(&e).is_some() => {
					return Err(ControlFlow::Err(e));
				}
				(Err(e), Some(target)) => {
					warn!(
						target: "surrealdb::core::exec::vm",
						route = route.name(),
						error = %e,
						"A scan the statement can do without did not read; it continues without it"
					);
					return Ok(Some(*target as usize));
				}
				(Err(e), None) => return Err(ControlFlow::Err(e)),
			}
		}
		StoreInstr::ScanAt {
			route,
			key,
			ty,
			version,
			dst,
		} => {
			let stamp = stamp_of(&read(regs, *version))?;
			let values = scan_at(*route, &read(regs, *key), *ty, Some(stamp), ctx)
				.await
				.map_err(ControlFlow::Err)?;
			write(regs, *dst, Value::Array(values.into()));
		}
		StoreInstr::VersionStamp {
			src,
			dst,
		} => {
			let stamp = read(regs, *src)
				.cast_to::<crate::val::Datetime>()
				.map_err(|e| ControlFlow::Err(anyhow!("{e}")))?
				.to_version_stamp(ctx.txn().version_impl().as_ref())
				.map_err(ControlFlow::Err)?;
			let stamp = i64::try_from(stamp)
				.map_err(|_| ControlFlow::Err(anyhow!("a version stamp {stamp} out of range")))?;
			write(regs, *dst, Value::from(stamp));
		}
		StoreInstr::Clear {
			route,
			key,
		} => {
			let key = codec::key::encode_id(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			ctx.txn().clr(key.into()).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::ClearRange {
			route,
			key,
		} => {
			let range = range(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			ctx.txn().clrr(range).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::Compact {
			route,
			key,
		} => {
			let range = range(*route, &read(regs, *key)).map_err(ControlFlow::Err)?;
			ctx.txn().compact_raw(range).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::CompactAll => ctx.txn().compact_all().await.map_err(ControlFlow::Err)?,
		StoreInstr::SetQueryTimeout {
			timeout,
		} => {
			let timeout = match read(regs, *timeout) {
				Value::Duration(d) => Some(d.0),
				Value::None => None,
				other => return Err(ControlFlow::Err(anyhow!("a query timeout {other:?}"))),
			};
			ctx.ctx().dynamic_configuration().set_query_timeout(timeout);
		}
		StoreInstr::DropMethods {
			actions,
			methods,
		} => {
			let Value::Array(drop) = read(regs, *methods) else {
				return Err(ControlFlow::Err(anyhow!("methods that are not a list")));
			};
			let Value::Array(mut list) = read(regs, *actions) else {
				return Err(ControlFlow::Err(anyhow!("actions that are not a list")));
			};
			let mut i = 0;
			while i < list.len() {
				let empty = match &mut list[i] {
					Value::Object(action) => match action.get_mut("methods") {
						Some(Value::Array(methods)) => {
							methods.retain(|m| !drop.contains(m));
							methods.is_empty()
						}
						_ => return Err(ControlFlow::Err(anyhow!("an action with no methods"))),
					},
					other => return Err(ControlFlow::Err(anyhow!("an action {other:?}"))),
				};
				if empty {
					list.swap_remove(i);
				} else {
					i += 1;
				}
			}
			write(regs, *actions, Value::Array(list));
		}
		StoreInstr::Push {
			list,
			value,
		} => {
			let Value::Array(mut items) = read(regs, *list) else {
				return Err(ControlFlow::Err(anyhow!("appending to a value that is not a list")));
			};
			items.push(read(regs, *value));
			write(regs, *list, Value::Array(items));
		}
		StoreInstr::Render {
			definitions,
			structured,
			list,
			dst,
			scope,
			version,
		} => {
			let version = match version {
				Some(version) => Some(stamp_of(&read(regs, *version))?),
				None => None,
			};
			let Value::Array(values) = read(regs, *list) else {
				return Err(ControlFlow::Err(anyhow!("definitions that are not a list")));
			};
			let scope = match scope {
				Some((ns, db)) => Some((
					crate::catalog::NamespaceId(id_of(&read(regs, *ns))?),
					crate::catalog::DatabaseId(id_of(&read(regs, *db))?),
				)),
				None => None,
			};
			let rendered =
				super::info::render(*definitions, *structured, &values, scope, version, ctx)
					.await?;
			write(regs, *dst, rendered);
		}
		StoreInstr::RenderOne {
			definitions,
			structured,
			src,
			dst,
		} => {
			let rendered = super::info::render_one(*definitions, *structured, &read(regs, *src))?;
			write(regs, *dst, rendered);
		}
		StoreInstr::CoerceString {
			src,
			dst,
		} => {
			let value = read(regs, *src)
				.coerce_to::<String>()
				.map_err(|e| ControlFlow::Err(anyhow!("{e}")))?;
			write(regs, *dst, Value::String(value.into()));
		}
		StoreInstr::CoerceName {
			src,
			dst,
			into,
			optional,
		} => {
			let into = into.label();
			let value = if *optional {
				read(regs, *src)
					.coerce_naming::<Option<String>>(into)
					.map_err(|e| ControlFlow::Err(e.into()))?
					.map_or(Value::None, |name| Value::String(name.into()))
			} else {
				let name = read(regs, *src)
					.coerce_naming::<String>(into)
					.map_err(|e| ControlFlow::Err(e.into()))?;
				Value::String(name.into())
			};
			write(regs, *dst, value);
		}
		StoreInstr::Convert {
			src,
			dst,
			clause,
		} => {
			let value = read(regs, *src);
			let converted = match clause {
				ClauseConversion::Duration => value
					.cast_to::<Option<crate::val::Duration>>()
					.map(|d| d.map_or(Value::None, Value::Duration))
					.map_err(anyhow::Error::from),
				ClauseConversion::Coerced => value
					.coerce_to::<String>()
					.map(|s| Value::String(s.into()))
					.map_err(anyhow::Error::from),
				ClauseConversion::Cast => value
					.cast_to::<String>()
					.map(|s| Value::String(s.into()))
					.map_err(anyhow::Error::from),
				ClauseConversion::Integer => {
					value.cast_to::<i64>().map(Value::from).map_err(anyhow::Error::from)
				}
				ClauseConversion::RequiredDuration => value
					.cast_to::<crate::val::Duration>()
					.map(Value::Duration)
					.map_err(anyhow::Error::from),
				ClauseConversion::Batch => {
					value.cast_to::<i64>().map_err(anyhow::Error::from).and_then(|batch| {
						u32::try_from(batch).map(|batch| Value::from(i64::from(batch))).map_err(|_| {
							anyhow::Error::new(crate::exec::Error::Query {
								message: format!(
									"`{batch}` is not valid batch size for a sequence definition. A batch size must be within 0..={}",
									u32::MAX
								),
							})
						})
					})
				}
			};
			write(regs, *dst, converted.map_err(ControlFlow::Err)?);
		}
		StoreInstr::AccessCheck {
			check,
		} => match check {
			AccessCheck::RecordToken {
				token,
			} => {
				if read(regs, *token).is_none() {
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::exec::Error::AccessRecordTokenDurationRequired,
					)));
				}
			}
			AccessCheck::SymmetricKeys {
				alg,
				verify,
				issue,
			} => {
				if read(regs, *verify) != read(regs, *issue) {
					return Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Query {
						message: format!(
							"Symmetric algorithm {alg} requires the same key for signing and verification. \
							Use the same key value for both KEY and WITH ISSUER KEY clauses, or omit WITH ISSUER KEY."
						),
					})));
				}
			}
			AccessCheck::Unredacted {
				key,
				name,
			} => {
				if matches!(read(regs, *key), Value::String(s) if s.as_str() == crate::catalog::schema::REDACTED)
				{
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::exec::Error::AccessRedactedKey {
							ac: match read(regs, *name) {
								Value::String(s) => s.as_str().to_owned(),
								other => surrealdb_types::ToSql::to_sql(&other),
							},
						},
					)));
				}
			}
			AccessCheck::RejectEs512 {
				existing,
				overwrite,
			} => {
				let import = ctx.options().is_some_and(|opt| opt.import);
				let existing_uses_es512 = match read(regs, *existing) {
					Value::Object(_) => {
						if let Ok(stored) =
							rust::<crate::catalog::StoredAccessDefinition>(&read(regs, *existing))
						{
							stored.access_type.uses_es512()
						} else {
							false
						}
					}
					_ => false,
				};
				if !(import || (existing_uses_es512 && *overwrite)) {
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::exec::Error::AccessUnsupportedAlgorithm,
					)));
				}
			}
			AccessCheck::Altered {
				access,
			} => {
				use crate::catalog::FromStored as _;
				let stored: crate::catalog::StoredAccessDefinition = rust(&read(regs, *access))?;
				let access = crate::catalog::AccessDefinition::from_stored(&stored)
					.map_err(ControlFlow::Err)?;
				super::schema_host::check_access_altered(&access).map_err(ControlFlow::Err)?;
			}
		},
		StoreInstr::ParseApiPath {
			src,
			text,
			rendered,
		} => {
			let given = match read(regs, *src) {
				Value::String(s) => s,
				other => {
					return Err(ControlFlow::Err(anyhow!(
						"an API path evaluated to {other:?}, not a string"
					)));
				}
			};
			let path: crate::api::path::Path = given.parse().map_err(ControlFlow::Err)?;
			write(regs, *text, Value::String(surrealdb_types::ToSql::to_sql(&path).into()));
			write(regs, *rendered, Value::String(path.to_string().into()));
		}
		StoreInstr::CastComment {
			src,
			dst,
		} => {
			let comment: Option<String> =
				read(regs, *src).cast_to().map_err(|e| ControlFlow::Err(e.into()))?;
			write(regs, *dst, comment.map_or(Value::None, |s| Value::String(s.into())));
		}
		StoreInstr::ProcessInfo {
			part,
			dst,
		} => write(regs, *dst, super::info::process(*part, ctx).await),
		StoreInstr::InfoIndex {
			table,
			index,
			dst,
		} => {
			let table = table_name_of(&read(regs, *table))?;
			let index = match read(regs, *index) {
				Value::String(s) => s.to_string(),
				other => surrealdb_types::ToSql::to_sql(&other),
			};
			let descriptor = crate::exec::vm::descriptor::table_at(ctx, &table, None)
				.await
				.map_err(ControlFlow::Err)?;
			let ix = crate::exec::vm::descriptor::index(ctx, &descriptor, &index)
				.await
				.map_err(ControlFlow::Err)?
				.ok_or_else(|| {
					ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::IxNotFound {
						name: index.clone(),
					}))
				})?;
			let (ns, db) = (descriptor.ns, descriptor.db);
			let txn = ctx.txn();
			let info = surrealdb_datastore::index_state::index_building_info(&txn, ns, db, &ix)
				.await
				.map_err(ControlFlow::Err)?;
			write(regs, *dst, info);
		}
		StoreInstr::JumpIfNoService {
			service,
			target,
		} => {
			let frozen = ctx.ctx();
			let absent = match service {
				Service::Broker => frozen.broker().is_none(),
				Service::Sequences => frozen.get_sequences().is_none(),
			};
			if absent {
				return Ok(Some(*target as usize));
			}
		}
		StoreInstr::RetireIndexLocally {
			ns,
			db,
			table,
			index,
		} => {
			use crate::catalog::FromStored as _;
			let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
			let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
			let table: crate::catalog::StoredTableDefinition = rust(&read(regs, *table))?;
			let table =
				crate::catalog::TableDefinition::from_stored(&table).map_err(ControlFlow::Err)?;
			let index: crate::catalog::StoredIndexDefinition = rust(&read(regs, *index))?;
			let index =
				crate::catalog::IndexDefinition::from_stored(&index).map_err(ControlFlow::Err)?;
			let frozen = ctx.ctx();
			frozen
				.get_index_stores()
				.index_removed(ns, db, &table, &index)
				.await
				.map_err(ControlFlow::Err)?;
			// The builder task is process memory, so it is aborted only once
			// the removal commits.
			if let Some(builder) = frozen.get_index_builder() {
				ctx.txn()
					.on_commit(crate::kvs::index::AbortLocalBuild::boxed(
						builder.clone(),
						ns,
						db,
						table.name.clone(),
						index.index_id,
					))
					.await;
			}
		}
		StoreInstr::RealtimeChecks => {
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a realtime statement run without the session's options"))
			})?;
			crate::exec::live::realtime_checks(ctx.ctx(), options).map_err(ControlFlow::Err)?;
		}
		StoreInstr::Live {
			target,
			template,
			dst,
		} => {
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a LIVE run without the session's options"))
			})?;
			let stored: crate::catalog::StoredSubscriptionDefinition =
				rust(&read(regs, *template))?;
			let definition = crate::catalog::SubscriptionDefinition::compile(&stored);
			if definition.query.compiled().is_none() {
				return Err(ControlFlow::Err(anyhow!(
					"the template of a LIVE statement holds text that does not compile"
				)));
			}
			let definition = crate::exec::live::completed(definition, ctx.ctx(), options);
			let id = crate::exec::live::live_register(
				read(regs, *target),
				definition,
				ctx.ctx(),
				options,
				None,
			)
			.await
			.map_err(ControlFlow::Err)?;
			write(regs, *dst, id);
		}
		StoreInstr::Kill {
			id,
			written,
		} => {
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a KILL run without the session's options"))
			})?;
			let id = read(regs, *id);
			let written = match read(regs, *written) {
				Value::String(text) => text.as_str().to_owned(),
				other => {
					return Err(ControlFlow::Err(anyhow!(
						"the written id of a KILL is {other:?}, which is no string"
					)));
				}
			};
			crate::exec::kill::kill_subscription(id, || written.clone(), ctx.ctx(), options)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::SessionOption {
			name,
			what,
		} => {
			let (sink, options) = session_sink_of(ctx, "OPTION")?;
			crate::exec::session::option_allowed(ctx.ctx(), options).map_err(ControlFlow::Err)?;
			sink.record(crate::exec::context::SessionEffect::Option {
				name: name.clone(),
				what: *what,
			});
		}
		StoreInstr::SessionUseName {
			value,
			database,
		} => {
			let level = if *database {
				"database"
			} else {
				"namespace"
			};
			let name = crate::exec::session::use_name(read(regs, *value), level)
				.map_err(ControlFlow::Err)?;
			write(regs, *value, Value::String(name.into()));
		}
		StoreInstr::SessionUse {
			default,
			ns,
			db,
			dst,
		} => {
			use crate::catalog::providers::RootProvider as _;
			let (sink, options) = session_sink_of(ctx, "USE")?;
			let txn = ctx.txn();
			let (use_ns, use_db) = match default {
				true => match txn.get_default_config().await? {
					Some(config) => (config.namespace.clone(), config.database.clone()),
					None => (None, None),
				},
				false => {
					let ns = ns
						.map(|reg| crate::exec::session::use_name(read(regs, reg), "namespace"))
						.transpose()
						.map_err(ControlFlow::Err)?;
					let db = db
						.map(|reg| crate::exec::session::use_name(read(regs, reg), "database"))
						.transpose()
						.map_err(ControlFlow::Err)?;
					(ns, db)
				}
			};
			// The session as the statement leaves it: what the frame selects, overlaid by the
			// levels the transactional half selected.
			let mut ns = options.ns.as_deref().map(str::to_owned);
			let mut db = options.db.as_deref().map(str::to_owned);
			let selected = crate::exec::session::use_targets(
				ctx.ctx(),
				options,
				&txn,
				use_ns,
				use_db,
				|level| {
					if let crate::exec::context::SessionEffect::Use {
						ns: level_ns,
						db: level_db,
					} = &level
					{
						ns = level_ns.as_ref().map(|name| name.as_str().to_owned()).or(ns.take());
						db = level_db.as_ref().map(|name| name.as_str().to_owned()).or(db.take());
					}
					sink.record(level);
				},
			)
			.await;
			selected.map_err(ControlFlow::Err)?;
			write(regs, *dst, crate::exec::session::use_answer(ns.as_deref(), db.as_deref()));
		}
		StoreInstr::SessionLetChecks {
			name,
		} => {
			// SECURITY: a protected name is refused before the value is evaluated, so a refused
			// `LET` has no side effects.
			if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::exec::Error::InvalidParam {
						name: name.as_str().to_owned(),
					},
				)));
			}
		}
		StoreInstr::SessionLet {
			name,
			kind,
			value,
		} => {
			let (sink, _) = session_sink_of(ctx, "LET")?;
			let mut bound = read(regs, *value);
			if let Some(kind) = kind {
				bound = bound.coerce_to_kind(&program.kinds[*kind as usize]).map_err(|error| {
					ControlFlow::Err(anyhow::Error::new(crate::exec::Error::SetCoerce {
						name: name.as_str().to_owned(),
						error: Box::new(error),
					}))
				})?;
			}
			// A top-level `LET` binds into the request-scoped context, which is not reset
			// between statements, so a chain of them would grow the value's nesting without
			// bound: the bound is checked here, as a storage guard checks a written value.
			crate::val::check_value_depth(&bound).map_err(ControlFlow::Err)?;
			sink.record(crate::exec::context::SessionEffect::Let {
				name: name.clone(),
				value: std::sync::Arc::new(bound),
			});
		}
		StoreInstr::KillOnCommit {
			subscription,
		} => {
			let Some(sender) = ctx.ctx().broker() else {
				return Ok(None);
			};
			let lv: crate::catalog::StoredSubscriptionDefinition =
				rust(&read(regs, *subscription))?;
			ctx.txn()
				.on_commit(crate::dbs::SendKill::boxed(
					std::sync::Arc::clone(sender),
					crate::dbs::RoutedNotification::new(
						lv.node,
						crate::types::PublicNotification::new(
							lv.id.into(),
							None,
							crate::types::PublicAction::Killed,
							crate::types::PublicValue::None,
							crate::types::PublicValue::None,
						),
					),
				))
				.await;
		}
		StoreInstr::RefuseField {
			refusal,
		} => {
			return Err(ControlFlow::Err(refusal.error()));
		}
		StoreInstr::FieldCheck {
			check,
		} => {
			use crate::catalog::FromStored as _;
			let field = |reg| -> FlowResult<crate::catalog::FieldDefinition> {
				let stored: crate::catalog::StoredFieldDefinition = rust(&read(regs, reg))?;
				crate::catalog::FieldDefinition::from_stored(&stored).map_err(ControlFlow::Err)
			};
			match check {
				FieldCheck::IdField {
					field: reg,
				} => crate::exec::field_rules::id_field_restrictions(&field(*reg)?)
					.map_err(ControlFlow::Err)?,
				FieldCheck::Inline {
					field: reg,
					table,
				} => {
					let field = field(*reg)?;
					if field.inline {
						let stored: crate::catalog::StoredTableDefinition =
							rust(&read(regs, *table))?;
						let table = crate::catalog::TableDefinition::from_stored(&stored)
							.map_err(ControlFlow::Err)?;
						let descriptor =
							crate::exec::vm::descriptor::table_at(ctx, &table.name, None)
								.await
								.map_err(ControlFlow::Err)?;
						crate::exec::field_rules::inline_restrictions(
							&field,
							&table,
							&descriptor.fields,
						)
						.map_err(ControlFlow::Err)?;
					}
				}
				FieldCheck::Flexible {
					field: reg,
					table,
				} => {
					let field = field(*reg)?;
					if field.flexible {
						let stored: crate::catalog::StoredTableDefinition =
							rust(&read(regs, *table))?;
						let table = crate::catalog::TableDefinition::from_stored(&stored)
							.map_err(ControlFlow::Err)?;
						crate::exec::field_rules::flexible_restrictions(&field, Some(&table))
							.map_err(ControlFlow::Err)?;
					}
				}
				FieldCheck::Nesting {
					field: reg,
					table,
				} => {
					let field = field(*reg)?;
					let stored: crate::catalog::StoredTableDefinition = rust(&read(regs, *table))?;
					let table = crate::catalog::TableDefinition::from_stored(&stored)
						.map_err(ControlFlow::Err)?;
					let descriptor = crate::exec::vm::descriptor::table_at(ctx, &table.name, None)
						.await
						.map_err(ControlFlow::Err)?;
					crate::exec::field_rules::define_field_nesting_restrictions(
						&descriptor.fields,
						&field,
					)
					.map_err(ControlFlow::Err)?;
				}
				FieldCheck::KeyComponents {
					field: reg,
					ns,
					db,
					kind,
				} => {
					let definition = field(*reg)?;
					let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
					let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
					let txn = ctx.txn();
					super::schema_host::check_key_components(&txn, ns, db, kind, &definition)
						.await
						.map_err(ControlFlow::Err)?;
				}
				FieldCheck::IdUnread {
					ns,
					db,
					table,
				} => {
					let ns = crate::catalog::NamespaceId(scope(regs, Some(*ns), "namespace")?);
					let db = crate::catalog::DatabaseId(scope(regs, Some(*db), "database")?);
					let table = table_named(&read(regs, *table))?;
					let txn = ctx.txn();
					super::schema_host::refuse_removing_id(&txn, ns, db, &table)
						.await
						.map_err(ControlFlow::Err)?;
				}
			}
		}
		StoreInstr::PurgeDroppedReferences {
			ns,
			db,
			table,
			old,
			new,
		} => {
			use crate::catalog::FromStored as _;
			let field = |value: Value| -> FlowResult<crate::catalog::FieldDefinition> {
				let stored: crate::catalog::StoredFieldDefinition = rust(&value)?;
				crate::catalog::FieldDefinition::from_stored(&stored).map_err(ControlFlow::Err)
			};
			let old = read(regs, *old);
			if !old.is_none() {
				let old = field(old)?;
				let new = match new {
					Some(new) => Some(field(read(regs, *new))?),
					None => None,
				};
				let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
				let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
				let table = table_name_of(&read(regs, *table))?;
				crate::exec::vm::descriptor::schema(
					crate::exec::reference_purge::purge_dropped_reference_keys(
						&ctx.txn(),
						ns,
						db,
						&table,
						&old,
						new.as_ref(),
					),
				)
				.await
				.map_err(ControlFlow::Err)?;
			}
		}
		StoreInstr::RefuseLightweight {
			table,
			refusal,
		} => {
			use crate::catalog::FromStored as _;
			let table: crate::catalog::StoredTableDefinition = rust(&read(regs, *table))?;
			let table =
				crate::catalog::TableDefinition::from_stored(&table).map_err(ControlFlow::Err)?;
			if crate::kvs::lightweight::lightweight_relation(&table.table_type).is_some() {
				return Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Thrown(
					refusal.message().to_owned(),
				))));
			}
		}
		StoreInstr::ClaimDocIdsReclaim {
			ns,
			db,
			tb,
			upgrading,
		} => {
			let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
			let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
			let tb = table_name_of(&read(regs, *tb))?;
			let retry = match upgrading {
				None => Some("DEFINE INDEX"),
				Some(index) => {
					use crate::catalog::FromStored as _;
					let stored: crate::catalog::StoredIndexDefinition = rust(&read(regs, *index))?;
					let index = crate::catalog::IndexDefinition::from_stored(&stored)
						.map_err(ControlFlow::Err)?;
					// Restamping at the current format moves an index with a
					// private layout onto the shared space. An index already on
					// the shared space keeps reading from it and is not moved.
					let mut restamped = index.clone();
					restamped.format_version = crate::catalog::INDEX_FORMAT_VERSION;
					let moves = index.format_version != crate::catalog::INDEX_FORMAT_VERSION
						&& !index.uses_shared_doc_ids()
						&& restamped.uses_shared_doc_ids();
					moves.then_some("REBUILD INDEX")
				}
			};
			if let Some(retry) = retry
				&& ctx
					.txn()
					.claim_tb_doc_ids_reclaim(ns, db, &tb)
					.await
					.map_err(ControlFlow::Err)?
					== crate::kvs::tx::DocIdsReclaimClaim::InProgress
			{
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::kvs::DatastoreError::QueryNotExecuted {
						message: format!(
							"The shared document-ID space for table `{tb}` is still being reclaimed; retry {retry} after cleanup completes"
						),
					},
				)));
			}
		}
		StoreInstr::NextIndexId {
			ns,
			db,
			tb,
			dst,
			draw,
			reserved,
		} => {
			let sequence = CatalogSequence::Indexes(
				crate::catalog::NamespaceId(id_of(&read(regs, *ns))?),
				crate::catalog::DatabaseId(id_of(&read(regs, *db))?),
				table_name_of(&read(regs, *tb))?,
			);
			let id = catalog_id(program, *reserved, reserved_taken, ctx, sequence, *draw).await?;
			write(regs, *dst, Value::from(id as i64));
		}
		StoreInstr::IndexReplayReuses {
			existing,
			template,
			dst,
		} => {
			use crate::catalog::FromStored as _;
			let decode = |value: &Value| -> FlowResult<crate::catalog::IndexDefinition> {
				let stored: crate::catalog::StoredIndexDefinition = rust(value)?;
				crate::catalog::IndexDefinition::from_stored(&stored).map_err(ControlFlow::Err)
			};
			let (existing, template) =
				(decode(&read(regs, *existing))?, decode(&read(regs, *template))?);
			let reuses = !existing.prepare_remove
				&& existing.table_name.as_str() == template.table_name.as_str()
				&& existing.cols == template.cols
				&& existing.index == template.index;
			write(regs, *dst, Value::Bool(reuses));
		}
		StoreInstr::FieldAdmitsSubFields {
			field,
			dst,
		} => {
			use crate::catalog::FromStored as _;
			let stored: crate::catalog::StoredFieldDefinition = rust(&read(regs, *field))?;
			let field =
				crate::catalog::FieldDefinition::from_stored(&stored).map_err(ControlFlow::Err)?;
			write(
				regs,
				*dst,
				Value::Bool(field.field_kind.as_ref().is_none_or(|kind| kind.allows_sub_fields())),
			);
		}
		StoreInstr::BuildIndex {
			table,
			index,
			blocking,
			fresh,
		} => {
			use crate::catalog::FromStored as _;
			let table: crate::catalog::StoredTableDefinition = rust(&read(regs, *table))?;
			let table =
				crate::catalog::TableDefinition::from_stored(&table).map_err(ControlFlow::Err)?;
			let index: crate::catalog::StoredIndexDefinition = rust(&read(regs, *index))?;
			let index =
				crate::catalog::IndexDefinition::from_stored(&index).map_err(ControlFlow::Err)?;
			let frozen = ctx.ctx();
			let builder = frozen.get_index_builder().ok_or_else(|| {
				ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::unreachable(
					"No Index Builder",
				)))
			})?;
			if *fresh {
				let txn = ctx.txn();
				txn.on_rollback(crate::kvs::index::CleanUncommittedBuild::boxed(
					builder.clone(),
					builder.transaction_factory(),
					txn.sequences(),
					table.namespace_id,
					table.database_id,
					table.name.clone(),
					index.index_id,
				))
				.await;
			}
			let options = ctx.options().cloned().ok_or_else(|| {
				ControlFlow::Err(anyhow!("an index built without the session's options"))
			})?;
			let index = std::sync::Arc::new(index);
			// A fresh definition is staged in this transaction and taken back if
			// its build fails, so its build is awaited inside the transaction. A
			// rebuilt index is already defined: its build is awaited after the
			// commit wherever the transaction's owner offers that.
			if *blocking && !*fresh {
				builder
					.run_awaited_after_commit(frozen, options, table.table_id, index, &ctx.txn())
					.await
			} else {
				builder.run(frozen, options, table.table_id, index, *blocking).await
			}
			.map_err(ControlFlow::Err)?;
		}
		StoreInstr::SharesDocIds {
			index,
			dst,
		} => {
			use crate::catalog::FromStored as _;
			let index: crate::catalog::StoredIndexDefinition = rust(&read(regs, *index))?;
			let index =
				crate::catalog::IndexDefinition::from_stored(&index).map_err(ControlFlow::Err)?;
			write(regs, *dst, Value::Bool(index.uses_shared_doc_ids()));
		}
		StoreInstr::LoadAnalyzer {
			analyzer,
			mappers,
		} => {
			let analyzer: crate::catalog::AnalyzerDefinition = rust(&read(regs, *analyzer))?;
			let frozen = ctx.ctx();
			let stores = frozen.get_index_stores();
			if *mappers {
				stores
					.mappers()
					.load(&analyzer, &frozen.config.idx.file_allowlist)
					.await
					.map_err(ControlFlow::Err)?;
			}
			stores.segmenters().validate(&analyzer).await.map_err(ControlFlow::Err)?;
		}
		StoreInstr::CleanupMappers {
			analyzers,
		} => {
			let analyzers = match read(regs, *analyzers) {
				Value::Array(analyzers) => analyzers
					.iter()
					.map(rust::<crate::catalog::AnalyzerDefinition>)
					.collect::<FlowResult<Vec<_>>>()?,
				other => {
					return Err(ControlFlow::Err(anyhow!(
						"analyzers that are not a list: {other:?}"
					)));
				}
			};
			ctx.ctx().get_index_stores().mappers().cleanup(&analyzers);
		}
		StoreInstr::EvictModule {
			ns,
			db,
			module,
		} => {
			#[cfg(feature = "surrealism")]
			if let Some(cache) = ctx.ctx().get_surrealism_cache() {
				use crate::catalog::{FromStored as _, ModuleExecutable};
				use crate::surrealism::cache::SurrealismCacheLookup;
				let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
				let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
				let md: crate::catalog::StoredModuleDefinition = rust(&read(regs, *module))?;
				let md =
					crate::catalog::ModuleDefinition::from_stored(&md).map_err(ControlFlow::Err)?;
				let lookup = match &md.executable {
					ModuleExecutable::Surrealism(surrealism) => {
						SurrealismCacheLookup::File(&ns, &db, &surrealism.bucket, &surrealism.key)
					}
					ModuleExecutable::Silo(silo) => SurrealismCacheLookup::Silo(
						&ns,
						&db,
						&silo.organisation,
						&silo.package,
						silo.major,
						silo.minor,
						silo.patch,
					),
				};
				cache.remove(&lookup);
			}
			#[cfg(not(feature = "surrealism"))]
			let _ = (ns, db, module);
		}
		StoreInstr::LoadModule {
			ns,
			db,
			module,
		} => {
			#[cfg(feature = "surrealism")]
			{
				use crate::catalog::{FromStored as _, ModuleExecutable};
				use crate::surrealism::cache::SurrealismCacheLookup;
				let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
				let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
				let md: crate::catalog::StoredModuleDefinition = rust(&read(regs, *module))?;
				let storage_name = md.get_storage_name().ok();
				let md =
					crate::catalog::ModuleDefinition::from_stored(&md).map_err(ControlFlow::Err)?;
				let lookup = match &md.executable {
					ModuleExecutable::Surrealism(surrealism) => {
						SurrealismCacheLookup::File(&ns, &db, &surrealism.bucket, &surrealism.key)
					}
					ModuleExecutable::Silo(silo) => SurrealismCacheLookup::Silo(
						&ns,
						&db,
						&silo.organisation,
						&silo.package,
						silo.major,
						silo.minor,
						silo.patch,
					),
				};
				if let Err(e) = ctx.ctx().get_surrealism_runtime(lookup).await {
					tracing::warn!(
						module = ?storage_name,
						error = %e,
						"Failed to eagerly load surrealism module into cache"
					);
				}
			}
			#[cfg(not(feature = "surrealism"))]
			let _ = (ns, db, module);
		}
		StoreInstr::ConnectBucket {
			ns,
			db,
			bucket,
		} => {
			let bucket: crate::catalog::StoredBucketDefinition = rust(&read(regs, *bucket))?;
			let Some(buckets) = ctx.ctx().get_buckets() else {
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::buc::Error::BucketUnavailable(bucket.name.as_str().to_owned()),
				)));
			};
			let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
			let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
			buckets
				.new_backend(
					ns,
					db,
					bucket.name.as_str(),
					bucket.readonly,
					bucket.backend.as_ref().map(|b| b.as_str()),
				)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::CheckDepth {
			value,
		} => {
			crate::val::check_value_depth(&read(regs, *value)).map_err(ControlFlow::Err)?;
		}
		StoreInstr::SequenceDefinedHere {
			ns,
			db,
			name,
		} => {
			let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
			let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
			let name = match read(regs, *name) {
				Value::String(s) => s,
				other => return Err(ControlFlow::Err(anyhow!("a sequence name {other:?}"))),
			};
			ctx.txn().sequence_defined_here(ns, db, name.to_string()).await;
		}
		StoreInstr::EvictSequence {
			ns,
			db,
			name,
		} => {
			if let Some(sequences) = ctx.ctx().get_sequences() {
				let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
				let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
				let name = match read(regs, *name) {
					Value::String(s) => s,
					other => return Err(ControlFlow::Err(anyhow!("a sequence name {other:?}"))),
				};
				sequences.sequence_removed(ns, db, name.as_str()).await;
			}
		}
		StoreInstr::JumpIfSpeculating {
			target,
		} => {
			if let Some(txn) = ctx.try_txn()
				&& txn.speculating()
			{
				txn.note_speculative_skip();
				return Ok(Some(*target as usize));
			}
		}
		StoreInstr::EvaluateOver {
			..
		}
		| StoreInstr::BackfillRow {
			..
		}
		| StoreInstr::BackfillGroup {
			..
		}
		| StoreInstr::BackfillGroupRow {
			..
		} => {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"a backfill instruction taken without the state it runs over".to_owned(),
			))));
		}
		StoreInstr::ResolvePath {
			source,
			validated,
			dst,
		} => {
			let idiom = resolve_path(source, regs).map_err(ControlFlow::Err)?;
			if *validated {
				idiom.validate_local("field name").map_err(ControlFlow::Err)?;
			}
			write(regs, *dst, Value::String(idiom.to_raw_string().into()));
		}
		StoreInstr::ResolveColumns {
			source,
			list,
		} => {
			let paths = match source {
				ColumnSource::Path(source) => {
					vec![resolve_path(source, regs).map_err(ControlFlow::Err)?]
				}
				ColumnSource::TypeFields(reg) => {
					super::schema_host::type_fields_paths(read(regs, *reg))
						.map_err(ControlFlow::Err)?
				}
				ColumnSource::TypeField(reg) => {
					vec![
						super::schema_host::type_field_path(read(regs, *reg))
							.map_err(ControlFlow::Err)?,
					]
				}
				ColumnSource::Refused(message) => {
					return Err(ControlFlow::Err(anyhow!("{message}")));
				}
			};
			let Value::Array(mut items) = read(regs, *list) else {
				return Err(ControlFlow::Err(anyhow!("columns that are not a list")));
			};
			for path in paths {
				items.push(Value::String(path.to_raw_string().into()));
			}
			write(regs, *list, Value::Array(items));
		}
		StoreInstr::DefineField {
			definition,
			kind,
		} => {
			use crate::catalog::FromStored as _;
			let stored: crate::catalog::StoredFieldDefinition = rust(&read(regs, *definition))?;
			let mut definition =
				crate::catalog::FieldDefinition::from_stored(&stored).map_err(ControlFlow::Err)?;
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a field defined without the session's options"))
			})?;
			definition.auth_limit =
				crate::iam::AuthLimit::new_from_auth(options.auth.as_ref()).into();
			let frozen = ctx.ctx();
			super::schema_host::define_field(kind, &definition, frozen, options)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::RemoveFieldDefinition {
			name,
			table,
			if_exists,
		} => {
			let name = idiom_of(&read(regs, *name))?;
			let table = table_name_of(&read(regs, *table))?;
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a field removed without the session's options"))
			})?;
			super::schema_host::remove_field(*if_exists, name, table, ctx.ctx(), options)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::AlterFieldDefinition {
			name,
			table,
			statement,
		} => {
			let name = idiom_of(&read(regs, *name))?;
			let table = table_name_of(&read(regs, *table))?;
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a field altered without the session's options"))
			})?;
			super::schema_host::alter_field(statement, name, table, ctx.ctx(), options)
				.await
				.map_err(ControlFlow::Err)?;
		}
		StoreInstr::AlterFieldClauses {
			name,
			table,
			altered,
			fields,
			kind_changed,
			if_exists,
		} => {
			let name = idiom_of(&read(regs, *name))?;
			let table = table_name_of(&read(regs, *table))?;
			use crate::catalog::FromStored as _;
			let stored: crate::catalog::StoredFieldDefinition = rust(&read(regs, *altered))?;
			let altered =
				crate::catalog::FieldDefinition::from_stored(&stored).map_err(ControlFlow::Err)?;
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a field altered without the session's options"))
			})?;
			super::schema_host::alter_field_clauses(
				&altered,
				fields,
				(*kind_changed, *if_exists),
				name,
				table,
				ctx.ctx(),
				options,
			)
			.await
			.map_err(ControlFlow::Err)?;
		}
		StoreInstr::CheckIndexColumns {
			ns,
			db,
			table,
			index,
			columns,
			schemafull,
		} => {
			let ns = crate::catalog::NamespaceId(id_of(&read(regs, *ns))?);
			let db = crate::catalog::DatabaseId(id_of(&read(regs, *db))?);
			let table = table_name_of(&read(regs, *table))?;
			let index = match read(regs, *index) {
				Value::String(s) => s.to_string(),
				other => surrealdb_types::ToSql::to_sql(&other),
			};
			let schemafull = matches!(read(regs, *schemafull), Value::Bool(true));
			let Value::Array(columns) = read(regs, *columns) else {
				return Err(ControlFlow::Err(anyhow!("columns that are not a list")));
			};
			let txn = ctx.txn();
			for column in columns.iter() {
				let column = idiom_of(column)?;
				super::schema_host::check_index_column(
					&txn, ns, db, &table, schemafull, &index, &column,
				)
				.await
				.map_err(ControlFlow::Err)?;
			}
		}
		StoreInstr::CheckTableAlter {
			previous,
			updated,
		} => {
			use crate::catalog::FromStored as _;
			let table = |reg: &u16| -> FlowResult<crate::catalog::TableDefinition> {
				let stored: crate::catalog::StoredTableDefinition = rust(&read(regs, *reg))?;
				crate::catalog::TableDefinition::from_stored(&stored).map_err(ControlFlow::Err)
			};
			let previous = table(previous)?;
			let mut altered = table(updated)?;
			let txn = ctx.txn();
			super::schema_host::check_table_altered(&txn, &previous, &mut altered)
				.await
				.map_err(ControlFlow::Err)?;
			let normalised =
				altered.to_stored().into_vm_value().map_err(|e| ControlFlow::Err(e.into()))?;
			write(regs, *updated, normalised);
		}
		StoreInstr::JumpIfSessionBase {
			base,
			target,
		} => {
			let options = ctx.options().ok_or_else(|| {
				ControlFlow::Err(anyhow!("a session's base read with no options"))
			})?;
			let selected = options.selected_base().map_err(|e| ControlFlow::Err(e.into()))?;
			if selected == *base {
				return Ok(Some(*target as usize));
			}
		}
	}
	Ok(None)
}

/// Every key strictly beneath the bound of route `route` built from `fields`.
fn range(route: RouteId, fields: &Value) -> anyhow::Result<surrealdb_kvs::key::RawRange> {
	let bound = codec::key::encode_id(route, fields)?;
	surrealdb_kvs::key::RawRange::prefix_of(bound.into())
		.ok_or_else(|| anyhow!("a `{}` bound with no successor", route.name()))
}

/// The values stored beneath the bound, in key order, decoded as `ty`.
async fn scan(
	route: RouteId,
	fields: &Value,
	ty: TypeId,
	ctx: &ExecutionContext,
) -> anyhow::Result<Vec<Value>> {
	scan_at(route, fields, ty, None, ctx).await
}

/// [`scan`] as the store stood at the version stamp `version`, when there is
/// one.
async fn scan_at(
	route: RouteId,
	fields: &Value,
	ty: TypeId,
	version: Option<u64>,
	ctx: &ExecutionContext,
) -> anyhow::Result<Vec<Value>> {
	let entries = ctx.txn().getr_raw(range(route, fields)?, version).await?;
	entries.iter().map(|(_, bytes)| codec::decode_id(ty, bytes)).collect()
}

/// The field path the source names, resolved from the registers holding what
/// it evaluated.
fn resolve_path(source: &PathSource, regs: &[Scalar<'_>]) -> anyhow::Result<crate::expr::Idiom> {
	match source {
		PathSource::Written {
			idiom,
			holes,
		} => super::schema_host::substitute_evaluated(
			idiom.clone(),
			holes.iter().map(|hole| read(regs, *hole)).collect(),
		),
		PathSource::Evaluated(reg) => {
			super::schema_host::parse_path(read(regs, *reg), "field name")
		}
	}
}

/// The field path the text in `value` names.
fn idiom_of(value: &Value) -> FlowResult<crate::expr::Idiom> {
	use std::str::FromStr as _;
	match value {
		Value::String(text) => crate::expr::Idiom::from_str(text.as_str())
			.map_err(|e| ControlFlow::Err(anyhow!("a field path that does not parse: {e}"))),
		other => Err(ControlFlow::Err(anyhow!("a field path that is {other:?}, not text"))),
	}
}

/// The version stamp a [`StoreInstr::VersionStamp`] left in a register.
fn stamp_of(value: &Value) -> FlowResult<u64> {
	match value {
		Value::Number(crate::val::Number::Int(stamp)) => u64::try_from(*stamp)
			.map_err(|_| ControlFlow::Err(anyhow!("a version stamp {stamp} out of range"))),
		other => Err(ControlFlow::Err(anyhow!("a version stamp {other:?}"))),
	}
}

/// The Rust value of a register's value, for a service that holds one: the
/// conversion of the type's own schema, the inverse of the one that builds the
/// value from a stored definition.
fn rust<T: StoredField>(value: &Value) -> FlowResult<T> {
	T::from_vm_value(value.clone()).map_err(|e| ControlFlow::Err(e.into()))
}

/// Store `value` at `key` as type `ty`. A catalog type is written through the
/// transaction's levelled write, which reads the entry it replaces, encodes at
/// the store's feature level and admits the release the bytes need; any other
/// type is written as its bytes.
async fn put_value(
	ctx: &ExecutionContext,
	key: Vec<u8>,
	ty: TypeId,
	value: &Value,
	mode: LevelledMode,
) -> FlowResult<()> {
	let txn = ctx.txn();
	if codec::is_catalog(ty) {
		return txn
			.put_levelled_raw(key.into(), mode, |level| codec::encode_id_at(ty, value, level))
			.await
			.map_err(ControlFlow::Err);
	}
	let bytes = codec::encode_id(ty, value).map_err(ControlFlow::Err)?;
	match mode {
		LevelledMode::Set => txn.set(key.into(), bytes).await,
		LevelledMode::Replace => txn.replace_raw(key.into(), bytes).await,
	}
	.map_err(ControlFlow::Err)
}

/// The error removing a table with views over it raises, from an object of
/// the `table`'s name and its `views`' definitions.
fn views_error(detail: &Value) -> anyhow::Error {
	let field = |name: &str| match detail {
		Value::Object(object) => object.get(name).cloned().unwrap_or(Value::None),
		_ => Value::None,
	};
	let table = match field("table") {
		Value::String(s) => s.as_str().to_owned(),
		other => surrealdb_types::ToSql::to_sql(&other),
	};
	let views: Vec<String> = match field("views") {
		Value::Array(views) => views
			.iter()
			.map(|view| match view {
				Value::Object(view) => match view.get("name") {
					Some(Value::String(s)) => s.as_str().to_owned(),
					other => format!("{other:?}"),
				},
				other => format!("{other:?}"),
			})
			.collect(),
		_ => Vec::new(),
	};
	anyhow::Error::new(crate::exec::Error::Query {
		message: format!(
			"Cannot delete table `{table}` on which a view is defined, table(s) `{}` are defined as a view on this table.",
			views.join("`, `")
		),
	})
}

/// The string field `name` of the object `value`, as its text.
fn string_field(value: &Value, name: &str) -> String {
	match value {
		Value::Object(object) => match object.get(name) {
			Some(Value::String(s)) => s.as_str().to_owned(),
			Some(other) => surrealdb_types::ToSql::to_sql(other),
			None => String::new(),
		},
		_ => String::new(),
	}
}

/// The table a register names, as a string.
/// Fails when the value is neither a string nor a table.
pub(super) fn table_name_of(value: &Value) -> FlowResult<crate::val::TableName> {
	match value {
		Value::String(s) => Ok(crate::val::TableName::new(s.as_str())),
		Value::Table(t) => Ok(t.clone()),
		other => Err(ControlFlow::Err(anyhow!("a table name {other:?}"))),
	}
}

fn id_of(value: &Value) -> FlowResult<u32> {
	match value {
		Value::Number(crate::val::Number::Int(v)) => u32::try_from(*v)
			.map_err(|_| ControlFlow::Err(anyhow!("a catalog id out of range: {v}"))),
		other => Err(ControlFlow::Err(anyhow!("a catalog id that is not an integer: {other:?}"))),
	}
}

/// The catalog id of a sequence's scope, held in a register.
fn scope(regs: &[Scalar<'_>], reg: Option<u16>, what: &str) -> FlowResult<u32> {
	let reg = reg.ok_or_else(|| ControlFlow::Err(anyhow!("an id with no {what} scope")))?;
	id_of(&read(regs, reg))
}

/// Whether a speculative run leaves `instr` for the statement's real run: it
/// acts on something the transaction's key-value view does not hold.
fn speculative_skip(instr: &StoreInstr) -> bool {
	matches!(
		instr,
		StoreInstr::BufferTableChange { .. }
			| StoreInstr::ClearDatastoreCache
			| StoreInstr::SetQueryTimeout { .. }
			| StoreInstr::Compact { .. }
			| StoreInstr::CompactAll
			| StoreInstr::RetireIndexLocally { .. }
			| StoreInstr::KillOnCommit { .. }
			| StoreInstr::BuildIndex { .. }
			| StoreInstr::DeleteModelArtifact { .. }
			| StoreInstr::LoadAnalyzer { .. }
			| StoreInstr::CleanupMappers { .. }
			| StoreInstr::EvictModule { .. }
			| StoreInstr::LoadModule { .. }
			| StoreInstr::ConnectBucket { .. }
			| StoreInstr::SequenceDefinedHere { .. }
			| StoreInstr::EvictSequence { .. }
			| StoreInstr::ValidateRecordKeys { .. }
	)
}

/// The options of the session an access statement runs in, which name its
/// selected base and its principal.
fn session_options(ctx: &ExecutionContext) -> FlowResult<&crate::dbs::Options> {
	ctx.options().ok_or_else(|| {
		ControlFlow::Err(anyhow!("an access statement run without the session's options"))
	})
}

/// Whether host pipeline `filter` of `program` answers truthy over `row`, a
/// grant as a statement shows it; every grant is admitted where there is no
/// filter. A `RETURN` in the filter answers its value.
async fn admits(
	program: &Program,
	filter: Option<u16>,
	row: &Value,
	state: &mut Option<super::interp::PooledState>,
	ctx: &ExecutionContext,
) -> FlowResult<bool> {
	let Some(filter) = filter else {
		return Ok(true);
	};
	let verdict = super::host::eval_on_row(program, filter, Cow::Borrowed(row), state, ctx)
		.await
		.catch_return()
		.map_err(ControlFlow::Err)?;
	Ok(verdict.is_truthy())
}

/// What [`StoreInstr::ShowGrants`] answers.
async fn show_grants(
	program: &Program,
	ctx: &ExecutionContext,
	ac: &str,
	base: Option<crate::expr::Base>,
	gr: Option<&str>,
	filter: Option<u16>,
) -> FlowResult<Value> {
	let opt = session_options(ctx)?;
	let frozen = ctx.ctx();
	let base = grant::access_method_base(base, ac, Action::View, frozen, opt)
		.await
		.map_err(ControlFlow::Err)?;
	if let Some(gr) = gr {
		return grant::shown_grant(base, ac, gr, frozen, opt).await.map_err(ControlFlow::Err);
	}
	let grants = grant::grants_at(base, ac, frozen, opt).await.map_err(ControlFlow::Err)?;
	let mut state = None;
	let mut shown = Vec::new();
	for held in grants.iter() {
		let row = grant::shown(held);
		if admits(program, filter, &row, &mut state, ctx).await? {
			shown.push(row);
		}
	}
	Ok(Value::Array(shown.into()))
}

/// What [`StoreInstr::RevokeGrants`] answers.
async fn revoke_grants(
	program: &Program,
	ctx: &ExecutionContext,
	ac: &str,
	base: Option<crate::expr::Base>,
	gr: Option<&str>,
	filter: Option<u16>,
) -> FlowResult<Value> {
	let opt = session_options(ctx)?;
	let frozen = ctx.ctx();
	let base = grant::access_method_base(base, ac, Action::Edit, frozen, opt)
		.await
		.map_err(ControlFlow::Err)?;
	let mut revoked = Vec::new();
	match gr {
		Some(gr) => revoked
			.push(grant::revoke_named(base, ac, gr, frozen, opt).await.map_err(ControlFlow::Err)?),
		None => {
			let grants = grant::grants_at(base, ac, frozen, opt).await.map_err(ControlFlow::Err)?;
			let mut state = None;
			for held in grants.iter() {
				if held.revocation.is_some()
					|| !admits(program, filter, &grant::shown(held), &mut state, ctx).await?
				{
					continue;
				}
				revoked.push(
					grant::revoke_held(base, ac, held, frozen, opt)
						.await
						.map_err(ControlFlow::Err)?,
				);
			}
		}
	}
	Ok(Value::Array(vec![Value::Array(revoked.into())].into()))
}

/// The id of catalog sequence `sequence` that draw `draw` takes: the one
/// reserved for it, confirmed with the node's sequences, when this run is the
/// first to reach it; otherwise one leased now, held with the speculation's
/// writes when the run is speculative.
async fn catalog_id(
	program: &Program,
	reserved: Option<u32>,
	reserved_taken: &mut [bool],
	ctx: &ExecutionContext,
	sequence: CatalogSequence,
	draw: u16,
) -> FlowResult<u32> {
	let frozen = ctx.ctx();
	if let Some(id) = take_reserved(program, reserved, reserved_taken) {
		let id = id as u32;
		if let Ok(sequences) = frozen.try_get_sequences() {
			sequences.confirm_catalog_id(&sequence, id);
		}
		return Ok(id);
	}
	let sequences = frozen.try_get_sequences().map_err(ControlFlow::Err)?;
	let id = match ctx.try_txn().is_some_and(|txn| txn.speculating()) {
		true => {
			let id = crate::exec::vm::descriptor::schema(
				sequences.hold_catalog_id(Some(frozen), &sequence),
			)
			.await
			.map_err(ControlFlow::Err)?;
			super::speculate::held(ctx, sequences, &sequence, id);
			#[cfg(test)]
			super::speculate::pause_after_hold().await;
			id
		}
		false => {
			crate::exec::vm::descriptor::schema(sequences.next_catalog_id(Some(frozen), &sequence))
				.await
				.map_err(ControlFlow::Err)?
		}
	};
	super::speculate::drawn(ctx, draw, id as i64 as u128);
	Ok(id)
}

/// What was reserved for a draw, when this run is the first to reach it. A
/// reservation of a catalog id is confirmed by the draw that takes it
/// ([`catalog_id`], `write::define_shared_table`), so the planning that held
/// it does not release it.
fn take_reserved(program: &Program, reserved: Option<u32>, taken: &mut [bool]) -> Option<u128> {
	let at = reserved? as usize;
	let slot = taken.get_mut(at)?;
	if std::mem::replace(slot, true) {
		return None;
	}
	program.reservations.get(at).copied()
}

/// The table `value` names.
fn table_named(value: &Value) -> FlowResult<crate::val::TableName> {
	match value {
		Value::String(name) => Ok(crate::val::TableName::from(name.as_str())),
		Value::Table(name) => Ok(name.clone()),
		other => Err(ControlFlow::Err(anyhow!("{other:?} names no table"))),
	}
}
