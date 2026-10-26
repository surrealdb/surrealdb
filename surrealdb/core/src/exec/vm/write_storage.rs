//! The host's side of the storage stages and structural checks run as
//! instructions: CheckRecordExists, CheckTableNotView,
//! CheckTableNotLightweight, CheckTableType, ClearRecordData, StoreRecordData,
//! StoreEdgesData, StoreInlineAdjacencyData, StoreIndexData and ReclaimDocId.
//!
//! Which checks run and what is written is the body's control flow; an
//! instruction here answers one fact of the record in flight or its table, or
//! performs one storage effect. A record's key is encoded by the VM's own key
//! codec (`vm::codec::key`) and its bytes by the record writer
//! (`catalog::encode_record_with`, given the format and layouts the write
//! context's slot resolves, or `Transaction::record_writer`), and the host
//! writes them: on the statement's transaction here, as an intent on the write fan-out.

// Runtime support for the instructions the IR lowering emits.
#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use surrealdb_datastore::values::graph::{AdjacencyValue, GraphFoldScope};
use surrealdb_kvs::value::KVValue;
use surrealdb_types::ToSql;

use super::{State, internal};
use crate::catalog::providers::TableProvider;
use crate::catalog::{IndexDefinition, LATEST_EDGE_VARIANT, Record, Relation, TableType};
use crate::doc::{Error as DocError, encode_inline_section, inline_fields};
use crate::err::EngineError;
use crate::exec::operators::write::ctx::{ResolvedFormat, WriteCtx};
use crate::exec::operators::write::metadata::{edge_variant, is_edge};
use crate::exec::operators::write::row::{WriteExtras, WriteRow};
use crate::exec::operators::write::stage::{LightweightReason, TableTypeFor};
use crate::exec::operators::write::stages::guard::{
	refuse_lightweight, refuse_table_type, table_is_lightweight,
};
use crate::exec::operators::write::stages::store::{endpoint, endpoint_moved, endpoint_override};
use crate::exec::vm::codec;
use crate::exec::vm::codec::key::KeyField;
use crate::exec::vm::program::{Label, Program};
use crate::exec::{Error as ExecError, FlowResult};
use crate::expr::paths::{IN, OUT};
use crate::expr::{ControlFlow, Dir};
use crate::iam::Action;
use crate::idx::docids::TableDocIds;
use crate::idx::entry::IndexEntryValue;
use crate::idx::index::{Indexable, recorded_shapes};
use crate::kvs::index::{ConsumeResult, IndexMutation};
use crate::kvs::storage_error;
use crate::val::Value;

/// An instruction of the storage stages.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum StorageInstr {
	/// Jump to `target` when `fact` holds of the record in flight as `when`
	/// says.
	JumpIf {
		fact: StorageFact,
		when: bool,
		target: Label,
	},
	/// Fail the statement when the record's table is a view, unless the
	/// statement replays an import: an export writes a view's stored rows back
	/// as ordinary writes.
	RefuseView,
	/// Fail the statement when the record's table is a lightweight relation,
	/// whose edges hold no data, naming `reason`.
	RefuseLightweight {
		reason: LightweightReason,
	},
	/// Fail the statement when the record's table does not admit the record
	/// type the write is of: a normal record for `CREATE`, `UPSERT` and a plain
	/// `INSERT`, a relation for `RELATE` and `INSERT RELATION`, which a
	/// lightweight relation refuses too.
	RefuseTableType {
		subject: TableTypeFor,
	},
	/// Empty the post-image and the record-type marker it carries.
	Clear,
	/// Store the post-image and its marker at the record's key: create-only
	/// when `create`, failing as a duplicate naming the record where one is
	/// stored, and an overwrite otherwise. Counts the record as written. A
	/// create leaves its post-image in the transaction's record cache when
	/// `cache`; otherwise a later read of the record decodes it from storage.
	Store {
		create: bool,
		cache: bool,
	},
	/// Fail the statement unless both of the edge's endpoints exist.
	RequireEndpoints,
	/// Fail the statement when either of the edge's endpoints is an edge of a
	/// lightweight relation, which cannot be a graph endpoint.
	RefuseLightweightEndpoints,
	/// Delete the two vertex-side pointer keys of the original adjacency
	/// layout, which name no target vertex.
	DeleteLegacyPointers,
	/// Write the edge's adjacency keys: its two vertex-side pointer keys,
	/// carrying the table's inline payload, and, unless `lightweight`, its two
	/// edge-side keys.
	WriteEdgeKeys {
		lightweight: bool,
	},
	/// Fold the edge into each endpoint's inline adjacency cache.
	CacheEdge,
	/// Set the post-image's `in` and `out` to the endpoints the adjacency was
	/// written under.
	PinEndpoints,
	/// Fail the statement when the post-image's `in` or `out` differs from the
	/// endpoint the stored edge was written under.
	RequireUnmovedEndpoints,
	/// Rewrite the stored edge's two vertex-side pointer values with the
	/// table's inline payload read off the post-image.
	RewriteInlinePointers,
	/// Delete the record's key and its own inline cache keys. Counts the
	/// record as written.
	DeleteRecord,
	/// Remove the adjacency keys an edge's endpoints hold, in the layout the
	/// before-image's marker names: all four keys of the original layout, and
	/// otherwise the two pointer keys — deleted, or tombstoned where the
	/// vertex's table folds adjacency — and, unless the table is a lightweight
	/// relation, the two edge-side keys; then drop the edge from each
	/// endpoint's inline cache.
	PurgePointers,
	/// A step of the cascades of the record's delete (`vm::write::cascade`).
	References(super::cascade::ReferenceInstr),
	/// Bring the reference keys each of `fields`, the table's `REFERENCE`
	/// fields, declares from the before-image's links to the post-image's, a
	/// place at a time; when `release`, from the post-image's links to none.
	SyncReferences {
		fields: std::sync::Arc<[crate::expr::Idiom]>,
		release: bool,
	},
	/// Fail the statement unless the record's table holds the index
	/// definitions `lowered` was lowered from, which every index instruction
	/// after it relies on. Fails the statement when the record has no id.
	CheckIndexes {
		lowered: Arc<LoweredIndexes>,
	},
	/// Bring the entries of B-tree or `UNIQUE` index `lowered.defs[position]`
	/// in line with the record's two images, when the values its columns
	/// answer for them differ: through an in-flight build of the index when
	/// one takes the change, and otherwise by removing the entries of the
	/// before-image's values and writing those of the post-image's, a `UNIQUE`
	/// entry create-only and failing the statement naming the record that
	/// holds it. A build that took the change of an index consuming the
	/// table's shared doc-id space defers the record's doc-id release.
	IndexEntries {
		lowered: Arc<LoweredIndexes>,
		position: u16,
	},
	/// Bring index `lowered.defs[position]`, of a kind that keeps its own
	/// structure, in line with the record's two images through the kind's own
	/// implementation, when the values its columns answer for them differ or,
	/// for a `COUNT` index with a guard, when the guard's answer does.
	IndexNative {
		lowered: Arc<LoweredIndexes>,
		position: u16,
	},
	/// Mark the record's doc-id for release by the replay of the index build
	/// that took the record's delete.
	MarkDocIdPending,
	/// Drop the record's entry from its table's shared doc-id space.
	ReleaseDocId,
}

#[cfg(test)]
thread_local! {
	/// How many times this thread's writes took [`StorageInstr::IndexEntries`]
	/// or [`StorageInstr::IndexNative`], for tests that must know which ran.
	pub(in crate::exec::vm) static INDEX_STEPS: std::cell::Cell<usize> =
		const { std::cell::Cell::new(0) };
}

assert_plain!(StorageInstr);

/// The indexes of a table as a program was lowered from them: the
/// definitions the catalog held, which stamp the lowering, and how each is
/// maintained.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct LoweredIndexes {
	/// The table's index definitions when the program was lowered, in the
	/// order the catalog holds them.
	pub(in crate::exec::vm) defs: Arc<[IndexDefinition]>,
	/// Parallel to `defs`.
	pub(in crate::exec::vm) per_index: Vec<IndexPlan>,
}

assert_plain!(LoweredIndexes);

/// How one index of a table is maintained for a record's two images.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum IndexPlan {
	/// The catalog has marked the index for removal; it is not maintained.
	Removed,
	/// A B-tree or `UNIQUE` index, whose entries the host writes from the
	/// values host pipeline `columns` answers for an image.
	Entries {
		columns: u16,
	},
	/// An index of a kind that keeps its own structure, moved through the
	/// kind's implementation from the values host pipeline `columns` answers
	/// for an image and, for a `COUNT` index with a guard, whether host
	/// pipeline `guard` answers truthy for it.
	Native {
		kind: NativeIndexKind,
		columns: u16,
		guard: Option<u16>,
	},
}

assert_plain!(IndexPlan);

/// The kinds of index that keep their own structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::vm) enum NativeIndexKind {
	/// A `COUNT` index.
	Count,
	/// A full-text index.
	FullText,
	/// An `HNSW` index.
	Hnsw,
	/// A `DiskANN` index.
	DiskAnn,
	/// A `SPATIAL` geometry index.
	Spatial,
}

assert_plain!(NativeIndexKind);

impl NativeIndexKind {
	/// The kind of native index `index` is, `None` for a B-tree or `UNIQUE`
	/// index, whose entries the host writes.
	pub(in crate::exec::vm) fn of(index: &crate::catalog::Index) -> Option<Self> {
		use crate::catalog::Index;
		match index {
			Index::Idx | Index::Uniq => None,
			Index::Count(_) => Some(Self::Count),
			Index::FullText(_) => Some(Self::FullText),
			Index::Hnsw(_) => Some(Self::Hnsw),
			Index::DiskAnn(_) => Some(Self::DiskAnn),
			Index::Spatial => Some(Self::Spatial),
		}
	}
}

impl LoweredIndexes {
	pub(in crate::exec::vm) fn new(
		defs: Arc<[IndexDefinition]>,
		per_index: Vec<IndexPlan>,
	) -> Self {
		Self {
			defs,
			per_index,
		}
	}

	/// Whether `indexes` are the definitions these were lowered from.
	fn lowered_from(&self, indexes: &Arc<[IndexDefinition]>) -> bool {
		Arc::ptr_eq(indexes, &self.defs) || **indexes == *self.defs
	}
}

/// A fact of the record in flight that [`StorageInstr::JumpIf`] tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::vm) enum StorageFact {
	/// The statement named a record that storage does not hold: the record has
	/// an id and no post-image.
	Absent,
	/// No mutation phase changed the record.
	Unmodified,
	/// The record's table is declared `DROP`: a write that would store the
	/// record stores nothing. A record stored before the clause was declared
	/// stays stored until it is deleted.
	TableDropped,
	/// The record's table is a lightweight relation, whose edges are their
	/// adjacency keys alone.
	TableLightweight,
	/// The record is being written for the first time in this iteration, which
	/// is what decides create-only against overwrite.
	IterationInitial,
	/// The record is an edge the statement relates: its endpoints are the
	/// statement's.
	Related,
	/// The record's table is a relation that admits edges only between
	/// endpoints that exist, and the statement is not replaying an import,
	/// which restores edges ahead of the vertices they join.
	EnforcedRelation,
	/// The record's before-image is an edge of the original adjacency layout.
	LegacyLayout,
	/// The record's before-image is an edge of the current adjacency layout.
	CurrentLayout,
	/// The record's table declares no `INLINE` field.
	NoInlineFields,
	/// The record's before-image is an edge.
	WasEdge,
	/// Neither did a mutation phase change the record nor does the session
	/// force index maintenance on every record it writes.
	Unmaintained,
	/// An in-flight index build took the record's change and still resolves
	/// the record through its doc-id. Fails the statement when the record has
	/// no id.
	DocIdDeferred,
	/// Something on the record's table consumes the table's shared doc-id
	/// space: an index whose entries or structure name doc-ids, or the
	/// numeric adjacency blocks its graph marker records, as the table's
	/// definition holds it.
	DocIdConsumer,
	/// The record's table's graph marker is set, as the transaction's settled
	/// read of it answers: a read that conflicts at commit with a concurrent
	/// write of the marker.
	GraphDocIdsSettled,
}

impl StorageInstr {
	/// The jump target the instruction holds, for the lowering to patch.
	pub(in crate::exec::vm) fn target_mut(&mut self) -> Option<&mut Label> {
		match self {
			StorageInstr::JumpIf {
				target,
				..
			} => Some(target),
			StorageInstr::References(instr) => instr.target_mut(),
			StorageInstr::CheckIndexes {
				..
			}
			| StorageInstr::IndexEntries {
				..
			}
			| StorageInstr::IndexNative {
				..
			}
			| StorageInstr::MarkDocIdPending
			| StorageInstr::ReleaseDocId
			| StorageInstr::RequireEndpoints
			| StorageInstr::RefuseLightweightEndpoints
			| StorageInstr::DeleteLegacyPointers
			| StorageInstr::WriteEdgeKeys {
				..
			}
			| StorageInstr::CacheEdge
			| StorageInstr::PinEndpoints
			| StorageInstr::RequireUnmovedEndpoints
			| StorageInstr::RewriteInlinePointers
			| StorageInstr::SyncReferences {
				..
			}
			| StorageInstr::DeleteRecord
			| StorageInstr::PurgePointers
			| StorageInstr::RefuseView
			| StorageInstr::RefuseLightweight {
				..
			}
			| StorageInstr::RefuseTableType {
				..
			}
			| StorageInstr::Clear
			| StorageInstr::Store {
				..
			} => None,
		}
	}
}

/// Take `instr` for the record in flight, and answer the instruction the body
/// continues at: `next`, or a target the instruction holds.
pub(super) async fn step(
	program: &Program,
	instr: &StorageInstr,
	next: usize,
	state: &mut State,
) -> FlowResult<usize> {
	if let StorageInstr::IndexEntries {
		lowered,
		position,
	}
	| StorageInstr::IndexNative {
		lowered,
		position,
	} = instr
	{
		// Boxed: index maintenance is the largest future of the storage
		// stages, and held inline it would size every record's write step,
		// on tables with no index too.
		crate::exec::vm::host::out_of_line(|| {
			index_step(program, lowered, *position as usize, state)
		})
		.await?;
		return Ok(next);
	}
	if let StorageInstr::References(instr) = instr {
		return super::cascade::step(program, instr, next, state).await;
	}
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	match instr {
		StorageInstr::CheckIndexes {
			lowered,
		} => {
			indexed_record(row)?;
			if !lowered.lowered_from(&ctx.indexes) {
				return Err(internal("the indexes of a table changed after the write was lowered"));
			}
			Ok(next)
		}
		StorageInstr::IndexEntries {
			..
		}
		| StorageInstr::IndexNative {
			..
		} => Err(internal("an index maintained without its images")),
		StorageInstr::MarkDocIdPending => {
			let rid = indexed_record(row)?;
			let key = codec::key::encode_fields(
				"DocPendingKey",
				&[
					("ns", KeyField::Int(ctx.db.namespace_id.0 as i64)),
					("db", KeyField::Int(ctx.db.database_id.0 as i64)),
					("tb", KeyField::Str(rid.table.as_str())),
					("id", KeyField::Id(&rid.key)),
				],
			)
			.map_err(ControlFlow::Err)?;
			ctx.txn().set_key(&Encoded::<()>::new(key), &()).await.map_err(ControlFlow::Err)?;
			Ok(next)
		}
		StorageInstr::ReleaseDocId => {
			let rid = indexed_record(row)?;
			TableDocIds::new(ctx.db.namespace_id, ctx.db.database_id, rid.table.clone())
				.remove(&ctx.txn(), &rid.key)
				.await
				.map_err(ControlFlow::Err)?;
			Ok(next)
		}
		StorageInstr::JumpIf {
			fact,
			when,
			target,
		} => {
			let holds = match fact {
				StorageFact::Absent => row.id.is_some() && row.current.is_none(),
				StorageFact::Unmodified => !row.is_modified(),
				StorageFact::TableDropped => ctx.table.drop,
				StorageFact::TableLightweight => table_is_lightweight(&ctx.table),
				StorageFact::IterationInitial => row.is_iteration_initial(),
				StorageFact::Related => matches!(row.extras, WriteExtras::Relate(_, _, _)),
				StorageFact::EnforcedRelation => {
					!ctx.exec().options().is_some_and(|options| options.import)
						&& matches!(
							ctx.table.table_type,
							TableType::Relation(Relation {
								enforced: true,
								..
							})
						)
				}
				StorageFact::LegacyLayout => {
					edge_variant(&row.initial_metadata).unwrap_or(LATEST_EDGE_VARIANT) == 1
				}
				StorageFact::CurrentLayout => {
					is_edge(&row.initial_metadata)
						&& edge_variant(&row.initial_metadata) == Some(LATEST_EDGE_VARIANT)
				}
				StorageFact::NoInlineFields => inline_fields(&ctx.fields).is_empty(),
				StorageFact::WasEdge => is_edge(&row.initial_metadata),
				StorageFact::Unmaintained => !index_forced(&ctx) && !row.is_modified(),
				StorageFact::DocIdDeferred => {
					indexed_record(row)?;
					row.doc_id_removal_deferred
				}
				StorageFact::DocIdConsumer => {
					ctx.table.graph_doc_ids
						|| ctx.indexes.iter().any(IndexDefinition::uses_shared_doc_ids)
				}
				StorageFact::GraphDocIdsSettled => ctx
					.txn()
					.graph_doc_ids_settled(ctx.db.namespace_id, ctx.db.database_id, &ctx.table.name)
					.await
					.map_err(ControlFlow::Err)?,
			};
			Ok(if holds == *when {
				*target as usize
			} else {
				next
			})
		}
		StorageInstr::RefuseView => {
			if ctx.exec().options().is_some_and(|options| options.import) {
				return Ok(next);
			}
			if ctx.table.view.is_some() {
				return Err(ControlFlow::Err(anyhow::Error::new(DocError::TableIsView {
					table: ctx.table.name.to_string(),
				})));
			}
			Ok(next)
		}
		StorageInstr::RefuseLightweight {
			reason,
		} => {
			refuse_lightweight(&ctx.table, reason.as_str())?;
			Ok(next)
		}
		StorageInstr::RefuseTableType {
			subject,
		} => {
			let table = &ctx.table;
			match subject {
				TableTypeFor::Create | TableTypeFor::Upsert => {
					if !table.allows_normal() {
						refuse_table_type(row, table, false)?;
					}
				}
				TableTypeFor::Relate => {
					if !table.allows_relation() {
						refuse_table_type(row, table, true)?;
					}
				}
				// The statement's own payload decides which record type an
				// `INSERT` is writing: `INSERT RELATION` carries the endpoints,
				// every other `INSERT` carries a row.
				TableTypeFor::Insert => match &row.extras {
					WriteExtras::Relate(_, _, _) => {
						if !table.allows_relation() {
							refuse_table_type(row, table, true)?;
						}
						refuse_lightweight(table, "written with INSERT")?;
					}
					WriteExtras::Normal | WriteExtras::Insert(_) => {
						if !table.allows_normal() {
							refuse_table_type(row, table, false)?;
						}
					}
				},
			}
			Ok(next)
		}
		StorageInstr::Clear => {
			row.current = Value::None;
			row.metadata = None;
			Ok(next)
		}
		StorageInstr::Store {
			create,
			cache,
		} => {
			store(row, &ctx, *create, *cache).await?;
			Ok(next)
		}
		StorageInstr::RequireEndpoints => {
			let (l, r) = related(row)?;
			let txn = ctx.txn();
			for endpoint in [&l, &r] {
				let exists = txn
					.record_exists(
						ctx.ns.namespace_id,
						ctx.db.database_id,
						&endpoint.table,
						&endpoint.key,
						ctx.exec().version_stamp(),
					)
					.await
					.map_err(ControlFlow::Err)?;
				if !exists {
					return Err(ControlFlow::Err(anyhow::Error::new(DocError::IdNotFound {
						rid: endpoint.to_sql(),
					})));
				}
			}
			Ok(next)
		}
		StorageInstr::RefuseLightweightEndpoints => {
			let (l, r) = related(row)?;
			for endpoint in [&l, &r] {
				let def = crate::exec::vm::descriptor::table_at(ctx.exec(), &endpoint.table, None)
					.await
					.map_err(ControlFlow::Err)?;
				if def.definition.as_ref().is_some_and(|def| table_is_lightweight(def)) {
					return Err(ControlFlow::Err(anyhow::Error::new(ExecError::Thrown(format!(
						"a LIGHTWEIGHT relation's edges cannot be graph endpoints: {}",
						endpoint.to_sql()
					)))));
				}
			}
			Ok(next)
		}
		StorageInstr::DeleteLegacyPointers => {
			let (l, r) = related(row)?;
			let rid = record_id(row)?;
			let txn = ctx.txn();
			let (ltr, rtl) =
				(graph_key(&ctx, &l, Dir::Out, &rid)?, graph_key(&ctx, &r, Dir::In, &rid)?);
			futures::try_join!(txn.del(ltr.into()), txn.del(rtl.into()))
				.map_err(ControlFlow::Err)?;
			Ok(next)
		}
		StorageInstr::WriteEdgeKeys {
			lightweight,
		} => {
			let (l, r) = related(row)?;
			let rid = record_id(row)?;
			let live = if *lightweight {
				AdjacencyValue::live()
			} else {
				inline_payload(row, &ctx)?
			}
			.kv_encode_value()
			.map_err(ControlFlow::Err)?;
			let txn = ctx.txn();
			let ltr = pointer_key(&ctx, &l, Dir::Out, &rid, &r)?;
			let rtl = pointer_key(&ctx, &r, Dir::In, &rid, &l)?;
			if *lightweight {
				// A lightweight edge is stored as these keys alone, so the
				// edge they create is noted as `store` notes a record it
				// creates: one relating an edge that exists creates nothing.
				// Whether it exists is read only while its relation is
				// watched.
				let (ns, db) = (ctx.ns.namespace_id, ctx.db.database_id);
				let noted = if txn.creations_watched(ns, db, &rid.table)
					&& !txn
						.record_exists(ns, db, &rid.table, &rid.key, None)
						.await
						.map_err(ControlFlow::Err)?
				{
					Some(record_key(ns, db, &rid)?)
				} else {
					None
				};
				futures::try_join!(txn.set(ltr.into(), live.clone()), txn.set(rtl.into(), live),)
					.map_err(ControlFlow::Err)?;
				if let Some(key) = noted {
					txn.note_creation(ns, db, &rid.table, key);
				}
			} else {
				let etl = graph_key(&ctx, &rid, Dir::In, &l)?;
				let etr = graph_key(&ctx, &rid, Dir::Out, &r)?;
				futures::try_join!(
					txn.set(ltr.into(), live.clone()),
					txn.set(etl.into(), Vec::new()),
					txn.set(etr.into(), Vec::new()),
					txn.set(rtl.into(), live),
				)
				.map_err(ControlFlow::Err)?;
			}
			Ok(next)
		}
		StorageInstr::CacheEdge => {
			let (l, r) = related(row)?;
			let rid = record_id(row)?;
			let txn = ctx.txn();
			let resolve = Some(ctx.exec().root().ctx.get_index_stores().adjacency_resolve());
			for (vertex, dir, target) in [(&l, Dir::Out, &r), (&r, Dir::In, &l)] {
				crate::idx::inline_cache::record_edge_write(
					&txn,
					ctx.ns.namespace_id,
					ctx.db.database_id,
					vertex,
					dir,
					&rid,
					target,
					resolve,
				)
				.await
				.map_err(ControlFlow::Err)?;
			}
			Ok(next)
		}
		StorageInstr::PinEndpoints => {
			let (l, r) = related(row)?;
			row.current.put(&IN, Value::RecordId(l));
			row.current.put(&OUT, Value::RecordId(r));
			Ok(next)
		}
		StorageInstr::RequireUnmovedEndpoints => {
			let (l, r) = stored_endpoints(row)?;
			let in_moved = endpoint_moved(&row.current, &IN, &l);
			let out_moved = endpoint_moved(&row.current, &OUT, &r);
			let checked = ctx
				.exec()
				.should_check_perms(Action::View)
				.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
			if let Some(error) = endpoint_override(checked, in_moved, out_moved) {
				return Err(ControlFlow::Err(anyhow::Error::new(error)));
			}
			Ok(next)
		}
		StorageInstr::DeleteRecord => {
			let rid = record_id(row)?;
			let ns = ctx.ns.namespace_id;
			let db = ctx.db.database_id;
			let key = record_key(ns, db, &rid)?;
			let txn = ctx.txn();
			txn.del(key.into()).await.map_err(ControlFlow::Err)?;
			txn.forget_record(ns, db, &rid.table, &rid.key);
			crate::idx::inline_cache::purge_record_caches(&txn, ns, db, &rid)
				.await
				.map_err(ControlFlow::Err)?;
			row.mutated = true;
			Ok(next)
		}
		StorageInstr::PurgePointers => {
			let (l, r) = stored_endpoints(row)?;
			let variant = edge_variant(&row.initial_metadata).unwrap_or_default();
			let rid = record_id(row)?;
			purge_pointers(&ctx, &rid, &l, &r, variant).await?;
			Ok(next)
		}
		StorageInstr::References(_) => Err(internal("a cascade step taken without its program")),
		StorageInstr::SyncReferences {
			fields,
			release,
		} => {
			let rid = record_id(row)?;
			for field in fields.iter() {
				let name = field.to_sql();
				for (place, value) in row.current.walk(field) {
					if *release {
						// A place holding nothing links to nothing.
						if value.is_none() || value.is_empty_array() {
							continue;
						}
						sync_reference(&ctx, &rid, &name, &value, &Value::None).await?;
					} else {
						let previous = row.initial.pick(&place);
						sync_reference(&ctx, &rid, &name, &previous, &value).await?;
					}
				}
			}
			Ok(next)
		}
		StorageInstr::RewriteInlinePointers => {
			let (l, r) = stored_endpoints(row)?;
			let rid = record_id(row)?;
			let value = inline_payload(row, &ctx)?.kv_encode_value().map_err(ControlFlow::Err)?;
			let txn = ctx.txn();
			let ltr = pointer_key(&ctx, &l, Dir::Out, &rid, &r)?;
			let rtl = pointer_key(&ctx, &r, Dir::In, &rid, &l)?;
			futures::try_join!(txn.set(ltr.into(), value.clone()), txn.set(rtl.into(), value))
				.map_err(ControlFlow::Err)?;
			Ok(next)
		}
	}
}

/// The record the index stages maintain entries for, which every order naming
/// them has given an id.
fn indexed_record(row: &WriteRow) -> FlowResult<&crate::val::RecordId> {
	row.id.as_ref().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"Expected a record id to be present",
		)))
	})
}

/// Whether the session asked for index maintenance on every record it
/// writes, rather than only on the ones it changed.
fn index_forced(ctx: &WriteCtx) -> bool {
	matches!(ctx.exec().options(), Some(opt) if matches!(opt.force, crate::dbs::Force::All))
}

/// A key the VM's key codec encoded, for the transaction's typed reads and
/// writes of the value `V` stored at it.
struct Encoded<V> {
	bytes: Vec<u8>,
	value: std::marker::PhantomData<V>,
}

impl<V> Encoded<V> {
	fn new(bytes: Vec<u8>) -> Self {
		Self {
			bytes,
			value: std::marker::PhantomData,
		}
	}
}

impl<V> std::fmt::Debug for Encoded<V> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_tuple("Encoded").field(&self.bytes).finish()
	}
}

impl<V: KVValue<KeyContext = ()>> surrealdb_kvs::key::KVKey for Encoded<V> {
	type Value = V;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		buffer.extend_from_slice(&self.bytes);
		Ok(())
	}

	fn value_context(&self) -> <V as KVValue>::KeyContext {}
}

/// Take [`StorageInstr::IndexEntries`] or [`StorageInstr::IndexNative`] for
/// the record in flight: the values the index's columns answer for each image
/// that is not nullish, each image run through the columns' host pipeline in
/// turn, and, for a `COUNT` index with a guard, whether each image satisfies
/// the guard. The entries are moved when the values differ, or when the
/// guard's answer does.
async fn index_step(
	program: &Program,
	lowered: &LoweredIndexes,
	position: usize,
	state: &mut State,
) -> FlowResult<()> {
	let (Some(ix), Some(plan)) = (lowered.defs.get(position), lowered.per_index.get(position))
	else {
		return Err(internal("an index maintained without a lowered plan"));
	};
	let (columns, guard, native) = match plan {
		IndexPlan::Entries {
			columns,
		} => (*columns, None, false),
		IndexPlan::Native {
			kind,
			columns,
			guard,
		} => {
			if NativeIndexKind::of(&ix.index) != Some(*kind) {
				return Err(internal("a native index of a kind other than the one lowered"));
			}
			(*columns, *guard, true)
		}
		IndexPlan::Removed => return Err(internal("an index marked for removal maintained")),
	};
	#[cfg(test)]
	INDEX_STEPS.with(|steps| steps.set(steps.get() + 1));
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	let rid = indexed_record(row)?.clone();
	let initial = row.initial.clone();
	let current = row.current.clone();
	let exec = ctx.exec();
	let mut values = [None, None];
	for (image, values) in [&initial, &current].into_iter().zip(values.iter_mut()) {
		if !image.is_nullish() {
			let res = super::run_image(program, columns, image.clone(), None, state, exec).await?;
			let Value::Array(vals) = res else {
				return Err(internal("an index's columns that answered no list"));
			};
			*values = Some(vals.0);
		}
	}
	let [o, n] = values;
	let mut matches = None;
	if let Some(guard) = guard {
		let mut answers = [false, false];
		for (image, answer) in [&initial, &current].into_iter().zip(answers.iter_mut()) {
			// The image names itself: a self-dereference in the guard answers
			// from the image under test, and an image with no id binds nothing.
			let computing = if let Value::Object(object) = image {
				if let Some(Value::RecordId(rid)) = object.get("id") {
					Some(Arc::new(crate::exec::eval_context::ComputingRecord {
						rid: rid.clone(),
						stored: Some(image.clone()),
					}))
				} else {
					None
				}
			} else {
				None
			};
			let outer = std::mem::replace(&mut state.computing, computing);
			let verdict = super::run_image(program, guard, image.clone(), None, state, exec).await;
			state.computing = outer;
			*answer = verdict?.is_truthy();
		}
		matches = Some((answers[0], answers[1]));
	}
	let guard_changed = matches!(matches, Some((o, n)) if o != n);
	if o == n && !guard_changed {
		return Ok(());
	}
	let deferred = if native {
		crate::exec::operators::write::stages::index::one_index(&ctx, ix, o, n, &rid, matches)
			.await?
	} else {
		move_entries(&ctx, ix, o, n, &rid).await?
	};
	if deferred {
		let write = state
			.write
			.as_deref_mut()
			.ok_or_else(|| internal("a write body with no write state"))?;
		write.in_flight()?.0.doc_id_removal_deferred = true;
	}
	Ok(())
}

/// Move B-tree or `UNIQUE` index `ix`'s entries for record `rid` from values
/// `o` to values `n`, as [`StorageInstr::IndexEntries`] states, and answer
/// whether the record's doc-id release is deferred to an in-flight build.
///
/// Each value list is taken as its combinations: every array value that is
/// not a flattened column's stands for each of its elements in turn. A
/// combination holding a `NONE`, or a `NULL` on an index without
/// `NULLS NOT DISTINCT`, is stored under the non-unique key, which names the
/// record, so any number of them coexist on a `UNIQUE` index.
/// An entry is deleted only while it names this record, and an entry of the
/// index's doc-id format may be stored without its doc-id.
async fn move_entries(
	ctx: &WriteCtx,
	ix: &IndexDefinition,
	o: Option<Vec<Value>>,
	n: Option<Vec<Value>>,
	rid: &crate::val::RecordId,
) -> FlowResult<bool> {
	let frozen = ctx.exec().ctx();
	let (o, n) = match frozen.get_index_builder() {
		Some(builder) => {
			let mutation = IndexMutation {
				old_values: o,
				new_values: n,
				rid,
				count_cond_match: None,
			};
			match builder.consume(&ctx.db, frozen, ix, mutation).await.map_err(ControlFlow::Err)? {
				ConsumeResult::Enqueued => return Ok(ix.uses_shared_doc_ids()),
				ConsumeResult::Ignored(o, n) => (o, n),
				ConsumeResult::Retired => return Ok(false),
			}
		}
		None => (o, n),
	};
	let unique = matches!(ix.index, crate::catalog::Index::Uniq);
	let ns = ctx.db.namespace_id;
	let db = ctx.db.database_id;
	let doc_id = if ix.has_entry_doc_ids() {
		Some(
			TableDocIds::new(ns, db, ix.table_name.clone())
				.resolve_or_assign(frozen, &rid.key)
				.await
				.map_err(ControlFlow::Err)?,
		)
	} else {
		None
	};
	let value = IndexEntryValue {
		rid: rid.clone(),
		doc_id,
		point: None,
	};
	let txn = ctx.txn();
	let keyspace = crate::idx::kernels::btree::IndexKeyspace::of(ns, db, ix);
	let key =
		|values: &[Value]| keyspace.stored_key(unique, values, &rid.key).map_err(ControlFlow::Err);
	if let Some(o) = o {
		for o in Indexable::new(o, ix) {
			delete_entry(&txn, &key(&o)?, &value).await?;
		}
	}
	if let Some(n) = n {
		record_shapes(ctx, ix, &n).await?;
		for n in Indexable::new(n, ix) {
			let key = key(&n)?;
			if !key.unique {
				txn.set_key(&key, &value).await.map_err(ControlFlow::Err)?;
				continue;
			}
			// An entry the build of an index this transaction defined wrote is
			// no entry of the transaction's snapshot, so a write over it would
			// pass the check below and fail the commit instead.
			let built = if txn.defined_unseen_indexes() {
				written_by_unseen_build(&txn, ctx, ix, &key).await?
			} else {
				None
			};
			if built.is_none() {
				match txn.put_compare_key(&key, &value, None).await {
					Ok(()) => continue,
					// The key is taken: report the record that holds it.
					Err(e) if condition_not_met(&e) => {}
					Err(e) => return Err(ControlFlow::Err(e)),
				}
			}
			let held: IndexEntryValue = match built {
				Some(held) => held,
				None => {
					txn.get_key(&key, None).await.map_err(ControlFlow::Err)?.ok_or_else(|| {
						ControlFlow::Err(
							crate::kvs::DatastoreError::CorruptedIndex(
								"a unique entry refused a write and is absent",
							)
							.into(),
						)
					})?
				}
			};
			if held.rid == *rid {
				continue;
			}
			let mut n = n;
			return Err(ControlFlow::Err(anyhow::Error::new(crate::idx::Error::IndexExists {
				record: held.rid,
				index: ix.name.to_string(),
				value: match n.0.len() {
					1 => n.0.remove(0).to_sql(),
					_ => n.to_sql(),
				},
			})));
		}
	}
	Ok(false)
}

/// The entry at `key` of `UNIQUE` index `ix`, when `ix` is an index this
/// transaction defined whose build wrote it. Asked only of a transaction that
/// defined an index it cannot read, so every other write pays nothing for it.
///
/// The build commits in transactions of its own, past this transaction's
/// snapshot, so this transaction's reads of the key do not show it. The entry is
/// read through the view opened once the build had returned, which nothing but
/// the build writes to while this transaction is open, and which a record this
/// transaction created never reaches: those entries are its own, and its own
/// reads show them.
///
/// The future is built here and returned boxed, so that the caller, on the
/// path every write takes, holds a pointer to it and not its size.
#[inline(never)]
fn written_by_unseen_build<'a>(
	txn: &'a crate::kvs::Transaction,
	ctx: &'a WriteCtx,
	ix: &'a IndexDefinition,
	key: &'a crate::idx::kernels::btree::StoredKey,
) -> Pin<Box<impl Future<Output = FlowResult<Option<IndexEntryValue>>> + 'a>> {
	Box::pin(read_written_by_unseen_build(txn, ctx, ix, key))
}

async fn read_written_by_unseen_build(
	txn: &crate::kvs::Transaction,
	ctx: &WriteCtx,
	ix: &IndexDefinition,
	key: &crate::idx::kernels::btree::StoredKey,
) -> FlowResult<Option<IndexEntryValue>> {
	use surrealdb_datastore::unseen_indexes::Unseen;
	let view = txn
		.with_unseen_index(
			ctx.db.namespace_id,
			ctx.db.database_id,
			&ix.table_name,
			ix.index_id,
			|unseen| match unseen {
				Unseen::Built {
					view,
					..
				} => view.clone(),
				Unseen::Queued => None,
			},
		)
		.flatten();
	match view {
		Some(view) => view.get_key(key, None).await.map_err(ControlFlow::Err),
		None => Ok(None),
	}
}

/// Record the shapes of the values `n` B-tree index `ix` stores beyond those
/// its columns are expected to ([`recorded_shapes`]), in the transaction that
/// writes their entries
/// ([`surrealdb_datastore::index_shapes`]). The shapes the write context read
/// answer a write that adds none without a read.
async fn record_shapes(ctx: &WriteCtx, ix: &IndexDefinition, n: &[Value]) -> FlowResult<()> {
	let known = ctx.index_shapes.of(ix.index_id);
	if known.is_some_and(|known| known.records_all(recorded_shapes(n, ix))) {
		return Ok(());
	}
	// The context's shapes were read before this statement. The transaction's
	// shapes are the current set, and answer every write after the first of a
	// shape without a write of their own.
	let txn = ctx.txn();
	let current = txn
		.index_shapes(ctx.db.namespace_id, ctx.db.database_id, &ix.table_name, ix.index_id)
		.await
		.map_err(ControlFlow::Err)?;
	if current.records_all(recorded_shapes(n, ix)) {
		return Ok(());
	}
	let shapes: Vec<_> = recorded_shapes(n, ix).collect();
	txn.record_index_shapes(
		ctx.db.namespace_id,
		ctx.db.database_id,
		&ix.table_name,
		ix.index_id,
		&shapes,
		surrealdb_datastore::index_shapes::ShapeRecorder::Write,
	)
	.await
	.map_err(ControlFlow::Err)
}

/// Whether `e` is a conditional write refused because the key did not hold
/// the expected value.
fn condition_not_met(e: &anyhow::Error) -> bool {
	matches!(storage_error(e), Some(surrealdb_kvs::Error::TransactionConditionNotMet))
}

/// Delete the index entry at `key` while it names the record `expected`
/// names, leaving an absent or foreign entry alone; an entry of the index's
/// doc-id format may be stored without its doc-id, and is deleted too.
///
/// An entry left alone is read under a commit-time conflict registration.
/// The delete is judged on this transaction's snapshot, and an entry for the
/// record's old value can be written after it — by an index build replaying
/// the write that produced that value. A miss that registered nothing would
/// commit beside that entry and leave the record indexed under a value it no
/// longer holds; registered, the commit fails as a retryable conflict.
async fn delete_entry(
	txn: &crate::kvs::Transaction,
	key: &crate::idx::kernels::btree::StoredKey,
	expected: &IndexEntryValue,
) -> FlowResult<()> {
	match txn.del_compare_key(key, Some(expected)).await {
		Err(e) if condition_not_met(&e) => {
			if expected.doc_id.is_some() {
				let bare = IndexEntryValue {
					rid: expected.rid.clone(),
					doc_id: None,
					point: None,
				};
				match txn.del_compare_key(key, Some(&bare)).await {
					Err(e) if condition_not_met(&e) => {}
					other => return other.map_err(ControlFlow::Err),
				}
			}
			txn.getu_key_raw(key).await.map(drop).map_err(ControlFlow::Err)
		}
		other => other.map_err(ControlFlow::Err),
	}
}

/// The key a record is stored at.
pub(super) fn record_key(
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	rid: &crate::val::RecordId,
) -> FlowResult<Vec<u8>> {
	table_record_key(ns, db, &rid.table, &rid.key)
}

/// Remove the adjacency keys edge `rid`, joining `l` to `r` and stored under
/// adjacency layout `variant`, left on its endpoints, as
/// [`StorageInstr::PurgePointers`] states.
async fn purge_pointers(
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	rid: &crate::val::RecordId,
	l: &crate::val::RecordId,
	r: &crate::val::RecordId,
	variant: u16,
) -> FlowResult<()> {
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	let etl = graph_key(ctx, rid, Dir::In, l)?;
	let etr = graph_key(ctx, rid, Dir::Out, r)?;
	if variant == 1 {
		let ltr = graph_key(ctx, l, Dir::Out, rid)?;
		let rtl = graph_key(ctx, r, Dir::In, rid)?;
		futures::try_join!(
			txn.del(ltr.into()),
			txn.del(etl.into()),
			txn.del(etr.into()),
			txn.del(rtl.into())
		)
		.map_err(ControlFlow::Err)?;
		return evict_edge(&txn, ns, db, l, r, rid).await;
	}
	let ltr = pointer_key(ctx, l, Dir::Out, rid, r)?;
	let rtl = pointer_key(ctx, r, Dir::In, rid, l)?;
	// A vertex table with folded adjacency may hold the edge inside a packed
	// block, which no key deletion reaches: a tombstone over the pointer key
	// is what the merged reader subtracts and the next fold consumes, and that
	// fold is queued at commit.
	let (l_folded, r_folded) = futures::try_join!(
		crate::idx::adjacency::vertex_table_folded(&txn, ns, db, &l.table),
		crate::idx::adjacency::vertex_table_folded(&txn, ns, db, &r.table)
	)
	.map_err(ControlFlow::Err)?;
	for (folded, vertex, dir) in [(l_folded, l, Dir::Out), (r_folded, r, Dir::In)] {
		if folded {
			txn.buffer_graph_fold_trigger(
				GraphFoldScope {
					ns,
					db,
					tb: vertex.table.clone(),
					id: vertex.key.clone(),
					dir,
				},
				ctx.exec().root().ctx.node_id(),
			);
		}
	}
	let tombstone = AdjacencyValue::tombstone().kv_encode_value().map_err(ControlFlow::Err)?;
	let clear = |key: Vec<u8>, folded: bool| {
		let txn = &txn;
		let tombstone = tombstone.clone();
		async move {
			if folded {
				txn.set(key.into(), tombstone).await
			} else {
				txn.del(key.into()).await
			}
		}
	};
	futures::try_join!(clear(ltr, l_folded), clear(rtl, r_folded)).map_err(ControlFlow::Err)?;
	// A lightweight edge never wrote edge-side keys.
	if !table_is_lightweight(&ctx.table) {
		futures::try_join!(txn.del(etl.into()), txn.del(etr.into())).map_err(ControlFlow::Err)?;
	}
	evict_edge(&txn, ns, db, l, r, rid).await
}

/// Drop edge `rid` from each endpoint's inline adjacency cache.
async fn evict_edge(
	txn: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	l: &crate::val::RecordId,
	r: &crate::val::RecordId,
	rid: &crate::val::RecordId,
) -> FlowResult<()> {
	crate::idx::inline_cache::record_edge_delete(txn, ns, db, l, Dir::Out, rid)
		.await
		.map_err(ControlFlow::Err)?;
	crate::idx::inline_cache::record_edge_delete(txn, ns, db, r, Dir::In, rid)
		.await
		.map_err(ControlFlow::Err)
}

/// Bring the reference keys of one field place from `previous`'s links to
/// `current`'s: the links dropped are deleted and the links gained written,
/// each mirrored in the referenced record's inline cache after its key.
async fn sync_reference(
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	rid: &crate::val::RecordId,
	field: &str,
	previous: &Value,
	current: &Value,
) -> FlowResult<()> {
	if previous == current {
		return Ok(());
	}
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let previous = referenced_ids(previous);
	let current = referenced_ids(current);
	let txn = ctx.txn();
	for (target, keep) in previous
		.difference(&current)
		.map(|target| (*target, false))
		.chain(current.difference(&previous).map(|target| (*target, true)))
	{
		let key = codec::key::encode_fields(
			"ReferenceKey",
			&[
				("ns", KeyField::Int(ns.0 as i64)),
				("db", KeyField::Int(db.0 as i64)),
				("tb", KeyField::Str(target.table.as_str())),
				("id", KeyField::Id(&target.key)),
				("foreign_table", KeyField::Str(rid.table.as_str())),
				("foreign_field", KeyField::Str(field)),
				("foreign_key", KeyField::Id(&rid.key)),
			],
		)
		.map_err(ControlFlow::Err)?;
		if keep {
			txn.set(key.into(), Vec::new()).await.map_err(ControlFlow::Err)?;
			crate::idx::inline_cache::record_ref_write(
				&txn, ns, db, target, &rid.table, field, &rid.key,
			)
			.await
			.map_err(ControlFlow::Err)?;
		} else {
			txn.del(key.into()).await.map_err(ControlFlow::Err)?;
			crate::idx::inline_cache::record_ref_delete(
				&txn, ns, db, target, &rid.table, field, &rid.key,
			)
			.await
			.map_err(ControlFlow::Err)?;
		}
	}
	Ok(())
}

/// The records a field's value links to: a record, or each record a
/// collection holds.
fn referenced_ids(value: &Value) -> std::collections::HashSet<&crate::val::RecordId> {
	if let Some(rid) = value.as_record() {
		return std::collections::HashSet::from([rid]);
	}
	if let Value::Array(array) = value {
		return array.iter().filter_map(|v| v.as_record()).collect();
	}
	if let Value::Set(set) = value {
		return set.iter().filter_map(|v| v.as_record()).collect();
	}
	std::collections::HashSet::new()
}

/// The record's id: every order derives or carries one before a storage
/// instruction runs, so a row without one is a broken order.
fn record_id(row: &WriteRow) -> FlowResult<crate::val::RecordId> {
	row.id.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"Expected a document id to be present",
		)))
	})
}

/// The endpoints of an edge the statement relates.
fn related(row: &WriteRow) -> FlowResult<(crate::val::RecordId, crate::val::RecordId)> {
	match &row.extras {
		WriteExtras::Relate(l, r, _) => Ok((l.clone(), r.clone())),
		WriteExtras::Normal | WriteExtras::Insert(_) => {
			Err(internal("an edge instruction for a record the statement does not relate"))
		}
	}
}

/// The endpoints the stored edge was written under, read off the before-image.
fn stored_endpoints(row: &WriteRow) -> FlowResult<(crate::val::RecordId, crate::val::RecordId)> {
	Ok((endpoint(&row.initial, &IN, "in")?, endpoint(&row.initial, &OUT, "out")?))
}

/// The pointer values' payload: the table's `INLINE` field values read off the
/// post-image, or the empty live marker where the table declares none.
fn inline_payload(
	row: &WriteRow,
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
) -> FlowResult<AdjacencyValue> {
	let inline = inline_fields(&ctx.fields);
	if inline.is_empty() {
		return Ok(AdjacencyValue::live());
	}
	let section = encode_inline_section(
		&row.current,
		&inline,
		ctx.table.graph_inline_gen,
		ctx.exec().root().ctx.config.idx.graph_inline_props_cap,
	)
	.map_err(ControlFlow::Err)?;
	Ok(AdjacencyValue {
		tombstone: false,
		sections: vec![section],
	})
}

/// An edge-side or original-layout adjacency key: stored on `vertex` in `dir`,
/// naming `foreign`.
fn graph_key(
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	vertex: &crate::val::RecordId,
	dir: Dir,
	foreign: &crate::val::RecordId,
) -> FlowResult<Vec<u8>> {
	codec::key::encode_fields("GraphKey", &graph_fields(ctx, vertex, dir, foreign))
		.map_err(ControlFlow::Err)
}

/// A vertex-side pointer key: stored on `vertex` in `dir`, naming `edge`, with
/// `target` embedded so a traversal resolves the far vertex without reading
/// the edge record.
fn pointer_key(
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	vertex: &crate::val::RecordId,
	dir: Dir,
	edge: &crate::val::RecordId,
	target: &crate::val::RecordId,
) -> FlowResult<Vec<u8>> {
	let [ns, db, tb, id, dir, foreign_table, foreign_key] = graph_fields(ctx, vertex, dir, edge);
	codec::key::encode_fields(
		"GraphPointerKey",
		&[
			ns,
			db,
			tb,
			id,
			dir,
			foreign_table,
			foreign_key,
			("target_table", KeyField::Str(target.table.as_str())),
			("target_key", KeyField::Id(&target.key)),
		],
	)
	.map_err(ControlFlow::Err)
}

/// The fields of an adjacency key, as the key codec takes them.
fn graph_fields<'a>(
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	vertex: &'a crate::val::RecordId,
	dir: Dir,
	foreign: &'a crate::val::RecordId,
) -> [(&'static str, KeyField<'a>); 7] {
	[
		("ns", KeyField::Int(ctx.ns.namespace_id.0 as i64)),
		("db", KeyField::Int(ctx.db.database_id.0 as i64)),
		("tb", KeyField::Str(vertex.table.as_str())),
		("id", KeyField::Id(&vertex.key)),
		("dir", KeyField::Dir(dir)),
		("foreign_table", KeyField::Str(foreign.table.as_str())),
		("foreign_key", KeyField::Id(&foreign.key)),
	]
}

/// The format the record `rid` of the statement's table is written in, with the layouts a packed
/// format reads its descriptor from.
///
/// The statement's slot answers for a record of the statement's own table
/// ([`RecordFormatSlot::resolve`](crate::exec::operators::write::ctx::RecordFormatSlot::resolve));
/// a record of another table never reads or fills it, and is resolved by the transaction's record
/// writer.
async fn record_format<'c>(
	ctx: &'c crate::exec::operators::write::ctx::WriteCtx,
	txn: &crate::kvs::Transaction,
	rid: &crate::val::RecordId,
) -> FlowResult<ResolvedFormat<'c>> {
	let (ns, db) = (ctx.ns.namespace_id, ctx.db.database_id);
	if rid.table == ctx.table.name {
		ctx.format.resolve(txn, ns, db, &rid.table).await
	} else {
		txn.record_writer(ns, db, &rid.table).await.map(ResolvedFormat::Writer)
	}
	.map_err(ControlFlow::Err)
}

/// Encodes the post-image of the record in flight, for the stage that stores it, from the part of a
/// write that runs apart from its apply.
///
/// Reads the format only where the table's slot already holds it at the transaction's current
/// layout epoch, so nothing is resolved, committed or forfeited here. A record that names another
/// table than the statement's, a statement that imports, a slot that holds no format and an image
/// that does not encode leave the record as it is: the storing stage encodes it, and raises what
/// encoding raises, where the serial run raises it.
#[cfg(not(target_family = "wasm"))]
pub(in crate::exec::vm) fn encode_ahead(row: &mut WriteRow, ctx: &WriteCtx) {
	let Some(rid) = row.id.as_ref() else {
		return;
	};
	if rid.table != ctx.table.name || ctx.exec().options().is_some_and(|options| options.import) {
		return;
	}
	let epoch = ctx.txn().layout_epoch();
	let Some(hit) = ctx.format.get(epoch) else {
		return;
	};
	let generation = crate::catalog::written_generation(hit.format, row.metadata.as_ref());
	let Ok(bytes) = crate::catalog::encode_record_with(
		hit.format,
		hit.layouts,
		row.metadata.as_ref(),
		&row.current,
		Some(rid),
	) else {
		return;
	};
	row.encoded = Some(crate::exec::operators::write::row::EncodedImage {
		bytes,
		epoch,
		generation,
	});
}

/// Write the record in flight at its key, as [`StorageInstr::Store`] states.
async fn store(
	row: &mut WriteRow,
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	create: bool,
	cache: bool,
) -> FlowResult<()> {
	let rid = row.id.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"Expected a document id to be present",
		)))
	})?;
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let key = record_key(ns, db, &rid)?;
	let txn = ctx.txn();
	let resolved = record_format(ctx, &txn, &rid).await?;
	// The generation the encoded bytes carry, which the transaction's cache holds the row under.
	let generation = crate::catalog::written_generation(resolved.format(), row.metadata.as_ref());
	// An `OPTION IMPORT` row skips the schema stage, so it may not hold what the table's
	// conformance record states: the record is forfeited before such a row lands, and a
	// verification under way starts over, since the row is one it could not judge. A claim covers
	// only the rows stamped with a generation, so a row stamped 0 forfeits nothing. A row of the
	// statement's own table skips the call while the transaction's layout epoch is the one read
	// after the last call, which a rollback of the forfeit moves.
	if generation.is_some() && ctx.exec().options().is_some_and(|options| options.import) {
		let own = rid.table == ctx.table.name;
		if !own || ctx.conformance_forfeit.load(Ordering::Acquire) != txn.layout_epoch() {
			txn.forfeit_conformance(ns, db, &rid.table).await.map_err(ControlFlow::Err)?;
			txn.restart_tb_verification(ns, db, &rid.table).await.map_err(ControlFlow::Err)?;
			if own {
				ctx.conformance_forfeit.store(txn.layout_epoch(), Ordering::Release);
			}
		}
	}
	// The record's encoding refuses a value nested past the cap: once stored,
	// every later read of it would recurse past what a thread's stack holds. It packs the row
	// when the table's layout is packed and the row fits it, and checks the layouts against the
	// row's table.
	let ahead = row
		.encoded
		.take()
		.filter(|encoded| encoded.epoch == txn.layout_epoch() && encoded.generation == generation);
	let bytes = match ahead {
		Some(encoded) => {
			// The bytes were made from the image the stages left, which no stage since has changed.
			debug_assert_eq!(
				Some(&encoded.bytes),
				crate::catalog::encode_record_with(
					resolved.format(),
					resolved.layouts(),
					row.metadata.as_ref(),
					&row.current,
					Some(&rid),
				)
				.ok()
				.as_ref(),
				"a record encoded ahead of its store differs from its image"
			);
			#[cfg(test)]
			crate::exec::vm::note_encoded_ahead();
			encoded.bytes
		}
		None => crate::catalog::encode_record_with(
			resolved.format(),
			resolved.layouts(),
			row.metadata.as_ref(),
			&row.current,
			Some(&rid),
		)
		.map_err(ControlFlow::Err)?,
	};
	if create {
		// A record created while a write reads its table is noted, so that
		// write's read passes over it (`kvs::creations`).
		let noted = txn.creations_watched(ns, db, &rid.table).then(|| key.clone());
		// The check that the key is absent is held for one check of many keys
		// (`surrealdb_datastore::absence`), made by the statement's end at the latest and before
		// anything reads the key. A held create's failure is raised by whichever operation
		// flushes it, and the statement reports it as that create's duplicate.
		let put = txn.put_held(key.into(), bytes).await;
		put.map_err(|err| match storage_error(&err) {
			Some(crate::kvs::Error::TransactionKeyAlreadyExists) => {
				ControlFlow::Err(anyhow::Error::new(DocError::RecordExists {
					record: rid.clone(),
				}))
			}
			Some(crate::kvs::Error::DeferredKeyExists {
				key,
			}) => ControlFlow::Err(crate::dbs::executor::held_create_exists(key)),
			_ => ControlFlow::Err(err),
		})?;
		if let Some(key) = noted {
			txn.note_creation(ns, db, &rid.table, key);
		}
		// The transaction holds the record it just put, as a record put
		// through its record API is held, once its absence in storage has
		// been checked; while the check is held, a read goes through the
		// transactor so the check runs first. A create-only put that is not
		// kept (`cache`) finds no record at the key, so the cache holds none
		// to drop.
		if txn.holds_creates() {
			txn.forget_record(ns, db, &rid.table, &rid.key);
		} else if cache {
			txn.cache_record(
				ns,
				db,
				&rid.table,
				&rid.key,
				Arc::new(Record {
					metadata: row.metadata.clone(),
					data: row.current.clone(),
				}),
				generation,
			);
		}
	} else {
		txn.set(key.into(), bytes).await.map_err(ControlFlow::Err)?;
		txn.forget_record(ns, db, &rid.table, &rid.key);
	}
	row.mutated = true;
	Ok(())
}

/// Note that the record of `table` under `key` was created, for a write
/// reading `table` to pass over it ([`crate::kvs::creations`]): called by a
/// writer that stores a record other than through [`store`], once it is
/// written. Nothing is encoded when no watch is open on `table`.
pub(in crate::exec::vm) fn note_created(
	txn: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &crate::val::TableName,
	key: &crate::val::RecordIdKey,
) -> FlowResult<()> {
	if txn.creations_watched(ns, db, table) {
		txn.note_creation(ns, db, table, table_record_key(ns, db, table, key)?);
	}
	Ok(())
}

/// The key the record of `table` under `key` is stored at.
fn table_record_key(
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &crate::val::TableName,
	key: &crate::val::RecordIdKey,
) -> FlowResult<Vec<u8>> {
	codec::key::encode_fields(
		"RecordKey",
		&[
			("ns", KeyField::Int(ns.0 as i64)),
			("db", KeyField::Int(db.0 as i64)),
			("tb", KeyField::Str(table.as_str())),
			("id", KeyField::Id(key)),
		],
	)
	.map_err(ControlFlow::Err)
}
