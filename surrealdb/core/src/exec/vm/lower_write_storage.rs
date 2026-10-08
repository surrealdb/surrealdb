//! The storage stages and structural checks lowered to instructions:
//! CheckRecordExists, CheckTableNotView, CheckTableNotLightweight,
//! CheckTableType, ClearRecordData, StoreRecordData, StoreEdgesData,
//! StoreInlineAdjacencyData, StoreIndexData and ReclaimDocId.
//!
//! Each lowers whatever the statement names: what it decides from the table
//! is read from the table the record's write context resolves, so no fact of
//! the catalog is assumed when the statement is planned. The indexes are the
//! exception: their column expressions and `COUNT` guards are lowered from
//! the definitions the catalog holds when the statement is planned, and a
//! record whose table holds other definitions by the time it is written fails
//! the statement.

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered};
use crate::catalog::IndexDefinition;
use crate::exec::operators::write::stage::StageId;
use crate::exec::vm::program::{FrameMode, Instr, Label, ROW, WriteInstr};
use crate::exec::vm::write::{
	IndexPlan, LoweredIndexes, NativeIndexKind, StorageFact, StorageInstr,
};

impl Lowering<'_, '_> {
	/// Emit stage `id` as instructions, recording in `skips` the jumps taken
	/// when it abandons the record.
	pub(super) async fn storage_stage(
		&mut self,
		id: StageId,
		skips: &mut Vec<Label>,
	) -> Lowered<()> {
		let storage = |instr| Instr::Write(WriteInstr::Storage(instr));
		match id {
			// A record the statement named that storage does not hold is
			// skipped, not failed.
			StageId::CheckRecordExists => {
				skips.push(self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::Absent,
					when: true,
					target: 0,
				})));
			}
			StageId::CheckTableNotView => {
				self.emit(storage(StorageInstr::RefuseView));
			}
			StageId::CheckTableNotLightweight(reason) => {
				self.emit(storage(StorageInstr::RefuseLightweight {
					reason,
				}));
			}
			StageId::CheckTableType(subject) => {
				self.emit(storage(StorageInstr::RefuseTableType {
					subject,
				}));
			}
			StageId::ClearRecordData => {
				self.emit(storage(StorageInstr::Clear));
			}
			// Nothing is written for a record no mutation phase changed, a
			// record of a `DROP` table, or an edge of a lightweight relation,
			// whose whole state is its adjacency keys. A record written for the
			// first time in its iteration is written create-only, so a caller
			// holding `create` alone cannot overwrite one it may not update.
			StageId::StoreRecordData => {
				let mut unwritten = Vec::new();
				for fact in [
					StorageFact::Unmodified,
					StorageFact::TableDropped,
					StorageFact::TableLightweight,
				] {
					unwritten.push(self.emit(storage(StorageInstr::JumpIf {
						fact,
						when: true,
						target: 0,
					})));
				}
				let overwrite = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::IterationInitial,
					when: false,
					target: 0,
				}));
				self.emit(storage(StorageInstr::Store {
					create: true,
				}));
				unwritten.push(self.emit(Instr::Jump {
					target: 0,
				}));
				let here = self.here();
				self.patch(overwrite, here);
				self.emit(storage(StorageInstr::Store {
					create: false,
				}));
				let end = self.here();
				self.patch_all(unwritten, end);
			}
			// An edge the statement relates writes its adjacency keys. An
			// enforced relation admits it only between endpoints that exist, and
			// no edge may hang off a lightweight relation's edge. A lightweight
			// relation writes its two pointer keys alone; a stored edge of the
			// original layout first loses the pointer keys that layout wrote.
			StageId::StoreEdgesData => {
				let dropped = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::TableDropped,
					when: true,
					target: 0,
				}));
				let unrelated = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::Related,
					when: false,
					target: 0,
				}));
				let unwritten = vec![dropped, unrelated];
				let unenforced = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::EnforcedRelation,
					when: false,
					target: 0,
				}));
				self.emit(storage(StorageInstr::RequireEndpoints));
				let here = self.here();
				self.patch(unenforced, here);
				self.emit(storage(StorageInstr::RefuseLightweightEndpoints));
				let lightweight = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::TableLightweight,
					when: true,
					target: 0,
				}));
				let current = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::LegacyLayout,
					when: false,
					target: 0,
				}));
				self.emit(storage(StorageInstr::DeleteLegacyPointers));
				let here = self.here();
				self.patch(current, here);
				self.emit(storage(StorageInstr::WriteEdgeKeys {
					lightweight: false,
				}));
				let written = self.emit(Instr::Jump {
					target: 0,
				});
				let here = self.here();
				self.patch(lightweight, here);
				self.emit(storage(StorageInstr::WriteEdgeKeys {
					lightweight: true,
				}));
				let here = self.here();
				self.patch(written, here);
				self.emit(storage(StorageInstr::CacheEdge));
				self.emit(storage(StorageInstr::PinEndpoints));
				let end = self.here();
				self.patch_all(unwritten, end);
			}
			// A stored edge of the current layout has its pointer values
			// restamped with the table's inline payload on every write but its
			// relate, and may not move its endpoints doing so.
			StageId::StoreInlineAdjacencyData => {
				let mut unwritten = Vec::new();
				for (fact, when) in [
					(StorageFact::Related, true),
					(StorageFact::CurrentLayout, false),
					(StorageFact::TableDropped, true),
					(StorageFact::TableLightweight, true),
					(StorageFact::NoInlineFields, true),
				] {
					unwritten.push(self.emit(storage(StorageInstr::JumpIf {
						fact,
						when,
						target: 0,
					})));
				}
				self.emit(storage(StorageInstr::RequireUnmovedEndpoints));
				self.emit(storage(StorageInstr::RewriteInlinePointers));
				let end = self.here();
				self.patch_all(unwritten, end);
			}
			// The reference keys the table's `REFERENCE` fields declare: kept in
			// step with the record on a write, and released on a delete.
			StageId::ProcessTableReferences | StageId::CleanupTableReferences => {
				let fields = crate::exec::vm::host::out_of_line(|| self.reference_fields()).await?;
				if !fields.is_empty() {
					self.emit(storage(StorageInstr::SyncReferences {
						fields,
						release: id == StageId::CleanupTableReferences,
					}));
				}
			}
			// A deleted record's storage, taken back out: the record and its own
			// cache keys, an edge's adjacency keys on its endpoints, and the
			// cascades over the records joined to it or referencing it. A record
			// no mutation phase changed is left alone.
			StageId::PurgeRecordData => {
				let unchanged = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::Unmodified,
					when: true,
					target: 0,
				}));
				self.emit(storage(StorageInstr::DeleteRecord));
				let vertex = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::WasEdge,
					when: false,
					target: 0,
				}));
				self.emit(storage(StorageInstr::PurgePointers));
				let here = self.here();
				self.patch(vertex, here);
				crate::exec::vm::host::out_of_line(|| self.cascades()).await?;
				let end = self.here();
				self.patch(unchanged, end);
			}
			// Every index of the table brought in line with the record's two
			// images, one instruction per index in the catalog's order: a
			// B-tree or `UNIQUE` index's entries from its columns, lowered here
			// as a host pipeline over an image, and any other kind through its
			// own implementation from the same columns and, for a `COUNT` index,
			// its guard. A record no mutation phase changed is left alone unless
			// the session forces index maintenance, and a record of a `DROP`
			// table has no entries. Every row reaches the stage with no doc-id
			// release deferred, which is what a skipped record reports.
			StageId::StoreIndexData => {
				let lowered = crate::exec::vm::host::out_of_line(|| self.lowered_indexes()).await?;
				// A table with no index keeps no entries for any record of a
				// write that cannot define one.
				if lowered.defs.is_empty() && self.write_catalog_fixed {
					#[cfg(test)]
					crate::exec::vm::speculate::STAGES_LEFT_OUT
						.with(|count| count.set(count.get() + 1));
					return Ok(());
				}
				let mut unmaintained = Vec::new();
				for fact in [StorageFact::Unmaintained, StorageFact::TableDropped] {
					unmaintained.push(self.emit(storage(StorageInstr::JumpIf {
						fact,
						when: true,
						target: 0,
					})));
				}
				self.emit(storage(StorageInstr::CheckIndexes {
					lowered: Arc::clone(&lowered),
				}));
				for (position, plan) in lowered.per_index.iter().enumerate() {
					let position = position as u16;
					match plan {
						IndexPlan::Removed => {}
						IndexPlan::Entries {
							..
						} => {
							self.emit(storage(StorageInstr::IndexEntries {
								lowered: Arc::clone(&lowered),
								position,
							}));
						}
						IndexPlan::Native {
							..
						} => {
							self.emit(storage(StorageInstr::IndexNative {
								lowered: Arc::clone(&lowered),
								position,
							}));
						}
					}
				}
				let end = self.here();
				self.patch_all(unmaintained, end);
			}
			// The record's doc-id released once every index has let go of it:
			// marked for the build's replay when an in-flight build took the
			// record's delete, and otherwise dropped from the table's shared
			// doc-id space — unless the table is being dropped, or nothing on
			// the table consumes that space, as the transaction's settled read of
			// the table's graph marker confirms.
			StageId::ReclaimDocId => {
				let release = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::DocIdDeferred,
					when: false,
					target: 0,
				}));
				self.emit(storage(StorageInstr::MarkDocIdPending));
				let mut done = vec![self.emit(Instr::Jump {
					target: 0,
				})];
				let here = self.here();
				self.patch(release, here);
				done.push(self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::TableDropped,
					when: true,
					target: 0,
				})));
				let consumed = self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::DocIdConsumer,
					when: true,
					target: 0,
				}));
				done.push(self.emit(storage(StorageInstr::JumpIf {
					fact: StorageFact::GraphDocIdsSettled,
					when: false,
					target: 0,
				})));
				let here = self.here();
				self.patch(consumed, here);
				self.emit(storage(StorageInstr::ReleaseDocId));
				let end = self.here();
				self.patch_all(done, end);
			}
			other => {
				return Err(NotCovered::internal(format!(
					"the {} stage is not a storage stage",
					other.name()
				)));
			}
		}
		Ok(())
	}

	/// The indexes of the one table the statement writes to, as its plan
	/// read them, each with the host pipelines that answer the values its
	/// columns hold for an image and, for a `COUNT` index, its guard.
	async fn lowered_indexes(&mut self) -> Lowered<Arc<LoweredIndexes>> {
		let (Some(table), Some(txn)) = (self.write_table.clone(), self.planner.txn().cloned())
		else {
			return Err(NotCovered::internal("indexes of a statement that names no single table"));
		};
		let defs = match self.catalog.written(&txn, &table)? {
			Some((_, written)) => Arc::clone(&written.indexes),
			None => Arc::from(Vec::new()),
		};
		let mut per_index = Vec::with_capacity(defs.len());
		for ix in defs.iter() {
			if ix.prepare_remove {
				per_index.push(IndexPlan::Removed);
				continue;
			}
			let columns = crate::exec::vm::host::out_of_line(|| self.index_columns(ix)).await?;
			per_index.push(match NativeIndexKind::of(&ix.index) {
				None => IndexPlan::Entries {
					columns,
				},
				Some(kind) => IndexPlan::Native {
					kind,
					columns,
					guard: crate::exec::vm::host::out_of_line(|| self.index_guard(ix)).await?,
				},
			});
		}
		Ok(Arc::new(LoweredIndexes::new(defs, per_index)))
	}

	/// A host pipeline answering the values index `ix` holds for the image it
	/// runs over, one per column in the definition's order, each evaluated in
	/// the index-entry frame with the image as the record a self-dereference
	/// answers from.
	async fn index_columns(&mut self, ix: &IndexDefinition) -> Lowered<u16> {
		let mut bodies = Vec::with_capacity(ix.cols.len());
		for col in &ix.cols {
			bodies.push(self.index_body(crate::expr::Expr::Idiom(col.clone()))?);
		}
		let aside = self.set_aside();
		// The image the pipeline runs over is the expression's document root.
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = crate::exec::vm::host::out_of_line(|| self.index_pipeline(&bodies, true)).await;
		self.restore(aside, built)
	}

	/// A host pipeline answering the `WHERE` guard of `COUNT` index `ix` for
	/// the image it runs over, in the index-entry frame; `None` for an index
	/// with no guard.
	async fn index_guard(&mut self, ix: &IndexDefinition) -> Lowered<Option<u16>> {
		let Some(cond) = &ix.count_cond else {
			return Ok(None);
		};
		let body = self.index_body(cond.0.clone())?;
		let aside = self.set_aside();
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = self.index_pipeline(&[body], false).await;
		Ok(Some(self.restore(aside, built)?))
	}

	/// The logical form of a stored expression of an index.
	fn index_body(&self, expr: crate::expr::Expr) -> Lowered<crate::expr::logical::LogicalExpr> {
		let body = crate::expr::logical::lower_expr(expr.into()).map_err(|e| {
			NotCovered::internal(format!("an index expression has no logical form: {e}"))
		})?;
		if crate::exec::logical::unplannable_write_in_expr(&body).is_some() {
			return Err(NotCovered::internal("an index expression that holds a write"));
		}
		Ok(body)
	}

	/// The pipeline body answering `bodies` in the index-entry frame: the list
	/// of their values when `list`, and otherwise the one value.
	async fn index_pipeline(
		&mut self,
		bodies: &[crate::expr::logical::LogicalExpr],
		list: bool,
	) -> Lowered<()> {
		// A guard is judged for an image that does not exist too, which is no
		// object: a field it names is read by the expression that names it,
		// and the record it computes is bound by the host that runs it.
		self.rows_are_objects = list;
		if list {
			self.computed_names.push(Vec::new());
			let names = (self.computed_names.len() - 1) as u32;
			let id = self.field("id");
			self.emit(Instr::BeginComputing {
				id,
				doc: Some(ROW),
				names,
			});
		}
		let frame = self.layer_frame(FrameMode::IndexEntry, &None);
		let outer = self.eval_frame.replace(frame);
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let built = crate::exec::vm::host::out_of_line(|| self.index_values(bodies, list)).await;
		self.eval_frame = outer;
		self.privileged = outer_privileged;
		built?;
		self.may_fail = true;
		self.emit(Instr::EndRow);
		Ok(())
	}

	async fn index_values(
		&mut self,
		bodies: &[crate::expr::logical::LogicalExpr],
		list: bool,
	) -> Lowered<()> {
		if !list {
			let value =
				crate::exec::vm::host::out_of_line(|| self.caught(&bodies[0], true)).await?;
			let dst = self.register()?;
			self.emit(Instr::Load {
				dst,
				src: value,
			});
			self.emit(Instr::Emit {
				src: dst,
			});
			return Ok(());
		}
		let values = self.register()?;
		self.emit(Instr::NewArray {
			dst: values,
		});
		for body in bodies {
			let next_reg = self.next_reg;
			let released = self.released.clone();
			let value = crate::exec::vm::host::out_of_line(|| self.caught(body, true)).await?;
			self.emit(Instr::ArrayPush {
				array: values,
				value,
			});
			self.released = released;
			self.released.extend(next_reg..self.next_reg);
		}
		self.emit(Instr::Emit {
			src: values,
		});
		Ok(())
	}

	/// The `REFERENCE` fields of the one table the statement writes to, as its
	/// plan read them.
	async fn reference_fields(&mut self) -> Lowered<std::sync::Arc<[crate::expr::Idiom]>> {
		let (Some(table), Some(txn)) = (self.write_table.clone(), self.planner.txn().cloned())
		else {
			return Err(NotCovered::internal(
				"references of a statement that names no single table",
			));
		};
		let fields = match self.catalog.table(&txn, &table)? {
			Some(read) => Arc::clone(&read.fields),
			None => Arc::from(Vec::new()),
		};
		Ok(fields.iter().filter(|fd| fd.reference.is_some()).map(|fd| fd.name.clone()).collect())
	}
}
