//! The resources an op reads and writes, keyed footprints included.
//!
//! The footprint of an op is the `footprint` of its transitive effects: the resources of the op
//! and of every op in its regions, with every element key widened to `KeyOf::Any` where it leaves
//! the loop that owns the element. A footprint is therefore stated relative to the innermost loop
//! enclosing the asking op, and [`relative_to`] restates it relative to a named loop.
//!
//! [`overlaps`] answers at statement scope: keys are ignored, because a statement spans every
//! iteration of every loop in it. [`disjoint_across_iterations`] is the one place keys are read.
//! A footprint no analysis can narrow holds `IrResource::Unknown`, which overlaps everything.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::{Strand, TableName};

#[cfg(test)]
use super::cx::default_cx;
use super::{effects, multiplicity, types};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{
	ElemPath, FieldSet, IrFootprint, IrResource, KeyOf, Multiplicity,
};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::statements::{Place, PlacePart, WriteForm};
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopSink, SourceRefShape, WriteStageKind};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::ops::PhysTag;
use crate::exec::logical::footprint::{CatalogKind, Footprint, Resource};

/// The most regions followed on the way from an op up to its enclosing loop. Nesting is finite
/// by construction, so the bound is never met by a well-formed container.
const MAX_ANCESTOR_HOPS: usize = 1024;

/// The footprint of `op` and of every op in its regions, under the default context.
///
/// A footprint is keyed by the element of the loop stream where the key is a function of the
/// element, so two iterations that write different elements do not overlap. An op nothing is
/// known about reads and writes [`IrResource::Unknown`].
#[cfg(test)]
pub(crate) fn of(ir: &Ir, facts: &mut FactCache, op: OpId) -> IrFootprint {
	of_with(ir, facts, &default_cx(ir), op)
}

/// The footprint of `op` and of every op in its regions under `cx`.
///
/// Every element key is stated relative to the innermost loop enclosing `op`. An op that is not
/// in the container reads and writes [`IrResource::Unknown`].
///
/// The resources are those of the transitive effects of `op`. Where an op's own resources are a
/// function of the element of the loop it stands in ([`key_of`]), they carry that key; every
/// resource that leaves a nested loop carries `KeyOf::Any`, except the subject access of a write
/// loop, which is keyed along the element of the loop that encloses it. The keys are recomputed
/// on every call, since they read the stream of the enclosing loop and the provenance of the
/// write's subject, which lie outside the subtree of `op`. A footprint that does not account for
/// every resource of the transitive effects is answered by the transitive effects themselves.
pub(crate) fn of_with(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> IrFootprint {
	let base = effects::of_with(ir, facts, cx, op).footprint;
	if ir.op(op).is_none() {
		return base;
	}
	let keyed = keyed_footprint(ir, facts, cx, op);
	if same_resources(&keyed.reads, &base.reads) && same_resources(&keyed.writes, &base.writes) {
		keyed
	} else {
		base
	}
}

/// [`of_with`], with every key stated relative to the element of `loop_op`.
///
/// Keys of an op that stands in a loop nested in `loop_op`'s body, or outside `loop_op`, are not a
/// function of `loop_op`'s element and widen to `KeyOf::Any`.
pub(crate) fn relative_to(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	op: OpId,
	loop_op: OpId,
) -> IrFootprint {
	let footprint = of_with(ir, facts, cx, op);
	if enclosing_loop(ir, op) == Some(loop_op) {
		footprint
	} else {
		IrFootprint {
			reads: widen(&footprint.reads),
			writes: widen(&footprint.writes),
		}
	}
}

/// The key `op` touches along the element of its innermost enclosing loop.
///
/// - `KeyOf::Elem(ElemPath::Elem)` when the op accesses the record its first operand names, and
///   that operand is the loop element, known to be a record id.
/// - `KeyOf::Elem(ElemPath::Id)` when it accesses the record named by the `id` field of the loop
///   element.
/// - For a write, the key of its subject along the enclosing loop's element: the subject must be
///   that element or its `id`, and pinned to one table by derivation; the key then follows the
///   stream's key path.
/// - For a stage of a write loop that touches only the subject record, the key of the loop's own
///   element along its key path.
///
/// Anything else, including an op outside every loop and an op that does not itself access the
/// record its first operand names, is `KeyOf::Any`.
pub(crate) fn key_of(ir: &Ir, op: OpId) -> KeyOf {
	keyed(ir, op).map_or(KeyOf::Any, |(key, _)| key)
}

/// The key `op` touches along the element of its innermost enclosing loop, with the one table the
/// key names a record of. `None` when the key is `KeyOf::Any` or no single table is known.
fn keyed(ir: &Ir, op: OpId) -> Option<(KeyOf, TableName)> {
	let loop_op = enclosing_loop(ir, op)?;
	let data = ir.op(op)?;
	let tag = data.kind.tag();
	if tag == OpTag::Logical(LogTag::Write) {
		let subject = types::subject_loop(ir, op)?;
		return lifted_key(ir, subject, Some(loop_op));
	}
	if tag == OpTag::Logical(LogTag::WriteStage) {
		return if is_write_sink(ir, loop_op) && is_subject_stage(&data.kind) {
			element_key(ir, loop_op)
		} else {
			None
		};
	}
	if !accesses_first_operand_record(&data.kind) {
		return None;
	}
	let element = multiplicity::loop_element(ir, loop_op)?;
	let first = data.operands.first().copied()?;
	if first == element {
		if !types::is_record(ir, element) {
			return None;
		}
		let table = types::pinned_table(ir, element)?;
		Some((KeyOf::Elem(ElemPath::Elem), table))
	} else if is_id_of(ir, first, element) {
		let table = multiplicity::stream_table(ir, loop_op)?;
		Some((KeyOf::Elem(ElemPath::Id), table))
	} else {
		None
	}
}

/// Whether an op of `kind` itself accesses the record its first operand names: a record source, a
/// dereference, or a read of a field of a record. An op of any other kind, a call or a traversal
/// among them, may touch records its operands do not name.
fn accesses_first_operand_record(kind: &OpKind) -> bool {
	match kind {
		OpKind::Expr(
			ExprOp::Field {
				..
			}
			| ExprOp::Access {
				..
			}
			| ExprOp::Deref {
				..
			}
			| ExprOp::ComputedRead(_)
			| ExprOp::Doc,
		) => true,
		OpKind::Logical(LogOp::Source(attrs)) => {
			matches!(attrs.source_ref, SourceRefShape::RecordId)
		}
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// The key a write loop's subject access carries once it leaves the loop, stated along the
/// element of `scope`, the loop that encloses the write loop, with the table the key names.
///
/// The write loop has one target, pinned to one table by derivation, and that target is the
/// element of `scope` or its `id`. Anything else is `None`: the access names a record that is
/// no function of the element.
fn lifted_key(ir: &Ir, write_loop: OpId, scope: Option<OpId>) -> Option<(KeyOf, TableName)> {
	let scope = scope?;
	let element = multiplicity::loop_element(ir, scope)?;
	let targets = types::loop_targets(ir, write_loop);
	let [target] = targets.as_slice() else {
		return None;
	};
	let table = types::pinned_table(ir, *target)?;
	if *target == element {
		element_key(ir, scope).map(|(key, _)| (key, table))
	} else if is_id_of(ir, *target, element) {
		Some((KeyOf::Elem(ElemPath::Id), table))
	} else {
		None
	}
}

/// The key of the record the element of `loop_op` names, along the stream's key path, with the
/// table of that record: its `id` for a stream of rows, and the element itself for a stream of
/// record ids.
fn element_key(ir: &Ir, loop_op: OpId) -> Option<(KeyOf, TableName)> {
	match multiplicity::key_path(ir, loop_op) {
		ElemPath::Id => {
			let table = multiplicity::stream_table(ir, loop_op)?;
			Some((KeyOf::Elem(ElemPath::Id), table))
		}
		ElemPath::Elem => {
			let element = multiplicity::loop_element(ir, loop_op)?;
			if !types::is_record(ir, element) {
				return None;
			}
			let table = types::pinned_table(ir, element)?;
			Some((KeyOf::Elem(ElemPath::Elem), table))
		}
	}
}

/// Whether `loop_op` is a loop whose sink applies a write's stages.
fn is_write_sink(ir: &Ir, loop_op: OpId) -> bool {
	matches!(
		ir.op(loop_op).map(|data| &data.kind),
		Some(OpKind::Logical(LogOp::Loop {
			sink: LoopSink::Write(_),
			..
		}))
	)
}

/// Whether `kind` is a write stage that reads or writes only the subject record of the write.
/// Stages that reach other records (references, views, events, live queries) or whose resources
/// are not keyed by the subject (adjacency and index entries) are not.
fn is_subject_stage(kind: &OpKind) -> bool {
	let OpKind::Logical(LogOp::WriteStage {
		stage,
		..
	}) = kind
	else {
		return false;
	};
	match stage {
		WriteStageKind::CheckRecordExists
		| WriteStageKind::CheckPermissionsQuickCreate
		| WriteStageKind::CheckUpdatePermissions
		| WriteStageKind::CheckDeletePermissions
		| WriteStageKind::CheckCreatePermissions
		| WriteStageKind::RecheckUpdatePermissions
		| WriteStageKind::CheckWhereCondition
		| WriteStageKind::ComputeInputData
		| WriteStageKind::CheckDataFields
		| WriteStageKind::ProcessRecordData
		| WriteStageKind::ProcessMergeData
		| WriteStageKind::DefaultRecordData
		| WriteStageKind::ProcessTableFields
		| WriteStageKind::CleanupTableFields
		| WriteStageKind::ClearRecordData
		| WriteStageKind::StoreRecordData
		| WriteStageKind::PurgeRecordData
		| WriteStageKind::MaterialiseObservedFields
		| WriteStageKind::CheckSelectPermissions(_)
		| WriteStageKind::OutputWrite => true,
		WriteStageKind::CheckRecordUserAccess
		| WriteStageKind::CheckTableNotView
		| WriteStageKind::CheckTableNotLightweight(_)
		| WriteStageKind::CheckTableType(_)
		| WriteStageKind::GenerateRecordId
		| WriteStageKind::CleanupTableReferences
		| WriteStageKind::StoreEdgesData
		| WriteStageKind::StoreInlineAdjacencyData
		| WriteStageKind::StoreIndexData
		| WriteStageKind::ReclaimDocId
		| WriteStageKind::ProcessTableReferences
		| WriteStageKind::ProcessTableViews(_)
		| WriteStageKind::ProcessTableEvents(_)
		| WriteStageKind::ProcessTableLives(_)
		| WriteStageKind::ProcessChangefeeds => false,
	}
}

/// How the resources of one op are keyed when they join a footprint.
#[derive(Clone)]
enum Keys {
	/// Every element key becomes `KeyOf::Any`.
	Widen,
	/// Every key the op's local footprint states is kept, and no key is added.
	Keep,
	/// A record resource of `table` with no key takes `key`. A resource of any other table, of no
	/// named table, or of any other kind keeps what it has.
	Narrow {
		key: KeyOf,
		table: TableName,
	},
}

/// The keys of the resources of `op` itself when they join the footprint of an enclosing op.
fn own_keys(ir: &Ir, op: OpId) -> Keys {
	match keyed(ir, op) {
		Some((key, table)) => Keys::Narrow {
			key,
			table,
		},
		None => Keys::Keep,
	}
}

/// The footprint of `op`, folded over the ops that run as part of it with each op's own
/// resources keyed.
fn keyed_footprint(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> IrFootprint {
	let mut out = IrFootprint {
		reads: SmallVec::new(),
		writes: SmallVec::new(),
	};
	fold(ir, facts, cx, op, false, &own_keys(ir, op), &mut out);
	let mut invoked: SmallVec<[OpId; 2]> = SmallVec::new();
	effects::collect_invoked(ir, op, &mut invoked);
	for creator in invoked {
		let body = effects::closure_body(ir, facts, cx, creator).footprint;
		add_all(&mut out, &body, &Keys::Widen);
	}
	out
}

/// Folds `o` and the ops of its regions into `out`. `nested` is true below the body of a loop
/// that stands in the folded subtree, where every key is widened; `own` keys the resources of
/// `o` itself.
fn fold(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	o: OpId,
	nested: bool,
	own: &Keys,
	out: &mut IrFootprint,
) {
	let Some(data) = ir.op(o) else {
		add_unknown(out);
		return;
	};
	if effects::is_write(&data.kind) && !effects::shows_write_stage(ir, o) {
		add_unknown(out);
	}
	let local = effects::local(ir, facts, cx, o).footprint;
	add_all(out, &local, own);
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			add_unknown(out);
			continue;
		};
		if !effects::runs(&data.kind, info.kind) {
			continue;
		}
		let loop_body = effects::is_loop(&data.kind) && effects::is_loop_body(info.kind);
		for child in ops_in(ir, region) {
			let child_own = if nested {
				Keys::Widen
			} else if loop_body {
				stage_keys(ir, o, child)
			} else {
				own_keys(ir, child)
			};
			fold(ir, facts, cx, child, nested || loop_body, &child_own, out);
		}
	}
}

/// The keys of a stage `child` of the write loop `write_loop` as it joins the footprint of the
/// loop that encloses the write loop.
fn stage_keys(ir: &Ir, write_loop: OpId, child: OpId) -> Keys {
	let subject_stage = ir.op(child).is_some_and(|data| is_subject_stage(&data.kind));
	if !(is_write_sink(ir, write_loop) && subject_stage) {
		return Keys::Widen;
	}
	match lifted_key(ir, write_loop, enclosing_loop(ir, write_loop)) {
		Some((key, table)) => Keys::Narrow {
			key,
			table,
		},
		None => Keys::Widen,
	}
}

/// Appends the resources of `footprint` to `out`, keyed as `keys` says.
fn add_all(out: &mut IrFootprint, footprint: &IrFootprint, keys: &Keys) {
	for resource in &footprint.reads {
		push_resource(&mut out.reads, key_resource(resource, keys));
	}
	for resource in &footprint.writes {
		push_resource(&mut out.writes, key_resource(resource, keys));
	}
}

fn add_unknown(out: &mut IrFootprint) {
	push_resource(&mut out.reads, IrResource::Unknown);
	push_resource(&mut out.writes, IrResource::Unknown);
}

fn key_resource(resource: &IrResource, keys: &Keys) -> IrResource {
	match (keys, resource) {
		(
			Keys::Narrow {
				key,
				table: keyed_table,
			},
			IrResource::Data {
				table: Some(table),
				fields,
				key: KeyOf::Any,
			},
		) if table == keyed_table => IrResource::Data {
			table: Some(table.clone()),
			fields: fields.clone(),
			key: *key,
		},
		(
			Keys::Narrow {
				..
			}
			| Keys::Keep,
			_,
		) => resource.clone(),
		(Keys::Widen, _) => widen_one(resource),
	}
}

/// Adds `resource` to `list`: a list holding `IrResource::Unknown` is that resource alone, and a
/// resource already held is not repeated.
fn push_resource(list: &mut SmallVec<[IrResource; 2]>, resource: IrResource) {
	if list.contains(&IrResource::Unknown) {
		return;
	}
	if resource == IrResource::Unknown {
		list.clear();
		list.push(resource);
	} else if !list.contains(&resource) {
		list.push(resource);
	}
}

/// Whether two lists name the same resources once every element key is widened.
fn same_resources(a: &[IrResource], b: &[IrResource]) -> bool {
	let covers = |from: &[IrResource], to: &[IrResource]| {
		from.iter().all(|resource| to.iter().any(|other| widen_one(other) == widen_one(resource)))
	};
	covers(a, b) && covers(b, a)
}

/// The nearest `logical.loop` whose body region contains `op`, or `None` when `op` stands in no
/// loop body.
pub(crate) fn enclosing_loop(ir: &Ir, op: OpId) -> Option<OpId> {
	let mut current = op;
	for _ in 0..MAX_ANCESTOR_HOPS {
		ir.op(current)?;
		let region = ir.enclosing_region(current);
		let owner = ir.parent_op(region)?;
		let kind = ir.region(region)?.kind;
		if multiplicity::is_loop_body(kind)
			&& ir.op(owner)?.kind.tag() == OpTag::Logical(LogTag::Loop)
		{
			return Some(owner);
		}
		current = owner;
	}
	None
}

/// Whether `value` is the `id` field of `element`: an `expr.field` or `expr.access` named `id`
/// over it.
fn is_id_of(ir: &Ir, value: ValueId, element: ValueId) -> bool {
	multiplicity::id_read_base(ir, value) == Some(element)
}

/// Whether an access `a` may touch what `b` touches.
///
/// The answer is at statement scope: keys of different iterations are not distinguished.
/// `Unknown` overlaps everything. Records overlap when either table is unnamed or the tables are
/// equal and the field sets meet, `All` meeting every set. Catalog definitions overlap by kind,
/// table and name, an unnamed table or name matching any, and a table's definition covers the
/// definitions on it. Sessions, queues, cascades and rejections overlap their own kind by name or
/// table. A snapshot overlaps no write.
pub(crate) fn overlaps(a: &IrResource, b: &IrResource) -> bool {
	match (a, b) {
		(IrResource::Unknown, _) | (_, IrResource::Unknown) => true,
		(
			IrResource::Data {
				table: ta,
				fields: fa,
				..
			},
			IrResource::Data {
				table: tb,
				fields: fb,
				..
			},
		) => same(ta, tb) && fields_meet(fa, fb),
		(
			IrResource::Catalog {
				kind: ka,
				table: ta,
				name: na,
			},
			IrResource::Catalog {
				kind: kb,
				table: tb,
				name: nb,
			},
		) => {
			if !same(ta, tb) {
				return false;
			}
			// A table's definition covers every definition on it.
			let tabled = |kind: &CatalogKind| {
				matches!(
					kind,
					CatalogKind::Field
						| CatalogKind::Index
						| CatalogKind::Event
						| CatalogKind::View
				)
			};
			match (ka, kb) {
				(CatalogKind::Table, other) | (other, CatalogKind::Table)
					if *other == CatalogKind::Table || tabled(other) =>
				{
					ka != kb || same(na, nb)
				}
				_ => ka == kb && same(na, nb),
			}
		}
		(
			IrResource::Session {
				name: a,
			},
			IrResource::Session {
				name: b,
			},
		) => a == b,
		(
			IrResource::Queue {
				event: a,
			},
			IrResource::Queue {
				event: b,
			},
		) => a == b,
		(
			IrResource::Cascade {
				table: a,
			},
			IrResource::Cascade {
				table: b,
			},
		) => a == b,
		(
			IrResource::Reject {
				table: a,
			},
			IrResource::Reject {
				table: b,
			},
		) => a == b,
		_ => false,
	}
}

/// Whether two names may denote one thing: an unnamed side matches any.
fn same<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
	match (a, b) {
		(Some(a), Some(b)) => a == b,
		_ => true,
	}
}

/// Whether two field sets share a field.
fn fields_meet(a: &FieldSet, b: &FieldSet) -> bool {
	match (a, b) {
		(FieldSet::All, _) | (_, FieldSet::All) => true,
		(FieldSet::Some(a), FieldSet::Some(b)) => a.iter().any(|field| b.contains(field)),
	}
}

/// Whether the `read` of one iteration of a loop is disjoint from the `write` of another.
///
/// Resources that do not overlap at statement scope are disjoint. Two record accesses of one
/// table are disjoint across iterations only when both keys are `KeyOf::Elem` along `key_path`
/// and the loop's stream is `Distinct` along it, so no two iterations name one record. A key of
/// `KeyOf::Any` overlaps every key of its table.
pub(crate) fn disjoint_across_iterations(
	read: &IrResource,
	write: &IrResource,
	key_path: ElemPath,
	multiplicity: Multiplicity,
) -> bool {
	if !overlaps(read, write) {
		return true;
	}
	match (read, write) {
		(
			IrResource::Data {
				key: KeyOf::Elem(read_path),
				..
			},
			IrResource::Data {
				key: KeyOf::Elem(write_path),
				..
			},
		) => {
			*read_path == key_path
				&& *write_path == key_path
				&& multiplicity == Multiplicity::Distinct
		}
		_ => false,
	}
}

/// The fields the write `write_op` may modify on `table`.
///
/// Only an update that names every modification answers a set of fields: the union of the first
/// path segments of the places of every `logical.assign` and `logical.unset` under it or under
/// its subject loop. It answers
/// `FieldSet::All` for any other form of write, when its writes include `IrResource::Unknown`,
/// when it holds a `logical.content` or a write-stage op, and when a place does not begin at a
/// named field. A write that touches no record of `table` modifies no field of it.
pub(crate) fn written_fields(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	write_op: OpId,
	table: &TableName,
) -> FieldSet {
	let Some(data) = ir.op(write_op) else {
		return FieldSet::All;
	};
	let OpKind::Logical(LogOp::Write(attrs)) = &data.kind else {
		return FieldSet::All;
	};
	if attrs.form != WriteForm::Update {
		return FieldSet::All;
	}
	let writes = of_with(ir, facts, cx, write_op).writes;
	if writes.contains(&IrResource::Unknown) {
		return FieldSet::All;
	}
	let mut names: SmallVec<[Strand; 4]> = SmallVec::new();
	let mut all = false;
	let mut regions: SmallVec<[RegionId; 4]> = data.regions.iter().copied().collect();
	if let Some(subject) = types::subject_loop(ir, write_op).and_then(|op| ir.op(op)) {
		regions.extend(subject.regions.iter().copied());
	}
	for region in regions {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			let Some(inner) = ir.op(op) else {
				return Walk::Continue;
			};
			let tag = inner.kind.tag();
			if tag == OpTag::Logical(LogTag::Content)
				|| tag == OpTag::Logical(LogTag::WriteStage)
				|| tag == OpTag::Phys(PhysTag::WriteStage)
			{
				all = true;
				return Walk::Stop;
			}
			let place = if let OpKind::Logical(LogOp::Assign(assign)) = &inner.kind {
				Some(&assign.place)
			} else if let OpKind::Logical(LogOp::Unset {
				place,
			}) = &inner.kind
			{
				Some(place)
			} else {
				None
			};
			if let Some(place) = place {
				match first_field(place) {
					Some(name) => {
						if !names.contains(name) {
							names.push(name.clone());
						}
					}
					None => {
						all = true;
						return Walk::Stop;
					}
				}
			}
			Walk::Continue
		});
		if all {
			return FieldSet::All;
		}
	}
	let target = IrResource::Data {
		table: Some(table.clone()),
		fields: FieldSet::All,
		key: KeyOf::Any,
	};
	if !writes.iter().any(|write| overlaps(write, &target)) {
		return FieldSet::Some(SmallVec::new());
	}
	FieldSet::Some(names)
}

/// The field a place begins at, or `None` when it begins anywhere else.
fn first_field(place: &Place) -> Option<&Strand> {
	if let Some(PlacePart::Field(name)) = place.parts.first() {
		Some(name)
	} else {
		None
	}
}

/// `list` with every element key widened to `KeyOf::Any`, without repeats.
fn widen(list: &[IrResource]) -> SmallVec<[IrResource; 2]> {
	let mut out: SmallVec<[IrResource; 2]> = SmallVec::new();
	for resource in list {
		let widened = widen_one(resource);
		if !out.contains(&widened) {
			out.push(widened);
		}
	}
	out
}

/// `resource` with its element key, if any, widened to `KeyOf::Any`.
fn widen_one(resource: &IrResource) -> IrResource {
	if let IrResource::Data {
		table,
		fields,
		key: KeyOf::Elem(_),
	} = resource
	{
		IrResource::Data {
			table: table.clone(),
			fields: fields.clone(),
			key: KeyOf::Any,
		}
	} else {
		resource.clone()
	}
}

/// Converts a logical plan footprint into table-granular resources: every field of every record
/// of a named table, under no key. A list holding `IrResource::Unknown` is that resource alone.
impl From<&Footprint> for IrFootprint {
	fn from(main: &Footprint) -> Self {
		IrFootprint {
			reads: convert(&main.reads),
			writes: convert(&main.writes),
		}
	}
}

fn convert(list: &[Resource]) -> SmallVec<[IrResource; 2]> {
	let mut out: SmallVec<[IrResource; 2]> = SmallVec::new();
	for resource in list {
		let converted = match resource {
			Resource::Data {
				table,
			} => IrResource::Data {
				table: table.clone(),
				fields: FieldSet::All,
				key: KeyOf::Any,
			},
			Resource::Catalog {
				kind,
				table,
				name,
			} => IrResource::Catalog {
				kind: *kind,
				table: table.clone(),
				name: name.as_deref().map(Strand::from),
			},
			Resource::Unknown => return smallvec![IrResource::Unknown],
		};
		if !out.contains(&converted) {
			out.push(converted);
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::iface::ids::SnapshotId;

	fn table(name: &str) -> Option<TableName> {
		Some(TableName::new(name))
	}

	fn data(name: &str, fields: FieldSet, key: KeyOf) -> IrResource {
		IrResource::Data {
			table: table(name),
			fields,
			key,
		}
	}

	fn fields(names: &[&str]) -> FieldSet {
		FieldSet::Some(names.iter().map(|name| Strand::from(*name)).collect())
	}

	fn catalog(kind: CatalogKind, name: Option<&str>, on: Option<&str>) -> IrResource {
		IrResource::Catalog {
			kind,
			table: on.map(TableName::new),
			name: name.map(Strand::from),
		}
	}

	#[test]
	fn overlap_table() {
		let any = KeyOf::Any;
		let a_all = data("a", FieldSet::All, any);
		let b_all = data("b", FieldSet::All, any);
		let a_x = data("a", fields(&["x"]), any);
		let a_y = data("a", fields(&["y"]), any);
		let anywhere = IrResource::Data {
			table: None,
			fields: FieldSet::All,
			key: any,
		};

		assert!(overlaps(&IrResource::Unknown, &a_all));
		assert!(overlaps(&b_all, &IrResource::Unknown));
		assert!(overlaps(&IrResource::Unknown, &IrResource::Snapshot(SnapshotId::new(0))));

		assert!(overlaps(&a_all, &a_all));
		assert!(!overlaps(&a_all, &b_all));
		assert!(overlaps(&anywhere, &b_all));
		assert!(overlaps(&a_all, &a_x));
		assert!(overlaps(&a_x, &a_x));
		assert!(!overlaps(&a_x, &a_y));
		assert!(
			overlaps(&a_all, &data("a", FieldSet::All, KeyOf::Elem(ElemPath::Id))),
			"keys are ignored at statement scope"
		);

		let table_def = catalog(CatalogKind::Table, None, Some("a"));
		let field_def = catalog(CatalogKind::Field, Some("f"), Some("a"));
		let other_field = catalog(CatalogKind::Field, Some("g"), Some("a"));
		let field_elsewhere = catalog(CatalogKind::Field, Some("f"), Some("b"));
		let any_field = catalog(CatalogKind::Field, None, None);
		let function = catalog(CatalogKind::Function, Some("f"), None);
		assert!(overlaps(&table_def, &field_def), "a table's definition covers its fields");
		assert!(overlaps(&field_def, &table_def));
		assert!(overlaps(&field_def, &field_def));
		assert!(!overlaps(&field_def, &other_field));
		assert!(!overlaps(&field_def, &field_elsewhere));
		assert!(overlaps(&any_field, &field_elsewhere), "an unnamed table is a wildcard");
		assert!(!overlaps(&field_def, &function));
		assert!(!overlaps(&table_def, &a_all), "catalog and data never meet");

		let snapshot = IrResource::Snapshot(SnapshotId::new(0));
		assert!(!overlaps(&snapshot, &a_all));
		assert!(!overlaps(&snapshot, &snapshot));

		let session = |name: &str| IrResource::Session {
			name: Strand::from(name),
		};
		let queue = |event: &str| IrResource::Queue {
			event: Strand::from(event),
		};
		let cascade = |name: &str| IrResource::Cascade {
			table: TableName::new(name),
		};
		let reject = |name: &str| IrResource::Reject {
			table: TableName::new(name),
		};
		assert!(overlaps(&session("s"), &session("s")));
		assert!(!overlaps(&session("s"), &session("t")));
		assert!(overlaps(&queue("e"), &queue("e")));
		assert!(!overlaps(&queue("e"), &queue("f")));
		assert!(overlaps(&cascade("a"), &cascade("a")));
		assert!(!overlaps(&cascade("a"), &cascade("b")));
		assert!(overlaps(&reject("a"), &reject("a")));
		assert!(!overlaps(&reject("a"), &reject("b")));
		assert!(!overlaps(&cascade("a"), &reject("a")));
		assert!(!overlaps(&session("s"), &queue("s")));
	}

	#[test]
	fn disjoint_only_along_the_key_path() {
		let id = KeyOf::Elem(ElemPath::Id);
		let elem = KeyOf::Elem(ElemPath::Elem);
		let read = data("t", FieldSet::All, id);
		let write = data("t", FieldSet::All, id);

		assert!(disjoint_across_iterations(&read, &write, ElemPath::Id, Multiplicity::Distinct));
		assert!(
			!disjoint_across_iterations(&read, &write, ElemPath::Id, Multiplicity::MayRepeat),
			"a stream that repeats an identity can write one record twice"
		);
		assert!(
			!disjoint_across_iterations(&read, &write, ElemPath::Elem, Multiplicity::Distinct),
			"distinctness holds along another path"
		);
		let by_elem = data("t", FieldSet::All, elem);
		assert!(
			!disjoint_across_iterations(&read, &by_elem, ElemPath::Id, Multiplicity::Distinct),
			"keys along different paths name unrelated records"
		);
		assert!(disjoint_across_iterations(
			&by_elem,
			&by_elem,
			ElemPath::Elem,
			Multiplicity::Distinct
		));

		let any = data("t", FieldSet::All, KeyOf::Any);
		assert!(!disjoint_across_iterations(&any, &write, ElemPath::Id, Multiplicity::Distinct));
		assert!(!disjoint_across_iterations(&read, &any, ElemPath::Id, Multiplicity::Distinct));
		assert!(!disjoint_across_iterations(&any, &any, ElemPath::Id, Multiplicity::Distinct));

		let other = data("u", FieldSet::All, KeyOf::Any);
		assert!(disjoint_across_iterations(&any, &other, ElemPath::Id, Multiplicity::MayRepeat));
		assert!(!disjoint_across_iterations(
			&IrResource::Unknown,
			&write,
			ElemPath::Id,
			Multiplicity::Distinct
		));
	}

	#[test]
	fn narrowing_keys_only_the_keyed_table() {
		let id = KeyOf::Elem(ElemPath::Id);
		let narrow = Keys::Narrow {
			key: id,
			table: TableName::new("t"),
		};
		let keyed = data("t", FieldSet::All, KeyOf::Any);
		assert_eq!(key_resource(&keyed, &narrow), data("t", FieldSet::All, id));
		let other = data("u", FieldSet::All, KeyOf::Any);
		assert_eq!(key_resource(&other, &narrow), other, "another table keeps its key");
		let unnamed = IrResource::Data {
			table: None,
			fields: FieldSet::All,
			key: KeyOf::Any,
		};
		assert_eq!(key_resource(&unnamed, &narrow), unnamed, "an unnamed table is never narrowed");
		assert_eq!(key_resource(&keyed, &Keys::Keep), keyed);
		let held = data("t", FieldSet::All, id);
		assert_eq!(key_resource(&held, &Keys::Widen), keyed);
	}

	#[test]
	fn from_main_footprint_is_table_granular() {
		let main = Footprint {
			reads: vec![
				Resource::table(TableName::new("a")),
				Resource::any_data(),
				Resource::table(TableName::new("a")),
			],
			writes: vec![
				Resource::table_catalog(TableName::new("a")),
				Resource::Catalog {
					kind: CatalogKind::Field,
					table: Some(TableName::new("a")),
					name: Some("f".to_owned()),
				},
			],
		};
		let converted = IrFootprint::from(&main);
		assert_eq!(
			converted.reads.as_slice(),
			&[
				IrResource::Data {
					table: table("a"),
					fields: FieldSet::All,
					key: KeyOf::Any,
				},
				IrResource::Data {
					table: None,
					fields: FieldSet::All,
					key: KeyOf::Any,
				},
			],
			"every field, no key, repeats dropped"
		);
		assert_eq!(
			converted.writes.as_slice(),
			&[
				catalog(CatalogKind::Table, None, Some("a")),
				catalog(CatalogKind::Field, Some("f"), Some("a")),
			]
		);
		assert_eq!(
			IrFootprint::from(&Footprint::unknown()),
			IrFootprint {
				reads: smallvec![IrResource::Unknown],
				writes: smallvec![IrResource::Unknown],
			}
		);
		let mixed = Footprint {
			reads: vec![Resource::table(TableName::new("a")), Resource::Unknown],
			writes: Vec::new(),
		};
		assert_eq!(IrFootprint::from(&mixed).reads.as_slice(), &[IrResource::Unknown]);
		assert!(IrFootprint::from(&mixed).writes.is_empty());
	}
}
