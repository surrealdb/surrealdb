//! Graph lookups: the hops of `->edge->vertex`, `<-`, `<->` and `<~`, read
//! from the adjacency and reference keys each record holds.
//!
//! A lookup answers origin by origin. An array of origins answers the
//! flattened answers of its elements; a record id is one origin, and an
//! object is the origin its `id` names; any other value answers `NONE`.
//!
//! For one origin each hop is taken in turn over what the one before it
//! reached, in that order, taking the record ids of each value (an object's
//! `id`, an array's elements). A graph hop reads each record's adjacency
//! along its directions, `<-` before `->` for `<->`, and within a direction
//! its edge tables in written order. It reads as `GraphEdgeScan` reads: from
//! the inline sidecar cache when the table keeps one and it is live, in the
//! cache's order within each edge table; otherwise from the merged adjacency
//! cursor in key order, synthesized from the id for a lightweight edge that
//! exists; and for a hop through an edge to the vertex its key names, from
//! the edge's own adjacency for a key that names none. A reference hop reads,
//! subject by subject in written order and for each subject every record in
//! turn, the records referencing it, from the inline reference cache when it
//! is live and otherwise in reference-key order, as `ReferenceScan` reads.
//!
//! Each record reached is then read as a read of its table is: for a checked
//! caller, a record that does not exist or that its table's `PERMISSIONS FOR
//! select` does not admit is dropped; a hop whose records are shown whole
//! shows each through its table's field view; and the hop's `WHERE` keeps the
//! records it admits, evaluated with `$parent` the row the lookup was taken
//! over. The last hop's records are taken back to their ids when they were
//! read for a `WHERE` alone, and `ONLY` answers the one value.
//!
//! A last hop paged by an integer `LIMIT` or `START` alone ([`LookupPage`])
//! reads no more of the adjacency than the page reaches: each origin's read
//! takes the first `start + limit` entries, in the order the hop reads them,
//! and the page is taken of the records the origins reach in turn. Entries the
//! caller's reads drop, or whose records are gone, do not count toward the
//! page: a read that falls short of the page over what it took is repeated
//! over twice as many entries.
//!
//! The permissions and field views are the ones lowering found for the caller
//! it lowered for. A lookup taken where the caller's reads are judged
//! otherwise — inside a permission predicate, whose reads are unchecked —
//! reads its records as the scans' own record resolution reads them.

use std::borrow::Cow;
use std::ops::Bound;

use surrealdb_datastore::values::inline_cache::{CacheValue, EdgeCacheEntry};

use super::descriptor::TableMemo;
use super::interp::State;
use super::program::{
	EdgeSpec, HopPermission, HopRead, HostScalar, KeyCond, KeyRange, KeyRanges, LookupHop,
	LookupSpec, Program, ReferenceSubject,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::operators::scan::common::{
	extract_record_ids_into, resolve_record_batch, resolve_version_stamp,
};
use crate::exec::operators::scan::graph_keys::{graph_ranges, observe_fold_candidate};
use crate::exec::{ControlFlowExt, Error as ExecError, ExecutionContext};
use crate::expr::{ControlFlow, Dir, FlowResult};
use crate::iam::Action;
use crate::idx::adjacency::{
	MergedAdjacencyCursor, VertexAdjacency, VisitFlow, vertex_adjacency_of,
};
use crate::key::KVKeyDecode;
use crate::key::schema::{DecodedGraph, EdgeCacheKey, RefCacheKey, ReferenceKey};
use crate::kvs::{CachePolicy, Direction, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// The value of lookup `spec` from `origin`, `doc` being the row it is taken
/// over, if any.
pub(super) async fn lookup(
	program: &Program,
	spec: &LookupSpec,
	origin: Value,
	doc: Option<&Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let ctx = if state.unchecked_reads {
		ctx.clone().with_skip_fetch_perms(true)
	} else {
		ctx.clone()
	};
	// `$parent` is the row the lookup is taken over, for every hop's `WHERE`.
	let conds = match doc {
		Some(doc) if spec.hops.iter().any(|hop| hop.cond.is_some()) => {
			Some(ctx.with_params([("parent", doc.clone())]))
		}
		_ => None,
	};
	let mut run = Run {
		program,
		spec,
		state,
		conds: conds.as_ref().unwrap_or(&ctx),
		ctx: &ctx,
		doc,
	};
	let room = spec.page.and_then(|page| page.needs());
	let answer = run.answer(&origin, room).await.map_err(ControlFlow::Err)?;
	let answer = match (spec.page, answer) {
		(Some(page), Value::Array(items)) => Value::Array(page.of(items.0).into()),
		(_, answer) => answer,
	};
	if !spec.only_path {
		return Ok(answer);
	}
	match answer {
		Value::Array(items) => match items.len() {
			0 => Ok(Value::None),
			1 => Ok(items.0.into_iter().next().expect("one result")),
			_ => Err(ControlFlow::Err(anyhow::anyhow!(ExecError::SingleOnlyOutput))),
		},
		other => Ok(other),
	}
}

struct Run<'r> {
	program: &'r Program,
	spec: &'r LookupSpec,
	state: &'r mut State,
	/// The context each hop's `WHERE` is evaluated in.
	conds: &'r ExecutionContext,
	ctx: &'r ExecutionContext,
	/// The row the lookup is taken over, the `$parent` of a table permission
	/// that reads it.
	doc: Option<&'r Value>,
}

impl Run<'_> {
	/// The lookup's value from `origin`, which holds at least its first
	/// `room` records where the answer is that long.
	async fn answer(&mut self, origin: &Value, room: Option<usize>) -> anyhow::Result<Value> {
		match origin {
			Value::RecordId(_) | Value::Object(_) => self.one(origin, room).await,
			Value::Array(items) => {
				let mut results = Vec::new();
				for item in items.iter() {
					let left = room.map(|room| room.saturating_sub(results.len()));
					if left == Some(0) {
						break;
					}
					match Box::pin(self.answer(item, left)).await? {
						Value::Array(inner) => results.extend(inner),
						other => results.push(other),
					}
				}
				Ok(Value::Array(results.into()))
			}
			_ => Ok(Value::None),
		}
	}

	/// The lookup's value from one record id or object, which holds at least
	/// its first `room` records where the answer is that long.
	async fn one(&mut self, origin: &Value, room: Option<usize>) -> anyhow::Result<Value> {
		let ctx = self.ctx;
		let db_ctx = ctx.database()?;
		crate::exec::permission::validate_record_user_access(db_ctx)?;
		let checked = ctx.should_check_perms(Action::View)?;
		let version = resolve_version_stamp(ctx, None).await.map_err(signal_error)?;
		let at = At {
			txn: ctx.txn(),
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			version,
		};
		let mut values = vec![origin.clone()];
		let last = self.spec.hops.len().saturating_sub(1);
		for (at_hop, hop) in self.spec.hops.iter().enumerate() {
			// Only the last hop's records are the page's.
			let room = room.filter(|_| at_hop == last);
			values = self.hop(hop, values, checked, &at, room).await.map_err(signal_error)?;
		}
		let results = if self.spec.extract_id {
			values
				.into_iter()
				.filter_map(|value| match value {
					Value::Object(ref obj) => {
						obj.get("id").filter(|id| matches!(id, Value::RecordId(_))).cloned()
					}
					Value::RecordId(_) => Some(value),
					_ => None,
				})
				.collect()
		} else {
			values
		};
		if self.spec.only {
			return match results.len() {
				0 => Ok(Value::None),
				1 => Ok(results.into_iter().next().expect("one result")),
				_ => Err(anyhow::anyhow!(ExecError::SingleOnlyOutput)),
			};
		}
		Ok(Value::Array(results.into()))
	}

	/// What `hop` reaches from `values`, `checked` being whether the caller's
	/// reads are checked. Under a `room`, the hop reads no more than its first
	/// `room` records need: what it answers holds those records where it
	/// reaches that many, and every record it reaches otherwise.
	async fn hop(
		&mut self,
		hop: &LookupHop,
		values: Vec<Value>,
		checked: bool,
		at: &At,
		room: Option<usize>,
	) -> FlowResult<Vec<Value>> {
		let mut sources = Vec::new();
		for value in values {
			extract_record_ids_into(value, &mut sources);
		}
		// How many adjacency entries or references one read takes. A record an
		// entry reaches may be dropped by the caller's permissions or be gone,
		// so a read that falls short of the room reads again over more.
		let mut cap = room.unwrap_or(usize::MAX);
		loop {
			let reached = self.reach(hop, &sources, at, cap).await?;
			let read = reached.len();
			let mut values = self.resolve(hop, reached, checked, at).await?;
			if let Some(cond) = hop.cond {
				let mut kept = Vec::with_capacity(values.len());
				for value in values {
					if self.admits(cond, &value).await? {
						kept.push(value);
					}
				}
				values = kept;
			}
			let Some(room) = room else {
				return Ok(values);
			};
			if values.len() >= room || read < cap {
				values.truncate(room);
				return Ok(values);
			}
			cap = cap.saturating_mul(2);
		}
	}

	/// The first `cap` records `hop` reaches from `sources`, before the
	/// caller's reads judge them, in the order the hop reads them.
	async fn reach(
		&mut self,
		hop: &LookupHop,
		sources: &[RecordId],
		at: &At,
		cap: usize,
	) -> FlowResult<Vec<RecordId>> {
		Ok(match &*hop.read {
			HopRead::Graph {
				specs,
				target_specs,
				fixed,
				..
			} => {
				let ranges = match fixed {
					Some(fixed) => std::sync::Arc::clone(fixed),
					// Boxed: evaluating a bound runs a host pipeline, whose state
					// the hop does not carry where its bounds are constants.
					None => std::sync::Arc::new(KeyRanges {
						specs: Box::pin(self.key_ranges(specs)).await?,
						targets: Box::pin(self.key_ranges(target_specs)).await?,
						whole: specs.iter().all(EdgeSpec::is_whole),
					}),
				};
				let entries = match &hop.key_cond {
					None => {
						adjacencies(
							&hop.read,
							&ranges,
							sources,
							at,
							self.ctx,
							&mut self.state.tables,
							cap,
						)
						.await?
					}
					Some(cond) => {
						let mut entries = Vec::new();
						for source in sources {
							let facts =
								facts(&source.table, self.ctx, &mut self.state.tables).await?;
							let mut read = Vec::new();
							adjacency(
								(&hop.read, &ranges),
								source,
								at,
								self.ctx,
								facts,
								cond.payload.is_some(),
								usize::MAX,
								&mut read,
							)
							.await?;
							// Boxed: the filter's batch reads are state the hop does not
							// carry when it has no key-only `WHERE`.
							Box::pin(self.key_filter(cond, source, read, at, &mut entries)).await?;
						}
						entries
					}
				};
				entries.into_iter().map(|entry| entry.id).collect()
			}
			HopRead::References(subjects) => {
				let mut reached = Vec::new();
				for subject in subjects {
					let start = Box::pin(self.key_bound(&subject.start)).await?;
					let end = Box::pin(self.key_bound(&subject.end)).await?;
					for source in sources {
						if reached.len() >= cap {
							break;
						}
						references(
							subject,
							(&start, &end),
							source,
							at,
							self.ctx,
							&mut self.state.tables,
							cap,
							&mut reached,
						)
						.await?;
					}
				}
				reached
			}
		})
	}

	/// `specs` with their bounds evaluated, with no row.
	async fn key_ranges(&mut self, specs: &[EdgeSpec]) -> FlowResult<Vec<KeyRange>> {
		let mut ranges = Vec::with_capacity(specs.len());
		for spec in specs {
			let start = self.key_bound(&spec.start).await?;
			let end = self.key_bound(&spec.end).await?;
			ranges.push((spec.table.clone(), start, end));
		}
		Ok(ranges)
	}

	/// `bound` evaluated, with no row, its value read as a record key.
	async fn key_bound(&mut self, bound: &Bound<HostScalar>) -> FlowResult<Bound<RecordIdKey>> {
		let key = async |scalar| {
			let value = super::host::host_scalar(self.program, scalar, self.ctx, None).await?;
			Ok::<_, ControlFlow>(crate::exec::operators::scan::common::value_to_record_id_key(
				value,
			))
		};
		Ok(match bound {
			Bound::Unbounded => Bound::Unbounded,
			Bound::Included(scalar) => Bound::Included(key(scalar).await?),
			Bound::Excluded(scalar) => Bound::Excluded(key(scalar).await?),
		})
	}

	/// Whether `WHERE` pipeline `cond` admits `value`: answered in place where
	/// the pipeline hands the host no step, as a `WHERE` over the record's own
	/// fields does.
	async fn admits(&mut self, cond: u16, value: &Value) -> FlowResult<bool> {
		if let Some(answer) = super::host::run_image_inline(self.program, cond, value, self.state) {
			return Ok(answer.is_truthy());
		}
		Ok(Box::pin(super::write::run_image(
			self.program,
			cond,
			value.clone(),
			Some(self.conds.clone()),
			self.state,
			self.ctx,
		))
		.await?
		.is_truthy())
	}

	/// Push onto `out` the entries of `source`'s adjacency that key-only
	/// `WHERE` `cond` admits, in order. An edge its key and inline fields do
	/// not answer for is judged over its record, the records of each batch
	/// of [`KEY_FILTER_BATCH`] entries read with one multi-get; a record that
	/// does not exist is not admitted.
	async fn key_filter(
		&mut self,
		cond: &KeyCond,
		source: &RecordId,
		entries: Vec<Entry>,
		at: &At,
		out: &mut Vec<Entry>,
	) -> FlowResult<()> {
		let mut entries = entries.into_iter().peekable();
		while entries.peek().is_some() {
			let batch: Vec<Entry> = entries.by_ref().take(KEY_FILTER_BATCH).collect();
			let candidates: Vec<Option<Vec<(String, Value)>>> =
				batch.iter().map(|entry| candidate_fields(cond, source, entry)).collect();
			if let Some(metrics) = analysed(self.ctx) {
				let answered = candidates.iter().filter(|candidate| candidate.is_some()).count();
				metrics.add_props_evals(answered as u64);
				metrics.add_props_fallbacks((candidates.len() - answered) as u64);
			}
			let unanswered: Vec<RecordId> = batch
				.iter()
				.zip(&candidates)
				.filter(|(_, candidate)| candidate.is_none())
				.map(|(entry, _)| entry.id.clone())
				.collect();
			let mut records = if unanswered.is_empty() {
				Vec::new()
			} else {
				at.txn
					.get_records(at.ns, at.db, &unanswered, at.version, CachePolicy::ReadWrite)
					.await
					.context("Failed to fetch the payload-fallback edge records")?
			}
			.into_iter();
			for (entry, answered) in batch.into_iter().zip(candidates) {
				let admitted = match answered {
					Some(fields) => {
						match cond.checks.as_deref().and_then(|c| checked(c, &fields)) {
							Some(admitted) => admitted,
							None => self.admits(cond.pipeline, &candidate(fields)).await?,
						}
					}
					None => match records.next() {
						Some(record) if record.metadata.is_some() || !record.data.is_none() => {
							self.admits(cond.pipeline, &record.data).await?
						}
						_ => false,
					},
				};
				if admitted {
					out.push(entry);
				}
			}
		}
		Ok(())
	}

	/// The records `reached` as the caller reads them: each dropped that a
	/// checked caller may not select, or that does not exist when it is read,
	/// and each shown whole when the hop shows its records.
	async fn resolve(
		&mut self,
		hop: &LookupHop,
		reached: Vec<RecordId>,
		checked: bool,
		at: &At,
	) -> FlowResult<Vec<Value>> {
		if !checked && !hop.full {
			return Ok(reached.into_iter().map(Value::RecordId).collect());
		}
		let lowered = checked == self.spec.checked
			&& reached.iter().all(|rid| hop.tables.iter().any(|(table, _)| *table == rid.table));
		if !lowered {
			return resolve_record_batch(
				self.ctx,
				&at.txn,
				at.ns,
				at.db,
				&reached,
				hop.full,
				checked,
				at.version,
				CachePolicy::ReadWrite,
				&mut Default::default(),
			)
			.await;
		}
		let records = at
			.txn
			.get_records(at.ns, at.db, &reached, at.version, CachePolicy::ReadWrite)
			.await
			.context("Failed to fetch records")?;
		let mut values = Vec::with_capacity(reached.len());
		for (rid, record) in reached.into_iter().zip(records) {
			super::fuel::tick(self.ctx, 1).await?;
			// A record that does not exist discloses nothing.
			if record.data.is_none() {
				continue;
			}
			let table = &hop
				.tables
				.iter()
				.find(|(table, _)| *table == rid.table)
				.expect("every table reached was checked above")
				.1;
			if checked {
				let admitted = match table.permission {
					HopPermission::Allow => true,
					HopPermission::Deny => false,
					HopPermission::Judged(pipeline) => super::write::run_image(
						self.program,
						pipeline,
						record.data.clone(),
						None,
						self.state,
						self.ctx,
					)
					.await?
					.is_truthy(),
					HopPermission::JudgedOverParent(pipeline) => {
						let parent = self.doc.map(|doc| {
							std::collections::HashMap::from([(
								surrealdb_strand::Strand::from("parent".to_owned()),
								doc.clone(),
							)])
						});
						let outer = std::mem::replace(&mut self.state.parent, parent);
						let judged = super::write::run_image(
							self.program,
							pipeline,
							record.data.clone(),
							None,
							self.state,
							self.ctx,
						)
						.await;
						self.state.parent = outer;
						judged?.is_truthy()
					}
				};
				if !admitted {
					continue;
				}
			}
			if !hop.full {
				values.push(Value::RecordId(rid));
				continue;
			}
			let data = match std::sync::Arc::try_unwrap(record) {
				Ok(record) => record.data,
				Err(shared) => shared.data.clone(),
			};
			values.push(match table.view {
				Some(view) => {
					super::write::run_image(self.program, view, data, None, self.state, self.ctx)
						.await?
				}
				None => data,
			});
		}
		Ok(values)
	}
}

/// Where the lookup reads: its transaction, database and version.
#[derive(Clone)]
struct At {
	txn: std::sync::Arc<Transaction>,
	ns: NamespaceId,
	db: DatabaseId,
	version: Option<u64>,
}

/// A signal out of a lookup's hops, which has no statement to leave: an
/// error.
fn signal_error(signal: ControlFlow) -> anyhow::Error {
	match signal {
		ControlFlow::Err(e) => e,
		ControlFlow::Return(v) => anyhow::anyhow!("Unexpected return in lookup: {:?}", v),
		ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
		ControlFlow::Continue => anyhow::anyhow!("Unexpected continue in lookup"),
	}
}

/// One entry of a record's adjacency as a hop reads it: the record it
/// reaches, and for an edge, the vertex its key names beyond it, the
/// direction it was read along, and the inline-property section its entry
/// carries, where the read asked for it.
struct Entry {
	id: RecordId,
	far: Option<RecordId>,
	dir: Dir,
	props: Option<Vec<u8>>,
}

/// The fields `WHERE` `cond` reads for `entry` of `source`'s adjacency
/// before the edge is read: from the key, and for a `WHERE` reading inline
/// fields, from the entry's inline section as the snapshot of its table reads
/// it, a later field of a name standing for an earlier. `None` when the entry
/// cannot answer them, and the edge record is the only source of truth.
fn candidate_fields(
	cond: &KeyCond,
	source: &RecordId,
	entry: &Entry,
) -> Option<Vec<(String, Value)>> {
	if entry.far.is_none() && (cond.needs_in || cond.needs_out || cond.payload.is_some()) {
		return None;
	}
	let fields = match &cond.payload {
		None => Vec::new(),
		Some(tables) => {
			if entry.dir == Dir::Both {
				return None;
			}
			let (_, table) = tables.iter().find(|(table, _)| *table == entry.id.table)?;
			crate::exec::operators::scan::payload_fields(table, entry.props.as_deref())?
		}
	};
	let mut out = Vec::with_capacity(fields.len() + 3);
	if cond.needs_id {
		out.push(("id".to_owned(), Value::RecordId(entry.id.clone())));
	}
	if let Some(far) = &entry.far
		&& (cond.needs_in || cond.needs_out)
	{
		let (from, to) = match entry.dir {
			Dir::In => (far, source),
			_ => (source, far),
		};
		if cond.needs_in {
			out.push(("in".to_owned(), Value::RecordId(from.clone())));
		}
		if cond.needs_out {
			out.push(("out".to_owned(), Value::RecordId(to.clone())));
		}
	}
	out.extend(fields);
	Some(out)
}

/// The object a key-only `WHERE` is judged against, holding `fields`.
fn candidate(fields: Vec<(String, Value)>) -> Value {
	let mut object = crate::val::Object::default();
	for (name, value) in fields {
		object.insert(name, value);
	}
	Value::Object(object)
}

/// Whether `checks` admit the object holding `fields`, each comparison
/// applied as the pipeline's comparison applies it; `None` where one fails,
/// which the pipeline then answers.
fn checked(checks: &[super::program::KeyCheck], fields: &[(String, Value)]) -> Option<bool> {
	for check in checks {
		let field = fields
			.iter()
			.rev()
			.find(|(name, _)| *name == check.field)
			.map_or(&Value::NONE, |(_, value)| value);
		let (field, value) =
			(super::scalar::Scalar::of(field), super::scalar::Scalar::of(&check.value));
		let answer = if check.swapped {
			check.op.apply_scalar(&value, &field)
		} else {
			check.op.apply_scalar(&field, &value)
		}
		.ok()?;
		if !answer.is_truthy() {
			return Some(false);
		}
	}
	Some(true)
}

/// A source table's adjacency layout and inline-cache cap.
type Facts = (VertexAdjacency, Option<u32>);

/// The [`Facts`] of `table`, from its descriptor as the statement's table
/// memo serves it: loaded once per table while the transaction stamps no
/// catalog version.
async fn facts(
	table: &TableName,
	ctx: &ExecutionContext,
	memo: &mut TableMemo,
) -> FlowResult<Facts> {
	let tb =
		memo.current(ctx, table).await.context("Failed to resolve the source vertex's table")?;
	let tb = tb.definition.as_deref();
	Ok((vertex_adjacency_of(tb), crate::idx::inline_cache::effective_edges_cap(tb)))
}

/// How many sources' adjacencies are read side by side, and the width of the
/// windows a hop's sources are read in.
#[cfg(not(target_family = "wasm"))]
const FANOUT: usize = 16;

/// How many adjacency entries a key-only `WHERE` judges per batch: the edges
/// among them it judges over their records are read with one multi-get.
const KEY_FILTER_BATCH: usize = 1024;

/// The measured per-source read below which reading side by side does not
/// pay for the tasks it spawns.
#[cfg(not(target_family = "wasm"))]
const SPAWN_PER_SOURCE_NANOS: u128 = 5_000;

/// The entries graph read `read` reaches from each of `sources`, in source
/// order: the first window read in turn, timing it, and the rest read side
/// by side a window at a time when a source's read took long enough to pay
/// for its task. An error is the first source's, in source order, that
/// raised one.
///
/// No more than `cap` entries are read: the sources are then read in turn,
/// each only as far as the entries still wanted.
async fn adjacencies(
	read: &std::sync::Arc<HopRead>,
	ranges: &std::sync::Arc<KeyRanges>,
	sources: &[RecordId],
	at: &At,
	ctx: &ExecutionContext,
	tables: &mut TableMemo,
	cap: usize,
) -> FlowResult<Vec<Entry>> {
	let mut entries = Vec::new();
	#[cfg(not(target_family = "wasm"))]
	let mut engaged: Option<bool> = None;
	let mut start = 0;
	while start < sources.len() {
		#[cfg(not(target_family = "wasm"))]
		let end = (start + FANOUT).min(sources.len());
		#[cfg(target_family = "wasm")]
		let end = sources.len();
		let mut known = Vec::with_capacity(end - start);
		for source in &sources[start..end] {
			known.push(facts(&source.table, ctx, tables).await?);
		}
		#[cfg(not(target_family = "wasm"))]
		if engaged == Some(true) && end - start > 1 && cap == usize::MAX {
			let mut set = tokio::task::JoinSet::new();
			for (offset, (source, facts)) in sources[start..end].iter().zip(known).enumerate() {
				let read = std::sync::Arc::clone(read);
				let ranges = std::sync::Arc::clone(ranges);
				let source = source.clone();
				let at = at.clone();
				let ctx = ctx.clone();
				set.spawn(crate::exec::compiles::while_running_task(async move {
					let mut out = Vec::new();
					let read = adjacency(
						(&read, &ranges),
						&source,
						&at,
						&ctx,
						facts,
						false,
						usize::MAX,
						&mut out,
					)
					.await;
					(offset, read.map(|()| out))
				}));
			}
			let mut read: Vec<Option<FlowResult<Vec<Entry>>>> =
				(start..end).map(|_| None).collect();
			while let Some(joined) = set.join_next().await {
				let (offset, out) = joined.map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("graph fan-out task failed: {e}"))
				})?;
				read[offset] = Some(out);
			}
			for out in read {
				entries.extend(out.expect("every task answered")?);
			}
			start = end;
			continue;
		}
		#[cfg(not(target_family = "wasm"))]
		let timer = engaged.is_none().then(web_time::Instant::now);
		for (source, facts) in sources[start..end].iter().zip(known) {
			if entries.len() >= cap {
				return Ok(entries);
			}
			adjacency((read, ranges), source, at, ctx, facts, false, cap, &mut entries).await?;
		}
		#[cfg(not(target_family = "wasm"))]
		if let Some(timer) = timer {
			engaged =
				Some(timer.elapsed().as_nanos() / (end - start) as u128 >= SPAWN_PER_SOURCE_NANOS);
		}
		start = end;
	}
	Ok(entries)
}

/// Push onto `out` the entries graph read `read`, over the key ranges
/// `ranges`, reaches from `source` by its adjacency alone: the edges, or the
/// vertices beyond them for a hop through an edge. With `values`, each entry
/// carries its inline-property section, which only the adjacency keys' values
/// hold: the read takes the cursor over them rather than the inline adjacency
/// cache.
///
/// The read ends once `out` holds `cap` entries: no later entry of the
/// adjacency is read from the cursor, decoded from the cache or counted as
/// scanned. A hop that reaches a vertex through an edge pushes fewer entries
/// than it reads, so only a hop with no such vertex reads under a `cap`.
#[allow(clippy::too_many_arguments)]
async fn adjacency(
	(read, ranges): (&HopRead, &KeyRanges),
	source: &RecordId,
	at: &At,
	ctx: &ExecutionContext,
	(kind, edges_cap): Facts,
	values: bool,
	cap: usize,
	out: &mut Vec<Entry>,
) -> FlowResult<()> {
	let HopRead::Graph {
		directions,
		prefixes,
		targets,
		..
	} = read
	else {
		return Err(ControlFlow::Err(anyhow::anyhow!("an adjacency read of a reference hop")));
	};
	// A source's adjacency is a unit to open, and each entry it yields one
	// more, spent as the reads go.
	super::fuel::tick(ctx, 1).await?;
	if out.len() >= cap {
		return Ok(());
	}
	let txn = &at.txn;
	// A lightweight edge's adjacency derives from its id, which says nothing
	// about whether it exists.
	if kind == VertexAdjacency::Lightweight
		&& !txn
			.record_exists(at.ns, at.db, &source.table, &source.key, at.version)
			.await
			.context("Failed to check a lightweight source's existence")?
	{
		return Ok(());
	}
	let fold_threshold = ctx.root().ctx.config.idx.graph_fold_threshold;
	let metrics = analysed(ctx);
	for &dir in directions {
		if out.len() >= cap {
			break;
		}
		// The sidecar cache holds the direction's whole current adjacency.
		if edges_cap.is_some() && at.version.is_none() && !values && ranges.whole {
			let key = EdgeCacheKey {
				ns: at.ns,
				db: at.db,
				tb: Cow::Borrowed(&source.table),
				id: Cow::Borrowed(&source.key),
				dir,
			};
			let cached = txn
				.get_many_key(vec![key], None)
				.await
				.context("Failed to read the inline adjacency caches")?
				.pop()
				.flatten();
			if let Some(metrics) = metrics {
				match &cached {
					Some(CacheValue::Live(_)) => metrics.add_cache_hits(1),
					_ => metrics.add_cache_misses(1),
				}
			}
			if let Some(CacheValue::Live(entries)) = cached {
				let wanted = cap.saturating_sub(out.len());
				let selected: Vec<&EdgeCacheEntry> = if prefixes.is_empty() {
					entries.iter().take(wanted).collect()
				} else {
					prefixes
						.iter()
						.flat_map(|prefix| {
							entries.iter().filter(move |e| e.edge.starts_with(prefix))
						})
						.take(wanted)
						.collect()
				};
				if let Some(metrics) = metrics {
					metrics.add_edges_scanned(selected.len() as u64);
				}
				// A unit per entry the cache answers, decoded in one go.
				super::fuel::tick(ctx, super::fuel::units(selected.len())).await?;
				for entry in selected {
					let (edge, target) = decode_cache_entry(entry)?;
					reach(targets.as_deref(), dir, edge, Some(target), None, out);
				}
				continue;
			}
		}
		let scopes = graph_ranges(at.ns, at.db, source, dir, &ranges.specs)?;
		let reached_before = out.len();
		let mut delta_hits = 0;
		for scope in scopes {
			if out.len() >= cap {
				break;
			}
			let mut untargeted = Vec::new();
			{
				let range = scope.as_adjacency_scope(at.ns, at.db, source, dir);
				let mut cursor = match kind {
					VertexAdjacency::Lightweight => {
						MergedAdjacencyCursor::open_lightweight(txn, &range)
							.context("Failed to open graph cursor")?
					}
					VertexAdjacency::Normal {
						folded,
					} if values => MergedAdjacencyCursor::open_with_values(
						txn,
						range,
						folded,
						at.version,
						Some(ctx.root().ctx.get_index_stores().adjacency_resolve()),
					)
					.await
					.context("Failed to open graph cursor")?,
					VertexAdjacency::Normal {
						folded,
					} => MergedAdjacencyCursor::open(
						txn,
						range,
						folded,
						at.version,
						Some(ctx.root().ctx.get_index_stores().adjacency_resolve()),
					)
					.await
					.context("Failed to open graph cursor")?,
				};
				let mut held = out.len();
				loop {
					// The entries the last read pushed, and the read itself.
					super::fuel::tick(ctx, super::fuel::units(out.len() - held).saturating_add(1))
						.await?;
					held = out.len();
					if out.len() >= cap {
						break;
					}
					// Each read asks storage for no more than the entries still
					// wanted, nor more than the fuel left.
					let most = super::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE) as usize;
					let fill = (cap - out.len()).clamp(1, most) as u32;
					// A never-folded adjacency is visited in the cursor's own
					// buffer, and only another shape materializes its batch.
					let visited = cursor
						.visit_unfolded_batch(fill, |key| {
							let decoded = DecodedGraph::decode(key)?;
							untargeted.extend(reach(
								targets.as_deref(),
								dir,
								decoded.edge,
								decoded.target,
								None,
								out,
							));
							Ok(if out.len() >= cap {
								VisitFlow::Stop
							} else {
								VisitFlow::Continue
							})
						})
						.await
						.context("Failed to scan graph edge")?;
					match visited {
						Some(0) => break,
						Some(_) => continue,
						None => {}
					}
					let batch =
						cursor.next_batch_scan(fill).await.context("Failed to scan graph edge")?;
					if batch.is_empty() {
						break;
					}
					for item in batch {
						if out.len() >= cap {
							break;
						}
						untargeted.extend(reach(
							targets.as_deref(),
							dir,
							item.edge,
							item.target,
							item.props,
							out,
						));
					}
				}
				delta_hits += cursor.stats().delta_hits;
			}
			// The vertex beyond an edge whose key names none is the far end of
			// the edge's own adjacency, which is never folded.
			for edge in untargeted {
				let inner = graph_ranges(at.ns, at.db, &edge, dir, &ranges.targets)?;
				for inner in inner {
					let mut cursor = txn
						.open_keys_cursor_raw(inner.range, Direction::Forward, 0, at.version)
						.await
						.context("Failed to open legacy-fallback graph cursor")?;
					loop {
						let keys = cursor
							.next_batch(super::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE))
							.await
							.context("Failed to scan edge adjacency for legacy graph fallback")?;
						if keys.is_empty() {
							break;
						}
						super::fuel::tick(ctx, super::fuel::units(keys.len())).await?;
						for key in &keys {
							out.push(Entry {
								id: inner
									.decoder
									.decode_edge(key)
									.context("Failed to decode graph key")?,
								far: None,
								dir,
								props: None,
							});
						}
					}
				}
			}
		}
		if let Some(metrics) = metrics {
			metrics.add_edges_scanned((out.len() - reached_before) as u64);
			metrics.add_delta_hits(delta_hits);
		}
		observe_fold_candidate(txn, at.ns, at.db, source, dir, delta_hits, fold_threshold);
	}
	Ok(())
}

/// The metrics the plan being analysed reports for the program running in
/// `ctx`, when it is analysed.
fn analysed(ctx: &ExecutionContext) -> Option<&crate::exec::metrics::OperatorMetrics> {
	ctx.vm_metrics().map(|metrics| &**metrics).filter(|metrics| metrics.is_enabled())
}

/// Push onto `out` what a hop reaches through adjacency entry `edge`, read
/// along `dir`, whose key names vertex `far` beyond it and whose value holds
/// inline-property section `props`: the edge, or for a hop to the vertices
/// of `targets` beyond it, that vertex. Answer the edge when the hop reaches
/// the vertex beyond it and the key names none.
fn reach(
	targets: Option<&[TableName]>,
	dir: Dir,
	edge: RecordId,
	far: Option<RecordId>,
	props: Option<Vec<u8>>,
	out: &mut Vec<Entry>,
) -> Option<RecordId> {
	match (targets, far) {
		(Some(targets), Some(target)) => {
			if targets.is_empty() || targets.contains(&target.table) {
				out.push(Entry {
					id: target,
					far: None,
					dir,
					props: None,
				});
			}
			None
		}
		(Some(_), None) => Some(edge),
		(None, far) => {
			out.push(Entry {
				id: edge,
				far,
				dir,
				props,
			});
			None
		}
	}
}

/// Push onto `out` the records referencing `target` that `subject` names, in
/// reference-key order: from the inline reference cache when `target`'s table
/// keeps one and it is live, otherwise from the reference keys. The read ends
/// once `out` holds `cap` records.
#[allow(clippy::too_many_arguments)]
async fn references(
	subject: &ReferenceSubject,
	(start, end): (&Bound<RecordIdKey>, &Bound<RecordIdKey>),
	target: &RecordId,
	at: &At,
	ctx: &ExecutionContext,
	memo: &mut TableMemo,
	cap: usize,
	out: &mut Vec<RecordId>,
) -> FlowResult<()> {
	super::fuel::tick(ctx, 1).await?;
	if out.len() >= cap {
		return Ok(());
	}
	let txn = &at.txn;
	// The inline reference cache holds a record's whole current set of
	// references, which a range of their keys narrows.
	let whole = matches!(start, Bound::Unbounded) && matches!(end, Bound::Unbounded);
	if at.version.is_none() && whole {
		let tb = memo
			.current(ctx, &target.table)
			.await
			.context("Failed to resolve the referenced record's table")?;
		if crate::idx::inline_cache::effective_refs_cap(tb.definition.as_deref()).is_some() {
			let key = RefCacheKey {
				ns: at.ns,
				db: at.db,
				tb: Cow::Borrowed(&target.table),
				id: Cow::Borrowed(&target.key),
			};
			let cached = txn
				.get_many_key(vec![key], None)
				.await
				.context("Failed to read the inline reference caches")?
				.pop()
				.flatten();
			if let Some(metrics) = analysed(ctx) {
				match &cached {
					Some(CacheValue::Live(_)) => metrics.add_cache_hits(1),
					_ => metrics.add_cache_misses(1),
				}
			}
			if let Some(CacheValue::Live(entries)) = cached {
				for entry in &entries {
					if out.len() >= cap {
						break;
					}
					if let Some(prefix) = &subject.prefix
						&& !entry.reference.starts_with(prefix)
					{
						continue;
					}
					let (table, field, key) =
						storekey::decode_borrow::<(TableName, String, RecordIdKey)>(
							&entry.reference,
						)
						.map_err(|e| anyhow::anyhow!("Failed to decode a cached reference: {e}"))?;
					if subject.table.as_ref().is_some_and(|t| *t != table)
						|| subject.field.as_deref().is_some_and(|f| f != field)
					{
						continue;
					}
					out.push(RecordId {
						table,
						key,
					});
				}
				return Ok(());
			}
		}
	}
	let range = crate::exec::operators::scan::ref_key_range(
		at.ns,
		at.db,
		target,
		subject.table.as_ref(),
		subject.field.as_deref(),
		start.clone(),
		end.clone(),
	)?;
	let mut cursor = txn
		.open_keys_cursor(range, Direction::Forward, 0, at.version)
		.await
		.context("Failed to open reference cursor")?;
	loop {
		let mut failed: Option<anyhow::Error> = None;
		let stats = cursor
			.for_each(super::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE), &mut |key| {
				match ReferenceKey::decode_key(key) {
					Ok(decoded) => {
						out.push(RecordId {
							table: decoded.foreign_table.into_owned(),
							key: decoded.foreign_key.into_owned(),
						});
						Ok(if out.len() >= cap {
							std::ops::ControlFlow::Break(())
						} else {
							std::ops::ControlFlow::Continue(())
						})
					}
					Err(e) => {
						failed = Some(e);
						Ok(std::ops::ControlFlow::Break(()))
					}
				}
			})
			.await
			.context("Failed to scan reference")?;
		if let Some(e) = failed {
			return Err(e).context("Failed to decode ref key")?;
		}
		// A unit per reference read, and one for the read itself.
		super::fuel::tick(ctx, super::fuel::units(stats.rows as usize).saturating_add(1)).await?;
		if stats.rows == 0 || out.len() >= cap {
			return Ok(());
		}
	}
}

/// The edge and the vertex beyond it a cached adjacency entry names.
fn decode_cache_entry(entry: &EdgeCacheEntry) -> anyhow::Result<(RecordId, RecordId)> {
	let (table, key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.edge)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge identity: {e}"))?;
	let (t_table, t_key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.target)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge target: {e}"))?;
	Ok((RecordId::new(table, key), RecordId::new(t_table, t_key)))
}
