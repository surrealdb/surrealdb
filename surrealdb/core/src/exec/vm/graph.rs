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
//! Where the pipeline's plan says so, a read of an adjacency or of references
//! is served from a frontier window ([`frontier`]) when one holds what the read
//! would take from storage: the inline cache of a source and the adjacency keys
//! of a never-folded range, read for a set of sources with one multi-get and one
//! multi-range read. The origins of an array, the sources of a hop and the rows
//! of a batch each fill one. A read the window does not hold reads storage for
//! itself, so a window changes where bytes come from and nothing a read does
//! with them.
//!
//! A lookup a query trace reports ([`crate::exec::trace`]) records, hop by hop,
//! the adjacency entries and references behind the records each hop answered,
//! after the caller's reads and the hop's `WHERE` judged them, so nothing a hop
//! dropped is ever recorded. Once the lookup has answered, what it recorded is
//! cut to what leads to its answer: an entry is kept where its edge, or the
//! record beyond it, is in the answer, or is the record a kept entry was read
//! from. A walk that ends on edges reports their ends as the caller sees them
//! through the edge's field view, never from the adjacency key, which a field
//! permission on `in` or `out` does not cover. A traced lookup is never fused
//! ([`super::ir_lower`]), so every edge it walks is a record its hop names.
//!
//! The permissions and field views are the ones lowering found for the caller
//! it lowered for. A lookup taken where the caller's reads are judged
//! otherwise — inside a permission predicate, whose reads are unchecked —
//! reads its records as the scans' own record resolution reads them.

pub(in crate::exec::vm) mod frontier;

use std::borrow::Cow;
use std::ops::Bound;

pub(super) use frontier::Frontier;
use frontier::{SOURCES_PER_FILL, Windows};
use surrealdb_datastore::values::inline_cache::{CacheValue, EdgeCacheEntry, RefCacheEntry};
use surrealdb_rpc::TraceKinds;

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
use crate::exec::operators::scan::graph_keys::{GraphRange, graph_ranges, observe_fold_candidate};
use crate::exec::trace::{StatementTrace, WALKED_BYTES, Walk, WalkKind, answered_ids, kinds_of};
use crate::exec::{ControlFlowExt, Error as ExecError, ExecutionContext};
use crate::expr::logical::LookupTrace;
use crate::expr::{ControlFlow, Dir, FlowResult};
use crate::iam::Action;
use crate::idx::adjacency::{
	MergedAdjacencyCursor, VertexAdjacency, VisitFlow, vertex_adjacency_of,
};
use crate::key::schema::{DecodedGraph, EdgeCacheKey, RefCacheKey, ReferenceKey};
use crate::key::{KVKeyDecode, Resumable};
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
	// A window that stands when the lookup starts is an enclosing lookup's to drop.
	#[cfg(test)]
	let entered_empty = state.frontier_hop.is_empty() && state.frontier_origins.is_empty();
	let trace = match spec.trace {
		LookupTrace::None => None,
		LookupTrace::Partial => {
			if let Some(trace) = ctx.ctx().query_trace() {
				let kinds = spec.hops.iter().fold(TraceKinds::NONE, |kinds, hop| {
					kinds.union(match &*hop.read {
						HopRead::Graph {
							..
						} => TraceKinds::RELATIONS,
						HopRead::References(_) => TraceKinds::REFERENCES,
					})
				});
				trace.mark_partial(kinds);
			}
			None
		}
		LookupTrace::Traced | LookupTrace::Inner => ctx.ctx().query_trace().cloned(),
	};
	let mut run = Run {
		program,
		spec,
		state,
		conds: conds.as_ref().unwrap_or(&ctx),
		ctx: &ctx,
		doc,
		trace,
		walked: Vec::new(),
		walked_bytes: 0,
	};
	let room = spec.page.and_then(|page| page.needs());
	let answer = run.answer(&origin, room).await.map_err(ControlFlow::Err);
	#[cfg(test)]
	if entered_empty
		&& !(run.state.frontier_hop.is_empty() && run.state.frontier_origins.is_empty())
	{
		frontier::counters::left_behind();
	}
	let answer = answer?;
	let answer = match (spec.page, answer) {
		(Some(page), Value::Array(items)) => Value::Array(page.of(items.0).into()),
		(_, answer) => answer,
	};
	if let Some(trace) = run.trace.take() {
		let walked = std::mem::take(&mut run.walked);
		if spec.trace == LookupTrace::Inner {
			// The lookup or step the path ends in reports it.
			trace.hold(walked);
		} else {
			// What the path's inner lookups walked leads to this answer or to nothing.
			trace.report(walked, &answer);
		}
	}
	if !spec.only_path {
		return Ok(answer);
	}
	match answer {
		Value::Array(items) => match items.len() {
			0 => Ok(Value::None),
			1 => Ok(items.0.into_iter().next().expect("one result")),
			_ => Err(ControlFlow::Err(anyhow::anyhow!(ExecError::SingleOnlyOutput))),
		},
		other @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Ok(other),
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
	/// The query trace a traced lookup reports to.
	trace: Option<std::sync::Arc<StatementTrace>>,
	/// What the hops of a traced lookup walked, for [`StatementTrace::report`].
	walked: Vec<Walk>,
	/// The bytes [`Self::walked`] holds ([`Walk::bytes`]).
	walked_bytes: usize,
}

impl Run<'_> {
	/// The lookup's value from `origin`, which holds at least its first `room` records where the
	/// answer is that long.
	async fn answer(&mut self, origin: &Value, room: Option<usize>) -> anyhow::Result<Value> {
		match origin {
			Value::RecordId(_) | Value::Object(_) => self.one(origin, room).await,
			Value::Array(items) => {
				let mut results = Vec::new();
				self.answer_array(items, room, &mut results).await?;
				Ok(Value::Array(results.into()))
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => Ok(Value::None),
		}
	}

	/// Append the lookup's value from `origin`, flattened, to `out`, which holds the records
	/// answered so far; `room` is measured against the whole of `out`.
	async fn answer_into(
		&mut self,
		origin: &Value,
		room: Option<usize>,
		out: &mut Vec<Value>,
	) -> anyhow::Result<()> {
		match origin {
			Value::RecordId(_) | Value::Object(_) => {
				let left = room.map(|room| room.saturating_sub(out.len()));
				let res = self.one(origin, left).await?;
				if let Value::Array(inner) = res {
					out.extend(inner);
				} else {
					out.push(res);
				}
			}
			Value::Array(items) => self.answer_array(items, room, out).await?,
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => out.push(Value::None),
		}
		Ok(())
	}

	/// Append the lookup's value from each of `items`, the origins of one array, to `out`. The
	/// origins window holds one group at a time, filled before the group is answered and dropped
	/// after it.
	async fn answer_array(
		&mut self,
		items: &[Value],
		room: Option<usize>,
		out: &mut Vec<Value>,
	) -> anyhow::Result<()> {
		for group in items.chunks(SOURCES_PER_FILL) {
			if room.is_some_and(|room| out.len() >= room) {
				break;
			}
			let owned = self.window_for(group).await;
			let answered = self.answer_each(group, room, out).await;
			if owned {
				self.state.frontier_origins.clear();
			}
			answered?;
		}
		Ok(())
	}

	/// Append the lookup's value from each of `items` in turn, flattened, to `out`, stopping
	/// once `out` holds `room` records.
	async fn answer_each(
		&mut self,
		items: &[Value],
		room: Option<usize>,
		out: &mut Vec<Value>,
	) -> anyhow::Result<()> {
		for item in items {
			if room.is_some_and(|room| out.len() >= room) {
				break;
			}
			Box::pin(self.answer_into(item, room, out)).await?;
		}
		Ok(())
	}

	/// Fill the origins window for the first hop's reads of `items`, a group of the origins of one
	/// array, where the lookup reads in windows ([`LookupSpec::window`]): one multi-get of their
	/// inline caches and one multi-range read of their adjacency keys, which each origin's own
	/// read then consumes. The window is the caller's to drop when the origins are answered:
	/// `true` where this call filled it, `false` where it filled nothing, among them where an
	/// enclosing array's window stands, which this lookup reads and never fills or drops.
	///
	/// Nothing is filled for fewer than two origins. A failure to read fills what it can, and is
	/// not raised: each origin reads what the window does not hold for itself, and raises what
	/// its own read raises.
	async fn window_for(&mut self, items: &[Value]) -> bool {
		if !self.spec.window || items.len() < 2 || !self.state.frontier_origins.is_empty() {
			return false;
		}
		let Some(hop) = self.spec.hops.first() else {
			return false;
		};
		if items.iter().filter_map(origin_id).nth(1).is_none() {
			return false;
		}
		let Ok(at) = at_of(self.ctx).await else {
			return false;
		};
		let budget = frontier::budget(self.ctx);
		let fill = frontier::Fill {
			at: &at,
			ctx: self.ctx,
			budget,
			rows: Some(&self.state.frontier_rows),
		};
		// A failed fill is not raised: a source the window does not answer reads for itself and
		// raises what its own read raises.
		let _ = match &*hop.read {
			HopRead::Graph {
				fixed: Some(ranges),
				..
			} => {
				frontier::fill_graph(
					&mut self.state.frontier_origins,
					&hop.read,
					ranges,
					items,
					fill,
					&mut self.state.tables,
				)
				.await
			}
			HopRead::References(subjects) => {
				frontier::fill_references(
					&mut self.state.frontier_origins,
					subjects,
					items,
					fill,
					&mut self.state.tables,
				)
				.await
			}
			HopRead::Graph {
				fixed: None,
				..
			} => return false,
		};
		true
	}

	/// The lookup's value from one record id or object, which holds at least
	/// its first `room` records where the answer is that long.
	async fn one(&mut self, origin: &Value, room: Option<usize>) -> anyhow::Result<Value> {
		let ctx = self.ctx;
		let db_ctx = ctx.database()?;
		crate::exec::permission::validate_record_user_access(db_ctx)?;
		let checked = ctx.should_check_perms(Action::View)?;
		let at = at_of(ctx).await?;
		let mut values = vec![origin.clone()];
		let last = self.spec.hops.len().saturating_sub(1);
		for (at_hop, hop) in self.spec.hops.iter().enumerate() {
			// Only the last hop's records are the page's.
			let room = room.filter(|_| at_hop == last);
			values = self
				.hop(hop, values, checked, &at, room, at_hop == last)
				.await
				.map_err(signal_error)?;
		}
		let results = if self.spec.extract_id {
			values
				.into_iter()
				.filter_map(|value| {
					if let Value::Object(ref obj) = value {
						obj.get("id").filter(|id| matches!(id, Value::RecordId(_))).cloned()
					} else if let Value::RecordId(_) = value {
						Some(value)
					} else {
						None
					}
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
		last: bool,
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
			let (reached, walks) = match self.trace.is_some() {
				true => self.reach_walked(hop, &sources, at, cap).await?,
				false => (self.reach(hop, &sources, at, cap).await?, Vec::new()),
			};
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
				self.walked_into(hop, walks, &values, checked, at, last).await?;
				return Ok(values);
			};
			if values.len() >= room || read < cap {
				values.truncate(room);
				self.walked_into(hop, walks, &values, checked, at, last).await?;
				return Ok(values);
			}
			cap = cap.saturating_mul(2);
		}
	}

	/// [`Self::reach`] for a traced lookup, source by source, with what each
	/// record reached was reached through: an edge whose adjacency key names the
	/// record beyond it, or a reference.
	async fn reach_walked(
		&mut self,
		hop: &LookupHop,
		sources: &[RecordId],
		at: &At,
		cap: usize,
	) -> FlowResult<(Vec<RecordId>, Vec<Walk>)> {
		let mut reached = Vec::new();
		let mut walks = Vec::new();
		// References are read as [`Self::references_of`] reads them, each subject
		// over every source in turn, so tracing keeps the order a page cuts.
		// Each bound is evaluated once for the hop, as the untraced read does, and
		// the sources are read in turn over it.
		if let HopRead::References(subjects) = &*hop.read {
			for subject in subjects {
				let start = Box::pin(self.key_bound(&subject.start)).await?;
				let end = Box::pin(self.key_bound(&subject.end)).await?;
				for source in sources {
					if reached.len() >= cap {
						return Ok((reached, walks));
					}
					let before = reached.len();
					references(
						subject,
						(&start, &end),
						source,
						at,
						self.ctx,
						&mut self.state.tables,
						cap,
						&mut reached,
						Windows::NONE,
					)
					.await?;
					for referrer in &reached[before..] {
						walks.push(Walk {
							source: source.clone(),
							reached: referrer.clone(),
							kind: WalkKind::Reference,
						});
					}
				}
			}
			return Ok((reached, walks));
		}
		let ranges = self.hop_ranges(hop).await?;
		for source in sources {
			let Some(left) = cap.checked_sub(reached.len()).filter(|left| *left > 0) else {
				break;
			};
			let one = std::slice::from_ref(source);
			for entry in self.entries_within(hop, &ranges, one, at, left).await? {
				if let Some(far) = entry.far {
					walks.push(Walk {
						source: source.clone(),
						reached: entry.id.clone(),
						kind: WalkKind::Edge {
							dir: entry.dir,
							far,
							ends: None,
						},
					});
				}
				reached.push(entry.id);
			}
		}
		Ok((reached, walks))
	}

	/// Keep for [`report`] the `walks` whose record `hop` answered in `values`:
	/// nothing the caller's reads or the hop's `WHERE` dropped is recorded. Where
	/// `last` and the caller's reads are checked, a walk that ends on an edge
	/// takes the edge's ends from the edge as the caller sees it.
	async fn walked_into(
		&mut self,
		hop: &LookupHop,
		mut walks: Vec<Walk>,
		values: &[Value],
		checked: bool,
		at: &At,
		last: bool,
	) -> FlowResult<()> {
		if walks.is_empty() {
			return Ok(());
		}
		let mut answered = std::collections::HashSet::new();
		for value in values {
			answered_ids(value, &mut answered);
		}
		walks.retain(|walk| answered.contains(&walk.reached));
		let ends_on_edges = matches!(
			&*hop.read,
			HopRead::Graph {
				targets: None,
				..
			}
		);
		// Every walk held for a lookup that ends on the edges, whether the path ends there or a
		// step cuts it later: what is held is reported, so its ends are only what the caller reads.
		if last && checked && ends_on_edges && !walks.is_empty() {
			let edges: Vec<RecordId> = walks.iter().map(|walk| walk.reached.clone()).collect();
			let shown = resolve_record_batch(
				self.ctx,
				&at.txn,
				at.ns,
				at.db,
				&edges,
				true,
				checked,
				at.version,
				CachePolicy::ReadWrite,
				&mut Default::default(),
			)
			.await?;
			let mut seen = std::collections::HashMap::new();
			for edge in shown {
				let Value::Object(edge) = edge else {
					continue;
				};
				let end = |name: &str| match edge.get(name) {
					Some(Value::RecordId(id)) => Some(id.clone()),
					_ => None,
				};
				if let Some(Value::RecordId(id)) = edge.get("id") {
					seen.insert(id.clone(), (end("in"), end("out")));
				}
			}
			for walk in &mut walks {
				if let WalkKind::Edge {
					ends,
					..
				} = &mut walk.kind
				{
					*ends = Some(seen.remove(&walk.reached).unwrap_or((None, None)));
				}
			}
		}
		let bytes = walks.iter().map(Walk::bytes).sum::<usize>();
		if self.walked_bytes.saturating_add(bytes) > WALKED_BYTES {
			if let Some(trace) = &self.trace {
				trace.truncate(kinds_of(&walks));
			}
			return Ok(());
		}
		self.walked.extend(walks);
		self.walked_bytes += bytes;
		Ok(())
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
				..
			} => self
				.graph_entries(hop, sources, at, cap)
				.await?
				.into_iter()
				.map(|entry| entry.id)
				.collect(),
			HopRead::References(subjects) => {
				let windowed = self.reads_in_windows(sources);
				let reached = self.references_of(subjects, sources, at, cap, windowed).await;
				// The window this call filled is dropped whether the reads answered or failed.
				if windowed {
					self.state.frontier_hop.clear();
				}
				reached?
			}
		})
	}

	/// The first `cap` adjacency entries graph hop `hop` reaches from
	/// `sources`, before the caller's reads judge their records, in the order
	/// the hop reads them.
	async fn graph_entries(
		&mut self,
		hop: &LookupHop,
		sources: &[RecordId],
		at: &At,
		cap: usize,
	) -> FlowResult<Vec<Entry>> {
		let ranges = self.hop_ranges(hop).await?;
		self.entries_within(hop, &ranges, sources, at, cap).await
	}

	/// The key ranges graph hop `hop` reads, its bounds evaluated once.
	async fn hop_ranges(&mut self, hop: &LookupHop) -> FlowResult<std::sync::Arc<KeyRanges>> {
		let HopRead::Graph {
			specs,
			target_specs,
			fixed,
			..
		} = &*hop.read
		else {
			return Err(ControlFlow::Err(anyhow::anyhow!("the key ranges of a reference hop")));
		};
		Ok(match fixed {
			Some(fixed) => std::sync::Arc::clone(fixed),
			// Boxed: evaluating a bound runs a host pipeline, whose state
			// the hop does not carry where its bounds are constants.
			None => std::sync::Arc::new(KeyRanges {
				specs: Box::pin(self.key_ranges(specs)).await?,
				targets: Box::pin(self.key_ranges(target_specs)).await?,
				whole: specs.iter().all(EdgeSpec::is_whole),
			}),
		})
	}

	/// [`Self::graph_entries`] within `ranges`, the hop's key ranges as
	/// [`Self::hop_ranges`] evaluated them.
	async fn entries_within(
		&mut self,
		hop: &LookupHop,
		ranges: &std::sync::Arc<KeyRanges>,
		sources: &[RecordId],
		at: &At,
		cap: usize,
	) -> FlowResult<Vec<Entry>> {
		Ok(match &*hop.read {
			HopRead::Graph {
				..
			} => {
				let ranges = std::sync::Arc::clone(ranges);
				match &hop.key_cond {
					None => self.adjacencies_of(&hop.read, &ranges, sources, at, cap).await?,
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
								Windows::NONE,
							)
							.await?;
							// Boxed: the filter's batch reads are state the hop does not
							// carry when it has no key-only `WHERE`.
							Box::pin(self.key_filter(cond, source, read, at, &mut entries)).await?;
						}
						entries
					}
				}
			}
			HopRead::References(_) => {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"adjacency entries of a reference hop"
				)));
			}
		})
	}

	/// Whether the hop over `sources` fills a window of its own: it has several sources, the plan
	/// serves its lookup, and no hop window stands. A hop window is filled and dropped by the hop
	/// that fills it, and no code but the hop's own reads runs while it stands, so a window found
	/// standing is an enclosing lookup's, which this hop reads and never fills or drops.
	fn reads_in_windows(&self, sources: &[RecordId]) -> bool {
		self.spec.window && sources.len() >= 2 && self.state.frontier_hop.is_empty()
	}

	/// The records of the reference `subjects` that reference each of `sources`, at most `cap`,
	/// in the order the hop reads them: each subject over all the sources in turn. Where
	/// `windowed`, the sources are taken [`SOURCES_PER_FILL`] at a time, and the inline caches and
	/// reference keys of a group are first read with one multi-get and one multi-range read, then
	/// read from the hop window, which holds the one group being read, for the one subject being
	/// read, and which the caller drops.
	async fn references_of(
		&mut self,
		subjects: &[ReferenceSubject],
		sources: &[RecordId],
		at: &At,
		cap: usize,
		windowed: bool,
	) -> FlowResult<Vec<RecordId>> {
		let budget = frontier::budget(self.ctx);
		let mut reached = Vec::new();
		for subject in subjects {
			let start = Box::pin(self.key_bound(&subject.start)).await?;
			let end = Box::pin(self.key_bound(&subject.end)).await?;
			for group in sources.chunks(SOURCES_PER_FILL) {
				if reached.len() >= cap {
					break;
				}
				if windowed {
					self.state.frontier_hop.clear();
					let fill = frontier::Fill {
						at,
						ctx: self.ctx,
						budget,
						rows: Some(&self.state.frontier_rows),
					};
					// A failed fill is not raised: a source the window does not answer reads for
					// itself and raises what its own read raises.
					let _ = frontier::fill_references(
						&mut self.state.frontier_hop,
						std::slice::from_ref(subject),
						group,
						fill,
						&mut self.state.tables,
					)
					.await;
				}
				for source in group {
					if reached.len() >= cap {
						break;
					}
					let windows = Windows {
						hop: Some(&self.state.frontier_hop),
						origins: Some(&self.state.frontier_origins),
						rows: Some(&self.state.frontier_rows),
					};
					references(
						subject,
						(&start, &end),
						source,
						at,
						self.ctx,
						&mut self.state.tables,
						cap,
						&mut reached,
						windows,
					)
					.await?;
				}
			}
		}
		Ok(reached)
	}

	/// [`adjacencies`] over `sources`, in windows where the hop reads them so: the sources are
	/// taken [`SOURCES_PER_FILL`] at a time, the hop window is filled for the group with one
	/// multi-get of their inline caches and one multi-range read of their adjacency keys, and the
	/// group is read from it. A window is filled only where the hop reads its whole adjacency and
	/// has more than one source ([`Run::reads_in_windows`]); the origins window and the window of
	/// the rows that the lookup and the pipeline filled before serve every read.
	///
	/// A failed fill is not raised: the window holds what it read before the failure, and each
	/// source reads what it does not hold for itself, raising what its own read raises. The hop
	/// window this call filled is dropped whether the reads answered or failed.
	async fn adjacencies_of(
		&mut self,
		read: &std::sync::Arc<HopRead>,
		ranges: &std::sync::Arc<KeyRanges>,
		sources: &[RecordId],
		at: &At,
		cap: usize,
	) -> FlowResult<Vec<Entry>> {
		let windowed = self.reads_in_windows(sources);
		let entries = self.read_groups((read, ranges), sources, at, cap, windowed).await;
		if windowed {
			self.state.frontier_hop.clear();
		}
		entries
	}

	/// [`adjacencies_of`] without the drop of the hop window: `sources` a group at a time, each
	/// group's window filled first where `windowed`. The hop window holds the group being read
	/// alone: the one before it has been read, and a source of a later group is never answered
	/// by it.
	async fn read_groups(
		&mut self,
		(read, ranges): (&std::sync::Arc<HopRead>, &std::sync::Arc<KeyRanges>),
		sources: &[RecordId],
		at: &At,
		cap: usize,
		windowed: bool,
	) -> FlowResult<Vec<Entry>> {
		let budget = frontier::budget(self.ctx);
		let mut entries = Vec::new();
		for group in sources.chunks(SOURCES_PER_FILL) {
			// A group reads only the entries the groups before it left wanted, so no source is read
			// past the first `cap` entries of the whole hop.
			if entries.len() >= cap {
				break;
			}
			let wanted = if cap == usize::MAX {
				cap
			} else {
				cap - entries.len()
			};
			if windowed {
				self.state.frontier_hop.clear();
				let fill = frontier::Fill {
					at,
					ctx: self.ctx,
					budget,
					rows: Some(&self.state.frontier_rows),
				};
				// A failed fill is not raised: a source the window does not answer reads for itself
				// and raises what its own read raises.
				let _ = frontier::fill_graph(
					&mut self.state.frontier_hop,
					read,
					ranges,
					group,
					fill,
					&mut self.state.tables,
				)
				.await;
			}
			let windows = Windows {
				hop: Some(&self.state.frontier_hop),
				origins: Some(&self.state.frontier_origins),
				rows: Some(&self.state.frontier_rows),
			};
			let read = adjacencies(
				(read, ranges),
				group,
				at,
				self.ctx,
				&mut self.state.tables,
				wanted,
				windows,
			)
			.await?;
			if entries.is_empty() {
				entries = read;
			} else {
				entries.extend(read);
			}
		}
		Ok(entries)
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
				crate::exec::migrate::get_records(
					self.ctx,
					&at.txn,
					at.ns,
					at.db,
					&unanswered,
					at.version,
					CachePolicy::ReadWrite,
				)
				.await?
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
		let records = crate::exec::migrate::get_records(
			self.ctx,
			&at.txn,
			at.ns,
			at.db,
			&reached,
			at.version,
			CachePolicy::ReadWrite,
		)
		.await?;
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

/// Fill the rows window for the first hop of the lookups of `chains` of `program`, over the rows
/// `rows` of one batch of a pipeline, before the rows run: the origins are the records the rows
/// name, and what a hop reads for them is what its own reads of the rows will read. A chain is a
/// lookup that starts from the row and the lookups that read each the answer of the one before:
/// the sources of the next lookup are the records the window's keys say the one before reaches, so
/// the window holds a level of the path at a time, across every row of the batch. The plan states
/// the chains, and each lookup of one serves a window.
///
/// The window is replaced. A chain ends where the window holds the bytes it may, and where the
/// window's keys say no record is reached. A failure to read is not raised: each row reads what the
/// window does not hold for itself.
pub(super) async fn fill_rows(
	program: &Program,
	chains: &[Box<[u32]>],
	rows: &[Value],
	state: &mut State,
	ctx: &ExecutionContext,
) {
	state.frontier_rows.clear();
	if rows.iter().filter_map(origin_id).nth(1).is_none() {
		return;
	}
	let Ok(at) = at_of(ctx).await else {
		return;
	};
	let budget = frontier::budget(ctx);
	let State {
		frontier_rows,
		frontier_levels: levels,
		tables,
		..
	} = state;
	for chain in chains {
		levels.clear();
		for (level, &lookup) in chain.iter().enumerate() {
			let Some(hop) = program.lookups[lookup as usize].hops.first() else {
				break;
			};
			let fill = frontier::Fill {
				at: &at,
				ctx,
				budget,
				rows: None,
			};
			// The sources of the level being filled: the rows' own records for the first, after
			// that the records the level before reaches.
			let filled = if level == 0 {
				fill_level(frontier_rows, hop, rows, fill, tables).await
			} else {
				fill_level(frontier_rows, hop, &levels.current, fill, tables).await
			};
			let Some(filled) = filled else {
				break;
			};
			#[cfg(test)]
			if filled.is_ok() {
				frontier::counters::level();
			}
			// The sources of the next lookup are what the last hop of this one reaches, and only
			// the first hop is read here: a lookup of several hops ends its chain.
			if filled.is_err()
				|| level + 1 == chain.len()
				|| program.lookups[lookup as usize].hops.len() > 1
				|| frontier_rows.is_full(budget)
			{
				break;
			}
			let HopRead::Graph {
				fixed: Some(ranges),
				..
			} = &*hop.read
			else {
				break;
			};
			let fill = frontier::Fill {
				at: &at,
				ctx,
				budget,
				rows: None,
			};
			let frontier::Levels {
				current,
				next,
				seen,
			} = &mut *levels;
			let reached = if level == 0 {
				frontier::reached_graph(
					frontier_rows,
					&hop.read,
					ranges,
					rows,
					fill,
					tables,
					(next, seen),
				)
				.await
			} else {
				frontier::reached_graph(
					frontier_rows,
					&hop.read,
					ranges,
					current,
					fill,
					tables,
					(next, seen),
				)
				.await
			};
			match reached {
				Ok(true) => levels.advance(),
				_ => break,
			}
		}
	}
	levels.clear();
	#[cfg(test)]
	if !frontier_rows.is_empty() {
		frontier::counters::rows_fill();
	}
}

/// Fill `window` for the first hop of `hop` over `sources`, answering `None` where the hop is one a
/// window is not filled for.
async fn fill_level<S: frontier::Source>(
	window: &mut Frontier,
	hop: &LookupHop,
	sources: &[S],
	fill: frontier::Fill<'_>,
	tables: &mut TableMemo,
) -> Option<FlowResult<()>> {
	Some(match &*hop.read {
		HopRead::Graph {
			fixed: Some(ranges),
			..
		} => frontier::fill_graph(window, &hop.read, ranges, sources, fill, tables).await,
		HopRead::References(subjects) => {
			frontier::fill_references(window, subjects, sources, fill, tables).await
		}
		HopRead::Graph {
			fixed: None,
			..
		} => return None,
	})
}

/// The record id a lookup reads from `origin` as one origin: a record id, or the `id` an object
/// holds.
fn origin_id(origin: &Value) -> Option<&RecordId> {
	if let Value::RecordId(id) = origin {
		Some(id)
	} else if let Value::Object(object) = origin {
		if let Some(Value::RecordId(id)) = object.get("id") {
			Some(id)
		} else {
			None
		}
	} else {
		None
	}
}

/// Where the lookup reads in `ctx`: its transaction, database and version.
async fn at_of(ctx: &ExecutionContext) -> anyhow::Result<At> {
	let db_ctx = ctx.database()?;
	let version = resolve_version_stamp(ctx, None).await.map_err(signal_error)?;
	Ok(At {
		txn: ctx.txn(),
		ns: db_ctx.ns_ctx.ns.namespace_id,
		db: db_ctx.db.database_id,
		version,
	})
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
			Dir::Out | Dir::Both => (source, far),
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

/// How many sources' adjacencies are read side by side: the width of a batch
/// of a hop's sources.
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
/// order. The sources are taken [`FANOUT`] at a time, in batches: while the
/// cost of a read is unmeasured a batch is read in turn and timed, and once a
/// source's read took long enough to pay for its task the sources of each
/// batch are read side by side. A source the `windows` hold any part of is read
/// in turn, taking what they hold and reading the rest after it; one they answer
/// whole is never timed. Only the sources they hold nothing of are read side by
/// side. An error is the first source's, in source order, that raised one.
///
/// No more than `cap` entries are read: the sources are then read in turn,
/// each only as far as the entries still wanted.
async fn adjacencies(
	(read, ranges): (&std::sync::Arc<HopRead>, &std::sync::Arc<KeyRanges>),
	sources: &[RecordId],
	at: &At,
	ctx: &ExecutionContext,
	tables: &mut TableMemo,
	cap: usize,
	windows: Windows<'_>,
) -> FlowResult<Vec<Entry>> {
	let mut entries = Vec::new();
	#[cfg(all(not(target_family = "wasm"), not(test)))]
	let mut engaged: Option<bool> = None;
	#[cfg(all(not(target_family = "wasm"), test))]
	let mut engaged: Option<bool> = frontier::counters::ENGAGED.with(std::cell::Cell::get);
	#[cfg(not(target_family = "wasm"))]
	let directions: &[Dir] = match &**read {
		HopRead::Graph {
			directions,
			..
		} => directions,
		HopRead::References(_) => &[],
	};
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
		// The sources whose reads the windows answer whole read in turn, from the windows: a task
		// would only pay for its own spawn. Only the others are timed. A source the windows hold
		// part of (a cut range, one direction of two) also reads in turn, so that what they hold
		// is taken from them and not read again; only the sources they hold nothing of are read
		// side by side.
		#[cfg(not(target_family = "wasm"))]
		let mut served = [false; FANOUT];
		#[cfg(not(target_family = "wasm"))]
		let mut held = [false; FANOUT];
		#[cfg(not(target_family = "wasm"))]
		for ((served, held), (source, facts)) in
			served.iter_mut().zip(held.iter_mut()).zip(sources[start..end].iter().zip(known.iter()))
		{
			*served = windows.serves_adjacency(at, source, *facts, (directions, ranges));
			*held = *served || windows.holds_adjacency(at, source, *facts, (directions, ranges));
		}
		#[cfg(not(target_family = "wasm"))]
		let unserved = held[..end - start].iter().filter(|held| !**held).count();
		#[cfg(not(target_family = "wasm"))]
		if engaged == Some(true) && unserved > 1 && cap == usize::MAX {
			let mut set = tokio::task::JoinSet::new();
			for (offset, (source, facts)) in
				sources[start..end].iter().zip(known.iter().copied()).enumerate()
			{
				if held[offset] {
					continue;
				}
				#[cfg(test)]
				frontier::counters::spawn();
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
						Windows::NONE,
					)
					.await;
					(offset, read.map(|()| out))
				}));
			}
			let mut answers: Vec<Option<FlowResult<Vec<Entry>>>> =
				(start..end).map(|_| None).collect();
			while let Some(joined) = set.join_next().await {
				let (offset, out) = joined.map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("graph fan-out task failed: {e}"))
				})?;
				answers[offset] = Some(out);
			}
			for (answer, (source, facts)) in
				answers.into_iter().zip(sources[start..end].iter().zip(known.iter().copied()))
			{
				match answer {
					Some(out) => entries.extend(out?),
					None => {
						adjacency(
							(read, ranges),
							source,
							at,
							ctx,
							facts,
							false,
							usize::MAX,
							&mut entries,
							windows,
						)
						.await?
					}
				}
			}
			start = end;
			continue;
		}
		#[cfg(not(target_family = "wasm"))]
		let measuring = engaged.is_none();
		#[cfg(not(target_family = "wasm"))]
		let mut timed = (std::time::Duration::ZERO, 0u32);
		for (offset, (source, facts)) in sources[start..end].iter().zip(known).enumerate() {
			if entries.len() >= cap {
				return Ok(entries);
			}
			#[cfg(not(target_family = "wasm"))]
			let timer = (measuring && !served[offset]).then(web_time::Instant::now);
			adjacency((read, ranges), source, at, ctx, facts, false, cap, &mut entries, windows)
				.await?;
			#[cfg(not(target_family = "wasm"))]
			if let Some(timer) = timer {
				timed = (timed.0 + timer.elapsed(), timed.1 + 1);
			}
			#[cfg(target_family = "wasm")]
			let _ = offset;
		}
		#[cfg(not(target_family = "wasm"))]
		if measuring && timed.1 > 0 {
			engaged = Some(timed.0.as_nanos() / timed.1 as u128 >= SPAWN_PER_SOURCE_NANOS);
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
	windows: Windows<'_>,
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
			// A window that probed the cache answers for the read.
			let probed = windows.edge_probe(at, source, dir);
			let fetched;
			let cached: Option<&CacheValue<EdgeCacheEntry>> = match probed {
				Some(value) => value,
				None => {
					let key = EdgeCacheKey {
						ns: at.ns,
						db: at.db,
						tb: Cow::Borrowed(&source.table),
						id: Cow::Borrowed(&source.key),
						dir,
					};
					fetched = txn
						.get_many_key(vec![key], None)
						.await
						.context("Failed to read the inline adjacency caches")?
						.pop()
						.flatten();
					fetched.as_ref()
				}
			};
			if let Some(metrics) = metrics {
				match cached {
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
		// The ranges of the direction are built where a read needs one: a window that holds a
		// range for the read answers without it.
		let mut scopes: Option<Vec<GraphRange>> = None;
		let reached_before = out.len();
		let mut delta_hits = 0;
		for place in 0..ranges.specs.len().max(1) {
			if out.len() >= cap {
				break;
			}
			let mut untargeted = Vec::new();
			{
				// The keys a window read for a never-folded adjacency are visited as the cursor
				// would visit its first chunk; a range the window cut goes on from a cursor
				// opened after the last key it holds. A window holds the current version's keys,
				// so a read at a version never consults one.
				let served = match (kind, values) {
					(
						VertexAdjacency::Normal {
							folded: false,
						},
						false,
					) if at.version.is_none() => {
						match windows.owned_range(
							at,
							source,
							dir,
							(frontier::hop_id(ranges), place as u16),
						) {
							Some(served) => Some(served),
							None => {
								let (range_start, range_end) =
									scopes_of(&mut scopes, at, source, dir, ranges)?[place]
										.range
										.bounds();
								windows.range(range_start, range_end)
							}
						}
					}
					_ => None,
				};
				let mut remainder = None;
				let mut finished = false;
				let mut held = out.len();
				if let Some(served) = served {
					super::fuel::tick(ctx, 1).await?;
					let mut visited = 0u64;
					let mut stopped = false;
					for key in served.keys() {
						visited += 1;
						let flow =
							take_key(key, targets.as_deref(), dir, cap, out, &mut untargeted)
								.context("Failed to scan graph edge")?;
						if matches!(flow, VisitFlow::Stop) {
							stopped = true;
							break;
						}
					}
					delta_hits += visited;
					super::fuel::tick(ctx, super::fuel::units(out.len() - held).saturating_add(1))
						.await?;
					held = out.len();
					finished = stopped || !served.cut() || out.len() >= cap;
					if !finished && let Some(last) = served.last() {
						let scope = &scopes_of(&mut scopes, at, source, dir, ranges)?[place];
						let mut from = last.to_vec();
						from.push(0);
						remainder = Some(scope.range.part(&from, scope.range.bounds().1));
					}
				}
				if !finished {
					let scope = &scopes_of(&mut scopes, at, source, dir, ranges)?[place];
					let mut range = scope.as_adjacency_scope(at.ns, at.db, source, dir);
					if let Some(remainder) = remainder {
						range.delta_range = remainder;
					}
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
					loop {
						// The entries the last read pushed, and the read itself.
						super::fuel::tick(
							ctx,
							super::fuel::units(out.len() - held).saturating_add(1),
						)
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
								take_key(key, targets.as_deref(), dir, cap, out, &mut untargeted)
							})
							.await
							.context("Failed to scan graph edge")?;
						match visited {
							Some(0) => break,
							Some(_) => continue,
							None => {}
						}
						let batch = cursor
							.next_batch_scan(fill)
							.await
							.context("Failed to scan graph edge")?;
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

/// The ranges of `source` along `dir` over `ranges`, built into `built` the first time they are
/// asked for.
fn scopes_of<'s>(
	built: &'s mut Option<Vec<GraphRange>>,
	at: &At,
	source: &RecordId,
	dir: Dir,
	ranges: &KeyRanges,
) -> Result<&'s [GraphRange], ControlFlow> {
	match built {
		Some(built) => Ok(built),
		slot @ None => {
			#[cfg(test)]
			frontier::counters::build();
			let built = slot.insert(graph_ranges(at.ns, at.db, source, dir, &ranges.specs)?);
			debug_assert_eq!(built.len(), ranges.specs.len().max(1));
			Ok(built.as_slice())
		}
	}
}

/// The metrics the plan being analysed reports for the program running in
/// `ctx`, when it is analysed.
fn analysed(ctx: &ExecutionContext) -> Option<&crate::exec::metrics::OperatorMetrics> {
	ctx.vm_metrics().map(|metrics| &**metrics).filter(|metrics| metrics.is_enabled())
}

/// Reach what the adjacency key `key` names, read along `dir`, as [`reach`] does, collecting onto
/// `untargeted` the edges whose key names no vertex, and say whether the read goes on: it stops
/// once `out` holds `cap` entries.
fn take_key(
	key: &[u8],
	targets: Option<&[TableName]>,
	dir: Dir,
	cap: usize,
	out: &mut Vec<Entry>,
	untargeted: &mut Vec<RecordId>,
) -> anyhow::Result<VisitFlow> {
	let decoded = DecodedGraph::decode(key)?;
	untargeted.extend(reach(targets, dir, decoded.edge, decoded.target, None, out));
	Ok(if out.len() >= cap {
		VisitFlow::Stop
	} else {
		VisitFlow::Continue
	})
}

/// Push onto `out` what a hop reaches through adjacency entry `edge`, read
/// along `dir`, whose key names vertex `far` beyond it and whose value holds
/// inline-property section `props`: the edge, or for a hop to the vertices
/// of `targets` beyond it, that vertex, once for each time `targets` names
/// its table, as a hop to the vertices of each named table in turn reaches it.
/// Answer the edge when the hop reaches the vertex beyond it and the key names
/// none.
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
			let named = if targets.is_empty() {
				1
			} else {
				targets.iter().filter(|table| **table == target.table).count()
			};
			for _ in 1..named {
				out.push(Entry {
					id: target.clone(),
					far: None,
					dir,
					props: None,
				});
			}
			if named > 0 {
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
	windows: Windows<'_>,
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
			// A window that probed the cache answers for the read.
			let probed = windows.ref_probe(at, target);
			let fetched;
			let cached: Option<&CacheValue<RefCacheEntry>> = match probed {
				Some(value) => value,
				None => {
					let key = RefCacheKey {
						ns: at.ns,
						db: at.db,
						tb: Cow::Borrowed(&target.table),
						id: Cow::Borrowed(&target.key),
					};
					fetched = txn
						.get_many_key(vec![key], None)
						.await
						.context("Failed to read the inline reference caches")?
						.pop()
						.flatten();
					fetched.as_ref()
				}
			};
			if let Some(metrics) = analysed(ctx) {
				match cached {
					Some(CacheValue::Live(_)) => metrics.add_cache_hits(1),
					_ => metrics.add_cache_misses(1),
				}
			}
			if let Some(CacheValue::Live(entries)) = cached {
				// A unit per entry the cache answers, as the adjacency's cache charges, whether a
				// window or this read probed it: the walk is this read's own.
				let mut walked = 0usize;
				for entry in entries {
					if out.len() >= cap {
						break;
					}
					walked += 1;
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
				super::fuel::tick(ctx, super::fuel::units(walked)).await?;
				return Ok(());
			}
		}
	}
	let built = || {
		#[cfg(test)]
		frontier::counters::build();
		crate::exec::operators::scan::ref_key_range(
			at.ns,
			at.db,
			target,
			subject.table.as_ref(),
			subject.field.as_deref(),
			start.clone(),
			end.clone(),
		)
	};
	// The keys a window read for the range are visited as the cursor would visit its first
	// chunk; a range the window cut goes on from a cursor opened after the last key it holds. A
	// window holds the current version's keys, so a read at a version never consults one. A
	// window that holds the range for this read answers without it being built.
	let mut range = None;
	let served = match at.version {
		None => {
			match whole.then(|| windows.owned_references(at, target, frontier::hop_id(subject))) {
				Some(Some(served)) => Some(served),
				_ => {
					let held = range.insert(built()?);
					windows.range(held.start().as_slice(), held.end().as_slice())
				}
			}
		}
		Some(_) => None,
	};
	if let Some(served) = served {
		let mut visited = 0usize;
		for key in served.keys() {
			visited += 1;
			if take_reference(key, cap, out).context("Failed to decode ref key")? {
				break;
			}
		}
		// A unit per reference read, and one for the read itself.
		super::fuel::tick(ctx, super::fuel::units(visited).saturating_add(1)).await?;
		if visited == 0 || out.len() >= cap {
			return Ok(());
		}
		if !served.cut() {
			super::fuel::tick(ctx, 1).await?;
			return Ok(());
		}
		if let Some(last) = served.last() {
			range = Some(
				match range.take() {
					Some(range) => range,
					None => built()?,
				}
				.resume_after(last, Direction::Forward),
			);
		}
	}
	let range = match range {
		Some(range) => range,
		None => built()?,
	};
	let mut cursor = txn
		.open_keys_cursor(range, Direction::Forward, 0, at.version)
		.await
		.context("Failed to open reference cursor")?;
	loop {
		let mut failed: Option<anyhow::Error> = None;
		let stats = cursor
			.for_each(super::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE), &mut |key| {
				match take_reference(key, cap, out) {
					Ok(stop) => Ok(if stop {
						std::ops::ControlFlow::Break(())
					} else {
						std::ops::ControlFlow::Continue(())
					}),
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

/// Push onto `out` the record the reference key `key` names, and say whether the read stops: it
/// does once `out` holds `cap` records.
fn take_reference(key: &[u8], cap: usize, out: &mut Vec<RecordId>) -> anyhow::Result<bool> {
	let decoded = ReferenceKey::decode_key(key)?;
	out.push(RecordId {
		table: decoded.foreign_table.into_owned(),
		key: decoded.foreign_key.into_owned(),
	});
	Ok(out.len() >= cap)
}

/// The edge a cached adjacency entry names.
fn decode_cache_edge(entry: &EdgeCacheEntry) -> anyhow::Result<RecordId> {
	let (table, key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.edge)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge identity: {e}"))?;
	Ok(RecordId::new(table, key))
}

/// The edge and the vertex beyond it a cached adjacency entry names.
fn decode_cache_entry(entry: &EdgeCacheEntry) -> anyhow::Result<(RecordId, RecordId)> {
	let (table, key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.edge)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge identity: {e}"))?;
	let (t_table, t_key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.target)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge target: {e}"))?;
	Ok((RecordId::new(table, key), RecordId::new(t_table, t_key)))
}
