//! What one statement reports to a query trace (`surrealdb_rpc::trace`).
//!
//! A statement whose request asks for `relations`, `references` or `links`
//! runs with a [`StatementTrace`] on its context. The lookups the statement
//! marked as traced ([`crate::expr::logical::trace`]) report what they walked
//! into it as they run; the executor takes what it holds each time it sends a
//! batch of the statement's rows ([`StatementTrace::take`]), and once more when
//! the statement ends, so it holds no more than one batch's entries at a time.
//!
//! Entries are deduplicated between two takes, never across them: a set of
//! everything seen would grow with the statement, and a client keys entries by
//! id anyway.
//!
//! Every entry is something the statement's own reads answered the session: a
//! lookup reports an edge only once its hop has read and judged it as a read of
//! its table, and a walk that ends on edges reports their ends as the session
//! sees them through the edge's field view.
//!
//! Recording never fails the statement. Past [`CAP_BYTES`] held at once, a kind
//! stops recording until the next take and is listed as truncated.

use std::collections::HashSet;
use std::sync::Mutex;

use surrealdb_rpc::TraceKinds;

use crate::expr::Dir;
use crate::val::{RecordId, Value, convert_value_to_public_value};

/// The most an untaken trace holds, by estimate. A streaming statement's trace
/// is taken with each batch of rows, so this bounds the trace a batch carries;
/// a statement that sends no rows on the way holds its whole trace under it.
pub(crate) const CAP_BYTES: usize = 1 << 20;

/// What a trace entry of each part costs, beyond its record ids.
const ENTRY_BYTES: usize = 16;

/// The kinds a statement's lookups and links record.
pub(crate) const RECORDED: TraceKinds =
	TraceKinds::RELATIONS.union(TraceKinds::REFERENCES).union(TraceKinds::LINKS);

/// One statement's untaken trace. See the [module](self).
pub(crate) struct StatementTrace {
	kinds: TraceKinds,
	held: Mutex<Held>,
	/// What the inner lookups of a traced path walked, for the lookup or step
	/// the path ends in to report ([`Self::report`]), and the bytes it holds.
	walks: Mutex<(Vec<Walk>, usize)>,
}

#[derive(Default)]
struct Held {
	relations: Vec<(Option<RecordId>, RecordId, Option<RecordId>)>,
	edges: HashSet<RecordId>,
	references: Vec<(RecordId, RecordId)>,
	referenced: HashSet<(RecordId, RecordId)>,
	links: Vec<(RecordId, String, RecordId)>,
	linked: HashSet<(RecordId, String, RecordId)>,
	partial: bool,
	truncated: TraceKinds,
	bytes: usize,
}

impl Held {
	/// Whether an entry of `bytes` fits; when it does not, `kind` is truncated.
	fn admits(&mut self, kind: TraceKinds, bytes: usize) -> bool {
		if self.bytes.saturating_add(bytes) > CAP_BYTES {
			self.truncated = self.truncated.union(kind);
			return false;
		}
		self.bytes += bytes;
		true
	}
}

/// What one record id costs a trace, by estimate.
fn id_bytes(id: &RecordId) -> usize {
	crate::exec::memory::size::record_id_bytes(id)
}

fn public(id: RecordId) -> Option<surrealdb_types::RecordId> {
	match convert_value_to_public_value(Value::RecordId(id)) {
		Ok(surrealdb_types::Value::RecordId(id)) => Some(id),
		_ => None,
	}
}

impl StatementTrace {
	/// The trace of a statement whose request asked for `kinds`, or `None`
	/// where none of them is recorded by a statement's reads.
	pub(crate) fn new(kinds: TraceKinds) -> Option<Self> {
		let recorded = kinds.intersection(RECORDED);
		(!recorded.is_empty()).then(|| Self {
			kinds: recorded,
			held: Mutex::new(Held::default()),
			walks: Mutex::new((Vec::new(), 0)),
		})
	}

	/// Whether `kind` is recorded.
	pub(crate) fn records(&self, kind: TraceKinds) -> bool {
		self.kinds.contains(kind)
	}

	fn held(&self) -> std::sync::MutexGuard<'_, Held> {
		self.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// Hold what an inner lookup walked for the lookup or step its path ends
	/// in.
	pub(crate) fn hold(&self, mut walked: Vec<Walk>) {
		let bytes = walked.iter().map(Walk::bytes).sum::<usize>();
		let mut walks = self.walks.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
		let (held, held_bytes) = &mut *walks;
		if held_bytes.saturating_add(bytes) > WALKED_BYTES {
			drop(walks);
			self.truncate(kinds_of(&walked));
			return;
		}
		held.append(&mut walked);
		*held_bytes += bytes;
	}

	/// Report what of `walked`, and of what the statement holds for it, leads to
	/// `answer`, the value a path ends in. What does not lead to it leads to
	/// nothing the result holds, and is dropped.
	pub(crate) fn report(&self, mut walked: Vec<Walk>, answer: &Value) {
		let mut walks = self.walks.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
		walked.append(&mut walks.0);
		walks.1 = 0;
		drop(walks);
		if !walked.is_empty() {
			report(self, walked, answer);
		}
	}

	/// A lookup or link read of `kinds` fed the result through a position that
	/// is not traced. Only a kind the request asks for makes the trace partial.
	pub(crate) fn mark_partial(&self, kinds: TraceKinds) {
		if !kinds.intersection(self.kinds).is_empty() {
			self.held().partial = true;
		}
	}

	/// Recording `kinds` stopped short of what the statement walked.
	pub(crate) fn truncate(&self, kinds: TraceKinds) {
		let mut held = self.held();
		held.truncated = held.truncated.union(kinds.intersection(self.kinds));
	}

	/// Record graph edge `edge` with its ends as the session sees them.
	pub(crate) fn relation(&self, r#in: Option<RecordId>, edge: RecordId, out: Option<RecordId>) {
		if !self.records(TraceKinds::RELATIONS) {
			return;
		}
		let mut held = self.held();
		if held.edges.contains(&edge) {
			return;
		}
		let bytes = ENTRY_BYTES
			+ id_bytes(&edge) * 2
			+ r#in.as_ref().map_or(0, id_bytes)
			+ out.as_ref().map_or(0, id_bytes);
		if held.admits(TraceKinds::RELATIONS, bytes) {
			held.edges.insert(edge.clone());
			held.relations.push((r#in, edge, out));
		}
	}

	/// Record that `referrer` references `referenced`.
	pub(crate) fn reference(&self, referrer: RecordId, referenced: RecordId) {
		if !self.records(TraceKinds::REFERENCES) {
			return;
		}
		let mut held = self.held();
		let key = (referrer, referenced);
		if held.referenced.contains(&key) {
			return;
		}
		let bytes = ENTRY_BYTES + (id_bytes(&key.0) + id_bytes(&key.1)) * 2;
		if held.admits(TraceKinds::REFERENCES, bytes) {
			held.referenced.insert(key.clone());
			held.references.push(key);
		}
	}

	/// Record that the result read through the link `field` of `record` holds
	/// to `target`.
	pub(crate) fn link(&self, record: RecordId, field: String, target: RecordId) {
		if !self.records(TraceKinds::LINKS) {
			return;
		}
		let mut held = self.held();
		let key = (record, field, target);
		if held.linked.contains(&key) {
			return;
		}
		let bytes = ENTRY_BYTES + (id_bytes(&key.0) + key.1.len() + id_bytes(&key.2)) * 2;
		if held.admits(TraceKinds::LINKS, bytes) {
			held.linked.insert(key.clone());
			held.links.push(key);
		}
	}

	/// Everything recorded since the last take, which is cleared.
	pub(crate) fn take(&self) -> surrealdb_rpc::Trace {
		self.take_sized().0
	}

	/// [`Self::take`], with the bytes the entries held by estimate, for a
	/// caller that charges them where it holds the trace.
	pub(crate) fn take_sized(&self) -> (surrealdb_rpc::Trace, usize) {
		let held = std::mem::take(&mut *self.held());
		let bytes = held.bytes;
		let relations = held
			.relations
			.into_iter()
			.filter_map(|(r#in, edge, out)| {
				Some(surrealdb_rpc::Relation {
					r#in: r#in.and_then(public),
					edge: public(edge)?,
					out: out.and_then(public),
				})
			})
			.collect();
		let references = held
			.references
			.into_iter()
			.filter_map(|(referrer, referenced)| Some((public(referrer)?, public(referenced)?)))
			.collect();
		let links = held
			.links
			.into_iter()
			.filter_map(|(record, field, target)| {
				Some(surrealdb_rpc::Link {
					record: public(record)?,
					field,
					target: public(target)?,
				})
			})
			.collect();
		(
			surrealdb_rpc::Trace {
				relations,
				references,
				links,
				partial: held.partial,
				truncated: held.truncated,
				..surrealdb_rpc::Trace::default()
			},
			bytes,
		)
	}
}

/// One adjacency entry or reference a traced lookup's hop took from `source`
/// to `reached`, a record the hop answered.
pub(crate) struct Walk {
	pub(crate) source: RecordId,
	pub(crate) reached: RecordId,
	pub(crate) kind: WalkKind,
}

pub(crate) enum WalkKind {
	/// `reached` is an edge read along `dir`, whose adjacency key names `far`
	/// beyond it. `ends` are its `in` and `out` as the caller sees them, where
	/// the walk ends on the edge and the caller's reads are checked.
	Edge {
		dir: Dir,
		far: RecordId,
		ends: Option<(Option<RecordId>, Option<RecordId>)>,
	},
	/// `reached` references `source`.
	Reference,
}

/// The kinds `walks` would have reported: relations for edges, references for
/// references.
pub(crate) fn kinds_of(walks: &[Walk]) -> TraceKinds {
	walks.iter().fold(TraceKinds::NONE, |kinds, walk| {
		kinds.union(match walk.kind {
			WalkKind::Edge {
				..
			} => TraceKinds::RELATIONS,
			WalkKind::Reference => TraceKinds::REFERENCES,
		})
	})
}

impl Walk {
	/// What the walk holds, by estimate: its record ids, keys included.
	pub(crate) fn bytes(&self) -> usize {
		let kind = match &self.kind {
			WalkKind::Edge {
				far,
				ends,
				..
			} => {
				id_bytes(far)
					+ ends.as_ref().map_or(0, |(r#in, out)| {
						r#in.as_ref().map_or(0, id_bytes) + out.as_ref().map_or(0, id_bytes)
					})
			}
			WalkKind::Reference => 0,
		};
		ENTRY_BYTES + id_bytes(&self.source) + id_bytes(&self.reached) + kind
	}
}

/// The most bytes of walks a traced lookup records, or a statement holds for
/// the lookup or step its path ends in, before the answer they are cut against
/// is known ([`Walk::bytes`]). Past it recording stops, and the statement's
/// relations and references are truncated.
pub(crate) const WALKED_BYTES: usize = CAP_BYTES;

/// Report to `trace` what of `walked` leads to `answer`: an entry is kept where
/// its edge, or the record beyond it, is in the answer, or is the source of a
/// kept entry, until nothing more is kept.
fn report(trace: &StatementTrace, walked: Vec<Walk>, answer: &Value) {
	let mut reached = std::collections::HashSet::new();
	anchored_ids(answer, &mut reached);
	// The walks that lead to each record: through the record they reached, or
	// the record beyond the edge they reached. Each record reached is visited
	// once, so the cut is linear in the walks.
	let mut leading: std::collections::HashMap<&RecordId, Vec<usize>> =
		std::collections::HashMap::new();
	for (at, walk) in walked.iter().enumerate() {
		leading.entry(&walk.reached).or_default().push(at);
		if let WalkKind::Edge {
			far,
			..
		} = &walk.kind
		{
			leading.entry(far).or_default().push(at);
		}
	}
	let mut kept = vec![false; walked.len()];
	let mut pending: Vec<RecordId> = reached.iter().cloned().collect();
	while let Some(id) = pending.pop() {
		for at in leading.remove(&id).unwrap_or_default() {
			if !kept[at] {
				kept[at] = true;
				if reached.insert(walked[at].source.clone()) {
					pending.push(walked[at].source.clone());
				}
			}
		}
	}
	for (walk, _) in walked.into_iter().zip(kept).filter(|(_, kept)| *kept) {
		match walk.kind {
			WalkKind::Edge {
				dir,
				far,
				ends,
			} => {
				// An entry is read along one direction: `<->` reads `<-`, then
				// `->`, so `dir` is never `Both` here.
				let (r#in, out) = ends.unwrap_or(match dir {
					Dir::In => (Some(far), Some(walk.source)),
					Dir::Out | Dir::Both => (Some(walk.source), Some(far)),
				});
				trace.relation(r#in, walk.reached, out);
			}
			WalkKind::Reference => trace.reference(walk.reached, walk.source),
		}
	}
}

/// The record ids anywhere in `value`, the answer a path ends in: through
/// arrays, sets and every value of an object, so a lookup's value held in a
/// field of what a step keeps still leads to it. A hop judges what it answered
/// by [`answered_ids`] instead, since a record's fields may name records the
/// hop did not answer.
fn anchored_ids(value: &Value, out: &mut std::collections::HashSet<RecordId>) {
	let mut pending = vec![value];
	while let Some(value) = pending.pop() {
		if let Value::RecordId(id) = value {
			out.insert(id.clone());
		} else if let Value::Array(items) = value {
			pending.extend(items.iter());
		} else if let Value::Set(items) = value {
			pending.extend(items.iter());
		} else if let Value::Object(object) = value {
			pending.extend(object.values());
		}
	}
}

/// The record ids `value` holds, through arrays and as the `id` of objects.
pub(crate) fn answered_ids(value: &Value, out: &mut std::collections::HashSet<RecordId>) {
	match value {
		Value::RecordId(id) => {
			out.insert(id.clone());
		}
		Value::Array(items) => items.iter().for_each(|item| answered_ids(item, out)),
		Value::Object(obj) => {
			if let Some(Value::RecordId(id)) = obj.get("id") {
				out.insert(id.clone());
			}
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_) => {}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn id(table: &str, key: String) -> RecordId {
		RecordId {
			table: table.into(),
			key: crate::val::RecordIdKey::String(key.into()),
		}
	}

	/// An entry is charged for its record ids' keys, so long keys reach the cap
	/// after a few entries rather than after thousands.
	#[test]
	fn long_keys_count_toward_the_cap() {
		let trace = StatementTrace::new(TraceKinds::RELATIONS).expect("relations are recorded");
		let long = |n: usize| format!("{n}{}", "x".repeat(100_000));
		for n in 0..20 {
			trace.relation(Some(id("person", long(n))), id("knows", long(n)), None);
		}
		let taken = trace.take();
		assert!(taken.relations.len() < 20, "kept {} entries", taken.relations.len());
		assert!(taken.truncated.contains(TraceKinds::RELATIONS));
	}

	/// Walks held for a later step are charged by their record ids' bytes, so
	/// long keys stop the holding long before the count would.
	#[test]
	fn held_walks_are_bounded_by_bytes() {
		let trace = StatementTrace::new(TraceKinds::RELATIONS).expect("relations are recorded");
		let long = |n: usize| format!("{n}{}", "x".repeat(100_000));
		for n in 0..20 {
			trace.hold(vec![Walk {
				source: id("person", long(n)),
				reached: id("knows", long(n)),
				kind: WalkKind::Edge {
					dir: Dir::Out,
					far: id("person", long(n + 100)),
					ends: None,
				},
			}]);
		}
		assert!(trace.take().truncated.contains(TraceKinds::RELATIONS));
	}

	/// Walks past the bound truncate only the kind they would have reported.
	#[test]
	fn an_overflow_truncates_only_its_own_kind() {
		let kinds = TraceKinds::RELATIONS.union(TraceKinds::REFERENCES);
		let trace = StatementTrace::new(kinds).expect("both kinds are recorded");
		let long = |n: usize| format!("{n}{}", "x".repeat(100_000));
		for n in 0..20 {
			trace.hold(vec![Walk {
				source: id("person", long(n)),
				reached: id("post", long(n)),
				kind: WalkKind::Reference,
			}]);
		}
		let truncated = trace.take().truncated;
		assert!(truncated.contains(TraceKinds::REFERENCES));
		assert!(!truncated.contains(TraceKinds::RELATIONS));
	}
}
