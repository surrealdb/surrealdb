//! What a streaming query reports about the graph it walked, on request.
//!
//! A client names the [`TraceKinds`] it wants in the `trace` option of a
//! `query_stream` call. The server then sends a [`Trace`] inside the frames it
//! already sends: a rows or value frame carries the entries for its
//! statement's rows, and the `finished` frame carries the statement-level part
//! (`variables`, `subscriptions`, `partial`, `truncated`) and any entries no
//! rows frame took.
//!
//! A trace is part of its statement's answer, so the stream's contract holds
//! for it unchanged: it is provisional until the statement's `finished` frame,
//! and a `finished` frame carrying an error retracts every trace sent before it
//! for that statement. A trace belongs to its statement, not to the exact rows
//! of the frame it rides on: a consumer collects it per statement.
//!
//! Entries are deduplicated within one frame only. A consumer keys them by id.

use surrealdb_types::{Array, Error, Object, RecordId, SerializationError, Uuid, Value};

/// The kinds of trace a client asks for. Empty asks for none, and costs
/// nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TraceKinds(u8);

impl TraceKinds {
	/// Asks for nothing.
	pub const NONE: Self = Self(0);
	/// Graph edges a returned lookup walked, as `[in, edge, out]`.
	pub const RELATIONS: Self = Self(1);
	/// References a returned lookup walked, as `[referrer, referenced]`.
	pub const REFERENCES: Self = Self(1 << 1);
	/// Record links the result reads through, as `[record, field, target]`.
	pub const LINKS: Self = Self(1 << 2);
	/// Parameters a top-level `LET` bound, by name.
	pub const VARIABLES: Self = Self(1 << 3);
	/// Live queries a statement started, by id.
	pub const SUBSCRIPTIONS: Self = Self(1 << 4);

	const NAMED: [(Self, &'static str); 5] = [
		(Self::RELATIONS, "relations"),
		(Self::REFERENCES, "references"),
		(Self::LINKS, "links"),
		(Self::VARIABLES, "variables"),
		(Self::SUBSCRIPTIONS, "subscriptions"),
	];

	/// Whether no kind is asked for.
	pub const fn is_empty(self) -> bool {
		self.0 == 0
	}

	/// Whether every kind in `other` is asked for.
	pub const fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0 && other.0 != 0
	}

	/// Both sets of kinds.
	#[must_use]
	pub const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	/// The kinds asked for in both.
	#[must_use]
	pub const fn intersection(self, other: Self) -> Self {
		Self(self.0 & other.0)
	}

	/// The kind a wire name stands for, or `None` for a name this server does
	/// not know.
	pub fn from_name(name: &str) -> Option<Self> {
		Self::NAMED.iter().find(|(_, n)| *n == name).map(|(kind, _)| *kind)
	}

	/// The wire names of the kinds asked for, in a fixed order.
	pub fn names(self) -> impl Iterator<Item = &'static str> {
		Self::NAMED.into_iter().filter(move |(kind, _)| self.contains(*kind)).map(|(_, n)| n)
	}
}

/// A graph edge, as `[in, edge, out]`. An end is `None` where the session
/// cannot read the edge's field that names it.
#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
	pub r#in: Option<RecordId>,
	pub edge: RecordId,
	pub out: Option<RecordId>,
}

/// A record link the result read through, as `[record, field, target]`.
#[derive(Clone, Debug, PartialEq)]
pub struct Link {
	pub record: RecordId,
	pub field: String,
	pub target: RecordId,
}

/// The trace one frame carries. Every part is empty unless its kind was asked
/// for; an empty part is left off the wire.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Trace {
	pub relations: Vec<Relation>,
	/// `[referrer, referenced]`: the record holding the field comes first.
	pub references: Vec<(RecordId, RecordId)>,
	pub links: Vec<Link>,
	/// What each top-level `LET` of the statement bound.
	pub variables: Vec<(String, Value)>,
	/// The live queries the statement started.
	pub subscriptions: Vec<Uuid>,
	/// A lookup or link fed the result through a position that is not traced,
	/// such as a function argument or an aggregate.
	pub partial: bool,
	/// The kinds whose recording stopped at a cap.
	pub truncated: TraceKinds,
}

impl Trace {
	/// Whether there is nothing to send.
	pub fn is_empty(&self) -> bool {
		self.relations.is_empty()
			&& self.references.is_empty()
			&& self.links.is_empty()
			&& self.variables.is_empty()
			&& self.subscriptions.is_empty()
			&& !self.partial
			&& self.truncated.is_empty()
	}

	/// What a relation holds, by estimate: its record ids, keys included.
	pub fn relation_bytes(relation: &Relation) -> usize {
		id_bytes(&relation.edge)
			+ relation.r#in.as_ref().map_or(0, id_bytes)
			+ relation.out.as_ref().map_or(0, id_bytes)
	}

	/// What a reference holds, by estimate.
	pub fn reference_bytes((referrer, referenced): &(RecordId, RecordId)) -> usize {
		id_bytes(referrer) + id_bytes(referenced)
	}

	/// What a link holds, by estimate.
	pub fn link_bytes(link: &Link) -> usize {
		id_bytes(&link.record) + link.field.len() + id_bytes(&link.target)
	}

	/// What a bound variable holds, by estimate: its name and its value.
	pub fn variable_bytes((name, value): &(String, Value)) -> usize {
		name.len() + value_bytes(value)
	}

	/// Append `other`'s entries after this trace's own.
	pub fn absorb(&mut self, other: Trace) {
		self.relations.extend(other.relations);
		self.references.extend(other.references);
		self.links.extend(other.links);
		self.variables.extend(other.variables);
		self.subscriptions.extend(other.subscriptions);
		self.partial |= other.partial;
		self.truncated = self.truncated.union(other.truncated);
	}

	/// The trace as the object a frame carries under `trace`.
	pub fn into_value(self) -> Value {
		fn id(id: Option<RecordId>) -> Value {
			id.map_or(Value::None, Value::RecordId)
		}
		let mut map = Object::new();
		if !self.relations.is_empty() {
			let entries = self
				.relations
				.into_iter()
				.map(|r| {
					Value::Array(Array::from(vec![id(r.r#in), Value::RecordId(r.edge), id(r.out)]))
				})
				.collect::<Vec<_>>();
			map.insert("relations", Value::Array(Array::from(entries)));
		}
		if !self.references.is_empty() {
			let entries = self
				.references
				.into_iter()
				.map(|(from, to)| {
					Value::Array(Array::from(vec![Value::RecordId(from), Value::RecordId(to)]))
				})
				.collect::<Vec<_>>();
			map.insert("references", Value::Array(Array::from(entries)));
		}
		if !self.links.is_empty() {
			let entries = self
				.links
				.into_iter()
				.map(|l| {
					Value::Array(Array::from(vec![
						Value::RecordId(l.record),
						Value::String(l.field),
						Value::RecordId(l.target),
					]))
				})
				.collect::<Vec<_>>();
			map.insert("links", Value::Array(Array::from(entries)));
		}
		if !self.variables.is_empty() {
			let mut vars = Object::new();
			for (name, value) in self.variables {
				vars.insert(name, value);
			}
			map.insert("variables", Value::Object(vars));
		}
		if !self.subscriptions.is_empty() {
			let ids = self.subscriptions.into_iter().map(Value::Uuid).collect::<Vec<_>>();
			map.insert("subscriptions", Value::Array(Array::from(ids)));
		}
		if self.partial {
			map.insert("partial", Value::Bool(true));
		}
		if !self.truncated.is_empty() {
			let kinds =
				self.truncated.names().map(|n| Value::String(n.to_owned())).collect::<Vec<_>>();
			map.insert("truncated", Value::Array(Array::from(kinds)));
		}
		Value::Object(map)
	}

	/// Parses a trace from the object a frame carries. The inverse of
	/// [`into_value`](Self::into_value).
	pub fn from_value(value: Value) -> Result<Self, Error> {
		let Value::Object(mut map) = value else {
			return Err(trace_error("Expected object for trace"));
		};
		let mut trace = Trace::default();
		if let Some(entries) = map.remove("relations") {
			for entry in array(entries)? {
				let [r#in, edge, out] = triple(entry)?;
				trace.relations.push(Relation {
					r#in: optional_id(r#in)?,
					edge: record_id(edge)?,
					out: optional_id(out)?,
				});
			}
		}
		if let Some(entries) = map.remove("references") {
			for entry in array(entries)? {
				let mut parts = array(entry)?.into_iter();
				match (parts.next(), parts.next(), parts.next()) {
					(Some(from), Some(to), None) => {
						trace.references.push((record_id(from)?, record_id(to)?));
					}
					_ => return Err(trace_error("Expected [referrer, referenced]")),
				}
			}
		}
		if let Some(entries) = map.remove("links") {
			for entry in array(entries)? {
				let [record, field, target] = triple(entry)?;
				let Value::String(field) = field else {
					return Err(trace_error("Expected a field name in a link"));
				};
				trace.links.push(Link {
					record: record_id(record)?,
					field,
					target: record_id(target)?,
				});
			}
		}
		if let Some(vars) = map.remove("variables") {
			let Value::Object(vars) = vars else {
				return Err(trace_error("Expected object for trace variables"));
			};
			trace.variables = vars.into_iter().collect();
		}
		if let Some(ids) = map.remove("subscriptions") {
			for id in array(ids)? {
				let Value::Uuid(id) = id else {
					return Err(trace_error("Expected uuid in trace subscriptions"));
				};
				trace.subscriptions.push(id);
			}
		}
		trace.partial = matches!(map.remove("partial"), Some(Value::Bool(true)));
		if let Some(kinds) = map.remove("truncated") {
			for kind in array(kinds)? {
				// A kind this build does not know is ignored, as an unknown
				// frame field is.
				if let Value::String(name) = kind
					&& let Some(kind) = TraceKinds::from_name(&name)
				{
					trace.truncated = trace.truncated.union(kind);
				}
			}
		}
		Ok(trace)
	}
}

/// What `value` holds, by estimate: a constant per node, and the heap each
/// holds — strings, byte buffers, names, record ids, a geometry's coordinates,
/// a range's bounds and the items of collections. Iterative, so a deeply
/// nested value cannot overflow the stack.
fn value_bytes(value: &Value) -> usize {
	let mut total = 0;
	let mut stack = vec![value];
	while let Some(value) = stack.pop() {
		total += 32;
		match value {
			Value::String(s) => total += s.len(),
			Value::Bytes(b) => total += b.len(),
			Value::Table(table) => total += table.as_str().len(),
			Value::RecordId(id) => total += id_bytes(id),
			Value::Geometry(geometry) => total += geometry.heap_bytes(),
			Value::File(file) => total += file.bucket.len() + file.key.len(),
			Value::Regex(regex) => total += regex.regex().as_str().len(),
			Value::Range(range) => {
				for bound in [&range.start, &range.end] {
					if let std::ops::Bound::Included(v) | std::ops::Bound::Excluded(v) = bound {
						stack.push(v);
					}
				}
			}
			Value::Array(items) => stack.extend(items.iter()),
			Value::Set(items) => stack.extend(items.iter()),
			Value::Object(object) => {
				for (key, item) in object.iter() {
					total += key.len();
					stack.push(item);
				}
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Uuid(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_) => {}
		}
	}
	total
}

/// What record id `id` holds, by estimate: its rendering grows with its key,
/// however long or structured, and a constant stands for its own size.
fn id_bytes(id: &RecordId) -> usize {
	surrealdb_types::ToSql::to_sql(id).len() + 32
}

fn trace_error(msg: &str) -> Error {
	Error::serialization(msg.to_string(), SerializationError::Deserialization)
}

fn array(value: Value) -> Result<Vec<Value>, Error> {
	let Value::Array(values) = value else {
		return Err(trace_error("Expected array in trace"));
	};
	Ok(values.into_inner())
}

fn triple(value: Value) -> Result<[Value; 3], Error> {
	<[Value; 3]>::try_from(array(value)?).map_err(|_| trace_error("Expected a trace triple"))
}

fn record_id(value: Value) -> Result<RecordId, Error> {
	let Value::RecordId(id) = value else {
		return Err(trace_error("Expected record id in trace"));
	};
	Ok(id)
}

/// An end of a relation: `NONE` where the session cannot read it.
fn optional_id(value: Value) -> Result<Option<RecordId>, Error> {
	if matches!(value, Value::None | Value::Null) {
		return Ok(None);
	}
	record_id(value).map(Some)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn id(table: &str, key: &str) -> RecordId {
		RecordId::new(table, key.to_owned())
	}

	#[test]
	fn a_trace_round_trips() {
		let trace = Trace {
			relations: vec![
				Relation {
					r#in: Some(id("person", "a")),
					edge: id("knows", "ab"),
					out: Some(id("person", "b")),
				},
				Relation {
					r#in: Some(id("person", "a")),
					edge: id("follows", "ah"),
					out: None,
				},
			],
			references: vec![(id("post", "1"), id("person", "a"))],
			links: vec![Link {
				record: id("post", "1"),
				field: "author".to_owned(),
				target: id("person", "a"),
			}],
			variables: vec![("vip".to_owned(), Value::Bool(true))],
			subscriptions: vec![Uuid::new_v4()],
			partial: true,
			truncated: TraceKinds::RELATIONS.union(TraceKinds::LINKS),
		};
		assert_eq!(Trace::from_value(trace.clone().into_value()).unwrap(), trace);
	}

	#[test]
	fn empty_parts_stay_off_the_wire() {
		let Value::Object(map) = (Trace {
			subscriptions: vec![Uuid::new_v4()],
			..Trace::default()
		})
		.into_value() else {
			panic!("a trace is an object");
		};
		assert_eq!(map.keys().collect::<Vec<_>>(), vec!["subscriptions"]);
	}

	#[test]
	fn kinds_are_named_on_the_wire() {
		for name in ["relations", "references", "links", "variables", "subscriptions"] {
			let kind = TraceKinds::from_name(name).unwrap();
			assert_eq!(kind.names().collect::<Vec<_>>(), vec![name]);
		}
		assert_eq!(TraceKinds::from_name("records"), None);
		assert!(!TraceKinds::NONE.contains(TraceKinds::NONE));
	}
}
