//! Where an op came from, for error positions and `EXPLAIN` only.
//!
//! A location never enters identity: two expressions with the same text at different positions
//! may share a value when every rule of value identity holds.

use std::sync::Arc;

use crate::exec::ir::iface::premise::CatalogObjectKey;
use crate::exec::vm::compile_cache::ObjectVersion;

/// A byte range of the request text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Span {
	pub(crate) start: u32,
	pub(crate) end: u32,
}

/// The position and provenance of text an op was built from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Location {
	/// The statement's position in its request or fragment.
	pub(crate) stmt: u32,
	/// The byte span in the request text, where the op comes from the text.
	pub(crate) span: Option<Span>,
	pub(crate) origin: LocOrigin,
}

/// Where the code of an op was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LocOrigin {
	/// The request text.
	Query,
	/// The request text, with the surface text of the expression the op computes. The binder
	/// records it for a statement under `EXPLAIN` and on the `KILL` op for the id it names, and
	/// the errors that name the statement print it. A pass that derives an op from another keeps
	/// the origin, so the text names the clause the op was written for, whichever equal form the
	/// plan evaluates. It is a label for the plan reader: it never enters identity, and the text
	/// form of a container prints it as `query`.
	Written(Arc<String>),
	/// Inlined from a catalog definition at a version. Boxed so a location stays small.
	Catalog(Box<(CatalogObjectKey, ObjectVersion)>),
	/// Created by a pass; the op's provenance names the ops it derives from.
	Synthesized,
}

impl Location {
	/// The surface text of the expression the op computes, when the binder recorded it.
	pub(crate) fn text(&self) -> Option<&str> {
		match &self.origin {
			LocOrigin::Written(text) => Some(text.as_str()),
			LocOrigin::Query | LocOrigin::Catalog(_) | LocOrigin::Synthesized => None,
		}
	}

	/// This location with `text` as the surface text of the expression the op computes. A location
	/// that is synthesized or inlined from the catalog keeps its origin.
	pub(crate) fn with_text(mut self, text: Arc<String>) -> Self {
		if matches!(self.origin, LocOrigin::Query | LocOrigin::Written(_)) {
			self.origin = LocOrigin::Written(text);
		}
		self
	}
}
