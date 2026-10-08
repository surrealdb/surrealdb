//! The record-type marker, read and stamped.
//!
//! A stored record is `{ metadata, data }`, and the write path
//! carries the two apart: the images are [`Value`](crate::val::Value), which is
//! what the expression layer evaluates against, and the marker travels beside
//! them on [`WriteRow`](super::row::WriteRow). The counterparts on
//! [`Record`](crate::catalog::Record) therefore cannot answer for a row
//! mid-order, and these can.
//!
//! Which of the row's two markers to ask is the caller's decision and it is
//! never arbitrary: a stage that writes adjacency in the layout the record is
//! *about to* carry reads `metadata`, and a stage that deletes or restamps keys
//! already on disk reads `initial_metadata`. The two disagree exactly when a
//! write advances an edge's layout generation.

use crate::catalog::{Metadata, RecordType};

/// The adjacency-key layout generation a record-type marker names, or `None`
/// when it names no edge.
pub(crate) fn edge_variant(metadata: &Option<Metadata>) -> Option<u16> {
	match metadata {
		Some(Metadata {
			record_type: RecordType::Edge {
				variant,
			},
			..
		}) => Some(*variant),
		_ => None,
	}
}

/// Whether a record-type marker names an edge.
pub(crate) fn is_edge(metadata: &Option<Metadata>) -> bool {
	edge_variant(metadata).is_some()
}

/// Stamp a record-type marker, creating one where the record carried none.
pub(crate) fn set_record_type(metadata: &mut Option<Metadata>, record_type: RecordType) {
	match metadata {
		Some(metadata) => metadata.record_type = record_type,
		none => {
			*none = Some(Metadata {
				record_type,
				aggregation_stats: Vec::new(),
			});
		}
	}
}
