//! The pre-decode filter over packed rows, held to the same filter over the same records stored as
//! documents, and to the predicate itself over the record the whole decode holds.

use std::collections::HashSet;
use std::sync::Arc;

use memchr::memmem;

use super::streaming::{ArrayElementContains, SubstringMatch};
use super::tests::{
	DOCUMENT, TEST_DEPTH_LIMIT, differential_predicates, fused_clause, leaf, set_membership, truth,
};
use super::wire_literal::LiteralWire;
use super::{
	AdmittedRow, FusedFlatClauses, LeafFallback, PathSegment, PreDecodeFilter,
	PreDecodeFilterOutcome, PredNode,
};
use crate::catalog::StoredRecord;
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::operators::test_util::TestDb;
use crate::exec::vm::tests_packed_reads::{dataset, layouts_of, rows, unverified_layouts_of};
use crate::expr::operator::BinaryOperator::{self, *};
use crate::val::{Datetime, Duration, Number, Uuid, Value};

fn path(segments: &[&str]) -> Vec<PathSegment> {
	segments.iter().copied().map(PathSegment::from).collect()
}

/// The paths the predicates read: every column kind of the dataset at the root, the columns of a
/// nested object and the keys of an open one, an absent key, and paths that step through a value
/// that is no object.
const PATHS: &[&[&str]] = &[
	&["i"],
	&["f"],
	&["dec"],
	&["b"],
	&["s"],
	&["t"],
	&["du"],
	&["u"],
	&["by"],
	&["o"],
	&["nl"],
	&["tags"],
	&["link"],
	&["lit"],
	&["obj"],
	&["flex"],
	&["pt"],
	&["added"],
	&["absent"],
	&["obj", "x"],
	&["obj", "y"],
	&["obj", "absent"],
	&["flex", "extra"],
	&["flex", "k"],
	&["flex", "a"],
	&["flex", "a", "b"],
	&["flex", "a", "b", "c"],
	&["flex", "c"],
	&["i", "x"],
	&["nl", "x"],
	&["o", "x"],
	&["s", "x"],
	&["lit", "x"],
	&["pt", "x"],
	&["tags", "x"],
	&["absent", "x"],
	&["added", "x"],
	&["obj", "x", "y"],
];

/// The literals the leaves compare with, of every kind a column holds and some that no column
/// does.
fn literals() -> Vec<Value> {
	let int = |i: i64| Value::Number(Number::Int(i));
	vec![
		int(1),
		int(5),
		int(-3),
		Value::Number(Number::Float(1.5)),
		Value::Number(Number::Decimal("3".parse().expect("decimal"))),
		Value::String("one".into()),
		Value::String(String::new().into()),
		Value::Bool(true),
		Value::Null,
		Value::None,
		Value::Datetime(Datetime(
			chrono::DateTime::from_timestamp(1_704_164_645, 0).expect("time"),
		)),
		Value::Duration(Duration(std::time::Duration::from_secs(90))),
		Value::Uuid(Uuid(uuid::Uuid::from_u128(1))),
		Value::from(vec![int(1), int(2)]),
	]
}

const OPERATORS: &[BinaryOperator] =
	&[Equal, NotEqual, LessThan, MoreThanEqual, ExactEqual, Contain];

/// Predicates of every node kind over [`PATHS`]: a comparison of each operator with each literal,
/// membership of a set, a substring and an element of an array, clauses fused over one object, a
/// navigated object, and conjunctions, disjunctions and negations of those.
fn predicates() -> Vec<PredNode> {
	let int = |i: i64| Value::Number(Number::Int(i));
	let text = |s: &str| Value::String(s.into());
	let mut out = Vec::new();
	for segments in PATHS {
		for op in OPERATORS {
			for literal in literals() {
				out.push(leaf(path(segments), op.clone(), literal, false));
			}
		}
		for literal in [int(1), text("one")] {
			out.push(leaf(path(segments), LessThan, literal, true));
		}
		out.push(set_membership(
			path(segments),
			Inside,
			Arc::new(HashSet::from([int(1), int(5), text("one")])),
			false,
		));
		out.push(set_membership(
			path(segments),
			NotInside,
			Arc::new(HashSet::from([int(2), text("four")])),
			false,
		));
		for (needle, negated) in [("on", false), ("zz", true)] {
			let evaluator = Arc::new(SubstringMatch {
				finder: memmem::Finder::new(needle.as_bytes()).into_owned(),
				negated,
			});
			out.push(PredNode::LeafStreaming {
				path: path(segments),
				evaluator,
				fallback: LeafFallback {
					op: if negated {
						NotContain
					} else {
						Contain
					},
					literal: text(needle),
					reversed: false,
				},
			});
		}
		for needle in [text("x"), int(1)] {
			let evaluator = Arc::new(ArrayElementContains {
				needle: Arc::new(needle.clone()),
				needle_wire: Arc::new(LiteralWire::from_value(&needle)),
				negated: false,
			});
			out.push(PredNode::LeafStreaming {
				path: path(segments),
				evaluator,
				fallback: LeafFallback {
					op: Contain,
					literal: needle,
					reversed: false,
				},
			});
		}
	}
	let fused = |clauses: Vec<(&str, BinaryOperator, Value)>| PredNode::FusedFlatMapAnd {
		clauses: FusedFlatClauses::try_new(
			clauses
				.into_iter()
				.map(|(key, op, literal)| fused_clause(key.as_bytes().to_vec(), op, literal, false))
				.collect(),
		)
		.expect("sorted unique"),
	};
	let navigate = |segment: &str, child: PredNode| PredNode::NavigatePrefix {
		segment: segment.into(),
		child: Box::new(child),
	};
	let cmp = |segments: &[&str], op: BinaryOperator, literal: Value| {
		leaf(path(segments), op, literal, false)
	};
	out.extend([
		fused(vec![("i", MoreThan, int(0)), ("s", Equal, text("one"))]),
		fused(vec![("absent", Equal, Value::None), ("b", Equal, Value::Bool(true))]),
		fused(vec![("nl", Equal, Value::Null), ("o", NotEqual, int(8))]),
		navigate("obj", cmp(&["x"], Equal, int(1))),
		navigate("obj", fused(vec![("x", MoreThan, int(4)), ("y", Equal, text("z"))])),
		navigate("flex", cmp(&["a", "b"], Equal, int(1))),
		navigate("flex", fused(vec![("extra", Equal, int(1)), ("k", Equal, text("v"))])),
		navigate("i", cmp(&["x"], Equal, int(1))),
		navigate("absent", fused(vec![("x", Equal, Value::None)])),
		navigate("nl", fused(vec![("x", Equal, Value::None)])),
		PredNode::And(vec![cmp(&["i"], MoreThan, int(1)), cmp(&["s"], Equal, text("five"))]),
		PredNode::Or(vec![cmp(&["i"], Equal, int(99)), cmp(&["b"], Equal, Value::Bool(true))]),
		PredNode::Not(Box::new(cmp(&["i"], Equal, int(1)))),
		PredNode::Not(Box::new(PredNode::Or(vec![
			cmp(&["nl"], Equal, Value::Null),
			cmp(&["obj", "x"], LessThan, int(5)),
		]))),
	]);
	out.extend(differential_predicates());
	out
}

/// `value` as the record `decoded` holds, stored as a document, as the filter judges one.
fn as_document(decoded: &Value) -> Vec<u8> {
	let Value::Object(object) = decoded else {
		panic!("a record is an object: {decoded:?}");
	};
	let mut object = object.clone();
	object.remove("id");
	crate::catalog::encode_record(DOCUMENT, None, &Value::Object(object), None).expect("encodes")
}

/// A packed row is judged as the same record stored as a document is, and is rejected only where
/// the predicate is false of the record its whole decode holds.
#[tokio::test]
async fn a_packed_row_is_judged_as_the_same_record_stored_as_a_document() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &rows).await;
	let filters: Vec<(PredNode, PreDecodeFilter)> = predicates()
		.into_iter()
		.map(|node| (node.clone(), PreDecodeFilter::new(node, TEST_DEPTH_LIMIT)))
		.collect();
	let (mut packed, mut rejected, mut differing) = (0usize, 0usize, Vec::new());
	for (key, value) in &rows {
		if !StoredRecord::is_packed_bytes(value) {
			continue;
		}
		packed += 1;
		let decoded = decode_record_value(key, value, Some(&layouts)).expect("a row decodes");
		let document = as_document(&decoded);
		let admitted = AdmittedRow::of(key, value, Some(&layouts));
		assert!(matches!(admitted, AdmittedRow::Packed { .. }), "{admitted:?}");
		let whole = admitted.decode(key, value, Some(&layouts)).expect("a checked row decodes");
		assert!(
			crate::exec::vm::compile_cache::same_value(&whole, &decoded),
			"{whole:?}\n{decoded:?}"
		);
		for (node, filter) in &filters {
			let from_columns = filter.apply(&admitted);
			let from_document = filter.apply(&AdmittedRow::Bytes(&document));
			if from_columns == PreDecodeFilterOutcome::Reject {
				rejected += 1;
				assert_eq!(
					truth(node, &decoded, &[]),
					Some(false),
					"rejected though {node:?} is not false of {decoded:?}"
				);
				assert_eq!(
					from_document,
					PreDecodeFilterOutcome::Reject,
					"rejected from its columns, not as a document: {node:?} over {decoded:?}"
				);
			}
			if from_columns != from_document {
				differing.push(format!(
					"{node:?} over {decoded:?}: {from_columns:?} against {from_document:?}"
				));
			}
		}
	}
	assert!(packed >= 8, "{packed} packed rows");
	assert!(rejected > 1_000, "{rejected} rejections");
	assert!(
		differing.is_empty(),
		"{} differ, among them:\n{}",
		differing.len(),
		differing.iter().take(8).cloned().collect::<Vec<_>>().join("\n")
	);
}

/// Whatever corrupts a packed row, it is judged unknown where the whole decode refuses it, and
/// rejected only where the whole decode of what is left holds a record the predicate is false of.
#[tokio::test]
async fn a_corrupt_packed_row_is_not_rejected() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = unverified_layouts_of(&db, "p", &rows).await;
	let filters: Vec<(PredNode, PreDecodeFilter)> = predicates()
		.into_iter()
		.step_by(7)
		.map(|node| (node.clone(), PreDecodeFilter::new(node, TEST_DEPTH_LIMIT)))
		.collect();
	let (mut refused, mut judged, mut rejected) = (0usize, 0usize, 0usize);
	let mut check = |key: &[u8], bytes: &[u8]| {
		let admitted = AdmittedRow::of(key, bytes, Some(&layouts));
		let decoded = decode_record_value(key, bytes, Some(&layouts));
		// The record an admission decodes is the whole decode's, and its refusal the decode's.
		match (admitted.decode(key, bytes, Some(&layouts)), &decoded) {
			(Ok(whole), Ok(decoded)) => {
				assert!(
					crate::exec::vm::compile_cache::same_value(&whole, decoded),
					"{whole:?}\n{decoded:?}"
				);
			}
			(Err(refused), Err(error)) => {
				assert_eq!(refused.to_string(), error.to_string(), "{bytes:02x?}");
			}
			(whole, decoded) => panic!("{whole:?} where the whole decode is {decoded:?}"),
		}
		for (node, filter) in &filters {
			let outcome = filter.apply(&admitted);
			match (&admitted, &decoded) {
				(AdmittedRow::Refused, Err(_)) => {
					refused += 1;
					assert_eq!(outcome, PreDecodeFilterOutcome::NeedFullDecode, "{bytes:02x?}");
				}
				(
					AdmittedRow::Packed {
						..
					},
					Ok(decoded),
				) => {
					judged += 1;
					if outcome == PreDecodeFilterOutcome::Reject {
						rejected += 1;
						assert_eq!(
							truth(node, decoded, &[]),
							Some(false),
							"{node:?} over {decoded:?}"
						);
					}
				}
				// A mutant that is no longer a packed row is a record in no encoding the filter
				// reads: it is kept for the decode to refuse.
				(AdmittedRow::Bytes(_), Err(_)) => {
					assert_ne!(outcome, PreDecodeFilterOutcome::Reject, "{bytes:02x?}");
				}
				_ => panic!("{admitted:?} where the whole decode is {decoded:?}"),
			}
		}
	};
	for (key, value) in &rows {
		if !StoredRecord::is_packed_bytes(value) {
			continue;
		}
		for at in 3..value.len() {
			for bit in 0..8 {
				let mut mutant = value.clone();
				mutant[at] ^= 1 << bit;
				check(key, &mutant);
			}
		}
		for length in 0..value.len() {
			check(key, &value[..length]);
		}
	}
	assert!(refused > 1_000, "{refused} refused");
	assert!(judged > 1_000, "{judged} judged");
	assert!(rejected > 100, "{rejected} rejected");
}
