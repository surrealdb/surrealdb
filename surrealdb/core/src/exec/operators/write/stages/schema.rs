//! The table's field schema, applied to one record.
//!
//! The field kernel: types, defaults, `VALUE`, `ASSERT`, the cleanup of what the schema leaves
//! empty, and the `COMPUTED` derivation the record's observers read.
//!
//! Definer's-rights narrowing applies here (ADR 0001): a field clause runs
//! with the definer's authority, not the caller's, and so does the decision of
//! whether that field's `PERMISSIONS` are enforced at all. The narrowing is
//! per field, because the fields of one table need not share a definer.
//!
//! # A stored clause needs a compilation route
//!
//! The kernel evaluates clauses the catalog stores as text, and the
//! engine reaches one through [`crate::exec::permission`], which compiles the
//! logical body the definition carries and, for a clause carrying none,
//! carries it as an [`UncompilableClause`](crate::exec::permission::UncompilableClause)
//! that fails where it is evaluated. A field carries a logical body for its
//! `DEFAULT`, its `VALUE`, its `ASSERT`, its `COMPUTED` and its `PERMISSIONS
//! FOR select`; its `PERMISSIONS FOR create` / `update` predicate carries none
//! and is therefore always the carried form. [`clause_frame`] binds the
//! parameters every clause reads, so a clause means the same thing whichever
//! form it takes. A clause that fails closed fails the write that reaches it
//! rather than letting the record through unjudged.

use std::collections::HashSet;

use crate::catalog::{FieldDefinition, TableDefinition, ViewDefinition};
use crate::doc::Error as DocError;
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::effects::effects_of_expr;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::ControlFlow;
use crate::expr::idiom::{Idiom, IdiomTrie, IdiomTrieContains};
use crate::expr::kind::{Kind, KindLiteral};
use crate::expr::logical::{EffectSet, LogicalExpr};
use crate::expr::part::Part;
use crate::expr::statements::define::kind_contains_object;
use crate::val::Value;
use crate::val::value::every::ArrayBehaviour;

/// Whether a place holds nothing under a kind that admits nothing.
///
/// Two rules read it. An absent optional parent makes its children skippable,
/// because there is no object for their clauses to be about; and an `ASSERT`
/// is not judged against an absent optional value, because the field declared
/// that absence acceptable.
pub(crate) fn optional_and_absent(fd: &FieldDefinition, val: &Value) -> bool {
	val.is_none() && fd.field_kind.as_ref().is_some_and(Kind::can_be_none)
}

/// Restore the uniqueness and order of every set holding a value the fields
/// `fields` rewrote.
///
/// A nested field — `field[*]`, `field[*][*]`, `field[*].x` — is applied to
/// each value its path reaches, in place, and coercion or a `VALUE` clause can
/// make two distinct set elements equal or move one out of its sorted
/// position. Every set a path passes through holds a rewritten value,
/// directly or within one of its own elements, so each is rebuilt in place,
/// innermost first across every path (see [`Value::normalize_sets_along`]),
/// so that no positional step of one field's path is reordered under it by
/// another's rebuild. A set is never written back through a path carrying
/// `[*]`: `put` writes one value into every element such a path reaches.
pub(crate) fn normalise_set_elements<'a>(
	doc: &mut Value,
	fields: impl IntoIterator<Item = &'a FieldDefinition>,
) {
	// A single-part path names a top-level field, which no set holds.
	let paths: Vec<&[Part]> = fields
		.into_iter()
		.map(|fd| fd.name.0.as_slice())
		.filter(|parts| parts.len() >= 2)
		.collect();
	doc.normalize_sets_along(&paths);
}

/// Raise on the first path of the post-image that the table's field
/// definitions do not reach.
///
/// The table is `SCHEMAFULL`, so an undefined field is refused rather than
/// dropped: a record that silently lost a field would be stored as something
/// other than what the caller wrote.
///
/// A definition reaches the places its path names, and a kind that holds a
/// literal object or array also reaches the places beneath its value that the
/// literal describes ([`describe`]). Each place admits an undefined interior
/// as the kind that describes it does: `any` admits one, an object admits one
/// only when the field is `FLEXIBLE`, and a literal admits only the places it
/// names. A member of a union is held to this alone, so a literal member opens
/// nothing a value of another member holds.
pub(crate) fn refuse_undefined_fields(
	current: &Value,
	fields: &[FieldDefinition],
	table: &crate::val::TableName,
) -> Result<(), ControlFlow> {
	if flat_and_defined(current, fields) {
		return Ok(());
	}
	// Which paths a definition reaches, and whether paths *below* each one are
	// admitted without a definition of their own.
	let mut defined: IdiomTrie<bool> = IdiomTrie::new();
	// A path that carries its own definition is never treated as the
	// schemaless interior of an ancestor, however permissive that ancestor is.
	let explicit: HashSet<&[Part]> = fields.iter().map(|fd| fd.name.0.as_slice()).collect();

	// Every place a definition's path names, with whether it admits an
	// undefined interior. The places a literal describes are entered while
	// these are gathered, and these are entered after them, so a definition
	// of a place overrides what a kind holding the place says of it,
	// whichever of the two is defined first.
	let mut named: Vec<(Idiom, bool)> = Vec::with_capacity(fields.len());
	for fd in fields.iter() {
		let field_kind = fd.field_kind.as_ref();
		for k in current.each(&fd.name) {
			let admits = match field_kind {
				Some(kind) if describes_places(kind) => {
					let mut admits = false;
					let mut place = k.0.clone();
					current.walk_ref(&k, &mut |value| {
						admits = describe(kind, value, fd.flexible, &mut place, &mut defined);
					});
					admits
				}
				kind => admits_interior(kind, fd.flexible),
			};
			named.push((k, admits));
		}
	}

	for (k, admits) in named.iter() {
		defined.insert(k, *admits);
		// The ancestors of an expanded path are reachable too. One no
		// definition reaches is a schemaless object on the way to a defined
		// field, so its interior is admitted. One a definition does reach —
		// an element `a[0]` of a defined `a.*`, or a key a literal names,
		// among them — keeps what that definition admits, whichever of the
		// two is expanded first.
		for i in 1..k.len() {
			let reached = matches!(defined.contains(&k[..i]), IdiomTrieContains::Exact(_));
			if !explicit.contains(&&k[..i]) && !reached {
				defined.insert(&k[..i], true);
			}
		}
	}

	for path in current.every(None, true, ArrayBehaviour::Full).iter() {
		// `id`, `in` and `out` are the record's own, not the schema's.
		if path.is_special() {
			continue;
		}
		match defined.contains(path) {
			// Defined, or below something that admits an undefined interior.
			IdiomTrieContains::Exact(_) | IdiomTrieContains::Ancestor(true) => continue,
			// Below a definition that admits no undefined interior. An array
			// index is addressed by the definition of the array itself, so it
			// needs none of its own; anything else is undefined.
			IdiomTrieContains::Ancestor(false) if path.last().is_some_and(Part::is_index) => {
				continue;
			}
			// Below nothing the schema defines at all. The index exemption does
			// not extend here: an index reached from no definition is not
			// addressed by one, and the path that carries it is refused on its
			// own account only when the schema reaches that path.
			IdiomTrieContains::Ancestor(false) | IdiomTrieContains::None => {
				return Err(ControlFlow::Err(anyhow::anyhow!(DocError::FieldUndefined {
					table: table.as_str().to_string(),
					field: path.clone()
				})));
			}
		}
	}
	Ok(())
}

/// Whether `current` is an object each of whose places is a key holding
/// neither an object nor an array, every key being the record's own (`id`,
/// `in`, `out`) or the whole path of a field definition.
///
/// Such a record holds no undefined field: each of its paths is a key a
/// definition names exactly or a special one, so [`refuse_undefined_fields`]
/// admits it, and answers so here without building its trie. `false` says
/// only that the full judgement is needed.
fn flat_and_defined(current: &Value, fields: &[FieldDefinition]) -> bool {
	let Value::Object(object) = current else {
		return false;
	};
	object.0.iter().all(|(key, value)| {
		!matches!(value, Value::Object(_) | Value::Array(_))
			&& (matches!(key.as_str(), "id" | "in" | "out")
				|| fields.iter().any(
					|fd| matches!(fd.name.0.as_slice(), [Part::Field(name)] if name.as_str() == key.as_str()),
				))
	})
}

/// Whether a field of kind `kind`, `FLEXIBLE` when `flexible`, admits paths
/// beneath its value that no definition reaches: `TYPE any` and a union with
/// an `any` member do ([`admits_anything`]), a kind that names a type does
/// (the type's coercion judges the shape of what it holds), a kind holding an
/// object does when the field is `FLEXIBLE`, and no other kind, a field with
/// no `TYPE` among them, does.
fn admits_interior(kind: Option<&Kind>, flexible: bool) -> bool {
	kind.is_some_and(|kind| {
		admits_anything(kind) || kind.has_named_type() || (flexible && kind_contains_object(kind))
	})
}

/// Whether `kind` is `any` or a union with an `any` member, nested unions
/// included: a kind every value is an instance of.
fn admits_anything(kind: &Kind) -> bool {
	if let Kind::Any = kind {
		true
	} else if let Kind::Either(members) = kind {
		members.iter().any(admits_anything)
	} else {
		false
	}
}

/// Whether `kind` describes places beneath a value of it that no definition
/// names: it holds a literal object or array, or an array or set whose
/// elements are of a kind that admits anything ([`admits_anything`]), as
/// `array`, `array<any>` and `array<array<any>>` hold, each as a member of a
/// union or the element kind of an array or set. An element of `any` is a
/// place whose interior is its own, so a value in it holds whatever keys and
/// elements it is written with.
fn describes_places(kind: &Kind) -> bool {
	if let Kind::Literal(KindLiteral::Object(_) | KindLiteral::Array(_)) = kind {
		true
	} else if let Kind::Either(members) = kind {
		members.iter().any(describes_places)
	} else if let Kind::Array(element, _) | Kind::Set(element, _) = kind {
		admits_anything(element) || describes_places(element)
	} else {
		false
	}
}

/// Enter into `defined` the places beneath `value`, the value of kind `kind`
/// at `place`, that `kind` describes, each with whether it admits an
/// undefined interior, and answer whether `place` itself admits one, for a
/// field that is `FLEXIBLE` when `flexible`.
///
/// A literal object describes the keys of the value it names, a literal array
/// the elements its length covers, and an array or set kind the elements of an
/// array, each judged by its own kind in turn, so an element of `any` admits
/// whatever it holds. A kind that describes no places ([`describes_places`])
/// describes nothing beneath it and is judged as a field of that kind would be
/// ([`admits_interior`]). A union judges the value by each
/// member the value conforms to, so a member the value is not an instance of
/// says nothing of it. A value no member conforms to, which an import replays
/// without coercing, is judged by every member. `place` is left as it was
/// found.
fn describe(
	kind: &Kind,
	value: &Value,
	flexible: bool,
	place: &mut Vec<Part>,
	defined: &mut IdiomTrie<bool>,
) -> bool {
	if !describes_places(kind) {
		return admits_interior(Some(kind), flexible);
	}
	match (kind, value) {
		(Kind::Either(members), value) => {
			let mut admits = false;
			let mut conformed = false;
			// The members the value conforms to, less any that would convert
			// it when the member it is taken as does not.
			let taken = value.union_member(members, Value::can_coerce_to_kind);
			let converts = taken.is_some_and(|k| value.converts_to(k));
			for member in members.iter().filter(|member| {
				value.can_coerce_to_kind(member) && (converts || !value.converts_to(member))
			}) {
				conformed = true;
				admits |= describe(member, value, flexible, place, defined);
			}
			if !conformed {
				for member in members.iter() {
					admits |= describe(member, value, flexible, place, defined);
				}
			}
			admits
		}
		(Kind::Literal(KindLiteral::Object(shape)), Value::Object(object)) => {
			for (key, kind) in shape.iter() {
				if let Some(value) = object.get(key.as_str()) {
					describe_held(Part::Field(key.clone()), kind, value, flexible, place, defined);
				}
			}
			false
		}
		(Kind::Literal(KindLiteral::Array(kinds)), Value::Array(array)) => {
			for (i, (kind, value)) in kinds.iter().zip(array.iter()).enumerate() {
				describe_held(Part::index_int(i as i64), kind, value, flexible, place, defined);
			}
			false
		}
		(Kind::Array(element, _) | Kind::Set(element, _), Value::Array(array)) => {
			for (i, value) in array.iter().enumerate() {
				describe_held(Part::index_int(i as i64), element, value, flexible, place, defined);
			}
			false
		}
		// A value of another shape than the kind holds nothing the kind
		// describes; `Value::every` lists nothing within a set.
		_ => false,
	}
}

/// Enter the place `part` names beneath `place`, holding `value` of kind
/// `kind`, into `defined` with what [`describe`] says of it. A place more than
/// one kind describes admits an undefined interior when any of them admits
/// one.
fn describe_held(
	part: Part,
	kind: &Kind,
	value: &Value,
	flexible: bool,
	place: &mut Vec<Part>,
	defined: &mut IdiomTrie<bool>,
) {
	place.push(part);
	let admits = describe(kind, value, flexible, place, defined)
		|| matches!(defined.contains(place), IdiomTrieContains::Exact(true));
	defined.insert(place, admits);
	place.pop();
}

/// Remove `NONE` recursively from objects, leaving array elements alone.
///
/// Answers whether `v` survives: an object or array always does, a `NONE`
/// never does, and anything else does. An array is walked so that a `NONE`
/// nested inside one of its objects is still removed, but a `NONE` that *is* an
/// element stays — dropping it would shift every element after it.
pub(crate) fn clean_none(v: &mut Value) -> bool {
	match v {
		Value::None => false,
		Value::Object(o) => {
			o.retain(|_, v| clean_none(v));
			true
		}
		Value::Array(x) => {
			x.iter_mut().for_each(|x| {
				clean_none(x);
			});
			true
		}
		Value::Null
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
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Closure(_) => true,
	}
}

/// Whether anything other than the statement's own projection reads this record
/// after it is written: a table event, a live-query subscription, a changefeed
/// on the table or on its database, or a foreign view the write maintains.
///
/// A view reads the images the write hands it as they are, and what it may read
/// off them is not stored: a `COMPUTED` field, and a component of a record id
/// that names its components, are derived fields an image carries only once the
/// observers' derivation has run. A view recomputes its row from the post-image
/// and, for an aggregated one, the group the record leaves from the pre-image,
/// so it reads both. The views that count are those [`views_can_observe`]
/// admits, and a view of a table whose `id` names its components.
///
/// Under `OPTION IMPORT` nothing does. An import runs no observer, so deriving
/// the computed values one would read evaluates bodies whose result is
/// discarded — and a body reading a table the dump defines later raises, which
/// would fail a restore of a database's own export.
pub(crate) fn has_record_observers(ctx: &WriteCtx) -> bool {
	if ctx.exec().options().is_some_and(|options| options.import) {
		return false;
	}
	!ctx.events.is_empty()
		|| ctx.live_observed
		|| ctx.table.changefeed.is_some()
		|| ctx.db.changefeed.is_some()
		|| ctx.views_observe
		|| (ctx.key.is_some() && !ctx.views.is_empty())
}

/// Whether the foreign views of a table may be handed images with its `COMPUTED`
/// fields derived onto them: at least one view is maintained, and none of them
/// can be made to fail by the derivation.
///
/// A materialized view reads the post-image alone: its row is keyed by the
/// record's id, so an update rewrites it and a delete removes it whatever the
/// pre-image holds. An aggregated view leaves the group the record was in by
/// the group its pre-image derives to, so a pre-image whose derived value is not
/// the one the record joined its group with names a group the view never held
/// and fails the write that finds none. That holds only when every computed
/// field derives the same value from the same record each time, which the
/// derivation hands every view of the table alike, so a table with such a view
/// counts none of them.
///
/// A view that nothing maintains ([`ViewDefinition::Select`], which the views
/// stage skips) reads nothing and does not count.
pub(crate) fn views_can_observe(
	views: &[TableDefinition],
	fields: &[FieldDefinition],
	registry: &FunctionRegistry,
) -> bool {
	let mut maintained = false;
	let mut reproducible = None;
	for view in views {
		match &view.view {
			Some(ViewDefinition::Materialized {
				..
			}) => maintained = true,
			Some(ViewDefinition::Aggregated {
				..
			}) => {
				if !*reproducible.get_or_insert_with(|| derives_from_the_record(fields, registry)) {
					return false;
				}
				maintained = true;
			}
			// Matched without a wildcard, so a new kind of view has to be
			// classified here as it is where the views stage lowers each kind.
			Some(ViewDefinition::Select {
				..
			})
			| None => {}
		}
	}
	maintained
}

/// What a computed body may read and still derive the same value from the same
/// record on every evaluation: nothing outside the record. Another record, the
/// catalog and the session are each stable within one statement and change
/// between two.
const FROM_OUTSIDE_THE_RECORD: EffectSet =
	EffectSet::READS_DATA.union(EffectSet::READS_CATALOG).union(EffectSet::READS_SESSION);

/// Whether every `COMPUTED` field of the table derives the same value from the
/// same record on every evaluation. A body with no logical form is taken not to.
fn derives_from_the_record(fields: &[FieldDefinition], registry: &FunctionRegistry) -> bool {
	fields.iter().filter(|field| field.computed.is_some()).all(|field| {
		let Some(Ok(body)) = field.computed_logical() else {
			return false;
		};
		let effects = effects_of_expr(body, registry);
		effects.pure()
			&& !effects.set.intersects(FROM_OUTSIDE_THE_RECORD)
			&& !reads_a_parameter(body)
	})
}

/// Whether the body, or a subquery within it, reads a parameter. The effect
/// analysis takes every parameter read to be in hand, so it does not see that a
/// `DEFINE PARAM` or a session parameter such as `$auth` holds what the
/// statement evaluating the body was handed, and that the next write may be
/// handed another. A parameter the body binds for itself, and the row's own
/// (`$this`, `$value`), are refused with the rest: the answer errs towards
/// refusing.
fn reads_a_parameter(body: &LogicalExpr) -> bool {
	body.visit_deep(&mut |node| {
		if matches!(node, LogicalExpr::Param(_)) {
			std::ops::ControlFlow::Break(())
		} else {
			std::ops::ControlFlow::Continue(())
		}
	})
	.is_break()
}

#[cfg(test)]
mod tests {
	use super::{has_record_observers, views_can_observe};
	use crate::catalog::{TableDefinition, ViewDefinition};
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::operators::write::ctx::{TableExistence, WriteCtx};
	use crate::val::TableName;

	/// The `id` of a table that names its components.
	const KEYED: &str = "
		DEFINE TABLE reading SCHEMALESS;
		DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
	";

	/// Whether a write to `table`, in a database `setup` defines, has an
	/// observer that reads the record after it is written.
	async fn observed(setup: &str, table: &str) -> bool {
		let db = TestDb::new(setup).await;
		let exec = db.exec_ctx().await;
		let observed =
			match WriteCtx::resolve(&exec, &TableName::from(table), TableExistence::Required).await
			{
				Ok(ctx) => has_record_observers(&ctx),
				Err(_) => panic!("the table `{table}` resolves"),
			};
		exec.txn().cancel().await.expect("the read transaction ends");
		observed
	}

	#[tokio::test]
	async fn a_keyed_table_nothing_reads_has_no_observer() {
		assert!(!observed(KEYED, "reading").await);
	}

	#[tokio::test]
	async fn a_foreign_view_of_a_keyed_table_is_an_observer() {
		let setup = format!(
			"{KEYED}
			DEFINE TABLE by_city AS SELECT city, count() AS n FROM reading GROUP BY city;"
		);
		assert!(observed(&setup, "reading").await);
	}

	#[tokio::test]
	async fn a_foreign_view_of_a_table_whose_id_names_nothing_is_an_observer() {
		// What the view reads off the image is the table's own `COMPUTED` field.
		let setup = "
			DEFINE TABLE reading SCHEMALESS;
			DEFINE FIELD city ON reading TYPE string;
			DEFINE FIELD label ON reading COMPUTED string::uppercase(city);
			DEFINE TABLE by_label AS SELECT label, count() AS n FROM reading GROUP BY label;
		";
		assert!(observed(setup, "reading").await);
	}

	#[tokio::test]
	async fn a_view_nothing_maintains_is_not_an_observer() {
		let setup = "
			DEFINE TABLE reading SCHEMALESS;
			DEFINE TABLE by_city AS SELECT city, count() AS n FROM reading GROUP BY city;
		";
		let db = TestDb::new(setup).await;
		let exec = db.exec_ctx().await;
		let ctx =
			match WriteCtx::resolve(&exec, &TableName::from("reading"), TableExistence::Required)
				.await
			{
				Ok(ctx) => ctx,
				Err(_) => panic!("the table `reading` resolves"),
			};
		let registry = exec.function_registry();
		assert!(
			views_can_observe(&ctx.views, &ctx.fields, registry),
			"the maintained view is an observer"
		);
		// The classification a stored view degrades to when this version can no
		// longer maintain it: the same clauses, nothing that writes them.
		let inert: Vec<TableDefinition> = ctx
			.views
			.iter()
			.map(|view| {
				let mut view = view.clone();
				let Some(ViewDefinition::Aggregated {
					fields,
					tables,
					condition,
					groups,
					..
				}) = view.view.take()
				else {
					panic!("the view of `reading` is aggregated");
				};
				view.view = Some(ViewDefinition::Select {
					fields,
					tables,
					condition,
					groups: Some(groups),
				});
				view
			})
			.collect();
		assert!(
			!views_can_observe(&inert, &ctx.fields, registry),
			"a view the engine does not maintain reads nothing"
		);
		exec.txn().cancel().await.expect("the read transaction ends");
	}

	/// A table whose computed field `x` has `body`, read by a view of the given
	/// kind, with whatever else the body needs defined first.
	fn computed(body: &str, view: &str) -> String {
		format!(
			"DEFINE FUNCTION fn::own($v: any) {{ RETURN $v; }};
			DEFINE PARAM $group VALUE 'a';
			DEFINE TABLE person SCHEMALESS;
			DEFINE TABLE reading SCHEMALESS;
			DEFINE FIELD src ON reading TYPE option<string>;
			DEFINE FIELD parent ON reading TYPE option<record<person>>;
			DEFINE FIELD x ON reading COMPUTED {body};
			{view}"
		)
	}

	const AGGREGATED: &str = "DEFINE TABLE by_x AS SELECT x, count() AS n FROM reading GROUP BY x;";
	const MATERIALIZED: &str = "DEFINE TABLE as_x AS SELECT x, src FROM reading;";

	/// Computed bodies that derive the same value from the same record each time.
	const FROM_THE_RECORD: [&str; 5] = [
		"src",
		"string::concat(src, '!')",
		"src = 'q'",
		"string::uppercase(src ?? '')",
		"math::abs(1) + 1",
	];

	/// Computed bodies that do not: randomness, the clock, another record, a
	/// body the engine cannot see, the session, a traversal, a parameter that a
	/// `DEFINE PARAM` can change between two writes.
	const NOT_FROM_THE_RECORD: [&str; 10] = [
		"rand::uuid()",
		"time::now()",
		"parent.name",
		"(SELECT VALUE name FROM ONLY person:1)",
		"fn::own(src)",
		"$auth.id",
		"<~person",
		"$group",
		"string::concat(src, $group)",
		"(RETURN $group)",
	];

	/// An aggregated view leaves the group its pre-image derives to, so it is
	/// handed derived images only when every computed body derives the same
	/// value from the same record each time.
	#[tokio::test]
	async fn an_aggregated_view_observes_only_what_derives_from_the_record_alone() {
		for body in FROM_THE_RECORD {
			let setup = computed(body, AGGREGATED);
			assert!(observed(&setup, "reading").await, "`{body}` derives from the record alone");
		}
		for body in NOT_FROM_THE_RECORD {
			let setup = computed(body, AGGREGATED);
			assert!(!observed(&setup, "reading").await, "`{body}` does not");
		}
	}

	/// A materialized view reads the post-image alone, so what a computed body
	/// reads does not decide whether it is handed derived images. This is also
	/// the control for the test above: every body defines and is observed here,
	/// so a `false` there is the body's and not a setup that did not take.
	#[tokio::test]
	async fn a_materialized_view_observes_whatever_the_computed_fields_read() {
		for body in FROM_THE_RECORD.into_iter().chain(NOT_FROM_THE_RECORD) {
			let setup = computed(body, MATERIALIZED);
			assert!(observed(&setup, "reading").await, "`{body}`");
		}
	}

	/// The derivation is handed to every view of the table alike, so one view it
	/// could fail keeps the others from being handed it.
	#[tokio::test]
	async fn one_unsafe_aggregated_view_keeps_every_view_of_the_table_from_observing() {
		let setup = computed("rand::uuid()", &format!("{MATERIALIZED} {AGGREGATED}"));
		assert!(!observed(&setup, "reading").await);
		let setup = computed("src", &format!("{MATERIALIZED} {AGGREGATED}"));
		assert!(observed(&setup, "reading").await);
	}

	#[tokio::test]
	async fn the_other_observers_of_a_keyed_table_still_are() {
		let event = format!("{KEYED} DEFINE EVENT seen ON reading THEN {{}};");
		assert!(observed(&event, "reading").await);
		let feed = "
			DEFINE TABLE reading SCHEMALESS CHANGEFEED 1h;
			DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
		";
		assert!(observed(feed, "reading").await);
	}
}
