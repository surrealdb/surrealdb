//! Kind-directed simplification: a cast to a kind an expression is checked to
//! have, and a broadcast over a value that is not a collection, go.
//!
//! - **Cast elision.** `<k> e` where [`LogicalExpr::checked_kind`] of `e` against the row's schema
//!   is `k`, and `k` is a kind a cast checks rather than converts to — not `any`, not an `either`
//!   or `option`, not a literal kind — is `e`: a cast of a value to its own kind yields the value.
//!   A checked kind is one the value is known to have, not one it is declared to have: a literal's,
//!   a cast's target, a comparison's `bool`, or the kind of a column a layer coerces on landing
//!   ([`FieldSchema::checked`](crate::expr::logical::FieldSchema)). A stored field's declared kind
//!   is enforced on write alone, so a value stored before the declaration, or before `DEFINE FIELD
//!   OVERWRITE` changed it, holds what it held; `<int> a` over a field declared `TYPE int` stays
//!   and converts the value at evaluation. A `SCHEMALESS` table's declared fields are typed like a
//!   `SCHEMAFULL` table's; its undeclared fields are `any`.
//! - **Broadcast resolution.** A [`LogicalExpr::Broadcast`] whose base is *checked* to be of a kind
//!   that is definitely not a collection — not `any`, `array`, `set`, an `either` or a literal kind
//!   — maps over nothing: its body is evaluated once with the base as the current value. The base's
//!   kind is read the same way a cast's operand is, through [`LogicalExpr::checked_kind`]: a value
//!   stored before its field was declared, or before `DEFINE FIELD OVERWRITE` retyped it, holds an
//!   array whatever the declaration now says, and the broadcast is what maps the body over its
//!   elements. It becomes the body with [`LogicalExpr::Current`] replaced by the base, when that
//!   replacement is faithful: the body reads the current value only through `Current` at its own
//!   level — no field read, no document, no `$this` or `$self` — since inside the broadcast those
//!   read the base and outside it they would read the row. The `flatten` flag applies to an array
//!   base alone and is dropped with the broadcast.
//!
//! The rule rewrites the scalars of every operator against the schema of the
//! row they are evaluated on ([`super::rewrite_row_positions`]); inside a body evaluated
//! against another value the schema is open, so only a literal's cast fires
//! there. A `MATCH` plan's scalars are left as they are.

use super::{ROW_PARAMS, RuleContext, RuleDef, rewrite_positions, visit_positions};
use crate::expr::logical::{LogicalExpr, LogicalOp, OpKind, Schema};
use crate::expr::{Kind, PrefixOperator};

pub(super) const RULE: RuleDef = RuleDef {
	name: "kind",
	// A cast or a broadcast can sit in any node's scalars.
	kinds: OpKind::ALL,
	apply,
};

fn apply(node: &mut LogicalOp, _cx: &mut RuleContext<'_>) -> bool {
	super::rewrite_row_positions(node, is_candidate, simplified)
}

/// Whether `expr` is an expression the rule may rewrite: a cast or a
/// broadcast.
fn is_candidate(expr: &LogicalExpr) -> bool {
	matches!(
		expr,
		LogicalExpr::Prefix {
			op: PrefixOperator::Cast(_),
			..
		} | LogicalExpr::Broadcast { .. }
	)
}

/// `expr` simplified, when it is a cast to a kind its operand is checked to
/// have or a broadcast over a value that is not a collection.
fn simplified(expr: &LogicalExpr, schema: &Schema) -> Option<LogicalExpr> {
	match expr {
		LogicalExpr::Prefix {
			op: PrefixOperator::Cast(kind),
			expr: inner,
		} => {
			if checks_only(kind) && inner.checked_kind(schema).as_ref() == Some(kind) {
				Some((**inner).clone())
			} else {
				None
			}
		}
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => {
			let base_is_scalar =
				base.checked_kind(schema).is_some_and(|kind| is_scalar_kind(&kind));
			if !base_is_scalar || !reads_current_only(body) {
				return None;
			}
			let mut out = (**body).clone();
			rewrite_positions(&mut out, true, &mut |e, at_row| {
				if at_row && matches!(e, LogicalExpr::Current) {
					*e = (**base).clone();
				}
			});
			Some(out)
		}
		LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

/// Whether a cast to `kind` of a value already of that kind yields the value:
/// the kind names one representation, so the cast has nothing to convert.
fn checks_only(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Either(_) | Kind::Literal(_) => false,
		Kind::Array(inner, _) | Kind::Set(inner, _) => checks_only(inner),
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_) => true,
	}
}

/// Whether every value of `kind` is a single value a broadcast does not map
/// over.
fn is_scalar_kind(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Array(..) | Kind::Set(..) | Kind::Either(_) | Kind::Literal(_) => false,
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_) => true,
	}
}

/// Whether `body`, at its own level, reads the value it is evaluated against
/// only through [`LogicalExpr::Current`], so that binding `Current` to the
/// base and evaluating the body against the row reads the same values.
fn reads_current_only(body: &LogicalExpr) -> bool {
	visit_positions(body, true, &mut |e, at_row| {
		let reads_value = at_row
			&& (matches!(e, LogicalExpr::Field(_) | LogicalExpr::Doc)
				|| matches!(e, LogicalExpr::Param(param) if ROW_PARAMS.contains(&param.as_str())));
		if reads_value {
			std::ops::ControlFlow::Break(())
		} else {
			std::ops::ControlFlow::Continue(())
		}
	})
	.is_continue()
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::super::super::lower_test as lower;
	use super::super::test_support::{resolved, rewritten};
	use super::*;
	use crate::expr::logical::{FieldPath, FieldSchema, LogicalFilter, LogicalProject};

	/// A table's declared fields: their kinds are declared, not checked.
	fn typed() -> Schema {
		Schema::closed(vec![
			FieldSchema::new(FieldPath::field("id"), Kind::Record(vec![TableName::from("t")])),
			FieldSchema::new(FieldPath::field("a"), Kind::Int),
			FieldSchema::new(FieldPath::field("o"), Kind::option(Kind::Int)),
			FieldSchema::new(FieldPath::field("obj"), Kind::Object),
			FieldSchema::new(FieldPath::field("objs"), Kind::Array(Box::new(Kind::Object), None)),
		])
	}

	/// The same fields as columns a layer coerces to their kinds on landing.
	fn checked() -> Schema {
		Schema::closed(
			typed()
				.fields
				.into_iter()
				.map(|field| FieldSchema::checked(field.path, field.kind))
				.collect(),
		)
	}

	/// The predicate of the filter of `SELECT * FROM t WHERE …`, rewritten
	/// over `schema`.
	fn predicate_over(sql: &str, schema: &Schema) -> LogicalExpr {
		let plan = rewritten(resolved(lower(sql), "t", schema));
		let mut found = None;
		let _ = plan.visit(&mut |node| {
			if let LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			}) = node
			{
				found = Some(predicate.clone());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		found.expect("a filter")
	}

	#[test]
	fn a_cast_to_the_checked_kind_is_elided() {
		let predicate = predicate_over("SELECT * FROM t WHERE <int> a = 2", &checked());
		assert!(
			matches!(&predicate, LogicalExpr::Binary { left, .. } if matches!(left.as_ref(), LogicalExpr::Field(name) if name.as_str() == "a")),
			"{predicate}"
		);
		assert_eq!(predicate.to_string(), "a = 2");
		// A cast's target, and a comparison's `bool`, are checked kinds.
		let predicate = predicate_over("SELECT * FROM t WHERE <int> <int> a = 2", &typed());
		assert_eq!(predicate.to_string(), "<int> a = 2");
		let predicate = predicate_over("SELECT * FROM t WHERE <bool> (a = 2)", &typed());
		assert_eq!(predicate.to_string(), "a = 2");
	}

	#[test]
	fn a_cast_of_a_stored_field_is_kept_at_its_declared_kind() {
		// The declaration is enforced on write; the stored value may predate it.
		let predicate = predicate_over("SELECT * FROM t WHERE <int> a = 2", &typed());
		assert_eq!(predicate.to_string(), "<int> a = 2");
	}

	#[test]
	fn a_cast_of_an_option_or_open_field_is_kept() {
		let predicate = predicate_over("SELECT * FROM t WHERE <int> o = 2", &checked());
		assert_eq!(predicate.to_string(), "<int> o = 2");
		let predicate = predicate_over("SELECT * FROM t WHERE <int> a = 2", &Schema::open());
		assert_eq!(predicate.to_string(), "<int> a = 2");
		// The kind must match exactly: a cast that widens stays.
		let predicate = predicate_over("SELECT * FROM t WHERE <number> a = 2", &checked());
		assert_eq!(predicate.to_string(), "<number> a = 2");
	}

	#[test]
	fn a_broadcast_over_an_object_column_is_resolved_and_over_an_array_kept() {
		// `obj.b.len()`: a mapping step followed by a method broadcasts over
		// `obj`; checked to be an object, `obj` is one value and the body
		// applies to it directly. The resolved form renders parenthesised,
		// because `obj.b.len()` is the spelling of the broadcast this rule
		// just removed.
		let predicate = predicate_over("SELECT * FROM t WHERE obj.b.len() = 1", &checked());
		assert!(!predicate.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{predicate}");
		assert_eq!(predicate.to_string(), "(obj.b).len() = 1");
		let predicate = predicate_over("SELECT * FROM t WHERE objs.b.len() = 1", &checked());
		assert!(predicate.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{predicate}");
		let predicate = predicate_over("SELECT * FROM t WHERE obj.b.len() = 1", &Schema::open());
		assert!(predicate.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{predicate}");
	}

	#[test]
	fn a_broadcast_over_an_object_literal_is_resolved_and_over_an_array_literal_kept() {
		// A substituted `$o.b[1]`: an object literal is one value whatever its
		// fields hold, so the body applies to it directly.
		let predicate = predicate_over("SELECT * FROM t WHERE a = { b: [1, 2] }.b[1]", &typed());
		assert!(!predicate.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{predicate}");
		let predicate = predicate_over("SELECT * FROM t WHERE a = [{ b: 1 }].b[0]", &typed());
		assert!(predicate.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{predicate}");
	}

	#[test]
	fn a_broadcast_over_a_stored_field_is_kept_at_its_declared_kind() {
		// The declaration is enforced on write; a value stored before it, or
		// before `DEFINE FIELD OVERWRITE` retyped the field, is still an array
		// and the broadcast is what maps the body over its elements.
		let predicate = predicate_over("SELECT * FROM t WHERE obj.b.len() = 1", &typed());
		assert!(predicate.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{predicate}");
	}

	#[test]
	fn a_body_reading_the_current_value_by_name_keeps_the_broadcast() {
		// Inside the broadcast `x` is a field of `obj`; outside it would be a
		// field of the row.
		let body = LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Access {
				base: Box::new(LogicalExpr::Current),
				field: "b".into(),
			}),
			op: crate::expr::BinaryOperator::Add,
			right: Box::new(LogicalExpr::Field("x".into())),
		};
		let broadcast = LogicalExpr::Broadcast {
			base: Box::new(LogicalExpr::Field("obj".into())),
			body: Box::new(body),
			flatten: false,
		};
		let plan = LogicalOp::Project(LogicalProject {
			input: Box::new(resolved(lower("SELECT * FROM t"), "t", &typed())),
			fields: crate::expr::logical::LogicalFields::Select(vec![
				crate::expr::logical::LogicalField::user(broadcast.clone(), None),
			]),
			omit: vec![],
		});
		let after = rewritten(plan);
		assert!(
			after.any(|node| {
				let mut found = false;
				let Ok(()) = node.visit_scalars(&mut |s| {
					if s == &broadcast {
						found = true;
					}
					Ok::<_, std::convert::Infallible>(())
				});
				found
			}),
			"{after}"
		);
	}
}
