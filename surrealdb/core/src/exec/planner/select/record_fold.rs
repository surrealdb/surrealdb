//! Plan-time folding of row-independent record-path traversals.
//!
//! After bind parameters are substituted into a `WHERE` predicate, a
//! comparison operand can be a path rooted at a literal record id —
//! `$scan.task.finishedAt` becomes `(scan:one).task.finishedAt`. The value of
//! such a path does not depend on the row being scanned, but index analysis
//! ([`IndexAnalyzer`](crate::exec::index::analysis::IndexAnalyzer)) only
//! matches `path op literal` comparisons, so the predicate would otherwise
//! plan as a full table scan.
//!
//! [`Planner::fold_constant_record_paths`] resolves those paths to their
//! values at plan time — through the same transaction the plan executes
//! against, so the values match what any per-row evaluation would read — and
//! replaces them with literals.
//!
//! # Parity contract
//!
//! Folding replaces the per-row runtime walk
//! ([`evaluate_field`](crate::exec::vm::access::field)) with one plan-time walk,
//! so it must produce exactly the value the runtime walk would produce. The
//! fold therefore only runs when the two are provably equivalent; otherwise
//! it leaves the path unfolded, and the predicate keeps its per-row
//! evaluation over a table scan:
//!
//! - Permission checks must be disabled at execute time ([`Planner::should_check_perms_for_view`]
//!   returns `false`): the runtime dereference applies table- and field-level SELECT permissions,
//!   which the plan-time walk does not replicate.
//! - Every table the walk fetches from must define no computed fields: the runtime dereference
//!   evaluates computed fields even when permission checks are disabled (see
//!   [`process_fetched_record`](crate::exec::operators::fetch)).
//! - The path must be a literal record id followed by plain field accesses, and every intermediate
//!   value must be an object, a record id (fetched, honouring the [`RecordIdKey::Object`]
//!   key-component shortcut), or a scalar (which yields `NONE`, as at runtime). Arrays and
//!   geometries have their own traversal semantics and are left unfolded.
//! - The folded value must have a literal form that evaluates back to the same value (see
//!   [`literal_if_round_trips`]): a closure loses its captures and a set becomes an array, and
//!   substituting a lossy form changes results.
//! - The walk never descends into a scalar subquery or a closure body (see [`is_fold_boundary`]):
//!   each evaluates in its own scope at execution time, possibly after writes.
//! - The call site additionally requires the whole statement to be read-only — a write anywhere in
//!   the statement could change a record between the plan-time read and the per-row evaluation the
//!   fold replaces — and excludes versioned SELECTs, including an enclosing version context: the
//!   plan-time walk reads current state, not a versioned snapshot.

use super::super::Planner;

impl<'ctx> Planner<'ctx> {}
