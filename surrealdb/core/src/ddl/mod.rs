//! Schema statements.
//!
//! `DEFINE`, `REMOVE`, `ALTER` and `REBUILD` write the catalog, and this module
//! is where each of them does it. They are the one family of statements with no
//! operator of their own: a plan binds against the catalog once, so a statement
//! that changes the catalog is executed rather than planned, and the plan that
//! holds it carries it whole. `ACCESS` ([`grant`]) is one of the family by the
//! same reasoning — it reads and writes the grants an access method holds —
//! and the executor runs it directly rather than through a plan.
//!
//! What a schema statement writes *about* — the name of the thing, the table it
//! hangs off, the field paths an index covers — is written as an expression and
//! has to be resolved before the catalog write can be formed. [`scalar`] is
//! that resolution, and [`frame`] is the execution context it runs in, built
//! from the frame the statement was reached under.
//!
//! A schema statement can also have records to write once the catalog entry
//! stands: defining a view backfills it from the rows its sources already hold.
//! [`run_select`] reads those rows and [`ViewRows`] runs over each written row
//! the same lifecycle an incremental maintenance runs.

pub(crate) mod alter;
pub(crate) mod define;
pub(crate) mod grant;
pub(crate) mod key_scan;
pub(crate) mod materialize;

/// The general route into the engine, named here because a schema
/// statement is its most frequent caller. See [`crate::exec::entry`].
pub(crate) use crate::exec::entry::frame;
pub(crate) mod rebuild;
pub(crate) mod remove;
pub(crate) mod scalar;
pub(crate) mod subscriptions;

pub(crate) use self::define::access::validate_access_context;
pub(crate) use self::define::field::{
	define_field_statement_disallow_mismatched_types,
	define_field_statement_process_recursive_definitions,
	define_field_statement_validate_computed_cycles,
	define_field_statement_validate_computed_options,
	define_field_statement_validate_computed_views,
	define_field_statement_validate_flexible_restrictions,
	define_field_statement_validate_key_components,
};
pub(crate) use self::define::user::{
	define_user_statement_compute, define_user_statement_to_definition,
};
pub(crate) use self::grant::{access_statement_compute, create_grant, revoke_grant};
