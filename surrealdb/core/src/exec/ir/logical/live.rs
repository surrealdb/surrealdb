//! The `LIVE SELECT` statement.
//!
//! A live query registers a subscription and answers its identifier; the expressions of its
//! filter and projection are stored with the subscription and run at notification time under the
//! authority frame the statement was bound with. The statement's shape (fields, diff or full
//! records, the table or record target) is the op's attribute.
//!
//! The subscription as it is stored is the op's template: its identity (`id`), the node that
//! serves it, the authentication and session of the client that issued the statement, and the
//! variables the statement captured are holes the statement fills when it runs. The target is the
//! op's operand; the projection, the `WHERE` and the `FETCH` are `StoredClause` regions.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeMap;

use super::ddl::{Planned, stored_value, template};
use super::ops::LiveAttrs;
use crate::exec::ir::iface::statements::{LiveShape, TemplateHole};
use crate::exec::vm::codec::Codec;
use crate::expr::statements::live::{LiveFields, LiveStatement};
use crate::expr::{Expr, Fields};

/// The holes of a live subscription's template.
const LIVE_HOLES: &[TemplateHole] = &[
	TemplateHole::Other("node"),
	TemplateHole::Other("auth"),
	TemplateHole::Other("session"),
	TemplateHole::Other("vars"),
];

/// A `LIVE` statement ready to bind.
pub(crate) struct LivePlan<'e> {
	pub(crate) attrs: LiveAttrs,
	/// The target, evaluated once.
	pub(crate) target: &'e Expr,
	/// The projection a notification delivers, when the statement selects fields.
	pub(crate) fields: Option<&'e Fields>,
	/// The filter a change must satisfy.
	pub(crate) cond: Option<&'e Expr>,
	/// The paths a notification fetches.
	pub(crate) fetch: Vec<&'e Expr>,
}

/// The attributes, target and stored clauses of `statement`.
pub(crate) fn plan_live<'e>(
	statement: &'e LiveStatement,
	codec: &'static Codec,
) -> Planned<LivePlan<'e>> {
	let (shape, fields, query_fields) = match &statement.fields {
		LiveFields::Diff => (LiveShape::Diff, None, crate::catalog::SubscriptionFields::Diff),
		LiveFields::Select(fields) => (
			LiveShape::Select,
			Some(fields),
			crate::catalog::SubscriptionFields::Select(fields.clone()),
		),
	};
	let definition = crate::catalog::SubscriptionDefinition {
		id: statement.id,
		node: statement.node,
		auth: None,
		session: None,
		vars: BTreeMap::new(),
		// Set by the registration, inside its transaction.
		watermark: 0,
		query: crate::catalog::SubscriptionQuery::Compiled(crate::catalog::CompiledSubscription {
			fields: query_fields,
			what: statement.what.clone(),
			cond: statement.cond.clone().map(|cond| cond.0),
			fetch: statement
				.fetch
				.as_ref()
				.map(|fetch| fetch.iter().map(|f| f.0.clone()).collect()),
		}),
	};
	let value = stored_value(codec, &definition.to_stored(), "StoredSubscriptionDefinition")?;
	Ok(LivePlan {
		attrs: LiveAttrs {
			id: crate::val::Uuid(statement.id),
			node: crate::val::Uuid(statement.node),
			fields: shape,
			template: template("StoredSubscriptionDefinition", value, LIVE_HOLES),
		},
		target: &statement.what,
		fields,
		cond: statement.cond.as_ref().map(|cond| &cond.0),
		fetch: statement.fetch.iter().flat_map(|fetch| fetch.iter().map(|f| &f.0)).collect(),
	})
}
