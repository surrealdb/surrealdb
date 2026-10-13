use std::collections::BTreeMap;

use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;

use crate::catalog::{DatabaseId, ExprText, FieldsText, FromStored, NamespaceId};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fetchs, Fields};
use crate::iam::Auth;
use crate::sql;
use crate::sql::statements::live::LiveFields;
use crate::val::{TableName, Value};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum StoredSubscriptionFields {
	Diff,
	/// Canonical SurrealQL text of the field selection.
	#[revision(end = 2, convert_fn = "convert_select", fields_name = "SelectV1")]
	Select(Fields),
	#[revision(start = 2)]
	Select(FieldsText),
}

impl StoredSubscriptionFields {
	// The `revisioned` macro calls convert_fns with an owned value by
	// contract; a reference parameter fails to type-check against its
	// generated call site.
	#[allow(clippy::needless_pass_by_value)]
	fn convert_select(fields: SelectV1, _revision: u16) -> Result<Self, revision::Error> {
		Ok(Self::Select(FieldsText::new(&fields.0)))
	}
}

impl InfoStructure for SubscriptionFields {
	fn structure(self) -> Value {
		match self {
			SubscriptionFields::Diff => "diff".to_string().into(),
			SubscriptionFields::Select(fields) => Value::from(fields.to_sql()),
		}
	}
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredSubscriptionDefinition {
	pub id: Uuid,
	pub node: Uuid,
	pub fields: StoredSubscriptionFields,
	/// Canonical SurrealQL text of the subscription's source.
	pub what: ExprText,
	/// Canonical SurrealQL text of the `WHERE` clause.
	pub cond: Option<ExprText>,
	/// Canonical SurrealQL text of each `FETCH` target.
	#[revision(end = 2, convert_fn = "convert_fetch")]
	pub old_fetch: Option<Fetchs>,
	#[revision(start = 2)]
	pub fetch: Option<Vec<ExprText>>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub auth: Option<Auth>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub session: Option<Value>,
	// When a live query is created, we analyze the query
	// and store the variables that are used in the query.
	pub vars: BTreeMap<String, Value>,
}

impl_kv_value_revisioned!(StoredSubscriptionDefinition);

impl StoredSubscriptionDefinition {
	/// Discards the old structured `Fetchs`: revision 2+ stores each fetch
	/// target's canonical SurrealQL text directly.
	fn convert_fetch(&mut self, _rev: u16, value: Option<Fetchs>) -> Result<(), revision::Error> {
		self.fetch = value.map(|fs| fs.into_iter().map(|f| ExprText::new(&f.0)).collect());
		Ok(())
	}
}

impl InfoStructure for SubscriptionDefinition {
	fn structure(self) -> Value {
		let mut out = map! {
			"id" => crate::val::Uuid(self.id).into(),
			"node" => crate::val::Uuid(self.node).into(),
		};
		match self.query {
			SubscriptionQuery::Compiled(q) => {
				out.insert("fields", q.fields.structure());
				out.insert("what", q.what.structure());
				if let Some(v) = q.cond {
					out.insert("cond", v.structure());
				}
				if let Some(v) = q.fetch {
					let fetch: Vec<Value> =
						v.into_iter().map(|e| Value::from(e.to_stored_sql())).collect();
					out.insert("fetch", fetch.into());
				}
			}
			SubscriptionQuery::Uncompilable {
				fields,
				what,
				cond,
				fetch,
				reason,
			} => {
				// Report the stored text as-is. It is what the subscription
				// holds; the point of surfacing an uncompilable subscription at
				// all is that an operator can see it and `KILL` it.
				// Same spelling the compiled arm produces via
				// `InfoStructure for SubscriptionFields`: a consumer switching
				// on this key must not have to know which arm it came from.
				out.insert(
					"fields",
					match fields {
						StoredSubscriptionFields::Diff => Value::from("diff"),
						StoredSubscriptionFields::Select(t) => t.into(),
					},
				);
				out.insert("what", what.into());
				if let Some(v) = cond {
					out.insert("cond", v.into());
				}
				if let Some(v) = fetch {
					let fetch: Vec<Value> = v.into_iter().map(Value::from).collect();
					out.insert("fetch", fetch.into());
				}
				out.insert("error", Value::from(reason));
			}
		}
		Value::from(out)
	}
}

/// Marker appended to an inert subscription's rendering.
///
/// A SurrealQL line comment, so the rendering stays valid SurrealQL, and part
/// of the rendering rather than of one projection of it, so every `INFO`
/// surface inherits it. Without it a subscription that can never match again
/// is indistinguishable from a live one in plain `INFO FOR TABLE`, which is
/// the surface an operator reaches for first. `INFO ... STRUCTURE` carries the
/// compile error alongside, under `error`.
pub const INERT_SUBSCRIPTION_MARKER: &str =
	" -- INERT: this subscription's stored text no longer parses, so it will never match again";

/// Renders the `LIVE SELECT` the subscription holds.
///
/// Both arms produce the same statement, because the stored text of each clause
/// is the canonical rendering of the expression it was compiled from: splicing
/// it is byte-identical to rendering the compiled form. The inert arm then adds
/// the marker.
impl ToSql for SubscriptionDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self.query {
			SubscriptionQuery::Compiled(q) => q.to_sql_definition().fmt_sql(f, fmt),
			SubscriptionQuery::Uncompilable {
				fields,
				what,
				cond,
				fetch,
				..
			} => {
				f.push_str("LIVE SELECT");
				match fields {
					StoredSubscriptionFields::Diff => f.push_str(" DIFF"),
					StoredSubscriptionFields::Select(t) => {
						f.push(' ');
						f.push_str(t.as_str());
					}
				}
				f.push_str(" FROM ");
				f.push_str(what.as_str());
				if let Some(cond) = cond {
					f.push_str(" WHERE ");
					f.push_str(cond.as_str());
				}
				if let Some(fetch) = fetch {
					f.push_str(" FETCH ");
					for (i, t) in fetch.iter().enumerate() {
						if i > 0 {
							f.push_str(", ");
						}
						f.push_str(t.as_str());
					}
				}
				f.push_str(INERT_SUBSCRIPTION_MARKER);
			}
		}
	}
}

/// Runtime form of [`StoredSubscriptionFields`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionFields {
	Diff,
	Select(Fields),
}

/// Runtime form of [`StoredSubscriptionDefinition`].
///
/// The identity fields are always present. The query is not: see
/// [`SubscriptionQuery`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionDefinition {
	pub id: Uuid,
	pub node: Uuid,
	pub auth: Option<Auth>,
	pub session: Option<Value>,
	pub vars: BTreeMap<String, Value>,
	pub query: SubscriptionQuery,
}

/// A subscription's query, which may no longer compile.
///
/// Compiling a subscription is total: text this engine can no longer parse
/// yields [`SubscriptionQuery::Uncompilable`] rather than an error. That is
/// what lets every reader hold one list. A subscription in that state can
/// never match another document, but it still has `lq`/`lv` keys and a client
/// waiting on them, so the statements that tear subscriptions down or report
/// them must still see it — dropping it instead would leave that client
/// waiting on a notification which can never arrive, and hide the row from the
/// `INFO` an operator would use to find it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionQuery {
	Compiled(CompiledSubscription),
	/// The stored clauses, verbatim, plus why they did not compile. Carried
	/// rather than discarded so that re-storing the subscription reproduces it
	/// exactly and `INFO` can still show what to `KILL`.
	Uncompilable {
		fields: StoredSubscriptionFields,
		what: ExprText,
		cond: Option<ExprText>,
		fetch: Option<Vec<ExprText>>,
		reason: String,
	},
}

/// The evaluable half of a subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledSubscription {
	pub fields: SubscriptionFields,
	pub what: Expr,
	pub cond: Option<Expr>,
	pub fetch: Option<Vec<Expr>>,
}

impl CompiledSubscription {
	fn to_sql_definition(&self) -> crate::sql::LiveStatement {
		let fields = match &self.fields {
			SubscriptionFields::Diff => LiveFields::Diff,
			SubscriptionFields::Select(fields) => {
				LiveFields::Select(sql::Fields::from(fields.clone()))
			}
		};
		let fetch = self.fetch.as_ref().map(|exprs| {
			sql::Fetchs(exprs.iter().map(|e| sql::Fetch(sql::Expr::from(e.clone()))).collect())
		});

		crate::sql::LiveStatement {
			fields,
			what: self.what.clone().into(),
			cond: self.cond.clone().map(|c| crate::sql::Cond(c.into())),
			fetch,
		}
	}
}

impl SubscriptionQuery {
	/// The compiled query, or `None` if the stored text no longer parses.
	pub fn compiled(&self) -> Option<&CompiledSubscription> {
		match self {
			SubscriptionQuery::Compiled(q) => Some(q),
			SubscriptionQuery::Uncompilable {
				..
			} => None,
		}
	}
}

impl SubscriptionDefinition {
	/// Compiles a stored subscription. Total: see [`SubscriptionQuery`].
	pub fn compile(stored: &StoredSubscriptionDefinition) -> SubscriptionDefinition {
		SubscriptionDefinition {
			id: stored.id,
			node: stored.node,
			auth: stored.auth.clone(),
			session: stored.session.clone(),
			vars: stored.vars.clone(),
			query: match compile_query(stored) {
				Ok(q) => SubscriptionQuery::Compiled(q),
				Err(reason) => SubscriptionQuery::Uncompilable {
					fields: stored.fields.clone(),
					what: stored.what.clone(),
					cond: stored.cond.clone(),
					fetch: stored.fetch.clone(),
					reason,
				},
			},
		}
	}

	/// Renders the runtime subscription back to its stored form. The LIVE
	/// statement builds the runtime form and calls this at the KV write.
	///
	/// Total in both directions: a subscription that did not compile carries
	/// its stored clauses, so re-storing it reproduces the original bytes
	/// rather than losing the definition.
	pub fn to_stored(&self) -> StoredSubscriptionDefinition {
		let (fields, what, cond, fetch) = match &self.query {
			SubscriptionQuery::Compiled(q) => (
				match &q.fields {
					SubscriptionFields::Diff => StoredSubscriptionFields::Diff,
					SubscriptionFields::Select(f) => {
						StoredSubscriptionFields::Select(FieldsText::new(f))
					}
				},
				ExprText::new(&q.what),
				q.cond.as_ref().map(ExprText::new),
				q.fetch.as_ref().map(|fs| fs.iter().map(ExprText::new).collect()),
			),
			SubscriptionQuery::Uncompilable {
				fields,
				what,
				cond,
				fetch,
				..
			} => (fields.clone(), what.clone(), cond.clone(), fetch.clone()),
		};
		StoredSubscriptionDefinition {
			id: self.id,
			node: self.node,
			fields,
			what,
			cond,
			fetch,
			auth: self.auth.clone(),
			session: self.session.clone(),
			vars: self.vars.clone(),
		}
	}
}

/// Compiles every clause of a stored subscription, reporting the first failure
/// as a message rather than an error: the caller turns it into
/// [`SubscriptionQuery::Uncompilable`], and nothing upstream can act on a
/// typed error.
fn compile_query(stored: &StoredSubscriptionDefinition) -> Result<CompiledSubscription, String> {
	let build = || -> anyhow::Result<CompiledSubscription> {
		Ok(CompiledSubscription {
			fields: match &stored.fields {
				StoredSubscriptionFields::Diff => SubscriptionFields::Diff,
				StoredSubscriptionFields::Select(t) => SubscriptionFields::Select(t.compile()?),
			},
			what: stored.what.compile()?,
			cond: stored.cond.as_ref().map(|c| c.compile()).transpose()?,
			fetch: stored
				.fetch
				.as_ref()
				.map(|fs| fs.iter().map(|t| t.compile()).collect::<anyhow::Result<_>>())
				.transpose()?,
		})
	};
	build().map_err(|e| e.to_string())
}

impl FromStored for SubscriptionDefinition {
	type Stored = StoredSubscriptionDefinition;

	fn from_stored(
		stored: &StoredSubscriptionDefinition,
	) -> anyhow::Result<SubscriptionDefinition> {
		Ok(SubscriptionDefinition::compile(stored))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct NodeLiveQuery {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: TableName,
}
impl_kv_value_revisioned!(NodeLiveQuery);

#[cfg(test)]
mod tests {
	use surrealdb_kvs::value::KVValue;

	use super::*;

	fn poisoned_subscription(id: Uuid, node: Uuid) -> StoredSubscriptionDefinition {
		StoredSubscriptionDefinition {
			id,
			node,
			fields: StoredSubscriptionFields::Diff,
			// Not SurrealQL under any grammar this engine has shipped.
			what: ExprText::from_raw("((( not surrealql"),
			cond: None,
			fetch: None,
			auth: None,
			session: None,
			vars: BTreeMap::new(),
		}
	}

	/// A subscription holding text that no longer parses must survive the round
	/// trip through storage with its identity intact.
	///
	/// This is the property the text migration exists for. While `what` and
	/// `cond` were structured `Expr`, they re-parsed during *decode*, so a
	/// single unreadable subscription failed the whole `lq` range read: the
	/// table could not be written to, its subscriptions could not be listed,
	/// and it could not be dropped.
	#[test]
	fn a_subscription_whose_text_no_longer_parses_still_decodes() {
		let node = Uuid::from_u128(7);
		let id = Uuid::from_u128(9);

		let bytes =
			poisoned_subscription(id, node).kv_encode_value().expect("encoding must not fail");
		let decoded = StoredSubscriptionDefinition::kv_decode_value(&bytes, ())
			.expect("decode must not parse the stored text");

		// Identity survives, which is all a KILLED notification needs.
		assert_eq!(decoded.id, id);
		assert_eq!(decoded.node, node);
	}

	/// Compiling is total: an uncompilable subscription is reported as one, not
	/// dropped and not an error. Dropping it is what left its client waiting on
	/// a `KILLED` that could never arrive.
	#[test]
	fn compiling_an_unparseable_subscription_yields_the_uncompilable_form() {
		let id = Uuid::from_u128(9);
		let stored = poisoned_subscription(id, Uuid::from_u128(7));

		let compiled = SubscriptionDefinition::compile(&stored);
		assert_eq!(compiled.id, id, "identity must survive a failed compile");
		assert!(compiled.query.compiled().is_none(), "the query must not claim to be evaluable");
		let SubscriptionQuery::Uncompilable {
			reason,
			..
		} = &compiled.query
		else {
			panic!("expected the uncompilable form");
		};
		assert!(!reason.is_empty(), "the failure must say why");
	}

	/// Re-storing a subscription that never compiled must reproduce it exactly.
	/// The uncompilable form carries the stored clauses verbatim precisely so
	/// that writing the definition back cannot silently lose it.
	#[test]
	fn an_uncompilable_subscription_round_trips_without_loss() {
		let stored = poisoned_subscription(Uuid::from_u128(9), Uuid::from_u128(7));
		let round_tripped = SubscriptionDefinition::compile(&stored).to_stored();
		assert_eq!(round_tripped, stored);
	}

	/// Round-trip real subscriptions, built the way the `LIVE` statement builds
	/// them rather than by hand.
	///
	/// The frozen compat corpus cannot reach these shapes: all three of its
	/// subscription fixtures set `what` to a string literal and `cond` to a
	/// string *containing* a condition, so no fixture exercises a table source,
	/// a parsed condition, a `DIFF` selection or a real fetch list. Those are
	/// the only shapes a real `LIVE` statement produces, so without this the
	/// stored form is pinned only against inputs nothing writes.
	#[test]
	fn real_subscriptions_survive_the_storage_round_trip() {
		for query in [
			"LIVE SELECT * FROM person",
			"LIVE SELECT DIFF FROM person",
			"LIVE SELECT name, age FROM person WHERE age > 18",
			"LIVE SELECT * FROM person WHERE $auth.admin = true FETCH friend, org",
			// A keyword-shaped table name, which must survive escaping in both
			// directions.
			"LIVE SELECT * FROM `select`",
		] {
			let ast = crate::syn::parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
			let crate::sql::TopLevelExpr::Live(live) = &ast.expressions[0] else {
				panic!("{query} did not parse as a LIVE statement");
			};

			let stored = StoredSubscriptionDefinition {
				id: Uuid::from_u128(1),
				node: Uuid::from_u128(2),
				fields: match &live.fields {
					LiveFields::Diff => StoredSubscriptionFields::Diff,
					LiveFields::Select(f) => {
						StoredSubscriptionFields::Select(FieldsText::new(&f.clone().into()))
					}
				},
				what: ExprText::new(&Expr::from(live.what.clone())),
				cond: live.cond.as_ref().map(|c| ExprText::new(&Expr::from(c.0.clone()))),
				fetch: live
					.fetch
					.as_ref()
					.map(|fs| fs.iter().map(|f| ExprText::new(&Expr::from(f.0.clone()))).collect()),
				auth: None,
				session: None,
				vars: BTreeMap::new(),
			};

			// It compiles, so a real subscription is never inert.
			let compiled = SubscriptionDefinition::compile(&stored);
			assert!(
				compiled.query.compiled().is_some(),
				"{query} produced a subscription that cannot be evaluated"
			);

			// Storing it back reproduces the same bytes, so a definition read
			// and rewritten by a later statement cannot drift.
			let restored = compiled.to_stored();
			assert_eq!(restored, stored, "{query} did not survive the round trip");
			assert_eq!(
				restored.kv_encode_value().unwrap(),
				stored.kv_encode_value().unwrap(),
				"{query} re-encoded to different bytes"
			);
		}
	}

	/// `LIVE SELECT ... FROM person` parses its source in table context, but
	/// stored text always compiles in field context, so the source comes back
	/// as an idiom rather than a table.
	///
	/// This normalisation predates the text migration: `Expr`'s own wire format
	/// is canonical text decoded the same way, so subscriptions on disk have
	/// always read back like this. It is inert because the source is only ever
	/// rendered after decode, and both shapes render identically. Pinned so
	/// that a future reader of `what` discovers the shape here rather than in
	/// production.
	#[test]
	fn the_subscription_source_normalises_to_an_idiom_on_decode() {
		let table = Expr::Table("person".into());
		let stored = ExprText::new(&table);
		assert_eq!(stored.as_str(), "person");

		let compiled = stored.compile().unwrap();
		assert!(
			matches!(&compiled, Expr::Idiom(_)),
			"expected the source to normalise to an idiom, got {compiled:?}"
		);
		// Inert: the two shapes are indistinguishable once rendered.
		assert_eq!(ExprText::new(&compiled).as_str(), "person");
	}
}
