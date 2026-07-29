use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::aggregation::AggregationAnalysis;
use crate::catalog::{ExprText, FieldsText, FromStored, IdiomText};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Expr, Fields, Group, Groups, View};
use crate::sql;
use crate::val::Value;

/// Stored form of [`ViewDefinition`].
///
/// The clauses are stored one per field, each under a grammar that parses on its
/// own, rather than as a single `AS SELECT ...` string. `AS SELECT ...` is not a
/// SurrealQL construct in its own right, and one string leaves nowhere to record
/// anything about a view that has no SurrealQL spelling; such metadata is added
/// to [`Clauses`](StoredViewDefinition::Clauses), as further fields.
///
/// # Why this is still revision 1
///
/// The three variants before `Clauses` are the shape views were written in
/// before the clauses became text, and they are still declared so their bytes
/// still decode. Adding the new shape as a further variant rather than bumping
/// the revision keeps the type at revision 1: a discriminant is as good a
/// discriminator as a revision header, and reserving a revision for a shape the
/// wire never needed to tell apart spends the compatibility ledger for nothing.
///
/// What that does not buy is backwards compatibility, and neither would a bump:
/// a reader that predates `Clauses` rejects discriminant 3 exactly as it would
/// reject revision 2. The gain is only that revision 1 keeps describing the one
/// thing a view has ever been.
///
/// Only `Clauses` is ever written — [`ViewDefinition::to_stored`] produces
/// nothing else, and `stored_views_are_always_written_as_clauses` holds it to
/// that. The legacy variants are decode-only, and a table re-storing one
/// migrates it in passing.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum StoredViewDefinition {
	/// Decode-only: a materialized view, before the clauses became text.
	Materialized {
		fields: Fields,
		tables: Vec<TableName>,
		condition: Option<Expr>,
	},
	/// Decode-only: an aggregated view, before the clauses became text. Carries
	/// the aggregation analysis derived from `fields` and `groups`, which
	/// compiling recomputes and so ignores.
	Aggregated {
		analysis: AggregationAnalysis,
		condition: Option<Expr>,
		tables: Vec<TableName>,
		groups: Groups,
		fields: Fields,
	},
	/// Decode-only: a view classified as unmaintained, before the clauses became
	/// text.
	Select {
		fields: Fields,
		tables: Vec<TableName>,
		condition: Option<Expr>,
		groups: Option<Groups>,
	},
	/// The clauses of the view's `AS SELECT ... FROM ... WHERE ... GROUP ...`.
	Clauses {
		/// The `SELECT` field list.
		fields: FieldsText,
		/// The tables the view reads from.
		tables: Vec<TableName>,
		/// The `WHERE` clause, if the view has one.
		condition: Option<ExprText>,
		/// The `GROUP` targets. `None` is a view with no `GROUP` clause; `Some`
		/// with no targets is `GROUP ALL`.
		groups: Option<Vec<IdiomText>>,
	},
}

impl StoredViewDefinition {
	/// Rebuilds the view's clauses as an AST, whichever shape they were stored
	/// in. The legacy variants already hold ASTs, so only `Clauses` compiles.
	fn to_view(&self) -> anyhow::Result<View> {
		match self {
			Self::Clauses {
				fields,
				tables,
				condition,
				groups,
			} => Ok(View {
				expr: fields.compile().context("the view's field list no longer compiles")?,
				what: tables.clone(),
				cond: condition
					.as_ref()
					.map(|c| c.compile().map(Cond))
					.transpose()
					.context("the view's condition no longer compiles")?,
				group: groups
					.as_ref()
					.map(|g| {
						g.iter()
							.map(|i| i.compile().map(Group))
							.collect::<anyhow::Result<Vec<_>>>()
							.map(Groups)
					})
					.transpose()
					.context("the view's group targets no longer compile")?,
			}),
			Self::Materialized {
				fields,
				tables,
				condition,
			} => Ok(View {
				expr: fields.clone(),
				what: tables.clone(),
				cond: condition.clone().map(Cond),
				group: None,
			}),
			Self::Aggregated {
				analysis,
				condition,
				tables,
				groups,
				fields,
			} => {
				// The analysis is derived from the two clauses below it;
				// classification derives it again.
				let _ = analysis;
				Ok(View {
					expr: fields.clone(),
					what: tables.clone(),
					cond: condition.clone().map(Cond),
					group: Some(groups.clone()),
				})
			}
			Self::Select {
				fields,
				tables,
				condition,
				groups,
			} => Ok(View {
				expr: fields.clone(),
				what: tables.clone(),
				cond: condition.clone().map(Cond),
				group: groups.clone(),
			}),
		}
	}

	/// Renders a view's clauses to the canonical text this type stores.
	fn clauses(
		fields: &Fields,
		tables: Vec<TableName>,
		condition: Option<&Expr>,
		groups: Option<&Groups>,
	) -> Self {
		Self::Clauses {
			fields: FieldsText::new(fields),
			tables,
			condition: condition.map(ExprText::new),
			groups: groups.map(|g| g.0.iter().map(|g| IdiomText::new(&g.0)).collect()),
		}
	}
}

impl FromStored for ViewDefinition {
	type Stored = StoredViewDefinition;

	fn from_stored(stored: &StoredViewDefinition) -> anyhow::Result<ViewDefinition> {
		let view = stored.to_view()?;

		match view.to_definition() {
			Ok(definition) => Ok(definition),
			// Whether a view can be maintained, and how, is decided here by the
			// aggregation analysis rather than read from storage. Clauses that
			// fail that analysis (possible for views defined before the analysis
			// rejected their shape) degrade to the inert `Select`
			// classification, which the pre-analysis runtime applied to such
			// views. Failing instead would fail every read of the table
			// definition, including the `INFO` and `REMOVE TABLE` needed to
			// diagnose and drop the view, as well as every write to its source
			// tables. The degrade loses no clause: `Select` carries all four, and
			// storing it back writes the same ones.
			//
			// The consequence is that the view stops being maintained, so it is
			// reported rather than applied silently. DEFINE-time validation is
			// unaffected: it analyzes the statement's own AST before any
			// definition is stored.
			Err(err) => {
				// Debug, not warn, and without the clauses: this runs on every
				// `TableDefinition::from_stored`, so on every catalog-cache miss
				// for the table, and the condition cannot self-heal. At warn
				// level a hot source table re-logs the view on every write.
				debug!(
					"View cannot be maintained by this version and will not be updated on writes to its source tables: {err}"
				);
				Ok(ViewDefinition::Select {
					fields: view.expr,
					tables: view.what,
					condition: view.cond.map(|c| c.0),
					groups: view.group,
				})
			}
		}
	}
}

/// Runtime form of [`StoredViewDefinition`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ViewDefinition {
	/// The view is cached, and has no aggregation.
	/// It is only updated any of the target tables are updated.
	Materialized {
		fields: Fields,
		tables: Vec<TableName>,
		condition: Option<Expr>,
	},
	/// The view has a group by and has a running compute.
	Aggregated {
		analysis: AggregationAnalysis,
		condition: Option<Expr>,
		tables: Vec<TableName>,
		// fields below are only used for reconstructing the query.
		groups: Groups,
		fields: Fields,
	},
	/// The view is computed by doing another select query.
	Select {
		fields: Fields,
		tables: Vec<TableName>,
		condition: Option<Expr>,
		groups: Option<Groups>,
	},
}

impl ViewDefinition {
	/// The source tables this view reads from (the `FROM` clause of the
	/// `AS SELECT ...` definition), whichever maintenance strategy applies.
	pub(crate) fn source_tables(&self) -> &[TableName] {
		match self {
			ViewDefinition::Materialized {
				tables,
				..
			}
			| ViewDefinition::Aggregated {
				tables,
				..
			}
			| ViewDefinition::Select {
				tables,
				..
			} => tables,
		}
	}

	pub(crate) fn to_stored(&self) -> StoredViewDefinition {
		match self {
			ViewDefinition::Materialized {
				fields,
				tables,
				condition,
			} => StoredViewDefinition::clauses(fields, tables.clone(), condition.as_ref(), None),
			ViewDefinition::Aggregated {
				fields,
				tables,
				condition,
				groups,
				..
			} => StoredViewDefinition::clauses(
				fields,
				tables.clone(),
				condition.as_ref(),
				Some(groups),
			),
			ViewDefinition::Select {
				fields,
				tables,
				condition,
				groups,
			} => StoredViewDefinition::clauses(
				fields,
				tables.clone(),
				condition.as_ref(),
				groups.as_ref(),
			),
		}
	}

	pub(crate) fn to_sql_definition(&self) -> sql::View {
		match self {
			ViewDefinition::Materialized {
				fields,
				tables,
				condition,
			} => sql::View {
				expr: fields.clone().into(),
				what: tables.clone().into_iter().map(Into::into).collect(),
				cond: condition.clone().map(|x| sql::Cond(x.into())),
				group: None,
			},
			ViewDefinition::Aggregated {
				tables,
				condition,
				groups,
				fields,
				..
			} => sql::View {
				expr: fields.clone().into(),
				what: tables.clone().into_iter().map(Into::into).collect(),
				cond: condition.clone().map(|x| sql::Cond(x.into())),
				group: Some(groups.clone().into()),
			},
			ViewDefinition::Select {
				fields,
				tables,
				condition,
				groups,
			} => sql::View {
				expr: fields.clone().into(),
				what: tables.clone().into_iter().map(Into::into).collect(),
				cond: condition.clone().map(|x| sql::Cond(x.into())),
				group: groups.clone().map(|x| x.into()),
			},
		}
	}
}

impl ToSql for ViewDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::field::Selector;
	use crate::expr::{Field, Idiom};

	/// Every clause survives the round trip through stored text, including the
	/// distinction between no `GROUP` clause and `GROUP ALL`.
	#[test]
	fn every_clause_round_trips() {
		let definition = ViewDefinition::Select {
			fields: Fields::Select(vec![Field::Single(Selector {
				expr: Expr::Idiom(Idiom::field("amount".to_owned())),
				alias: None,
			})]),
			tables: vec![TableName::from("orders")],
			condition: Some(Expr::Idiom(Idiom::field("paid".to_owned()))),
			groups: Some(Groups(vec![Group(Idiom::field("customer".to_owned()))])),
		};

		let compiled = ViewDefinition::from_stored(&definition.to_stored()).unwrap();
		assert_eq!(compiled.to_stored(), definition.to_stored());
		assert_eq!(compiled.to_sql(), definition.to_sql());
	}

	/// `GROUP ALL` groups everything into one row; no `GROUP` clause at all
	/// groups nothing. Both hold an empty target list, so the stored form has to
	/// keep them apart or a `GROUP ALL` view silently stops aggregating.
	#[test]
	fn group_all_is_distinguishable_from_an_absent_group_clause() {
		let select = |groups| ViewDefinition::Select {
			fields: Fields::all(),
			tables: vec![TableName::from("orders")],
			condition: None,
			groups,
		};
		let group_all = select(Some(Groups::default()));
		let no_group = select(None);

		assert_eq!(group_all.to_sql(), "AS SELECT * FROM orders GROUP ALL");
		assert_eq!(no_group.to_sql(), "AS SELECT * FROM orders");
		assert_ne!(group_all.to_stored(), no_group.to_stored());
	}

	#[test]
	fn an_unmaintainable_view_degrades_without_losing_a_clause() {
		// A `VALUE` selector combined with `GROUP` is rejected by the current
		// aggregation analysis but was definable before that rejection existed.
		// Such a stored view compiles to the inert `Select` classification
		// instead of failing, and because the classification is derived rather
		// than stored, storing the degraded definition back reproduces the same
		// clauses — re-storing the table cannot lose one.
		let stored = StoredViewDefinition::Clauses {
			fields: FieldsText::from_raw("VALUE math::sum(amount)"),
			tables: vec![TableName::from("orders")],
			condition: None,
			groups: Some(Vec::new()),
		};

		let compiled = ViewDefinition::from_stored(&stored)
			.expect("an unmaintainable view degrades rather than failing to compile");
		assert!(
			matches!(compiled, ViewDefinition::Select { .. }),
			"expected the inert classification, got {compiled:?}"
		);
		assert_eq!(compiled.to_stored(), stored);
	}
	/// The legacy variants are decode-only. They are declared so revision 1's
	/// bytes still decode, and nothing may write them: a `Materialized` or
	/// `Aggregated` view stored in the old shape would carry a classification and
	/// an analysis that this version derives rather than reads, and re-storing it
	/// is what migrates it.
	#[test]
	fn stored_views_are_always_written_as_clauses() {
		let aggregate: Fields =
			FieldsText::from_raw("count()").compile().expect("`count()` is a field list");
		let analysis =
			AggregationAnalysis::analyze_fields_groups(&aggregate, &Groups::default(), false)
				.expect("`SELECT count() GROUP ALL` analyzes");
		let tables = vec![TableName::from("orders")];

		for definition in [
			ViewDefinition::Materialized {
				fields: Fields::all(),
				tables: tables.clone(),
				condition: None,
			},
			ViewDefinition::Aggregated {
				analysis,
				condition: None,
				tables: tables.clone(),
				groups: Groups::default(),
				fields: aggregate,
			},
			ViewDefinition::Select {
				fields: Fields::all(),
				tables,
				condition: None,
				groups: None,
			},
		] {
			assert!(
				matches!(definition.to_stored(), StoredViewDefinition::Clauses { .. }),
				"{definition:?} stored as something other than its clauses"
			);
		}
	}

	/// Revision 1's own shapes still decode, and compiling one classifies it from
	/// the clauses rather than trusting what it recorded — an `Aggregated` view
	/// whose stored analysis is stale is re-analyzed, not believed.
	#[test]
	fn a_legacy_stored_view_compiles_and_migrates_on_the_way_back() {
		let legacy = StoredViewDefinition::Materialized {
			fields: Fields::all(),
			tables: vec![TableName::from("orders")],
			condition: Some(Expr::Idiom(Idiom::field("paid".to_owned()))),
		};

		let compiled = ViewDefinition::from_stored(&legacy).expect("a legacy view still compiles");
		assert_eq!(compiled.to_sql(), "AS SELECT * FROM orders WHERE paid");
		assert!(matches!(compiled.to_stored(), StoredViewDefinition::Clauses { .. }));
	}
}
