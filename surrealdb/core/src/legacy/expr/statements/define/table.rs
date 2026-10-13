use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::aggregation::{
	self, AggregateFields, Aggregation, AggregationAnalysis, AggregationStat,
};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{
	DatabaseId, Error as CatalogError, FieldDefinition, Metadata, NamespaceId, Record, RecordType,
	TableDefinition, TableType, ViewDefinition,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{self, CursorDoc, Document, DocumentContext, NsDbCtx};
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::field::Selector;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::table::DefineTableStatement;
use crate::expr::{
	Base, Cond, Expr, Field, Fields, Function, FunctionCall, Group, Groups, Idiom, Kind, Literal,
	SelectStatement,
};
use crate::iam::{Action, ResourceKind};
use crate::key::schema::{
	EdgeCachePrefix, FieldKey, ForeignTableKey, RecordKey, RecordPrefix, RefCachePrefix, TblRoot,
};
use crate::kvs::Transaction;
use crate::legacy::{expr_to_ident, kill_table_subscriptions};
use crate::val::{Array, Number, RecordId, RecordIdKey, TableName, Value};

#[instrument(level = "trace", name = "DefineTableStatement::compute", skip_all)]
pub(crate) async fn define_table_statement_compute(
	this: &DefineTableStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Table, Base::Db)?;

	// Validate any GRAPHQL_ALIAS at definition time so typos surface here
	// rather than silently falling back at schema-generation time.
	crate::legacy::expr::statements::define::validate_graphql_alias(&this.graphql_alias, "table")?;

	// Process the name
	let name = TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.name, "table name").await?);

	// The inline cache caps rewrite a whole cache value per maintained
	// entry, so their cost grows quadratically with the cap; the hard bound
	// keeps that below the cursor cost the cache replaces.
	for cap in [this.inline_edges_cap, this.inline_refs_cap] {
		anyhow::ensure!(
			cap.is_none_or(|cap| cap <= crate::idx::inline_cache::MAX_INLINE_CACHE_CAP),
			ExecError::Thrown(format!(
				"an INLINE cache cap cannot exceed {}",
				crate::idx::inline_cache::MAX_INLINE_CACHE_CAP
			))
		);
	}

	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;

	// Fetch the transaction
	let txn = ctx.tx();

	let ns = txn.expect_ns_by_name(ns_name).await?;
	let db = txn.expect_db_by_name(ns_name, db_name).await?;

	// Check if the definition exists. A redefinition keeps the existing
	// table id and carries the graph markers forward: they record that
	// packed adjacency blocks may exist under the table's subspace, or hold
	// its record ids, which redefining the schema does not change.
	let existing = txn.get_tb(ns.namespace_id, db.database_id, &name, None).await?;
	let (table_id, graph_folded, graph_doc_ids, graph_inline_gen, old_edges_cap, old_refs_cap) =
		if let Some(tb) = &existing {
			match this.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(CatalogError::TbAlreadyExists {
							name: name.as_str().to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}

			(
				tb.table_id,
				tb.graph_folded,
				tb.graph_doc_ids,
				tb.graph_inline_gen,
				tb.inline_edges_cap,
				tb.inline_refs_cap,
			)
		} else {
			(
				txn.get_next_tb_id(Some(ctx), ns.namespace_id, db.database_id).await?,
				false,
				false,
				0,
				None,
				None,
			)
		};

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	// Process the statement
	let cache_ts = Uuid::now_v7();
	let mut tb_def = TableDefinition {
		namespace_id: ns.namespace_id,
		database_id: db.database_id,
		table_id,
		name: name.clone(),
		drop: this.drop,
		schemafull: this.full,
		table_type: this.table_type.clone(),
		// `to_definition` validates the view (aggregation analysis,
		// VALUE-selector rejection) before anything is stored.
		view: this.view.as_ref().map(crate::catalog::view::view_to_definition).transpose()?,
		permissions: this.permissions.clone(),
		comment,
		changefeed: this.changefeed,

		cache_fields_ts: cache_ts,
		cache_events_ts: cache_ts,
		cache_indexes_ts: cache_ts,
		cache_tables_ts: cache_ts,
		cache_lives_ts: cache_ts,
		graphql_alias: this.graphql_alias.clone(),
		graphql_deprecated: this.graphql_deprecated.clone(),
		graph_folded,
		graph_doc_ids,
		graph_inline_gen,
		inline_edges_cap: this.inline_edges_cap,
		inline_refs_cap: this.inline_refs_cap,
	};

	// Validate (and normalise) a lightweight relation definition against
	// the table's previous shape and current contents.
	validate_lightweight_table(
		&txn,
		ns.namespace_id,
		db.database_id,
		existing.as_deref(),
		&mut tb_def,
	)
	.await?;
	// A type change away from RELATION must not orphan INLINE fields.
	validate_inline_fields_survive_type_change(
		&txn,
		ns.namespace_id,
		db.database_id,
		existing.as_deref(),
		&tb_def,
	)
	.await?;

	// Add table relational fields
	crate::legacy::define_table_statement_add_in_out_fields(
		&txn,
		ns.namespace_id,
		db.database_id,
		&mut tb_def,
	)
	.await?;

	// Record definition change
	if this.changefeed.is_some() {
		txn.changefeed_buffer_table_change(
			ns.namespace_id,
			db.database_id,
			&name,
			&tb_def.to_stored(),
		);
		// A table's retention counts towards its database's collection
		// watermark, so the fence is bumped in the same transaction: a
		// collection page that armed against it is rejected rather than
		// deleting under the old policy.
		txn.bump_changefeed_retention_fence(ns.namespace_id, db.database_id).await?;
	}

	// Update the catalog. The locked read inside `replace_tb` serializes
	// this write against a concurrent fold's `graph_folded` flip: without
	// it, an overwrite whose snapshot predates the flip would write the
	// marker back to false on a last-writer-wins backend, hiding every
	// folded edge.
	let tb = txn.replace_tb(ns_name, db_name, &tb_def).await?;

	// A cache is only trustworthy under the cap regime it was built under,
	// so any change to a cap — set, raised, lowered or dropped — deletes the
	// table's cache subspace in this transaction. Caches re-materialise
	// lazily from the first eligible write.
	if old_edges_cap != tb_def.inline_edges_cap {
		let key = EdgeCachePrefix {
			ns: ns.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&name),
		};
		txn.del_prefix_key(&key).await?;
	}
	if old_refs_cap != tb_def.inline_refs_cap {
		let key = RefCachePrefix {
			ns: ns.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&name),
		};
		txn.del_prefix_key(&key).await?;
	}

	// Clear the cache
	txn.clear_cache();

	let parent = NsDbCtx {
		ns: Arc::clone(&ns),
		db: Arc::clone(&db),
	};
	let doc_ctx =
		DocumentContext::initialise(ctx, &parent, Arc::clone(&tb), &name, opt.version, true)
			.await?;

	// Check if table is a view
	if let Some(view) = &tb.view {
		// Redefining a table as a view wipes its whole key range, which
		// includes the `lq` rows of every subscription on it, so each of
		// those clients is owed a KILLED exactly as it would be by a
		// `REMOVE TABLE`.
		kill_table_subscriptions(ctx, &txn, ns.namespace_id, db.database_id, &name).await?;
		// Remove the table data
		let key = TblRoot {
			ns: ns.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&name),
		};
		txn.del_prefix_key(&key).await?;

		let tables = view.source_tables();

		// Process each foreign table
		for ft in tables.iter() {
			// Save the view config
			let key = ForeignTableKey {
				ns: ns.namespace_id,
				db: db.database_id,
				tb: Cow::Borrowed(ft),
				ft: Cow::Borrowed(&name),
			};
			txn.set_key(&key, &tb_def.to_stored()).await?;
			// Refresh the table cache
			let Some(foreign_tb) = txn.get_tb(ns.namespace_id, db.database_id, ft, None).await?
			else {
				bail!(CatalogError::TbNotFound {
					name: ft.clone(),
				});
			};

			txn.replace_tb(
				ns_name,
				db_name,
				&TableDefinition {
					cache_tables_ts: Uuid::now_v7(),
					..(*foreign_tb).clone()
				},
			)
			.await?;

			// Clear the cache
			txn.clear_cache();
		}

		crate::legacy::define_table_statement_initialize_view(stk, ctx, opt, &doc_ctx, &name, view)
			.await?;
	}
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}

pub(crate) async fn define_table_statement_initialize_view(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc_ctx: &DocumentContext,
	view_table_name: &TableName,
	view: &ViewDefinition,
) -> Result<()> {
	match view {
		ViewDefinition::Select {
			..
		} => {}
		ViewDefinition::Materialized {
			fields,
			tables,
			condition,
		} => {
			crate::legacy::define_table_statement_initialize_materialized_view(
				stk,
				ctx,
				opt,
				doc_ctx,
				view_table_name,
				fields,
				tables,
				condition.as_ref(),
			)
			.await?;
		}
		ViewDefinition::Aggregated {
			analysis,
			tables,
			condition,
			..
		} => {
			crate::legacy::define_table_statement_initialize_aggregate_view(
				stk,
				ctx,
				opt,
				doc_ctx,
				view_table_name,
				analysis,
				condition.as_ref(),
				tables,
			)
			.await?;
		}
	}
	Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn define_table_statement_initialize_materialized_view(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc_ctx: &DocumentContext,
	view_table_name: &TableName,
	fields: &Fields,
	tables: &[TableName],
	condition: Option<&Expr>,
) -> Result<()> {
	// Build the initialization SELECT with `id` always included so we can
	// extract the source record's key regardless of the user's field list.
	let init_fields = match fields {
		Fields::Select(user_fields) => {
			let id_field = Field::Single(Selector {
				expr: Expr::Idiom(Idiom::from(ID.to_vec())),
				alias: None,
			});
			let mut all = vec![id_field];
			all.extend(user_fields.iter().cloned());
			Fields::Select(all)
		}
		other => other.clone(),
	};

	let select = SelectStatement {
		fields: init_fields,
		what: tables.iter().map(|x| Expr::Table(x.clone())).collect(),
		cond: condition.cloned().map(Cond),
		omit: vec![],
		only: false,
		with: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};

	let Value::Array(Array(v)) =
		crate::legacy::select_statement_compute(&select, stk, ctx, opt, None).await?
	else {
		fail!("initial select for view did not return an array");
	};

	let tx = ctx.tx();
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;

	for v in v {
		let Value::Object(mut o) = v else {
			fail!("initial select for view did not return an array of objects");
		};

		let Some(Value::RecordId(id)) = o.remove("id") else {
			fail!("select results did not contain a record id");
		};

		let key = RecordKey {
			ns,
			db,
			tb: Cow::Borrowed(view_table_name),
			id: Cow::Borrowed(&id.key),
		};
		let record = Arc::new(Record::new(Value::Object(o)));
		tx.put_key(&key, &record).await?;

		let ns = doc_ctx.ns();
		let db = doc_ctx.db();
		let tb =
			ctx.tx().get_or_add_tb(Some(ctx), &ns.name, &db.name, view_table_name, None).await?;
		let parent = NsDbCtx {
			ns: Arc::clone(ns),
			db: Arc::clone(db),
		};
		let doc_ctx =
			DocumentContext::initialise(ctx, &parent, tb, view_table_name, opt.version, true)
				.await?;

		Document::run_triggers(
			stk,
			ctx,
			opt,
			doc_ctx.clone(),
			id.into(),
			doc::Action::Create,
			None,
			Some(record),
		)
		.await?;

		yield_now!();
	}

	Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn define_table_statement_initialize_aggregate_view(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc_ctx: &DocumentContext,
	view_table_name: &TableName,
	analysis: &AggregationAnalysis,
	condition: Option<&Expr>,
	tables: &[TableName],
) -> Result<()> {
	// To initialize the materialized aggregate view we not only need to initialize records in
	// the view but also find the AggregationStat values.
	// For math::max, and count this is easy, just run a select with the same aggregate.
	// However for mean we don't need to know the mean itself but actually the sum and count.

	#[derive(Clone, Eq, PartialEq, Hash)]
	pub enum SelectAggr {
		// Only used to do the initial select. `math::variance` is the accurate
		// two-pass scalar, which together with the mean gives the shifted
		// accumulators exactly -- see `shifted_state_from_summary`.
		SampleVariance(usize),
		Base(Aggregation),
	}

	// Find out what we need to calculate to initialize the aggregation stats.
	let mut required_values = HashMap::new();
	for aggregation in analysis.aggregations.iter() {
		match aggregation {
			Aggregation::Count => {
				let len = required_values.len();
				required_values.entry(SelectAggr::Base(Aggregation::Count)).or_insert(len);
			}
			Aggregation::CountValue(arg) => {
				let len = required_values.len();
				required_values
					.entry(SelectAggr::Base(Aggregation::CountValue(*arg)))
					.or_insert(len);
			}
			Aggregation::NumberMax(arg) => {
				let len = required_values.len();
				required_values
					.entry(SelectAggr::Base(Aggregation::NumberMax(*arg)))
					.or_insert(len);
			}
			Aggregation::NumberMin(arg) => {
				let len = required_values.len();
				required_values
					.entry(SelectAggr::Base(Aggregation::NumberMin(*arg)))
					.or_insert(len);
			}
			Aggregation::Sum(arg) => {
				let len = required_values.len();
				required_values.entry(SelectAggr::Base(Aggregation::Sum(*arg))).or_insert(len);
			}
			Aggregation::Mean(arg) => {
				// So here for example we need to know 2 things. First the sum for argument
				// `arg` and the record count for the group.
				let len = required_values.len();
				required_values.entry(SelectAggr::Base(Aggregation::Sum(*arg))).or_insert(len);
				let len = required_values.len();
				required_values.entry(SelectAggr::Base(Aggregation::Count)).or_insert(len);
			}
			Aggregation::DatetimeMax(arg) => {
				let len = required_values.len();
				required_values
					.entry(SelectAggr::Base(Aggregation::DatetimeMax(*arg)))
					.or_insert(len);
			}
			Aggregation::DatetimeMin(arg) => {
				let len = required_values.len();
				required_values
					.entry(SelectAggr::Base(Aggregation::DatetimeMin(*arg)))
					.or_insert(len);
			}
			Aggregation::StdDev(arg) | Aggregation::Variance(arg) => {
				let len = required_values.len();
				required_values.entry(SelectAggr::Base(Aggregation::Mean(*arg))).or_insert(len);
				let len = required_values.len();
				required_values.entry(SelectAggr::SampleVariance(*arg)).or_insert(len);
				let len = required_values.len();
				required_values.entry(SelectAggr::Base(Aggregation::Count)).or_insert(len);
			}
			Aggregation::Accumulate(_) => {
				fail!("Accumulate aggregation is not supported in materialized views")
			}
		}
	}

	let mut aggregate_value_expr = Vec::with_capacity(required_values.len());
	for (aggregation, idx) in required_values.iter() {
		let expr = Expr::FunctionCall(Box::new(match aggregation {
			SelectAggr::SampleVariance(arg) => FunctionCall {
				receiver: Function::Normal("math::variance".to_string()),
				arguments: vec![analysis.aggregate_arguments[*arg].clone()],
			},
			SelectAggr::Base(aggregation) => match aggregation {
				Aggregation::Count => FunctionCall {
					receiver: Function::Normal("count".to_string()),
					arguments: Vec::new(),
				},
				Aggregation::CountValue(arg) => FunctionCall {
					receiver: Function::Normal("count".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::NumberMax(arg) => FunctionCall {
					receiver: Function::Normal("math::max".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::NumberMin(arg) => FunctionCall {
					receiver: Function::Normal("math::min".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::Sum(arg) => FunctionCall {
					receiver: Function::Normal("math::sum".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::Mean(arg) => FunctionCall {
					receiver: Function::Normal("math::mean".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::DatetimeMax(arg) => FunctionCall {
					receiver: Function::Normal("time::max".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::DatetimeMin(arg) => FunctionCall {
					receiver: Function::Normal("time::min".to_string()),
					arguments: vec![analysis.aggregate_arguments[*arg].clone()],
				},
				Aggregation::StdDev(_) | Aggregation::Variance(_) => {
					// Not used for initialization.
					unreachable!()
				}
				Aggregation::Accumulate(_) => {
					fail!("Accumulate aggregation is not supported in materialized views")
				}
			},
		}));

		if aggregate_value_expr.len() > *idx {
			aggregate_value_expr[*idx] = expr;
		} else {
			for _ in aggregate_value_expr.len()..*idx {
				// Temp value, overwritten later
				aggregate_value_expr.push(Expr::Break);
			}
			aggregate_value_expr.push(expr)
		}
	}

	let mut fields = Vec::new();

	let mut groups = Vec::new();
	for (idx, g) in analysis.group_expressions.iter().enumerate() {
		let alias = format!("g{}", idx);
		fields.push(Field::Single(Selector {
			expr: g.clone(),
			alias: Some(Idiom::field(alias.clone())),
		}));
		groups.push(Group(Idiom::field(alias)));
	}

	// calculated aggregations return in field 'a'
	fields.push(Field::Single(Selector {
		expr: Expr::Literal(Literal::Array(aggregate_value_expr)),
		alias: Some(Idiom::field("a".to_string())),
	}));

	let stmt = SelectStatement {
		// SELECT [aggregate1, aggregate2, ..] as a, group_expr1 as g0, group_expr2 as g1, ..
		fields: Fields::Select(fields),
		// WHERE cond
		cond: condition.cloned().map(Cond),
		// GROUP BY g0,g1,..
		group: Some(Groups(groups)),
		what: tables.iter().map(|x| Expr::Table(x.clone())).collect(),
		omit: vec![],
		only: false,
		with: None,
		split: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	let res = crate::legacy::select_statement_compute(&stmt, stk, ctx, opt, None).await?;
	let Value::Array(res) = res else {
		fail!("initial select for view did not return an array");
	};

	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let tx = ctx.tx();

	for r in res {
		let Value::Object(mut obj) = r else {
			fail!("select without VALUE did not return an object");
		};

		let mut group = Vec::with_capacity(analysis.group_expressions.len());
		for g in 0..analysis.group_expressions.len() {
			let Some(x) = obj.remove(&format!("g{g}")) else {
				fail!("select result did not contain a field for a selection");
			};
			group.push(x);
		}

		let Some(Value::Array(Array(aggregate_stats))) = obj.remove("a") else {
			fail!("select result did not contain a field for a selection");
		};

		let mut stats = Vec::with_capacity(analysis.aggregations.len());
		for a in analysis.aggregations.iter() {
			match *a {
				Aggregation::Count => {
					let idx = required_values[&SelectAggr::Base(Aggregation::Count)];
					let Value::Number(Number::Int(i)) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					stats.push(AggregationStat::Count {
						count: *i,
					});
				}
				Aggregation::CountValue(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::CountValue(arg))];
					let Value::Number(Number::Int(i)) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					stats.push(AggregationStat::CountValue {
						arg,
						count: *i,
					});
				}
				Aggregation::NumberMax(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::NumberMax(arg))];
					let Value::Number(n) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					stats.push(AggregationStat::NumberMax {
						arg,
						max: *n,
					});
				}
				Aggregation::NumberMin(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::NumberMin(arg))];
					let Value::Number(n) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					stats.push(AggregationStat::NumberMin {
						arg,
						min: *n,
					});
				}
				Aggregation::Sum(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::Sum(arg))];
					let Value::Number(n) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};

					stats.push(AggregationStat::Sum {
						arg,
						sum: *n,
					});
				}
				Aggregation::Mean(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::Sum(arg))];
					let Value::Number(n) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					let idx = required_values[&SelectAggr::Base(Aggregation::Count)];
					let Value::Number(Number::Int(i)) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};

					stats.push(AggregationStat::Mean {
						arg,
						sum: *n,
						count: *i,
					});
				}
				Aggregation::DatetimeMax(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::DatetimeMax(arg))];
					let Value::Datetime(d) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};

					stats.push(AggregationStat::TimeMax {
						arg,
						max: *d,
					});
				}
				Aggregation::DatetimeMin(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::DatetimeMin(arg))];
					let Value::Datetime(d) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};

					stats.push(AggregationStat::TimeMin {
						arg,
						min: *d,
					});
				}
				Aggregation::StdDev(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::Mean(arg))];
					let Value::Number(mean) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					let idx = required_values[&SelectAggr::SampleVariance(arg)];
					let Value::Number(variance) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					let idx = required_values[&SelectAggr::Base(Aggregation::Count)];
					let Value::Number(Number::Int(count)) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};

					let (shift, sum, sum_of_squares) =
						aggregation::shifted_state_from_summary(*mean, *variance, *count)?;
					stats.push(AggregationStat::StdDev {
						arg,
						shift,
						sum,
						sum_of_squares,
						count: *count,
					});
				}
				Aggregation::Variance(arg) => {
					let idx = required_values[&SelectAggr::Base(Aggregation::Mean(arg))];
					let Value::Number(mean) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					let idx = required_values[&SelectAggr::SampleVariance(arg)];
					let Value::Number(variance) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};
					let idx = required_values[&SelectAggr::Base(Aggregation::Count)];
					let Value::Number(Number::Int(count)) = &aggregate_stats[idx] else {
						fail!("initial select statement did not return the right value")
					};

					let (shift, sum, sum_of_squares) =
						aggregation::shifted_state_from_summary(*mean, *variance, *count)?;
					stats.push(AggregationStat::Variance {
						arg,
						shift,
						sum,
						sum_of_squares,
						count: *count,
					});
				}
				Aggregation::Accumulate {
					..
				} => fail!("Accumulate aggregation is not supported in materialized views"),
			}
		}

		// We have now computed the aggregation stats so now we need to insert the record.
		// first calculate the actual value for the record.

		let doc = Value::Object(aggregation::create_field_document(&group, &stats)).into();

		let mut data = Value::empty_object();

		match &analysis.fields {
			AggregateFields::Value(_) => {
				fail!("Value selectors are not supported on views");
			}
			AggregateFields::Fields(items) => {
				for (name, expr) in items {
					let res = stk
						.run(|stk| crate::legacy::expr_compute(expr, stk, ctx, opt, Some(&doc)))
						.await
						.catch_return()?;
					crate::legacy::value_set(&mut data, stk, ctx, opt, name.as_ref(), res).await?;
				}
			}
		};

		let record = Arc::new(Record {
			metadata: Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: stats,
			}),
			data,
		});

		let key = RecordIdKey::Array(Array(group));
		tx.put_record(ns, db, view_table_name, &key, Arc::clone(&record)).await?;

		let id = Arc::new(RecordId {
			table: view_table_name.clone(),
			key,
		});
		Document::run_triggers(
			stk,
			ctx,
			opt,
			doc_ctx.clone(),
			id,
			doc::Action::Create,
			None,
			Some(record),
		)
		.await?;

		yield_now!();
	}

	Ok(())
}

/// Used to add relational fields to existing table records
///
/// Returns the cache key ts.
pub(crate) async fn define_table_statement_add_in_out_fields(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &mut TableDefinition,
) -> Result<()> {
	// Add table relational fields
	if let TableType::Relation(rel) = &tb.table_type {
		let tb_name = tb.name.clone();
		// Set the `in` field as a DEFINE FIELD definition
		{
			let key = FieldKey {
				ns,
				db,
				tb: Cow::Borrowed(&tb_name),
				fd: Cow::Borrowed("in"),
			};
			let fd = FieldDefinition {
				name: Idiom::from(IN.to_vec()),
				table: tb_name.clone(),
				field_kind: Some(Kind::Record(rel.from.clone())),
				..Default::default()
			};
			txn.set_key(&key, &fd.to_stored()).await?;
		}
		// Set the `out` field as a DEFINE FIELD definition
		{
			let key = FieldKey {
				ns,
				db,
				tb: Cow::Borrowed(&tb_name),
				fd: Cow::Borrowed("out"),
			};
			let fd = FieldDefinition {
				name: Idiom::from(OUT.to_vec()),
				table: tb_name.clone(),
				field_kind: Some(Kind::Record(rel.to.clone())),
				..Default::default()
			};
			txn.set_key(&key, &fd.to_stored()).await?;
		}
		// Refresh the table cache for the fields
		tb.cache_fields_ts = Uuid::now_v7();
	}
	Ok(())
}

/// Rejects a table-type change away from `RELATION` while the table still
/// carries `INLINE` fields: their payloads ride the edges' adjacency
/// values, which only relation records have, so the fields must be
/// un-inlined (or removed) first.
pub(crate) async fn validate_inline_fields_survive_type_change(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: Option<&TableDefinition>,
	def: &TableDefinition,
) -> Result<()> {
	let was_relation = existing.is_some_and(|e| matches!(e.table_type, TableType::Relation(_)));
	if !was_relation || matches!(def.table_type, TableType::Relation(_)) {
		return Ok(());
	}
	let has_inline = txn.all_tb_fields(ns, db, &def.name, None).await?.iter().any(|fd| fd.inline);
	anyhow::ensure!(
		!has_inline,
		crate::exec::Error::Thrown(format!(
			"the relation `{}` still has INLINE fields; remove or un-inline them before \
			 changing its type",
			def.name
		))
	);
	Ok(())
}

/// Validates a table definition that declares — or previously declared — a
/// lightweight relation, normalising what the flag implies.
///
/// A lightweight relation's edges are vertex-side adjacency keys alone, so
/// the definition must rule out everything that assumes records exist:
/// endpoints must be explicit (the record-less scan enumerates the `IN`
/// tables) and may only grow while edges exist, enforcement is implied
/// (vertex deletion is the only cleanup path), and the record-consuming
/// clauses — views, changefeeds, DROP, SCHEMAFULL — are rejected. Turning
/// the flag on over an existing table requires it to be provably empty of
/// records, graph keys and user schema objects; turning it off is never
/// allowed, because the stored edges have no records to fall back to.
pub(crate) async fn validate_lightweight_table(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: Option<&TableDefinition>,
	def: &mut TableDefinition,
) -> Result<()> {
	use surrealdb_types::ToSql;

	use crate::exec::Error as ExecError;
	use crate::kvs::lightweight::lightweight_relation;

	let was = existing.and_then(|e| lightweight_relation(&e.table_type)).cloned();
	let is_lightweight = matches!(&def.table_type, TableType::Relation(rel) if rel.lightweight);
	if !is_lightweight {
		// Clearing the flag is allowed only while the relation is provably
		// empty: its stored edges have no records to fall back to, so a
		// non-empty relation losing the flag would strand them. The empty
		// escape also makes a table recoverable if a racing writer ever
		// slips an edge past the becoming-lightweight probes below (see
		// their comment).
		if let Some(old) = was {
			anyhow::ensure!(
				crate::kvs::lightweight::lightweight_relation_is_empty(
					txn, ns, db, &def.name, &old
				)
				.await?,
				ExecError::Thrown(
					"a LIGHTWEIGHT relation can only be redefined as something else while it \
					 holds no edges: delete them first"
						.to_owned()
				)
			);
		}
		return Ok(());
	}
	let TableType::Relation(rel) = &mut def.table_type else {
		unreachable!("is_lightweight implies a relation");
	};
	anyhow::ensure!(
		!rel.from.is_empty() && !rel.to.is_empty(),
		ExecError::Thrown(
			"a LIGHTWEIGHT relation requires explicit IN and OUT tables: the record-less scan \
			 enumerates its edges from the IN tables' adjacency"
				.to_owned()
		)
	);
	// Vertex deletion is the only cleanup path a record-less edge has, so
	// dangling endpoints would be permanent garbage: enforcement is implied.
	rel.enforced = true;
	for (rejected, why) in [
		(def.drop, "DROP"),
		(def.view.is_some(), "AS"),
		(def.changefeed.is_some(), "CHANGEFEED"),
		(def.schemafull, "SCHEMAFULL"),
	] {
		anyhow::ensure!(
			!rejected,
			ExecError::Thrown(format!(
				"a LIGHTWEIGHT relation cannot be declared {why}: its edges store no records"
			))
		);
	}
	let rel = rel.clone();
	match (existing, was) {
		(_, Some(old)) => {
			// Narrowing IN or OUT would orphan edges the scan and the auto
			// in/out field checks then no longer cover; allowed only while
			// the relation is provably empty.
			let narrowed = old.from.iter().any(|t| !rel.from.contains(t))
				|| old.to.iter().any(|t| !rel.to.contains(t));
			if narrowed {
				anyhow::ensure!(
					crate::kvs::lightweight::lightweight_relation_is_empty(
						txn, ns, db, &def.name, &old
					)
					.await?,
					ExecError::Thrown(
						"a LIGHTWEIGHT relation's IN and OUT tables can only grow while it \
						 holds edges"
							.to_owned()
					)
				);
			}
		}
		(Some(_), None) => {
			// Becoming lightweight: the table must be provably empty — no
			// records, no graph keys of its own, no user schema objects —
			// because nothing migrates existing state into the record-less
			// form.
			//
			// The proof reads at this DDL transaction's snapshot, so — as
			// with every schema-shape flip in this executor — a writer
			// concurrently inserting into the pre-flip table can commit
			// beside it with a disjoint write set. The empty-only clearing
			// escape above keeps that window recoverable: delete the
			// slipped edges and redefine.
			let records = RecordPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&def.name),
			};
			let graph = crate::key::schema::GraphPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&def.name),
			};
			let has_records = !txn.keys_raw(records.range()?, 1, 0, None).await?.is_empty();
			let has_graph = !txn.keys_raw(graph.range()?, 1, 0, None).await?.is_empty();
			anyhow::ensure!(
				!has_records && !has_graph,
				ExecError::Thrown(
					"only an empty table can become a LIGHTWEIGHT relation: it still holds \
					 records or graph keys"
						.to_owned()
				)
			);
			let user_fields = txn.all_tb_fields(ns, db, &def.name, None).await?.iter().any(|fd| {
				let name = fd.name.to_sql();
				name != "in" && name != "out"
			});
			let has_events = !txn.all_tb_events(ns, db, &def.name, None).await?.is_empty();
			let has_indexes = !txn.all_tb_indexes(ns, db, &def.name, None).await?.is_empty();
			anyhow::ensure!(
				!user_fields && !has_events && !has_indexes,
				ExecError::Thrown(
					"only a table without fields, events or indexes can become a LIGHTWEIGHT \
					 relation: its edges store no records for them to apply to"
						.to_owned()
				)
			);
		}
		(None, None) => {}
	}
	Ok(())
}
