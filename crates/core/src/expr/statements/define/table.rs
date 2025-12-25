use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::aggregation::{
	self, AggregateFields, Aggregation, AggregationAnalysis, AggregationStat,
};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{
	DatabaseId, FieldDefinition, Metadata, NamespaceId, Permissions, Record, RecordType,
	TableDefinition, TableType, ViewDefinition,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{self, CursorDoc, Document, DocumentContext, NsDbTbCtx};
use crate::err::Error;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::field::Selector;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::paths::{IN, OUT};
use crate::expr::{
	Base, BinaryOperator, Cond, Expr, Field, Fields, FlowResultExt, Function, FunctionCall, Group,
	Groups, Idiom, Kind, Literal, SelectStatement, View,
};
use crate::iam::{Action, ResourceKind};
use crate::key;
use crate::kvs::Transaction;
use crate::val::{Array, Number, RecordId, RecordIdKey, TableName, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineTableStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Expr,
	pub table_type: TableType,
}

impl Default for DefineTableStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::String(String::new())),
			drop: false,
			full: false,
			view: None,
			permissions: Permissions::default(),
			changefeed: None,
			comment: Expr::Literal(Literal::None),
			table_type: TableType::default(),
		}
	}
}

impl DefineTableStatement {
	#[instrument(level = "trace", name = "DefineTableStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;

		// Process the name
		let name =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.name, "table name").await?);

		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;

		// Fetch the transaction
		let txn = ctx.tx();

		let ns = txn.expect_ns_by_name(ns_name).await?;
		let db = txn.expect_db_by_name(ns_name, db_name).await?;

		// Check if the definition exists
		let table_id = if let Some(tb) = txn.get_tb(ns.namespace_id, db.database_id, &name).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::TbAlreadyExists {
							name: name.clone().into_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}

			tb.table_id
		} else {
			txn.get_next_tb_id(Some(ctx), ns.namespace_id, db.database_id).await?
		};

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
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
			drop: self.drop,
			schemafull: self.full,
			table_type: self.table_type.clone(),
			view: self.view.clone().map(|v| v.to_definition()).transpose()?,
			permissions: self.permissions.clone(),
			comment,
			changefeed: self.changefeed,

			cache_fields_ts: cache_ts,
			cache_events_ts: cache_ts,
			cache_indexes_ts: cache_ts,
			cache_tables_ts: cache_ts,
		};

		// Add table relational fields
		Self::add_in_out_fields(&txn, ns.namespace_id, db.database_id, &mut tb_def).await?;

		// Record definition change
		if self.changefeed.is_some() {
			txn.changefeed_buffer_table_change(ns.namespace_id, db.database_id, &name, &tb_def);
		}

		// Update the catalog
		let tb = txn.put_tb(ns_name, db_name, &tb_def).await?;
		let fields = txn.all_tb_fields(ns.namespace_id, db.database_id, &name, opt.version).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns.namespace_id, db.database_id, &name);
		}
		// Clear the cache
		txn.clear_cache();

		let doc_ctx = DocumentContext::NsDbTbCtx(NsDbTbCtx {
			ns: Arc::clone(&ns),
			db: Arc::clone(&db),
			tb,
			fields,
		});

		// Check if table is a view
		if let Some(view) = &tb_def.view {
			// Remove the table data
			let key = crate::key::table::all::new(ns.namespace_id, db.database_id, &name);
			txn.delp(&key).await?;

			let (ViewDefinition::Materialized {
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
			}) = &view;

			// Process each foreign table
			for ft in tables.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(ns.namespace_id, db.database_id, ft, &name);
				txn.set(&key, &tb_def, None).await?;
				// Refresh the table cache
				let Some(foreign_tb) = txn.get_tb(ns.namespace_id, db.database_id, ft).await?
				else {
					bail!(Error::TbNotFound {
						name: ft.clone(),
					});
				};

				txn.put_tb(
					ns_name,
					db_name,
					&TableDefinition {
						cache_tables_ts: Uuid::now_v7(),
						..foreign_tb.as_ref().clone()
					},
				)
				.await?;

				// Clear the cache
				if let Some(cache) = ctx.get_cache() {
					cache.clear_tb(ns.namespace_id, db.database_id, ft);
				}
				// Clear the cache
				txn.clear_cache();
			}

			Self::initialize_view(stk, ctx, opt, &doc_ctx, &name, view).await?;
		}
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns.namespace_id, db.database_id, &name);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}

	async fn initialize_view(
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
				Self::initialize_materialized_view(
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
				Self::initialize_aggregate_view(
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
	async fn initialize_materialized_view(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: &DocumentContext,
		view_table_name: &TableName,
		fields: &Fields,
		tables: &[TableName],
		condition: Option<&Expr>,
	) -> Result<()> {
		let select = SelectStatement {
			expr: fields.clone(),
			what: tables.iter().map(|x| Expr::Table(x.clone())).collect(),
			cond: condition.cloned().map(Cond),
			..Default::default()
		};

		let Value::Array(Array(v)) = select.compute(stk, ctx, opt, None).await? else {
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

			let key = key::record::new(ns, db, view_table_name, &id.key);
			let record = Arc::new(Record::new(Value::Object(o).into()));
			tx.put(&key, &record, None).await?;

			let ns = doc_ctx.ns();
			let db = doc_ctx.db();
			let tb = ctx.tx().get_or_add_tb(Some(ctx), &ns.name, &db.name, view_table_name).await?;
			let fields = ctx
				.tx()
				.all_tb_fields(ns.namespace_id, db.database_id, view_table_name, opt.version)
				.await?;
			let doc_ctx = DocumentContext::NsDbTbCtx(NsDbTbCtx {
				ns: Arc::clone(ns),
				db: Arc::clone(db),
				tb,
				fields,
			});

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
	async fn initialize_aggregate_view(
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
			// Only used to do initial select.
			PowSum(usize),
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
					required_values.entry(SelectAggr::Base(Aggregation::Sum(*arg))).or_insert(len);
					let len = required_values.len();
					required_values.entry(SelectAggr::PowSum(*arg)).or_insert(len);
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
				SelectAggr::PowSum(arg) => {
					let expr = Expr::Binary {
						left: Box::new(analysis.aggregate_arguments[*arg].clone()),
						op: BinaryOperator::Power,
						right: Box::new(Expr::Literal(Literal::Integer(2))),
					};
					FunctionCall {
						receiver: Function::Normal("count".to_string()),
						arguments: vec![expr],
					}
				}
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
			expr: Fields::Select(fields),
			// WHERE cond
			cond: condition.cloned().map(Cond),
			// GROUP BY g0,g1,..
			group: Some(Groups(groups)),
			what: tables.iter().map(|x| Expr::Table(x.clone())).collect(),
			..Default::default()
		};
		let res = stmt.compute(stk, ctx, opt, None).await?;
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
							max: d.clone(),
						});
					}
					Aggregation::DatetimeMin(arg) => {
						let idx = required_values[&SelectAggr::Base(Aggregation::DatetimeMax(arg))];
						let Value::Datetime(d) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};

						stats.push(AggregationStat::TimeMax {
							arg,
							max: d.clone(),
						});
					}
					Aggregation::StdDev(arg) => {
						let idx = required_values[&SelectAggr::Base(Aggregation::Sum(arg))];
						let Value::Number(sum) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};
						let idx = required_values[&SelectAggr::PowSum(arg)];
						let Value::Number(sum_of_squares) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};
						let idx = required_values[&SelectAggr::Base(Aggregation::Count)];
						let Value::Number(Number::Int(count)) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};

						stats.push(AggregationStat::StdDev {
							arg,
							sum: *sum,
							sum_of_squares: *sum_of_squares,
							count: *count,
						});
					}
					Aggregation::Variance(arg) => {
						let idx = required_values[&SelectAggr::Base(Aggregation::Sum(arg))];
						let Value::Number(sum) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};
						let idx = required_values[&SelectAggr::PowSum(arg)];
						let Value::Number(sum_of_squares) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};
						let idx = required_values[&SelectAggr::Base(Aggregation::Count)];
						let Value::Number(Number::Int(count)) = &aggregate_stats[idx] else {
							fail!("initial select statement did not return the right value")
						};

						stats.push(AggregationStat::Variance {
							arg,
							sum: *sum,
							sum_of_squares: *sum_of_squares,
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
							.run(|stk| expr.compute(stk, ctx, opt, Some(&doc)))
							.await
							.catch_return()?;
						data.set(stk, ctx, opt, name.as_ref(), res).await?;
					}
				}
			};

			let record = Arc::new(Record {
				metadata: Some(Metadata {
					record_type: RecordType::Table,
					aggregation_stats: stats,
				}),
				data: data.into(),
			});

			let key = RecordIdKey::Array(Array(group));
			tx.put_record(ns, db, view_table_name, &key, record.clone(), None).await?;

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
	pub async fn add_in_out_fields(
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &mut TableDefinition,
	) -> Result<()> {
		// Add table relational fields
		if let TableType::Relation(rel) = &tb.table_type {
			// Set the `in` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "in");
				let val = rel.from.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					&key,
					&FieldDefinition {
						name: Idiom::from(IN.to_vec()),
						table: tb.name.clone(),
						field_kind: Some(val),
						..Default::default()
					},
					None,
				)
				.await?;
			}
			// Set the `out` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "out");
				let val = rel.to.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					&key,
					&FieldDefinition {
						name: Idiom::from(OUT.to_vec()),
						table: tb.name.clone(),
						field_kind: Some(val),
						..Default::default()
					},
					None,
				)
				.await?;
			}
			// Refresh the table cache for the fields
			tb.cache_fields_ts = Uuid::now_v7();
		}
		Ok(())
	}
}
