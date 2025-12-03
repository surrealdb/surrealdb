use core::f64;
use std::collections::BTreeMap;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::aggregation::{self, AggregateFields, AggregationAnalysis, AggregationStat};
use crate::ctx::Context;
use crate::dbs::plan::Explanation;
use crate::dbs::store::MemoryCollector;
use crate::dbs::{Options, Statement};
use crate::doc::CursorDoc;
use crate::expr::FlowResultExt as _;
use crate::idx::planner::RecordStrategy;
use crate::val::{Number, TryFloatDiv, Value};

/// A collector for statements which have a group by clause.
///
/// This works by having the iterator return the full value of the record.
/// The group collector has pulled out all the aggregate expressions from selectors and is updating
/// those as it recieves values.
///
/// Once all the values are collected the collector then does the field calculation replacing the
/// spaces in the expressions where the aggregate expressions used to be with the values it
/// calcualted.
#[derive(Debug)]
pub struct GroupCollector {
	analysis: AggregationAnalysis,

	/// buffers reused during pushing
	exprs_buffer: Vec<Value>,
	group_buffer: Vec<Value>,

	/// The results of the group by.
	results: BTreeMap<Vec<Value>, Vec<AggregationStat>>,
}

impl GroupCollector {
	pub fn new(stm: &Statement<'_>) -> Result<Self> {
		let Some(fields) = stm.expr() else {
			fail!("Tried to group a statement without a selector");
		};
		let Some(groups) = stm.group() else {
			fail!("Tried to group a statement without a group");
		};

		let analysis = AggregationAnalysis::analyze_fields_groups(fields, groups, false)?;

		Ok(GroupCollector {
			analysis,

			exprs_buffer: Vec::new(),
			group_buffer: Vec::new(),

			results: BTreeMap::new(),
		})
	}

	pub fn len(&self) -> usize {
		self.results.len()
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		let aggr_agrs = self
			.analysis
			.aggregate_arguments
			.iter()
			.enumerate()
			.map(|(idx, x)| (format!("expr{idx}"), Value::from(x.to_sql())))
			.collect::<Value>();

		let group_expr = self
			.analysis
			.group_expressions
			.iter()
			.enumerate()
			.map(|(idx, x)| (format!("_g{idx}"), Value::from(x.to_sql())))
			.collect::<Value>();

		let selector = match &self.analysis.fields {
			AggregateFields::Value(expr) => Value::from(expr.to_sql()),
			AggregateFields::Fields(items) => {
				items.iter().map(|(k, v)| (k.to_sql(), Value::from(v.to_sql()))).collect()
			}
		};

		let aggregates = self
			.analysis
			.aggregations
			.iter()
			.enumerate()
			.map(|(idx, x)| {
				let res = match x {
					aggregation::Aggregation::Count => "Count".to_string(),
					aggregation::Aggregation::CountValue(x) => format!("CountValue(expr{x})"),
					aggregation::Aggregation::NumberMax(x) => format!("NumberMax(expr{x})"),
					aggregation::Aggregation::NumberMin(x) => format!("NumberMin(expr{x})"),
					aggregation::Aggregation::Sum(x) => format!("Sum(expr{x})"),
					aggregation::Aggregation::Mean(x) => format!("Mean(expr{x})"),
					aggregation::Aggregation::StdDev(x) => format!("StdDev(expr{x})"),
					aggregation::Aggregation::Variance(x) => format!("Variance(expr{x})"),
					aggregation::Aggregation::DatetimeMax(x) => format!("DatetimeMax(expr{x})"),
					aggregation::Aggregation::DatetimeMin(x) => format!("DatetimeMin(expr{x})"),
					aggregation::Aggregation::Accumulate(x) => format!("Accumulate(expr{x})"),
				};
				(format!("_a{idx}"), Value::from(res))
			})
			.collect();

		exp.add_collector(
			"Group",
			vec![
				("Aggregate expressions", aggr_agrs),
				("Group expressions", group_expr),
				("Aggregations", aggregates),
				("Select expression", selector),
			],
		);
	}

	pub async fn push(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rs: RecordStrategy,
		obj: Value,
	) -> Result<()> {
		// compute the group expressions
		let doc = obj.into();
		self.group_buffer.clear();
		for g in self.analysis.group_expressions.iter() {
			let v = stk.run(|stk| g.compute(stk, ctx, opt, Some(&doc))).await.catch_return()?;
			self.group_buffer.push(v);
		}

		// Optimize for likely case that the group is already in the set.
		let aggragates = if let Some(x) = self.results.get_mut(&self.group_buffer) {
			x
		} else {
			self.results
				.entry(self.group_buffer.clone())
				.or_insert_with(|| self.analysis.aggregations.iter().map(|x| x.to_stat()).collect())
		};

		if let RecordStrategy::Count = rs {
			let Value::Number(n) = doc.doc.data.as_ref() else {
				fail!("Value for Count RecordStrategy was not a number");
			};

			for a in aggragates.iter_mut() {
				if let AggregationStat::Count {
					count,
				} = a
				{
					*count = n.as_int();
				}
			}
		} else {
			// calculate the arguments for the aggregate functions
			self.exprs_buffer.clear();
			for v in self.analysis.aggregate_arguments.iter() {
				let v = stk.run(|stk| v.compute(stk, ctx, opt, Some(&doc))).await.catch_return()?;
				self.exprs_buffer.push(v);
			}

			aggregation::add_to_aggregation_stats(&self.exprs_buffer, aggragates)?;
		}

		Ok(())
	}

	pub(super) async fn output(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
	) -> Result<MemoryCollector> {
		let mut collector = MemoryCollector::default();

		let mut field_buffer = String::new();
		let mut doc: CursorDoc = Value::empty_object().into();

		for (group, result) in std::mem::take(&mut self.results) {
			let Value::Object(doc_obj) = doc.doc.data.to_mut() else {
				// We create the document above as a object so it must be an object.
				unreachable!()
			};

			//setup the document for final value calculation
			for (idx, a) in result.into_iter().enumerate() {
				field_buffer.clear();
				aggregation::write_aggregate_field_name(&mut field_buffer, idx);

				let value = match a {
					AggregationStat::Count {
						count,
					}
					| AggregationStat::CountValue {
						count,
						..
					} => Value::from(Number::from(count)),
					AggregationStat::NumberMax {
						max,
						..
					} => max.into(),
					AggregationStat::NumberMin {
						min,
						..
					} => min.into(),
					AggregationStat::Sum {
						sum,
						..
					} => sum.into(),
					AggregationStat::Mean {
						sum,
						count,
						..
					} => sum.try_float_div(count.into()).unwrap_or(f64::NAN.into()).into(),
					AggregationStat::TimeMax {
						max,
						..
					} => max.into(),
					AggregationStat::TimeMin {
						min,
						..
					} => min.into(),
					AggregationStat::Accumulate {
						values,
						..
					} => values.into(),
					AggregationStat::StdDev {
						sum,
						sum_of_squares,
						count,
						..
					} => {
						let num = if count <= 1 {
							Number::from(0.0)
						} else {
							let mean = sum / Number::from(count);
							let variance =
								(sum_of_squares - (sum * mean)) / Number::from(count - 1);
							if variance == Number::from(0.0) {
								Number::from(0.0)
							} else {
								variance.sqrt()
							}
						};
						num.into()
					}
					AggregationStat::Variance {
						sum,
						sum_of_squares,
						count,
						..
					} => {
						let num = if count <= 1 {
							Number::from(0.0)
						} else {
							let mean = sum / Number::from(count);
							(sum_of_squares - (sum * mean)) / Number::from(count - 1)
						};
						num.into()
					}
				};

				// Optimize for the common case where the field is already in the document.
				if let Some(x) = doc_obj.get_mut(&field_buffer) {
					*x = value;
				} else {
					doc_obj.insert(field_buffer.clone(), value);
				}
			}
			for (idx, g) in group.into_iter().enumerate() {
				field_buffer.clear();
				aggregation::write_group_field_name(&mut field_buffer, idx);

				// Optimize for the common case where the field is already in the document.
				if let Some(x) = doc_obj.get_mut(&field_buffer) {
					*x = g;
				} else {
					doc_obj.insert(field_buffer.clone(), g);
				}
			}

			// Calculate the final value for the fields.
			match &self.analysis.fields {
				AggregateFields::Value(expr) => {
					let res = stk
						.run(|stk| expr.compute(stk, ctx, opt, Some(&doc)))
						.await
						.catch_return()?;
					collector.push(res);
				}
				AggregateFields::Fields(items) => {
					let mut obj = Value::empty_object();
					for (name, expr) in items {
						let res = stk
							.run(|stk| expr.compute(stk, ctx, opt, Some(&doc)))
							.await
							.catch_return()?;
						obj.set(stk, ctx, opt, name.as_ref(), res).await?;
					}
					collector.push(obj);
				}
			}
		}

		Ok(collector)
	}
}
