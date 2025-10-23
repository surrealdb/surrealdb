use core::f64;
use std::collections::BTreeMap;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::AggregationStat;
use crate::ctx::Context;
use crate::dbs::aggregation::{self, AggregateFields, AggregationAnalysis};
use crate::dbs::plan::Explanation;
use crate::dbs::store::MemoryCollector;
use crate::dbs::{Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, FlowResultExt as _, Idiom};
use crate::idx::planner::RecordStrategy;
use crate::val::{Datetime, Number, TryAdd, TryFloatDiv, Value};

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub enum Aggregate {
	Count {
		count: u64,
	},
	CountFn {
		/// Index into the exprs field on GroupCollector.
		arg: usize,
		count: u64,
	},
	NumMax {
		arg: usize,
		max: Number,
	},
	NumMin {
		arg: usize,
		min: Number,
	},
	NumSum {
		arg: usize,
		sum: Number,
	},
	NumMean {
		arg: usize,
		sum: Number,
		count: u64,
	},
	TimeMax {
		arg: usize,
		max: Datetime,
	},
	TimeMin {
		arg: usize,
		min: Datetime,
	},
}

#[derive(Debug)]
pub enum CollectorFields {
	Value(Expr),
	Fields(Vec<(Idiom, Expr)>),
}

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

		let analysis = AggregationAnalysis::analyze_fields_groups(fields, groups)?;

		Ok(dbg!(GroupCollector {
			analysis,

			exprs_buffer: Vec::new(),
			group_buffer: Vec::new(),

			results: BTreeMap::new(),
		}))
	}

	pub fn len(&self) -> usize {
		self.results.len()
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {

		/*
		let mut explain = BTreeMap::new();
		let idioms: Vec<String> = self.idioms.iter().cloned().map(|i| i.to_string()).collect();
		for (i, a) in idioms.into_iter().zip(&self.base) {
		explain.insert(i, a.explain());
		}
		exp.add_collector("Group", vec![("idioms", explain.into())]);
		*/
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
			self.results.entry(self.group_buffer.clone()).or_insert_with(|| {
				self.analysis.aggregations.iter().map(|x| x.into_stat()).collect()
			})
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

			// update all aggregates
			for a in aggragates {
				match a {
					AggregationStat::Count {
						count,
					} => {
						*count += 1;
					}
					AggregationStat::CountFn {
						arg,
						count,
					} => {
						*count += self.exprs_buffer[*arg].is_truthy() as i64;
					}
					AggregationStat::NumMax {
						arg,
						max,
					} => {
						let Value::Number(ref n) = self.exprs_buffer[*arg] else {
							bail!(Error::InvalidArguments {
								name: "math::max".to_string(),
								message: format!(
									"Argument 1 was the wrong type. Expected `number` but found `{}`",
									self.exprs_buffer[*arg]
								),
							})
						};
						if *max < *n {
							*max = *n
						}
					}
					AggregationStat::NumMin {
						arg,
						min,
					} => {
						let Value::Number(ref n) = self.exprs_buffer[*arg] else {
							bail!(Error::InvalidArguments {
								name: "math::min".to_string(),
								message: format!(
									"Argument 1 was the wrong type. Expected `number` but found `{}`",
									self.exprs_buffer[*arg]
								),
							})
						};
						if *min > *n {
							*min = *n
						}
					}
					AggregationStat::NumSum {
						arg,
						sum,
					} => {
						let Value::Number(ref n) = self.exprs_buffer[*arg] else {
							bail!(Error::InvalidArguments {
								name: "math::sum".to_string(),
								message: format!(
									"Argument 1 was the wrong type. Expected `number` but found `{}`",
									self.exprs_buffer[*arg]
								),
							})
						};
						*sum = (*sum).try_add(*n)?;
					}
					AggregationStat::NumMean {
						arg,
						sum,
						count,
					} => {
						let Value::Number(ref n) = self.exprs_buffer[*arg] else {
							bail!(Error::InvalidArguments {
								name: "math::mean".to_string(),
								message: format!(
									"Argument 1 was the wrong type. Expected `number` but found `{}`",
									self.exprs_buffer[*arg]
								),
							})
						};

						*sum = (*sum).try_add(*n)?;
						*count += 1;
					}
					AggregationStat::TimeMax {
						arg,
						max,
					} => {
						let Value::Datetime(ref d) = self.exprs_buffer[*arg] else {
							bail!(Error::InvalidArguments {
								name: "time::max".to_string(),
								message: format!(
									"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
									self.exprs_buffer[*arg]
								),
							})
						};

						if *max < *d {
							*max = d.clone();
						}
					}
					AggregationStat::TimeMin {
						arg,
						min,
					} => {
						let Value::Datetime(ref d) = self.exprs_buffer[*arg] else {
							bail!(Error::InvalidArguments {
								name: "time::min".to_string(),
								message: format!(
									"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
									self.exprs_buffer[*arg]
								),
							})
						};

						if *min > *d {
							*min = d.clone();
						}
					}
				}
			}
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
			for (idx, a) in result.iter().enumerate() {
				field_buffer.clear();
				aggregation::write_aggregate_field_name(&mut field_buffer, idx);

				let value = match a {
					AggregationStat::Count {
						count,
					}
					| AggregationStat::CountFn {
						count,
						..
					} => Value::from(Number::from(*count)),
					AggregationStat::NumMax {
						max,
						..
					} => (*max).into(),
					AggregationStat::NumMin {
						min,
						..
					} => (*min).into(),
					AggregationStat::NumSum {
						sum,
						..
					} => (*sum).into(),
					AggregationStat::NumMean {
						sum,
						count,
						..
					} => sum.try_float_div((*count).into()).unwrap_or(f64::NAN.into()).into(),
					AggregationStat::TimeMax {
						max,
						..
					} => max.clone().into(),
					AggregationStat::TimeMin {
						min,
						..
					} => min.clone().into(),
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
