use core::f64;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::plan::Explanation;
use crate::dbs::store::MemoryCollector;
use crate::dbs::{Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{Expr, Field, Fields, FlowResultExt as _, Function, Groups, Idiom, Part};
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
	/// A list of expressions that must be calculated for the calculation of the aggregate value pushed into the collector.
	exprs: Vec<Expr>,
	/// The list of aggregate expressions found.
	aggregates: Vec<Aggregate>,
	/// The modified expressions which can be used to construct the final value.
	fields: CollectorFields,
	/// The group expressions that define the group of a record.
	groups: Vec<Expr>,

	/// buffers reused during pushing
	exprs_buffer: Vec<Value>,
	group_buffer: Vec<Value>,

	/// The results of the group by.
	results: BTreeMap<Vec<Value>, Vec<Aggregate>>,
}

/// Visitor which walks an expression to pull out the aggregate expressions to calculate.
pub struct AggregateExprCollector<'a> {
	within_aggregate_argument: bool,
	exprs_map: &'a mut HashMap<Expr, usize>,
	aggregates: &'a mut Vec<Aggregate>,
	groups: &'a Groups,
}

fn write_aggregate_field_name(s: &mut String, idx: usize) {
	// Writing into a string cannot error.
	write!(s, "_a{}", idx).unwrap();
}

fn write_group_field_name(s: &mut String, idx: usize) {
	// Writing into a string cannot error.
	write!(s, "_g{}", idx).unwrap();
}

fn aggregate_field_name(idx: usize) -> String {
	let mut res = String::new();
	write_aggregate_field_name(&mut res, idx);
	res
}

fn group_field_name(idx: usize) -> String {
	let mut res = String::new();
	write_group_field_name(&mut res, idx);
	res
}

impl MutVisitor for AggregateExprCollector<'_> {
	type Error = anyhow::Error;

	fn visit_mut_idiom(&mut self, idiom: &mut Idiom) -> Result<(), Self::Error> {
		if !self.within_aggregate_argument {
			if let Some(Part::Field(_)) = idiom.0.first() {
				bail!(Error::InvalidAggregationSelector {
					expr: idiom.to_string(),
				})
			}
		}
		idiom.visit_mut(self)
	}

	fn visit_mut_expr(&mut self, s: &mut Expr) -> Result<(), Self::Error> {
		fn get_aggregate_argument<'a>(name: &str, args: &'a [Expr]) -> Result<&'a Expr> {
			ensure!(
				args.len() == 1,
				Error::InvalidArguments {
					name: name.to_string(),
					message: "Expected 1 argument".to_string()
				}
			);
			Ok(&args[0])
		}

		match s {
			Expr::FunctionCall(f) => {
				if let Function::Normal(x) = &f.receiver {
					match x.as_str() {
						"count" => {
							if f.arguments.is_empty() {
								self.aggregates.push(Aggregate::Count {
									count: 0,
								});
							} else {
								let expr = get_aggregate_argument("count", &f.arguments)?;
								let len = self.exprs_map.len();
								let arg =
									*self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
								self.aggregates.push(Aggregate::CountFn {
									arg,
									count: 0,
								})
							}
						}
						"math::max" => {
							let expr = get_aggregate_argument("math::max", &f.arguments)?;
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
							self.aggregates.push(Aggregate::NumMax {
								arg,
								max: f64::NEG_INFINITY.into(),
							})
						}
						"math::min" => {
							let expr = get_aggregate_argument("math::min", &f.arguments)?;
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
							self.aggregates.push(Aggregate::NumMin {
								arg,
								min: f64::INFINITY.into(),
							})
						}
						"math::sum" => {
							let expr = get_aggregate_argument("math::sum", &f.arguments)?;
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
							self.aggregates.push(Aggregate::NumSum {
								arg,
								sum: Number::Int(0),
							})
						}
						"math::mean" => {
							let expr = get_aggregate_argument("math::mean", &f.arguments)?;
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
							self.aggregates.push(Aggregate::NumMean {
								arg,
								sum: Number::Int(0),
								count: 0,
							})
						}
						"time::max" => {
							let expr = get_aggregate_argument("time::max", &f.arguments)?;
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
							self.aggregates.push(Aggregate::TimeMax {
								arg,
								max: Datetime::MIN_UTC,
							});
						}
						"time::min" => {
							let expr = get_aggregate_argument("math::min", &f.arguments)?;
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
							self.aggregates.push(Aggregate::TimeMin {
								arg,
								min: Datetime::MAX_UTC,
							});
						}
						_ => {
							return f.visit_mut(self);
						}
					}
				} else {
					return f.visit_mut(self);
				}
				self.within_aggregate_argument = true;
				for a in f.arguments.iter_mut() {
					a.visit_mut(self)?;
				}
				self.within_aggregate_argument = false;
				// HACK: We replace the aggregate expression here with an field so that we can later
				// inject the value via the current doc.
				*s = Expr::Idiom(Idiom::field(aggregate_field_name(self.aggregates.len() - 1)));
				Ok(())
			}
			Expr::Idiom(i) => {
				if !self.within_aggregate_argument {
					if let Some(group_idx) = self.groups.0.iter().position(|x| x.0 == *i) {
						i.visit_mut(self)?;
						// HACK: We replace the idioms which refer to the grouping expression here with an field so
						// that we can later inject the value via the current doc.
						*s = Expr::Idiom(Idiom::field(group_field_name(group_idx)));
					} else if let Some(Part::Field(_)) = i.0.first() {
						bail!(Error::InvalidAggregationSelector {
							expr: i.to_string(),
						})
					}
					Ok(())
				} else {
					i.visit_mut(self)
				}
			}
			x => x.visit_mut(self),
		}
	}
}

impl GroupCollector {
	pub fn new(stm: &Statement<'_>) -> Result<Self> {
		let Some(fields) = stm.expr() else {
			fail!("Tried to group a statement without a selector");
		};
		let Some(groups) = stm.group() else {
			fail!("Tried to group a statement without a group");
		};

		// Find all the aggregates within the select statement.
		let mut aggregates = Vec::new();
		let mut exprs_map = HashMap::new();
		let mut aggr_groups = Vec::with_capacity(groups.len());

		for g in groups.0.iter() {
			aggr_groups.push(Expr::Idiom(g.0.clone()))
		}

		let mut collect = AggregateExprCollector {
			within_aggregate_argument: false,
			exprs_map: &mut exprs_map,
			aggregates: &mut aggregates,
			groups,
		};

		// Collect the expressions which calculate the fields of the final object after
		// aggregation.
		let fields = match fields {
			Fields::Value(field) => {
				// alias is unused when using a value selector.
				let Field::Single {
					expr,
					..
				} = field.as_ref()
				else {
					// all is not a valid aggregate selector.
					bail!(Error::InvalidAggregationSelector {
						expr: field.to_string()
					})
				};
				let mut expr = expr.clone();
				// TODO: Check out other places where I might have mistakenly switched
				// a.visit_mut(b) for b.visit_mut_*(a)
				collect.visit_mut_expr(&mut expr)?;
				CollectorFields::Value(expr)
			}
			Fields::Select(fields) => {
				let mut collect_fields = Vec::with_capacity(fields.len());
				for f in fields.iter() {
					let Field::Single {
						expr,
						alias,
					} = f
					else {
						// all is not a valid aggregate selector.
						bail!(Error::InvalidAggregationSelector {
							expr: f.to_string()
						})
					};

					if let Some(alias) = alias
						&& let Some(x) = aggr_groups.iter().position(|x| {
							if let Expr::Idiom(i) = x {
								*i == *alias
							} else {
								false
							}
						}) {
						// The alias is used within the group statement, therefore this expression
						// is a group expression. i.e. year in the statement:
						// `SELECT time::year(time) as year FROM table GROUP BY year`;
						// Replace the calculated group with the expression and then add it to the
						// collected fields via a retrieval from the group.
						aggr_groups[x] = expr.clone();
						collect_fields
							.push((alias.clone(), Expr::Idiom(Idiom::field(group_field_name(x)))));
					} else {
						let name = alias.clone().unwrap_or_else(|| expr.to_idiom());
						let mut expr = expr.clone();
						collect.visit_mut_expr(&mut expr)?;
						collect_fields.push((name, expr))
					}
				}
				CollectorFields::Fields(collect_fields)
			}
		};

		// Place the expression which need to be calculated for the aggregate in the right index.
		let mut exprs = Vec::with_capacity(exprs_map.len());
		for (k, v) in exprs_map.into_iter() {
			if exprs.len() == v {
				exprs.push(k);
			} else if exprs.len() > v {
				exprs[v] = k
			} else {
				for _ in 0..v {
					// push a temp expression that will be overwritten while we collect all the
					// expressions.
					exprs.push(Expr::Break)
				}
				exprs.push(k)
			}
		}

		Ok(dbg!(GroupCollector {
			exprs,
			aggregates,
			fields,
			groups: aggr_groups,

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
		stm: &Statement<'_>,
		rs: RecordStrategy,
		obj: Value,
	) -> Result<()> {
		// compute the group expressions
		let doc = obj.into();
		self.group_buffer.clear();
		for g in self.groups.iter() {
			let v = stk.run(|stk| g.compute(stk, ctx, opt, Some(&doc))).await.catch_return()?;
			self.group_buffer.push(v);
		}

		// Optimize for likely case that the group is already in the set.
		let aggragates = if let Some(x) = self.results.get_mut(&self.group_buffer) {
			x
		} else {
			self.results.entry(self.group_buffer.clone()).or_insert_with(|| self.aggregates.clone())
		};

		if let RecordStrategy::Count = rs {
			let Value::Number(n) = doc.doc.data.as_ref() else {
				fail!("Value for Count RecordStrategy was not a number");
			};

			for a in aggragates.iter_mut() {
				if let Aggregate::Count {
					count,
				} = a
				{
					*count = n.as_int() as u64;
				}
			}
		} else {
			// calculate the arguments for the aggregate functions
			self.exprs_buffer.clear();
			for v in self.exprs.iter() {
				let v = stk.run(|stk| v.compute(stk, ctx, opt, Some(&doc))).await.catch_return()?;
				self.exprs_buffer.push(v);
			}

			// update all aggregates
			for a in aggragates {
				match a {
					Aggregate::Count {
						count,
					} => {
						*count += 1;
					}
					Aggregate::CountFn {
						arg,
						count,
					} => {
						*count += self.exprs_buffer[*arg].is_truthy() as u64;
					}
					Aggregate::NumMax {
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
					Aggregate::NumMin {
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
					Aggregate::NumSum {
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
					Aggregate::NumMean {
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
					Aggregate::TimeMax {
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
					Aggregate::TimeMin {
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
		stm: &Statement<'_>,
	) -> Result<MemoryCollector> {
		let mut collector = MemoryCollector::default();

		let mut field_buffer = String::new();
		let mut doc: CursorDoc = Value::empty_object().into();

		for (group, result) in std::mem::take(&mut self.results) {
			let Value::Object(doc_obj) = doc.doc.data.to_mut() else {
				// We create the document above as a object so it must be an object.
				unreachable!()
			};
			//setup the document
			for (idx, a) in result.iter().enumerate() {
				field_buffer.clear();
				write_aggregate_field_name(&mut field_buffer, idx);

				let value = match a {
					Aggregate::Count {
						count,
					}
					| Aggregate::CountFn {
						count,
						..
					} => Value::from(Number::from(*count as i64)),
					Aggregate::NumMax {
						max,
						..
					} => (*max).into(),
					Aggregate::NumMin {
						min,
						..
					} => (*min).into(),
					Aggregate::NumSum {
						sum,
						..
					} => (*sum).into(),
					Aggregate::NumMean {
						sum,
						count,
						..
					} => sum.try_float_div((*count as i64).into()).unwrap_or(f64::NAN.into()).into(),
					Aggregate::TimeMax {
						max,
						..
					} => max.clone().into(),
					Aggregate::TimeMin {
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
				write_group_field_name(&mut field_buffer, idx);

				// Optimize for the common case where the field is already in the document.
				if let Some(x) = doc_obj.get_mut(&field_buffer) {
					*x = g;
				} else {
					doc_obj.insert(field_buffer.clone(), g);
				}
			}

			// Calculate the final value for the fields.
			match &self.fields {
				CollectorFields::Value(expr) => {
					let res = stk
						.run(|stk| expr.compute(stk, ctx, opt, Some(&doc)))
						.await
						.catch_return()?;
					collector.push(res);
				}
				CollectorFields::Fields(items) => {
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
