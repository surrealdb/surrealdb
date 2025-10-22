use core::f64;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use ahash::HashSet;
use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::plan::Explanation;
use crate::dbs::store::MemoryCollector;
use crate::dbs::{Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::visit::{MutVisitor, Visit, VisitMut, Visitor};
use crate::expr::{
	Expr, Field, Fields, FlowResultExt as _, Function, FunctionCall, Idiom, Param, Part,
};
use crate::idx::planner::RecordStrategy;
use crate::val::{Array, Datetime, Number, TryAdd, TryDiv, TryFloatDiv, Value};

pub(super) struct GroupsCollector {
	base: Vec<Aggregator>,
	idioms: Vec<Idiom>,
	grp: BTreeMap<Array, Vec<Aggregator>>,
	pub _tmp: GroupCollector,
}

#[derive(Default)]
struct Aggregator {
	array: Option<Array>,
	first_val: Option<Value>,
	count: Option<usize>,
	count_function: Option<usize>,
	math_max: Option<Value>,
	math_min: Option<Value>,
	math_sum: Option<Value>,
	math_mean: Option<(Value, usize)>,
	time_max: Option<Value>,
	time_min: Option<Value>,
}

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub enum Aggregate {
	Count {
		/// Index into the arguments map.
		count: u64,
	},
	CountFn {
		/// Index into the arguments map.
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
	/// A list of expressions that must be calculation for every value pushed into the collector.
	exprs: Vec<Expr>,
	/// A map from expression to index within the exprs.
	exprs_map: HashMap<Expr, usize>,
	/// The list of aggregate expressions found.
	aggregates: Vec<Aggregate>,
	/// The modified expressions which can be used to construct the final value.
	fields: CollectorFields,

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
}

fn aggregate_param_name(idx: usize) -> String {
	format!("__\n\naggregate{}", idx)
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
		let Expr::FunctionCall(f) = s else {
			return s.visit_mut(self);
		};
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
						let arg = *self.exprs_map.entry(expr.clone()).or_insert_with(|| len);
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
		// HACK: We replace the aggregate expression here with an parameter so that we can later
		// inject the value. It is technically possible to access this value in via a parameter in
		// the expression.
		*s = Expr::Param(Param::new(aggregate_param_name(self.aggregates.len() - 1)));
		Ok(())
	}
}

impl GroupCollector {
	pub fn new(stm: &Statement<'_>) -> Result<Self> {
		let Some(fields) = stm.expr() else {
			fail!("Tried to group a statement without a selector");
		};
		let mut aggregates = Vec::new();
		let mut exprs_map = HashMap::new();

		let mut collect = AggregateExprCollector {
			within_aggregate_argument: false,
			exprs_map: &mut exprs_map,
			aggregates: &mut aggregates,
		};

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
					let name = alias.clone().unwrap_or_else(|| expr.to_idiom());
					let mut expr = dbg!(expr.clone());
					collect.visit_mut_expr(&mut expr)?;
					collect_fields.push((name, expr))
				}
				CollectorFields::Fields(collect_fields)
			}
		};

		let mut exprs = Vec::with_capacity(exprs_map.len());
		for (k, v) in exprs_map.iter() {
			if exprs.len() == *v {
				exprs.push(k.clone());
			} else if exprs.len() > *v {
				exprs[*v] = k.clone()
			} else {
				for _ in 0..(*v) {
					// push a temp expression that will be overwritten while we collect all the
					// expressions.
					exprs.push(Expr::Break)
				}
			}
		}

		Ok(GroupCollector {
			exprs,
			exprs_map,
			aggregates,
			fields,

			exprs_buffer: Vec::new(),
			group_buffer: Vec::new(),

			results: BTreeMap::new(),
		})
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
		let Some(group) = stm.group() else {
			fail!("Tried to pushing into a grouping collector without having a group");
		};

		self.group_buffer.clear();
		for g in group.iter() {
			self.group_buffer.push(obj.pick(g));
		}

		// Optimize for likely case that the group is already in the set.
		let aggragates = if let Some(x) = self.results.get_mut(&self.group_buffer) {
			x
		} else {
			self.results.entry(self.group_buffer.clone()).or_insert_with(|| self.aggregates.clone())
		};

		if let RecordStrategy::Count = rs {
			let Value::Number(n) = obj else {
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
			let doc = obj.into();
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
							todo!()
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
							todo!()
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
							todo!()
						};
						*sum = (*sum).try_add(*n)?;
					}
					Aggregate::NumMean {
						arg,
						sum,
						count,
					} => {
						let Value::Number(ref n) = self.exprs_buffer[*arg] else {
							todo!()
						};

						*sum = (*sum).try_add(*n)?;
						*count += 1;
					}
					Aggregate::TimeMax {
						arg,
						max,
					} => {
						let Value::Datetime(ref d) = self.exprs_buffer[*arg] else {
							todo!()
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
							todo!()
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

		let mut doc: CursorDoc = Value::empty_object().into();

		for (_, result) in std::mem::take(&mut self.results) {
			let doc_obj = doc.doc.data.to_mut();
			//setup the document
			for (idx, a) in result.iter().enumerate() {
				let name = aggregate_param_name(idx);
				let Value::Object(obj) = doc_obj else {
					unreachable!()
				};

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
					} => sum.try_div((*count as i64).into()).unwrap_or(f64::NAN.into()).into(),
					Aggregate::TimeMax {
						max,
						..
					} => max.clone().into(),
					Aggregate::TimeMin {
						min,
						..
					} => min.clone().into(),
				};

				obj.0.insert(name, value);
			}

			match &self.fields {
				CollectorFields::Value(expr) => {
					let res =
						stk.run(|stk| expr.compute(stk, ctx, opt, None)).await.catch_return()?;
					collector.push(res);
				}
				CollectorFields::Fields(items) => {
					let mut obj = Value::empty_object();
					for (name, expr) in items {
						let res = stk
							.run(|stk| expr.compute(stk, ctx, opt, None))
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

pub struct AggregatorCollector<'a> {
	pub within_aggregator_arg: bool,
	pub arguments_map: &'a mut HashMap<Expr, usize>,
	pub arguments: &'a mut Vec<Expr>,
	pub aggregates: &'a mut HashSet<Aggregate>,
}

impl GroupsCollector {
	pub(super) fn new(stm: &Statement<'_>) -> Self {
		let _tmp = dbg!(GroupCollector::new(stm).unwrap());

		let mut idioms_agr: HashMap<Idiom, Aggregator> = HashMap::new();
		if let Some(fields) = stm.expr() {
			for field in fields.iter_non_all_fields() {
				if let Field::Single {
					expr,
					alias,
				} = field
				{
					let idiom = alias.as_ref().cloned().unwrap_or_else(|| expr.to_idiom());
					idioms_agr.entry(idiom).or_default().prepare(expr);
				}
			}
		}
		let mut base = Vec::with_capacity(idioms_agr.len());
		let mut idioms = Vec::with_capacity(idioms_agr.len());
		for (idiom, agr) in idioms_agr {
			base.push(agr);
			idioms.push(idiom);
		}
		Self {
			base,
			idioms,
			grp: Default::default(),
			_tmp,
		}
	}

	pub(super) async fn push(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		rs: RecordStrategy,
		obj: Value,
	) -> Result<()> {
		dbg!(&obj);
		self._tmp.push(stk, ctx, opt, stm, rs, obj.clone()).await.unwrap();
		if let Some(groups) = stm.group() {
			// Create a new column set
			let mut arr = Array::with_capacity(groups.len());
			// Loop over each group clause
			for group in groups.iter() {
				// Get the value at the path
				let val = obj.pick(group);
				// Set the value at the path
				arr.push(val);
			}
			// Add to grouped collection
			let agr = self
				.grp
				.entry(arr)
				.or_insert_with(|| self.base.iter().map(|a| a.new_instance()).collect());
			Self::pushes(stk, ctx, opt, agr, &self.idioms, rs, obj).await?
		}
		Ok(())
	}

	async fn pushes(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		agrs: &mut [Aggregator],
		idioms: &[Idiom],
		rs: RecordStrategy,
		obj: Value,
	) -> Result<()> {
		let mut count_value = None;
		if matches!(rs, RecordStrategy::Count) {
			if let Value::Number(n) = obj {
				count_value = Some(Value::Number(n));
			}
		}
		for (agr, idiom) in agrs.iter_mut().zip(dbg!(idioms)) {
			let val = if let Some(ref v) = count_value {
				v.clone()
			} else {
				stk.run(|stk| obj.get(stk, ctx, opt, None, idiom)).await.catch_return()?
			};
			agr.push(rs, val).await?;
		}
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
		self.grp.len()
	}

	pub(super) async fn output(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<MemoryCollector> {
		dbg!(&mut self._tmp).output(stk, ctx, opt, stm).await?;
		let mut results = MemoryCollector::default();
		if let Some(fields) = stm.expr() {
			// Loop over each grouped collection
			for aggregator in self.grp.values_mut() {
				// Create a new value
				let mut obj = Value::empty_object();
				// Loop over each group clause
				for field in fields.iter_non_all_fields() {
					// Process the field
					let Field::Single {
						expr,
						alias,
					} = field
					else {
						// iter_non_all_fields, should remove all non Field::Single entries.
						unreachable!()
					};
					let idiom = alias
						.as_ref()
						.map(Cow::Borrowed)
						.unwrap_or_else(|| Cow::Owned(expr.to_idiom()));
					if let Some(idioms_pos) = self.idioms.iter().position(|i| i.eq(idiom.as_ref()))
					{
						if let Some(agr) = aggregator.get_mut(idioms_pos) {
							match dbg!(expr) {
								Expr::FunctionCall(f) if f.receiver.is_aggregate() => {
									let aggregate = OptimisedAggregate::from_function_call(f);
									let x = if matches!(aggregate, OptimisedAggregate::None) {
										// The aggregation is not optimised, let's compute it with
										// the values
										let mut args = vec![agr.take()];
										for e in f.arguments.iter().skip(1) {
											args.push(
												stk.run(|stk| e.compute(stk, ctx, opt, None))
													.await
													.catch_return()?,
											);
										}

										f.receiver
											.compute(stk, ctx, opt, None, args)
											.await
											.catch_return()?
									} else {
										// The aggregation is optimised, just get the value
										agr.compute(aggregate)?
									};
									obj.set(stk, ctx, opt, idiom.as_ref(), x).await?;
								}
								_ => {
									let x = agr.take().first();
									obj.set(stk, ctx, opt, idiom.as_ref(), x).await?;
								}
							}
						}
					}
				}
				// Add the object to the results
				results.push(obj);
			}
		}
		Ok(results)
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		let mut explain = BTreeMap::new();
		let idioms: Vec<String> = self.idioms.iter().cloned().map(|i| i.to_string()).collect();
		for (i, a) in idioms.into_iter().zip(&self.base) {
			explain.insert(i, a.explain());
		}
		exp.add_collector("Group", vec![("idioms", explain.into())]);
	}
}

pub enum OptimisedAggregate {
	Count,
	CountFunction,
	MathMax,
	MathMean,
	MathMin,
	MathSum,
	TimeMax,
	TimeMin,
	None,
}

impl OptimisedAggregate {
	fn from_function_call(f: &FunctionCall) -> Self {
		match f.receiver {
			Function::Normal(ref x) => match x.as_str() {
				"count" => {
					if f.arguments.is_empty() {
						OptimisedAggregate::Count
					} else {
						OptimisedAggregate::CountFunction
					}
				}
				"math::max" => OptimisedAggregate::MathMax,
				"math::mean" => OptimisedAggregate::MathMean,
				"math::min" => OptimisedAggregate::MathMin,
				"math::sum" => OptimisedAggregate::MathSum,
				"time::max" => OptimisedAggregate::TimeMax,
				"time::min" => OptimisedAggregate::TimeMin,
				_ => OptimisedAggregate::None,
			},
			_ => OptimisedAggregate::None,
		}
	}
}

impl Aggregator {
	fn prepare(&mut self, expr: &Expr) {
		let (a, _) = match expr {
			Expr::FunctionCall(f) => (OptimisedAggregate::from_function_call(f), Some(f)),
			_ => {
				// We set it only if we don't already have an array
				if self.array.is_none() && self.first_val.is_none() {
					self.first_val = Some(Value::None);
					return;
				}
				(OptimisedAggregate::None, None)
			}
		};
		match a {
			OptimisedAggregate::None => {
				if self.array.is_none() {
					self.array = Some(Array::new());
					// We don't need both the array and the first val
					self.first_val = None;
				}
			}
			OptimisedAggregate::Count => {
				if self.count.is_none() {
					self.count = Some(0);
				}
			}
			OptimisedAggregate::CountFunction => {
				if self.count_function.is_none() {
					self.count_function = Some(0);
				}
			}
			OptimisedAggregate::MathMax => {
				if self.math_max.is_none() {
					self.math_max = Some(Value::None);
				}
			}
			OptimisedAggregate::MathMin => {
				if self.math_min.is_none() {
					self.math_min = Some(Value::None);
				}
			}
			OptimisedAggregate::MathSum => {
				if self.math_sum.is_none() {
					self.math_sum = Some(0.into());
				}
			}
			OptimisedAggregate::MathMean => {
				if self.math_mean.is_none() {
					self.math_mean = Some((0.into(), 0));
				}
			}
			OptimisedAggregate::TimeMax => {
				if self.time_max.is_none() {
					self.time_max = Some(Value::None);
				}
			}
			OptimisedAggregate::TimeMin => {
				if self.time_min.is_none() {
					self.time_min = Some(Value::None);
				}
			}
		}
	}

	fn new_instance(&self) -> Self {
		Self {
			array: self.array.as_ref().map(|_| Array::new()),
			first_val: self.first_val.as_ref().map(|_| Value::None),
			count: self.count.as_ref().map(|_| 0),
			count_function: self.count_function.as_ref().map(|_| 0),
			math_max: self.math_max.as_ref().map(|_| Value::None),
			math_min: self.math_min.as_ref().map(|_| Value::None),
			math_sum: self.math_sum.as_ref().map(|_| 0.into()),
			math_mean: self.math_mean.as_ref().map(|_| (0.into(), 0)),
			time_max: self.time_max.as_ref().map(|_| Value::None),
			time_min: self.time_min.as_ref().map(|_| Value::None),
		}
	}

	async fn push(&mut self, rs: RecordStrategy, val: Value) -> Result<()> {
		if let Some(ref mut c) = self.count {
			let mut count = 1;
			if matches!(rs, RecordStrategy::Count) {
				if let Value::Number(n) = val {
					count = n.to_usize();
				}
			}
			*c += count;
		}
		if let Some(c) = self.count_function.as_mut() {
			// NOTE: There was some rather complicated juggling of a function here where the
			// argument was replaced but as far as I can tell the whole thing was just
			// equivalent to the one liner below.
			*c += val.is_truthy() as usize;
		}
		if val.is_number() {
			if let Some(s) = self.math_sum.take() {
				self.math_sum = Some(s.try_add(val.clone())?);
			}
			if let Some((s, i)) = self.math_mean.take() {
				let s = s.try_add(val.clone())?;
				self.math_mean = Some((s, i + 1));
			}
			if let Some(m) = self.math_min.take() {
				self.math_min = Some(if m.is_none() {
					val.clone()
				} else {
					m.min(val.clone())
				});
			}
			if let Some(m) = self.math_max.take() {
				self.math_max = Some(if m.is_none() {
					val.clone()
				} else {
					m.max(val.clone())
				});
			}
		}
		if val.is_datetime() {
			if let Some(m) = self.time_min.take() {
				self.time_min = Some(if m.is_none() {
					val.clone()
				} else {
					m.min(val.clone())
				});
			}
			if let Some(m) = self.time_max.take() {
				self.time_max = Some(if m.is_none() {
					val.clone()
				} else {
					m.max(val.clone())
				});
			}
		}
		if let Some(ref mut a) = self.array {
			a.0.push(val);
		} else if let Some(ref mut v) = self.first_val {
			if v.is_none() {
				*v = val;
			}
		}
		Ok(())
	}

	fn compute(&mut self, a: OptimisedAggregate) -> Result<Value> {
		let value = match a {
			OptimisedAggregate::None => Value::None,
			OptimisedAggregate::Count => self.count.take().map(|v| v.into()).unwrap_or(Value::None),
			OptimisedAggregate::CountFunction => {
				self.count_function.take().map(|v| v.into()).unwrap_or(Value::None)
			}
			OptimisedAggregate::MathMax => self.math_max.take().unwrap_or(Value::None),
			OptimisedAggregate::MathMin => self.math_min.take().unwrap_or(Value::None),
			OptimisedAggregate::MathSum => self.math_sum.take().unwrap_or(Value::None),
			OptimisedAggregate::MathMean => {
				if let Some((v, i)) = self.math_mean.take() {
					v.try_float_div(i.into()).unwrap_or(f64::NAN.into())
				} else {
					Value::None
				}
			}
			OptimisedAggregate::TimeMax => self.time_max.take().unwrap_or(Value::None),
			OptimisedAggregate::TimeMin => self.time_min.take().unwrap_or(Value::None),
		};
		Ok(value)
	}

	fn take(&mut self) -> Value {
		// We return a clone because the same value may be returned for different groups
		if let Some(v) = self.first_val.as_ref().cloned() {
			Array::from(vec![v]).into()
		} else if let Some(a) = self.array.as_ref().cloned() {
			a.into()
		} else {
			Value::None
		}
	}

	fn explain(&self) -> Value {
		let mut collections: Vec<Value> = vec![];
		if self.array.is_some() {
			collections.push("array".into());
		}
		if self.first_val.is_some() {
			collections.push("first".into());
		}
		if self.count.is_some() {
			collections.push("count".into());
		}
		if self.count_function.is_some() {
			collections.push("count+func".into());
		}
		if self.math_mean.is_some() {
			collections.push("math::mean".into());
		}
		if self.math_max.is_some() {
			collections.push("math::max".into());
		}
		if self.math_min.is_some() {
			collections.push("math::min".into());
		}
		if self.math_sum.is_some() {
			collections.push("math::sum".into());
		}
		if self.time_max.is_some() {
			collections.push("time::max".into());
		}
		if self.time_min.is_some() {
			collections.push("time::min".into());
		}
		collections.into()
	}
}
