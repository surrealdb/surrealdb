use crate::ctx::Context;
use crate::dbs::plan::Explanation;
use crate::dbs::store::MemoryCollector;
use crate::dbs::{Options, Statement};
use crate::err::Error;
use crate::sql::function::OptimisedAggregate;
use crate::sql::value::{TryAdd, TryDiv, Value};
use crate::sql::{Array, Field, Function, Idiom};
use reblessive::tree::Stk;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

pub(super) struct GroupsCollector {
	base: Vec<Aggregator>,
	idioms: Vec<Idiom>,
	grp: BTreeMap<Array, Vec<Aggregator>>,
}

#[derive(Default)]
struct Aggregator {
	array: Option<Array>,
	first_val: Option<Value>,
	count: Option<usize>,
	count_function: Option<(Box<Function>, usize)>,
	math_max: Option<Value>,
	math_min: Option<Value>,
	math_sum: Option<Value>,
	math_mean: Option<(Value, usize)>,
	time_max: Option<Value>,
	time_min: Option<Value>,
}

impl GroupsCollector {
	pub(super) fn new(stm: &Statement<'_>) -> Self {
		let mut idioms_agr: HashMap<Idiom, Aggregator> = HashMap::new();
		if let Some(fields) = stm.expr() {
			for field in fields.other() {
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
		}
	}

	pub(super) async fn push(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		obj: Value,
	) -> Result<(), Error> {
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
			Self::pushes(stk, ctx, opt, agr, &self.idioms, obj).await?
		}
		Ok(())
	}

	async fn pushes(
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		agrs: &mut [Aggregator],
		idioms: &[Idiom],
		obj: Value,
	) -> Result<(), Error> {
		for (agr, idiom) in agrs.iter_mut().zip(idioms) {
			let val = stk.run(|stk| obj.get(stk, ctx, opt, None, idiom)).await?;
			agr.push(stk, ctx, opt, val).await?;
		}
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
		self.grp.len()
	}

	pub(super) async fn output(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<MemoryCollector, Error> {
		let mut results = MemoryCollector::default();
		if let Some(fields) = stm.expr() {
			// Loop over each grouped collection
			for aggregator in self.grp.values_mut() {
				// Create a new value
				let mut obj = Value::base();
				// Loop over each group clause
				for field in fields.other() {
					// Process the field
					if let Field::Single {
						expr,
						alias,
					} = field
					{
						let idiom = alias
							.as_ref()
							.map(Cow::Borrowed)
							.unwrap_or_else(|| Cow::Owned(expr.to_idiom()));
						if let Some(idioms_pos) =
							self.idioms.iter().position(|i| i.eq(idiom.as_ref()))
						{
							if let Some(agr) = aggregator.get_mut(idioms_pos) {
								match expr {
									Value::Function(f) if f.is_aggregate() => {
										let a = f.get_optimised_aggregate();
										let x = if matches!(a, OptimisedAggregate::None) {
											// The aggregation is not optimised, let's compute it with the values
											let vals = agr.take();
											f.aggregate(vals).compute(stk, ctx, opt, None).await?
										} else {
											// The aggregation is optimised, just get the value
											agr.compute(a)?
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
				}
				// Add the object to the results
				results.push(obj);
			}
		}
		Ok(results)
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		let mut explain = BTreeMap::new();
		let idioms: Vec<String> =
			self.idioms.iter().cloned().map(|i| Value::from(i).to_string()).collect();
		for (i, a) in idioms.into_iter().zip(&self.base) {
			explain.insert(i, a.explain());
		}
		exp.add_collector("Group", vec![("idioms", explain.into())]);
	}
}

impl Aggregator {
	fn prepare(&mut self, expr: &Value) {
		let (a, f) = match expr {
			Value::Function(f) => (f.get_optimised_aggregate(), Some(f)),
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
					self.count_function = Some((f.unwrap().clone(), 0));
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
			count_function: self.count_function.as_ref().map(|(f, _)| (f.clone(), 0)),
			math_max: self.math_max.as_ref().map(|_| Value::None),
			math_min: self.math_min.as_ref().map(|_| Value::None),
			math_sum: self.math_sum.as_ref().map(|_| 0.into()),
			math_mean: self.math_mean.as_ref().map(|_| (0.into(), 0)),
			time_max: self.time_max.as_ref().map(|_| Value::None),
			time_min: self.time_min.as_ref().map(|_| Value::None),
		}
	}

	async fn push(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		val: Value,
	) -> Result<(), Error> {
		if let Some(ref mut c) = self.count {
			*c += 1;
		}
		if let Some((ref f, ref mut c)) = self.count_function {
			if f.aggregate(val.clone()).compute(stk, ctx, opt, None).await?.is_truthy() {
				*c += 1;
			}
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

	fn compute(&mut self, a: OptimisedAggregate) -> Result<Value, Error> {
		Ok(match a {
			OptimisedAggregate::None => Value::None,
			OptimisedAggregate::Count => self.count.take().map(|v| v.into()).unwrap_or(Value::None),
			OptimisedAggregate::CountFunction => {
				self.count_function.take().map(|(_, v)| v.into()).unwrap_or(Value::None)
			}
			OptimisedAggregate::MathMax => self.math_max.take().unwrap_or(Value::None),
			OptimisedAggregate::MathMin => self.math_min.take().unwrap_or(Value::None),
			OptimisedAggregate::MathSum => self.math_sum.take().unwrap_or(Value::None),
			OptimisedAggregate::MathMean => {
				if let Some((v, i)) = self.math_mean.take() {
					v.try_div(i.into()).unwrap_or(f64::NAN.into())
				} else {
					Value::None
				}
			}
			OptimisedAggregate::TimeMax => self.time_max.take().unwrap_or(Value::None),
			OptimisedAggregate::TimeMin => self.time_min.take().unwrap_or(Value::None),
		})
	}

	fn take(&mut self) -> Value {
		// We return a clone because the same value may be returned for different groups
		if let Some(v) = self.first_val.as_ref().cloned() {
			Array::from(v).into()
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
