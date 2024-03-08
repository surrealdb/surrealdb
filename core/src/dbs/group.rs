use crate::ctx::Context;
use crate::dbs::store::StoreCollector;
use crate::dbs::{Options, Statement, Transaction};
use crate::err::Error;
use crate::sql::function::OptimisedAggregate;
use crate::sql::value::{TryAdd, TryDiv, Value};
use crate::sql::{Array, Field, Idiom};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::mem;

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
					if let Some(agr) = idioms_agr.get_mut(&idiom) {
						agr.prepare(expr);
					} else {
						let mut agr = Aggregator::default();
						agr.prepare(expr);
						idioms_agr.insert(idiom, agr);
					}
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
			match self.grp.get_mut(&arr) {
				Some(agr) => Self::pushes(ctx, opt, txn, agr, &self.idioms, obj).await?,
				None => {
					let mut agr = self.base.iter().map(|a| a.new_instance()).collect();
					Self::pushes(ctx, opt, txn, &mut agr, &self.idioms, obj).await?;
					self.grp.insert(arr, agr);
				}
			}
		}
		Ok(())
	}

	async fn pushes(
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		agrs: &mut Vec<Aggregator>,
		idioms: &[Idiom],
		obj: Value,
	) -> Result<(), Error> {
		for (agr, idiom) in agrs.iter_mut().zip(idioms) {
			let val = obj.get(ctx, opt, txn, None, idiom).await?;
			agr.push(val)?;
		}
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
		self.grp.len()
	}

	pub(super) async fn output(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<StoreCollector, Error> {
		let mut results = StoreCollector::default();
		if let Some(fields) = stm.expr() {
			let grp = mem::take(&mut self.grp);
			// Loop over each grouped collection
			for (_, mut aggregator) in grp {
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
							let agr = &mut aggregator[idioms_pos];
							match expr {
								Value::Function(f) if f.is_aggregate() => {
									let a = f.get_optimised_aggregate();
									let x = if matches!(a, OptimisedAggregate::None) {
										// The aggregation is not optimised, let's compute it with the values
										let vals = agr.take();
										let x = vals
											.all()
											.get(ctx, opt, txn, None, idiom.as_ref())
											.await?;
										f.aggregate(x).compute(ctx, opt, txn, None).await?
									} else {
										// The aggregation is optimised, just get the value
										agr.compute(a)?
									};
									obj.set(ctx, opt, txn, idiom.as_ref(), x).await?;
								}
								_ => {
									let vals = agr.take();
									let x = vals.first();
									// TODO Check why this seems to not be required anymore
									// let x = if let Some(alias) = alias {
									// 	let cur = (&x).into();
									// 	alias.compute(ctx, opt, txn, Some(&cur)).await?
									// } else {
									// 	let cur = (&x).into();
									// 	expr.compute(ctx, opt, txn, Some(&cur)).await?
									// };
									obj.set(ctx, opt, txn, idiom.as_ref(), x).await?;
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
}

impl Aggregator {
	fn prepare(&mut self, expr: &Value) {
		let a = match expr {
			Value::Function(f) => f.get_optimised_aggregate(),
			_ => {
				// We set it only if we don't already have an array
				if self.array.is_none() && self.first_val.is_none() {
					self.first_val = Some(Value::None);
					return;
				}
				OptimisedAggregate::None
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
			math_max: self.math_max.as_ref().map(|_| Value::None),
			math_min: self.math_min.as_ref().map(|_| Value::None),
			math_sum: self.math_sum.as_ref().map(|_| 0.into()),
			math_mean: self.math_mean.as_ref().map(|_| (0.into(), 0)),
			time_max: self.time_max.as_ref().map(|_| Value::None),
			time_min: self.time_min.as_ref().map(|_| Value::None),
		}
	}

	fn push(&mut self, val: Value) -> Result<(), Error> {
		if let Some(ref mut c) = self.count {
			*c += 1;
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
			OptimisedAggregate::MathMax => self.math_max.take().unwrap_or(Value::None),
			OptimisedAggregate::MathMin => self.math_min.take().unwrap_or(Value::None),
			OptimisedAggregate::MathSum => self.math_sum.take().unwrap_or(Value::None),
			OptimisedAggregate::MathMean => {
				if let Some((v, i)) = self.math_mean.take() {
					v.try_div(i.into())?
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
		} else {
			if let Some(a) = self.array.as_ref().cloned() {
				a.into()
			} else {
				Value::None
			}
		}
	}
}
