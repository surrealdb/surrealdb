//! Common code related to aggregation calculations.
//!
//! Uses in aggergate views and group by selects.
//!
//! # Basic overview.
//!
//! Aggregation calculation works in 3 steps.
//!
//! - First on view definition, or when issued a select we analyse the group and selector
//!   expressions to find the aggregate functions, the agruments for those functions, the expression
//!   which defines the group of an entry, and the expression to generate a result for that group.
//! - Then we accumulate all the records the aggregation needs to be calculated over, run the
//!   expressions which generate the agruments for the aggregate functions and then manually update
//!   the computation for the aggregation functions.
//! - Finally when all the values have been consumed we construct a document which has all the
//!   values for the computed aggregates as fields and run the expression we generated in the first
//!   step to compute the final result for the aggregation.
//!
//! Example:
//! ```txt
//!    SELECT foo, math::pow(math::mean(v),2), math::max(v), math::min(x + 1) FROM foo GROUP foo.
//!
//!    the aggregate argument expressions are,
//!        1: v,
//!        2: x + 1,
//!
//!    The aggregates functions are:
//!        ag1: math::mean operating on expression 1
//!        ag2: math::max operating on expression 1
//!        ag3: math::min operating on expression 2
//!
//!    the final expression to calculate the result is:
//!        g1, math::pow(ag1,2), ag2, ag3
//!
//!        her `g1` refers to the group.
//!
//!```
//!

use ahash::HashMap;
use anyhow::{Result, bail, ensure};
use revision::revisioned;

use crate::{
	catalog::AggregationStat,
	err::Error,
	expr::{
		Expr, Field, Fields, Function, Groups, Idiom, Part,
		visit::{MutVisitor, VisitMut},
	},
	val::{Array, Datetime, Number, Object, TryAdd as _, TryFloatDiv, TryMul, Value},
};
use std::{fmt::Write, mem};

/// An expression which will be aggregated over for each group.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Aggregation {
	Count,
	/// The usizes are index into the exprs field on the aggregate collector and represent the
	/// expression which was fed as an argument to the aggregate expression
	CountValue(usize),
	NumberMax(usize),
	NumberMin(usize),
	Sum(usize),
	Mean(usize),
	StdDev(usize),
	Variance(usize),
	DatetimeMax(usize),
	DatetimeMin(usize),
	Accumulate(usize),
}

impl Aggregation {
	pub fn to_stat(&self) -> AggregationStat {
		match *self {
			Aggregation::Count => AggregationStat::Count {
				count: 0,
			},
			Aggregation::CountValue(arg) => AggregationStat::CountValue {
				arg,
				count: 0,
			},
			Aggregation::NumberMax(arg) => AggregationStat::NumberMax {
				arg,
				max: f64::NEG_INFINITY.into(),
			},
			Aggregation::NumberMin(arg) => AggregationStat::NumberMin {
				arg,
				min: f64::INFINITY.into(),
			},
			Aggregation::Sum(arg) => AggregationStat::Sum {
				arg,
				sum: 0.0.into(),
			},
			Aggregation::Mean(arg) => AggregationStat::Mean {
				arg,
				count: 0,
				sum: 0.0.into(),
			},
			Aggregation::StdDev(arg) => AggregationStat::StdDev {
				arg,
				sum: 0.0.into(),
				sum_of_squares: 0.0.into(),
				count: 0,
			},
			Aggregation::Variance(arg) => AggregationStat::Variance {
				arg,
				sum: 0.0.into(),
				sum_of_squares: 0.0.into(),
				count: 0,
			},
			Aggregation::DatetimeMax(arg) => AggregationStat::TimeMax {
				arg,
				max: Datetime::MIN_UTC,
			},
			Aggregation::DatetimeMin(arg) => AggregationStat::TimeMin {
				arg,
				min: Datetime::MAX_UTC,
			},
			Aggregation::Accumulate(arg) => AggregationStat::Accumulate {
				arg,
				values: Vec::new(),
			},
		}
	}
}

pub fn write_aggregate_field_name(s: &mut String, idx: usize) {
	// Writing into a string cannot error.
	write!(s, "_a{}", idx).unwrap();
}

pub fn write_group_field_name(s: &mut String, idx: usize) {
	// Writing into a string cannot error.
	write!(s, "_g{}", idx).unwrap();
}

pub fn aggregate_field_name(idx: usize) -> String {
	let mut res = String::new();
	write_aggregate_field_name(&mut res, idx);
	res
}

pub fn group_field_name(idx: usize) -> String {
	let mut res = String::new();
	write_group_field_name(&mut res, idx);
	res
}

pub fn add_to_aggregation_stats(arguments: &[Value], stats: &mut [AggregationStat]) -> Result<()> {
	for stat in stats {
		match stat {
			AggregationStat::Count {
				count,
			} => {
				*count += 1;
			}
			AggregationStat::CountValue {
				arg,
				count,
			} => {
				*count += arguments[*arg].is_truthy() as i64;
			}
			AggregationStat::NumberMax {
				arg,
				max,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "math::max".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg]
						),
					})
				};
				if *max < *n {
					*max = *n
				}
			}
			AggregationStat::NumberMin {
				arg,
				min,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "math::min".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg]
						),
					})
				};
				if *min > *n {
					*min = *n
				}
			}
			AggregationStat::Sum {
				arg,
				sum,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "math::sum".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg]
						),
					})
				};
				*sum = (*sum).try_add(*n)?;
			}
			AggregationStat::Mean {
				arg,
				sum,
				count,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "math::mean".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg]
						),
					})
				};

				*sum = (*sum).try_add(*n)?;
				*count += 1;
			}
			AggregationStat::StdDev {
				arg,
				sum,
				sum_of_squares,
				count,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "math::stddev".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg]
						),
					})
				};

				*sum = (*sum).try_add(*n)?;
				*sum_of_squares = (*sum_of_squares).try_add(n.try_mul(*n)?)?;
				*count += 1;
			}
			AggregationStat::Variance {
				arg,
				sum,
				sum_of_squares,
				count,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "math::variance".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg]
						),
					})
				};

				*sum = (*sum).try_add(*n)?;
				*sum_of_squares = (*sum_of_squares).try_add(n.try_mul(*n)?)?;
				*count += 1;
			}
			AggregationStat::TimeMax {
				arg,
				max,
			} => {
				let Value::Datetime(ref d) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "time::max".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
							arguments[*arg]
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
				let Value::Datetime(ref d) = arguments[*arg] else {
					bail!(Error::InvalidArguments {
						name: "time::min".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
							arguments[*arg]
						),
					})
				};

				if *min > *d {
					*min = d.clone();
				}
			}
			AggregationStat::Accumulate {
				arg,
				values,
			} => {
				values.push(arguments[*arg].clone());
			}
		}
	}
	Ok(())
}

/// Creates object that can act as a document to calculate the final value for an aggregated statement.
pub fn create_field_document(group: &[Value], stats: &[AggregationStat]) -> Object {
	let mut res = Object::default();
	//setup the document for final value calculation
	for (idx, a) in stats.iter().enumerate() {
		let value = match a {
			AggregationStat::Count {
				count,
			}
			| AggregationStat::CountValue {
				count,
				..
			} => Value::from(Number::from(*count)),
			AggregationStat::NumberMax {
				max,
				..
			} => (*max).into(),
			AggregationStat::NumberMin {
				min,
				..
			} => (*min).into(),
			AggregationStat::Sum {
				sum,
				..
			} => (*sum).into(),
			AggregationStat::Mean {
				sum,
				count,
				..
			} => sum.try_float_div((*count).into()).unwrap_or(f64::NAN.into()).into(),
			AggregationStat::StdDev {
				sum,
				sum_of_squares,
				count,
				..
			} => {
				let num = if *count <= 1 {
					Number::from(0.0)
				} else {
					let mean = *sum / Number::from(*count);
					let variance = (*sum_of_squares - (*sum * mean)) / Number::from(*count - 1);
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
				let num = if *count <= 1 {
					Number::from(0.0)
				} else {
					let mean = *sum / Number::from(*count);
					(*sum_of_squares - (*sum * mean)) / Number::from(*count - 1)
				};
				num.into()
			}
			AggregationStat::TimeMax {
				max,
				..
			} => max.clone().into(),
			AggregationStat::TimeMin {
				min,
				..
			} => min.clone().into(),
			AggregationStat::Accumulate {
				values,
				..
			} => Value::Array(Array(values.clone())),
		};
		res.0.insert(aggregate_field_name(idx), value);
	}
	for (idx, g) in group.iter().enumerate() {
		res.0.insert(group_field_name(idx), g.clone());
	}
	res
}

/// Visitor which walks an expression to pull out the aggregate expressions to calculate.
struct AggregateExprCollector<'a> {
	support_acummulate: bool,
	within_aggregate_argument: bool,
	exprs_map: &'a mut HashMap<Expr, usize>,
	aggregations: &'a mut Vec<Aggregation>,
	groups: &'a Groups,
}

impl AggregateExprCollector<'_> {
	fn push_aggregate_function<F: Fn(usize) -> Aggregation>(
		&mut self,
		name: &str,
		args: &[Expr],
		f: F,
	) -> Result<()> {
		ensure!(
			args.len() == 1,
			Error::InvalidArguments {
				name: name.to_string(),
				message: "Expected 1 argument".to_string()
			}
		);
		let expr = args[0].clone();
		let len = self.exprs_map.len();
		let arg = *self.exprs_map.entry(expr).or_insert_with(|| len);
		self.aggregations.push(f(arg));
		Ok(())
	}
}

impl MutVisitor for AggregateExprCollector<'_> {
	type Error = anyhow::Error;

	fn visit_mut_expr(&mut self, s: &mut Expr) -> Result<(), Self::Error> {
		match s {
			Expr::FunctionCall(f) => {
				if let Function::Normal(x) = &f.receiver {
					match x.as_str() {
						"count" => {
							if f.arguments.is_empty() {
								self.aggregations.push(Aggregation::Count);
							} else {
								self.push_aggregate_function(
									"count",
									&f.arguments,
									Aggregation::CountValue,
								)?;
							}
						}
						"math::max" => {
							self.push_aggregate_function(
								"math::max",
								&f.arguments,
								Aggregation::NumberMax,
							)?;
						}
						"math::min" => {
							self.push_aggregate_function(
								"math::min",
								&f.arguments,
								Aggregation::NumberMin,
							)?;
						}
						"math::sum" => {
							self.push_aggregate_function(
								"math::sum",
								&f.arguments,
								Aggregation::Sum,
							)?;
						}
						"math::mean" => {
							self.push_aggregate_function(
								"math::mean",
								&f.arguments,
								Aggregation::Mean,
							)?;
						}
						"math::stddev" => {
							self.push_aggregate_function(
								"math::stddev",
								&f.arguments,
								Aggregation::StdDev,
							)?;
						}
						"math::variance" => {
							self.push_aggregate_function(
								"math::variance",
								&f.arguments,
								Aggregation::Variance,
							)?;
						}
						"time::max" => {
							self.push_aggregate_function(
								"time::max",
								&f.arguments,
								Aggregation::DatetimeMax,
							)?;
						}
						"time::min" => {
							self.push_aggregate_function(
								"time::min",
								&f.arguments,
								Aggregation::DatetimeMin,
							)?;
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
				*s = Expr::Idiom(Idiom::field(aggregate_field_name(self.aggregations.len() - 1)));
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
						if self.support_acummulate {
							let expr = mem::replace(
								s,
								Expr::Idiom(Idiom::field(aggregate_field_name(
									self.aggregations.len(),
								))),
							);
							let len = self.exprs_map.len();
							let arg = *self.exprs_map.entry(expr).or_insert_with(|| len);
							self.aggregations.push(Aggregation::Accumulate(arg))
						} else {
							bail!(Error::InvalidAggregationSelector {
								expr: i.to_string(),
							})
						}
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

/// Enum for the field expression of an aggregate.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateFields {
	/// the selector had a `VALUE` clause
	Value(Expr),
	/// Normal selector.
	Fields(Vec<(Idiom, Expr)>),
}

/// A struct which contains an anaylzed aggregation and data on how to compute that aggregation.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AggregationAnalysis {
	/// The expressions which calculate the arguments to an aggregate.
	pub aggregate_arguments: Vec<Expr>,
	/// The aggregated expressions that are calculated.
	pub aggregations: Vec<Aggregation>,
	/// The expressions which identify the group.
	pub group_expressions: Vec<Expr>,
	/// The expression to compute the resulting object from the calculated aggregates.
	pub fields: AggregateFields,
}

impl AggregationAnalysis {
	/// Analyze the groups and fields and produce a analysis for how to run the aggregate
	/// expressions.
	///
	/// if the `force_count` argument is true the function will add a `Count` aggregation when
	/// there is no aggregate which maintains a per group record count.
	pub fn analyze_fields_groups(
		fields: &Fields,
		groups: &Groups,
		materialized_view: bool,
	) -> Result<Self> {
		// Find all the aggregates within the select statement.
		let mut aggregations = Vec::new();
		let mut exprs_map = HashMap::default();
		let mut group_expressions = Vec::with_capacity(groups.len());

		for g in groups.0.iter() {
			group_expressions.push(Expr::Idiom(g.0.clone()))
		}

		let mut collect = AggregateExprCollector {
			support_acummulate: !materialized_view,
			within_aggregate_argument: false,
			exprs_map: &mut exprs_map,
			aggregations: &mut aggregations,
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
				AggregateFields::Value(expr)
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
						&& let Some(x) = group_expressions.iter().position(|x| {
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
						group_expressions[x] = expr.clone();
						collect_fields
							.push((alias.clone(), Expr::Idiom(Idiom::field(group_field_name(x)))));
					} else {
						let name = alias.clone().unwrap_or_else(|| expr.to_idiom());
						let mut expr = expr.clone();
						collect.visit_mut_expr(&mut expr)?;
						collect_fields.push((name, expr))
					}
				}
				AggregateFields::Fields(collect_fields)
			}
		};

		// Place the expression which need to be calculated for the aggregate in the right index.
		let mut aggregate_arguments = Vec::with_capacity(exprs_map.len());
		for (k, v) in exprs_map.into_iter() {
			if aggregate_arguments.len() > v {
				aggregate_arguments[v] = k
			} else {
				for _ in aggregate_arguments.len()..v {
					// push a temp expression that will be overwritten while we collect all the
					// expressions.
					aggregate_arguments.push(Expr::Break)
				}
				aggregate_arguments.push(k)
			}
		}

		// Ensure there is atleast one count aggregation to delete the record when the number of
		// entries reaches zero.
		if materialized_view
			&& !aggregations.iter().any(|x| {
				matches!(
					x,
					Aggregation::Count
						| Aggregation::Mean(_)
						| Aggregation::StdDev(_)
						| Aggregation::Variance(_)
				)
			}) {
			aggregations.push(Aggregation::Count)
		}

		Ok(Self {
			aggregations,
			aggregate_arguments,
			group_expressions,
			fields,
		})
	}
}
