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
	val::Datetime,
};
use std::fmt::Write;

/// An expression which will be aggregated over for each group.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Aggregation {
	Count,
	/// The usizes are index into the exprs field on the aggregate collector and represent the
	/// expression which was fed as an argument to the aggregate expression
	CountFn(usize),
	NumMax(usize),
	NumMin(usize),
	NumSum(usize),
	NumMean(usize),
	TimeMax(usize),
	TimeMin(usize),
}

impl Aggregation {
	pub fn into_stat(&self) -> AggregationStat {
		match *self {
			Aggregation::Count => AggregationStat::Count {
				count: 0,
			},
			Aggregation::CountFn(arg) => AggregationStat::CountFn {
				arg,
				count: 0,
			},
			Aggregation::NumMax(arg) => AggregationStat::NumMax {
				arg,
				max: f64::NEG_INFINITY.into(),
			},
			Aggregation::NumMin(arg) => AggregationStat::NumMin {
				arg,
				min: f64::INFINITY.into(),
			},
			Aggregation::NumSum(arg) => AggregationStat::NumSum {
				arg,
				sum: 0.0.into(),
			},
			Aggregation::NumMean(arg) => AggregationStat::NumMean {
				arg,
				count: 0,
				sum: 0.0.into(),
			},
			Aggregation::TimeMax(arg) => AggregationStat::TimeMax {
				arg,
				max: Datetime::MIN_UTC,
			},
			Aggregation::TimeMin(arg) => AggregationStat::TimeMax {
				arg,
				max: Datetime::MAX_UTC,
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

/// Visitor which walks an expression to pull out the aggregate expressions to calculate.
struct AggregateExprCollector<'a> {
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
									Aggregation::CountFn,
								)?;
							}
						}
						"math::max" => {
							self.push_aggregate_function(
								"math::max",
								&f.arguments,
								Aggregation::NumMax,
							)?;
						}
						"math::min" => {
							self.push_aggregate_function(
								"math::min",
								&f.arguments,
								Aggregation::NumMin,
							)?;
						}
						"math::sum" => {
							self.push_aggregate_function(
								"math::sum",
								&f.arguments,
								Aggregation::NumSum,
							)?;
						}
						"math::mean" => {
							self.push_aggregate_function(
								"math::mean",
								&f.arguments,
								Aggregation::NumMean,
							)?;
						}
						"time::max" => {
							self.push_aggregate_function(
								"time::max",
								&f.arguments,
								Aggregation::TimeMax,
							)?;
						}
						"time::min" => {
							self.push_aggregate_function(
								"time::min",
								&f.arguments,
								Aggregation::TimeMin,
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
	pub fn analyze_fields_groups(fields: &Fields, groups: &Groups) -> Result<Self> {
		// Find all the aggregates within the select statement.
		let mut aggregations = Vec::new();
		let mut exprs_map = HashMap::default();
		let mut group_expressions = Vec::with_capacity(groups.len());

		for g in groups.0.iter() {
			group_expressions.push(Expr::Idiom(g.0.clone()))
		}

		let mut collect = AggregateExprCollector {
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
			if aggregate_arguments.len() == v {
				aggregate_arguments.push(k);
			} else if aggregate_arguments.len() > v {
				aggregate_arguments[v] = k
			} else {
				for _ in 0..v {
					// push a temp expression that will be overwritten while we collect all the
					// expressions.
					aggregate_arguments.push(Expr::Break)
				}
				aggregate_arguments.push(k)
			}
		}

		Ok(Self {
			aggregations,
			aggregate_arguments,
			group_expressions,
			fields,
		})
	}
}
