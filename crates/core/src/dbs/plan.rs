use std::collections::HashMap;

use crate::ctx::Context;
use crate::dbs::result::Results;
use crate::dbs::{Iterable, Statement};
use crate::expr::lookup::LookupKind;
use crate::idx::planner::RecordStrategy;
use crate::val::{Object, Strand, Value};

pub(super) struct Plan {
	pub(super) do_iterate: bool,
	pub(super) explanation: Option<Explanation>,
}

impl Plan {
	pub(super) fn new(
		ctx: &Context,
		stm: &Statement<'_>,
		iterables: &Vec<Iterable>,
		results: &Results,
	) -> Self {
		let (do_iterate, explanation) = match stm.explain() {
			None => (true, None),
			Some(e) => {
				let mut exp = Explanation::default();
				for i in iterables {
					exp.add_iter(ctx, i);
				}
				if let Some(qp) = ctx.get_query_planner() {
					for reason in qp.fallbacks() {
						exp.add_fallback(reason.to_string());
					}
				}
				results.explain(&mut exp);
				(e.0, Some(exp))
			}
		};
		Self {
			do_iterate,
			explanation,
		}
	}
}

#[derive(Default)]
pub(super) struct Explanation(Vec<ExplainItem>);

impl Explanation {
	fn add_iter(&mut self, ctx: &Context, iter: &Iterable) {
		self.0.push(ExplainItem::new_iter(ctx, iter));
	}

	pub(super) fn add_fetch(&mut self, count: usize) {
		self.0.push(ExplainItem::new_fetch(count));
	}

	pub(super) fn add_collector(
		&mut self,
		collector_type: &str,
		details: Vec<(&'static str, Value)>,
	) {
		self.0.push(ExplainItem::new_collector(collector_type, details));
	}

	fn add_fallback(&mut self, reason: String) {
		self.0.push(ExplainItem::new_fallback(reason));
	}

	pub(super) fn add_record_strategy(&mut self, rs: RecordStrategy) {
		self.0.push(ExplainItem::new_record_strategy(rs));
	}

	pub(super) fn add_start_limit(
		&mut self,
		start_skip: Option<usize>,
		cancel_on_limit: Option<u32>,
	) {
		self.0.push(ExplainItem::new_start_limit(start_skip, cancel_on_limit));
	}
	pub(super) fn output(self) -> Vec<Value> {
		self.0.into_iter().map(|e| e.into()).collect()
	}
}

struct ExplainItem {
	name: Value,
	details: Vec<(&'static str, Value)>,
}

impl ExplainItem {
	fn new_fetch(count: usize) -> Self {
		Self {
			name: "Fetch".into(),
			details: vec![("count", count.into())],
		}
	}

	fn new_fallback(reason: String) -> Self {
		Self {
			name: "Fallback".into(),
			details: vec![("reason", reason.into())],
		}
	}

	fn new_iter(ctx: &Context, iter: &Iterable) -> Self {
		match iter {
			Iterable::Value(v) => Self {
				name: "Iterate Value".into(),
				details: vec![("value", v.to_owned())],
			},
			Iterable::Yield(t) => Self {
				name: "Iterate Yield".into(),
				details: vec![("table", Value::from(t.clone().into_strand()))],
			},
			Iterable::Thing(t) => Self {
				name: "Iterate Thing".into(),
				details: vec![("thing", Value::RecordId(t.clone()))],
			},
			Iterable::Defer(t) => Self {
				name: "Iterate Defer".into(),
				details: vec![("thing", Value::RecordId(t.clone()))],
			},
			Iterable::Lookup {
				from,
				kind,
				..
			} => match kind {
				LookupKind::Graph(_) => Self {
					name: "Iterate Edges".into(),
					details: vec![("from", Value::RecordId(from.clone()))],
				},
				LookupKind::Reference => Self {
					name: "Iterate References".into(),
					details: vec![("from", Value::RecordId(from.clone()))],
				},
			},
			Iterable::Table(t, rs, sc) => Self {
				name: match rs {
					RecordStrategy::Count => "Iterate Table Count",
					RecordStrategy::KeysOnly => "Iterate Table Keys",
					RecordStrategy::KeysAndValues => "Iterate Table",
				}
				.into(),
				details: vec![
					("table", Value::from(t.clone().into_strand())),
					("direction", sc.to_string().into()),
				],
			},
			Iterable::Range(tb, r, rs, sc) => Self {
				name: match rs {
					RecordStrategy::Count => "Iterate Range Count",
					RecordStrategy::KeysOnly => "Iterate Range Keys",
					RecordStrategy::KeysAndValues => "Iterate Range",
				}
				.into(),
				details: vec![
					//TODO: Properly handle possible null byte.
					("table", Value::Strand(Strand::new(tb.to_owned()).unwrap())),
					("range", Value::Range(Box::new(r.clone().into_value_range()))),
					("direction", sc.to_string().into()),
				],
			},
			Iterable::Mergeable(t, v) => Self {
				name: "Iterate Mergeable".into(),
				details: vec![("thing", Value::RecordId(t.to_owned())), ("value", v.to_owned())],
			},
			Iterable::Relatable(t1, t2, t3, None) => Self {
				name: "Iterate Relatable".into(),
				details: vec![
					("thing-1", Value::RecordId(t1.to_owned())),
					("thing-2", Value::RecordId(t2.to_owned())),
					("thing-3", Value::RecordId(t3.to_owned())),
				],
			},
			Iterable::Relatable(t1, t2, t3, Some(v)) => Self {
				name: "Iterate Relatable".into(),
				details: vec![
					("thing-1", Value::RecordId(t1.to_owned())),
					("thing-2", Value::RecordId(t2.to_owned())),
					("thing-3", Value::RecordId(t3.to_owned())),
					("value", v.to_owned()),
				],
			},
			Iterable::Index(t, ir, rs) => {
				let mut details = vec![("table", Value::Strand(t.clone().into_strand()))];
				if let Some(qp) = ctx.get_query_planner() {
					if let Some(exe) = qp.get_query_executor(t.as_str()) {
						details.push(("plan", exe.explain(*ir)));
					}
				}
				Self {
					name: match rs {
						RecordStrategy::Count => "Iterate Index Count",
						RecordStrategy::KeysOnly => "Iterate Index Keys",
						RecordStrategy::KeysAndValues => "Iterate Index",
					}
					.into(),
					details,
				}
			}
		}
	}

	pub(super) fn new_collector(
		collector_type: &str,
		mut details: Vec<(&'static str, Value)>,
	) -> Self {
		details.insert(0, ("type", collector_type.into()));
		Self {
			name: "Collector".into(),
			details,
		}
	}
	pub(super) fn new_record_strategy(rs: RecordStrategy) -> Self {
		Self {
			name: "RecordStrategy".into(),
			details: vec![(
				"type",
				match rs {
					RecordStrategy::Count => "Count",
					RecordStrategy::KeysOnly => "KeysOnly",
					RecordStrategy::KeysAndValues => "KeysAndValues",
				}
				.into(),
			)],
		}
	}

	pub(super) fn new_start_limit(start_skip: Option<usize>, cancel_on_limit: Option<u32>) -> Self {
		let mut details = vec![];
		if let Some(s) = start_skip {
			details.push(("SkipStart", s.into()));
		}
		if let Some(l) = cancel_on_limit {
			details.push(("CancelOnLimit", l.into()));
		}
		Self {
			name: "StartLimitStrategy".into(),
			details,
		}
	}
}

impl From<ExplainItem> for Value {
	fn from(i: ExplainItem) -> Self {
		let explain = Object::from(HashMap::from([
			("operation", i.name),
			("detail", Value::Object(Object::from(HashMap::from_iter(i.details)))),
		]));
		Value::from(explain)
	}
}
