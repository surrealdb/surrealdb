use crate::ctx::Context;
use crate::dbs::result::Results;
use crate::dbs::{Iterable, Statement};
use crate::sql::{Object, Value};
use std::collections::HashMap;

pub(super) struct Plan {
	pub(super) do_iterate: bool,
	pub(super) explanation: Option<Explanation>,
}

impl Plan {
	pub(super) fn new(
		ctx: &Context<'_>,
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
	fn add_iter(&mut self, ctx: &Context<'_>, iter: &Iterable) {
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

	fn new_iter(ctx: &Context<'_>, iter: &Iterable) -> Self {
		match iter {
			Iterable::Value(v) => Self {
				name: "Iterate Value".into(),
				details: vec![("value", v.to_owned())],
			},
			Iterable::Table(t) => Self {
				name: "Iterate Table".into(),
				details: vec![("table", Value::from(t.0.to_owned()))],
			},
			Iterable::Thing(t) => Self {
				name: "Iterate Thing".into(),
				details: vec![("thing", Value::Thing(t.to_owned()))],
			},
			Iterable::Defer(t) => Self {
				name: "Iterate Defer".into(),
				details: vec![("thing", Value::Thing(t.to_owned()))],
			},
			Iterable::Range(r) => Self {
				name: "Iterate Range".into(),
				details: vec![("table", Value::from(r.tb.to_owned()))],
			},
			Iterable::Edges(e) => Self {
				name: "Iterate Edges".into(),
				details: vec![("from", Value::Thing(e.from.to_owned()))],
			},
			Iterable::Mergeable(t, v) => Self {
				name: "Iterate Mergeable".into(),
				details: vec![("thing", Value::Thing(t.to_owned())), ("value", v.to_owned())],
			},
			Iterable::Relatable(t1, t2, t3, None) => Self {
				name: "Iterate Relatable".into(),
				details: vec![
					("thing-1", Value::Thing(t1.to_owned())),
					("thing-2", Value::Thing(t2.to_owned())),
					("thing-3", Value::Thing(t3.to_owned())),
				],
			},
			Iterable::Relatable(t1, t2, t3, Some(v)) => Self {
				name: "Iterate Relatable".into(),
				details: vec![
					("thing-1", Value::Thing(t1.to_owned())),
					("thing-2", Value::Thing(t2.to_owned())),
					("thing-3", Value::Thing(t3.to_owned())),
					("value", v.to_owned().into()),
				],
			},
			Iterable::Index(t, ir) => {
				let mut details = vec![("table", Value::from(t.0.to_owned()))];
				if let Some(qp) = ctx.get_query_planner() {
					if let Some(exe) = qp.get_query_executor(&t.0) {
						details.push(("plan", exe.explain(*ir)));
					}
				}
				Self {
					name: "Iterate Index".into(),
					details,
				}
			}
		}
	}

	pub(super) fn new_collector(
		collector_type: &str,
		mut details: Vec<(&'static str, Value)>,
	) -> ExplainItem {
		details.insert(0, ("type", collector_type.into()));
		Self {
			name: "Collector".into(),
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
