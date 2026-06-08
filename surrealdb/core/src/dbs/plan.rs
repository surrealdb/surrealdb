use std::collections::HashMap;

use surrealdb_strand::Strand;

use crate::ctx::FrozenContext;
use crate::dbs::result::Results;
use crate::dbs::{Iterable, Statement};
use crate::expr::lookup::LookupKind;
use crate::idx::planner::RecordStrategy;
use crate::val::{Object, RecordId, Value};

pub(super) struct Plan {
	pub(super) do_iterate: bool,
	pub(super) explanation: Option<Explanation>,
}

impl Plan {
	pub(super) fn new(
		ctx: &FrozenContext,
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
						exp.add_fallback(reason.clone());
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
	fn add_iter(&mut self, ctx: &FrozenContext, iter: &Iterable) {
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
			name: Value::String(Strand::new_static("Fetch")),
			details: vec![("count", count.into())],
		}
	}

	fn new_fallback(reason: String) -> Self {
		Self {
			name: Value::String(Strand::new_static("Fallback")),
			details: vec![("reason", reason.into())],
		}
	}

	fn new_iter(ctx: &FrozenContext, iter: &Iterable) -> Self {
		match iter {
			Iterable::Value(_doc_ctx, v) => Self {
				name: Value::String(Strand::new_static("Iterate Value")),
				details: vec![("value", v.to_owned())],
			},
			Iterable::GenerateRecordId(_doc_ctx, t) => Self {
				name: Value::String(Strand::new_static("Iterate Yield")),
				details: vec![("table", Value::String(t.clone().into()))],
			},
			Iterable::RecordId(_doc_ctx, t) => Self {
				name: Value::String(Strand::new_static("Iterate Record")),
				details: vec![("record", Value::RecordId(t.clone()))],
			},
			Iterable::Defer(_doc_ctx, t) => Self {
				name: Value::String(Strand::new_static("Iterate Defer")),
				details: vec![("record", Value::RecordId(t.clone()))],
			},
			Iterable::Lookup {
				from,
				kind,
				..
			} => match kind {
				LookupKind::Graph(_) => Self {
					name: Value::String(Strand::new_static("Iterate Edges")),
					details: vec![("from", Value::RecordId(from.clone()))],
				},
				LookupKind::Reference => Self {
					name: Value::String(Strand::new_static("Iterate References")),
					details: vec![("from", Value::RecordId(from.clone()))],
				},
			},
			Iterable::Table(_doc_ctx, t, rs, sc) => Self {
				name: Value::String(Strand::new_static(match rs {
					RecordStrategy::Count => "Iterate Table Count",
					RecordStrategy::KeysOnly => "Iterate Table Keys",
					RecordStrategy::KeysAndValues => "Iterate Table",
				})),
				details: vec![
					("table", Value::String(t.clone().into())),
					("direction", sc.to_string().into()),
				],
			},
			Iterable::Range(_doc_ctx, tb, r, rs, sc) => Self {
				name: Value::String(Strand::new_static(match rs {
					RecordStrategy::Count => "Iterate Range Count",
					RecordStrategy::KeysOnly => "Iterate Range Keys",
					RecordStrategy::KeysAndValues => "Iterate Range",
				})),
				details: vec![
					("table", Value::String(tb.clone().into())),
					("range", Value::Range(Box::new(r.clone().into_value_range()))),
					("direction", sc.to_string().into()),
				],
			},
			Iterable::Mergeable(_doc_ctx, tb, None, v) => Self {
				name: Value::String(Strand::new_static("Iterate Mergeable")),
				details: vec![("table", Value::String(tb.clone().into())), ("value", v.to_owned())],
			},
			Iterable::Mergeable(_doc_ctx, tb, Some(id), v) => Self {
				name: Value::String(Strand::new_static("Iterate Mergeable")),
				details: vec![
					("record", Value::RecordId(RecordId::new(tb.to_owned(), id.to_owned()))),
					("value", v.to_owned()),
				],
			},
			Iterable::Relatable(_doc_ctx, t1, t2, t3, None) => Self {
				name: Value::String(Strand::new_static("Iterate Relatable")),
				details: vec![
					("record-1", Value::RecordId(t1.to_owned())),
					("record-2", t2.clone().into()),
					("record-3", Value::RecordId(t3.to_owned())),
				],
			},
			Iterable::Relatable(_doc_ctx, t1, t2, t3, Some(v)) => Self {
				name: Value::String(Strand::new_static("Iterate Relatable")),
				details: vec![
					("record-1", Value::RecordId(t1.to_owned())),
					("record-2", t2.clone().into()),
					("record-3", Value::RecordId(t3.to_owned())),
					("value", v.to_owned()),
				],
			},
			Iterable::Index(_doc_ctx, t, ir, rs) => {
				let mut details = vec![("table", Value::String(t.clone().into()))];
				if let Some(qp) = ctx.get_query_planner()
					&& let Some(exe) = qp.get_query_executor(t)
				{
					details.push(("plan", exe.explain(*ir)));
				}
				Self {
					name: Value::String(Strand::new_static(match rs {
						RecordStrategy::Count => "Iterate Index Count",
						RecordStrategy::KeysOnly => "Iterate Index Keys",
						RecordStrategy::KeysAndValues => "Iterate Index",
					})),
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
			name: Value::String(Strand::new_static("Collector")),
			details,
		}
	}
	pub(super) fn new_record_strategy(rs: RecordStrategy) -> Self {
		Self {
			name: Value::String(Strand::new_static("RecordStrategy")),
			details: vec![(
				"type",
				match rs {
					RecordStrategy::Count => Value::String(Strand::new_static("Count")),
					RecordStrategy::KeysOnly => Value::String(Strand::new_static("KeysOnly")),
					RecordStrategy::KeysAndValues => {
						Value::String(Strand::new_static("KeysAndValues"))
					}
				},
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
			name: Value::String(Strand::new_static("StartLimitStrategy")),
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
