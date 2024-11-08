use crate::idx::planner::executor::KnnExpressions;
use crate::sql::id::range::IdRange;
use crate::sql::part::DestructurePart;
use crate::sql::{
	Array, Cast, Cond, Expression, Function, Id, Idiom, Model, Object, Part, Range, Thing, Value,
};

use std::collections::BTreeMap;
use std::ops::Bound;

pub(super) struct KnnConditionRewriter<'a>(&'a KnnExpressions);

impl<'a> KnnConditionRewriter<'a> {
	// This function rebuild the same condition, but replaces any KnnExpression by a `true` value
	#[allow(clippy::mutable_key_type)]
	pub(super) fn build(expressions: &'a KnnExpressions, cond: &Cond) -> Option<Cond> {
		let b = Self(expressions);
		b.eval_value(&cond.0).map(Cond)
	}

	fn eval_value(&self, v: &Value) -> Option<Value> {
		match v {
			Value::Array(a) => self.eval_value_array(a),
			Value::Object(o) => self.eval_value_object(o),
			Value::Thing(t) => self.eval_value_thing(t),
			Value::Idiom(i) => self.eval_value_idiom(i),
			Value::Cast(c) => self.eval_value_cast(c),
			Value::Range(r) => self.eval_value_range(r),
			Value::Edges(_)
			| Value::Block(_)
			| Value::Future(_)
			| Value::Subquery(_)
			| Value::Query(_) => None,
			Value::Function(f) => self.eval_value_function(f),
			Value::Expression(e) => self.eval_value_expression(e),
			Value::Model(m) => self.eval_value_model(m),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Strand(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Uuid(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Param(_)
			| Value::Table(_)
			| Value::Mock(_)
			| Value::Regex(_)
			| Value::Constant(_)
			| Value::Closure(_) => Some(v.clone()),
		}
	}

	fn eval_value_array(&self, a: &Array) -> Option<Value> {
		self.eval_array(a).map(|a| a.into())
	}

	fn eval_array(&self, a: &Array) -> Option<Array> {
		self.eval_values(&a.0).map(|v| v.into())
	}

	fn eval_values(&self, values: &[Value]) -> Option<Vec<Value>> {
		let mut new_vec = Vec::with_capacity(values.len());
		for v in values {
			if let Some(v) = self.eval_value(v) {
				new_vec.push(v);
			} else {
				return None;
			}
		}
		Some(new_vec)
	}

	fn eval_destructure_part(&self, part: &DestructurePart) -> Option<DestructurePart> {
		match part {
			DestructurePart::Aliased(f, v) => {
				self.eval_idiom(v).map(|v| DestructurePart::Aliased(f.clone(), v))
			}
			DestructurePart::Destructure(f, v) => {
				self.eval_destructure_parts(v).map(|v| DestructurePart::Destructure(f.clone(), v))
			}
			p => Some(p.clone()),
		}
	}

	fn eval_destructure_parts(&self, parts: &[DestructurePart]) -> Option<Vec<DestructurePart>> {
		let mut new_vec = Vec::with_capacity(parts.len());
		for part in parts {
			if let Some(part) = self.eval_destructure_part(part) {
				new_vec.push(part);
			} else {
				return None;
			}
		}
		Some(new_vec)
	}

	fn eval_value_object(&self, o: &Object) -> Option<Value> {
		self.eval_object(o).map(|o| o.into())
	}
	fn eval_object(&self, o: &Object) -> Option<Object> {
		let mut new_o = BTreeMap::new();
		for (k, v) in &o.0 {
			if let Some(v) = self.eval_value(v) {
				new_o.insert(k.to_owned(), v);
			} else {
				return None;
			}
		}
		Some(new_o.into())
	}

	fn eval_value_thing(&self, t: &Thing) -> Option<Value> {
		self.eval_thing(t).map(|t| t.into())
	}

	fn eval_thing(&self, t: &Thing) -> Option<Thing> {
		self.eval_id(&t.id).map(|id| Thing {
			tb: t.tb.clone(),
			id,
		})
	}

	fn eval_id(&self, id: &Id) -> Option<Id> {
		match id {
			Id::Number(_) | Id::String(_) | Id::Generate(_) | Id::Uuid(_) => Some(id.clone()),
			Id::Array(a) => self.eval_array(a).map(Id::Array),
			Id::Object(o) => self.eval_object(o).map(Id::Object),
			Id::Range(r) => self.eval_id_range(r).map(|v| Id::Range(Box::new(v))),
		}
	}

	fn eval_value_idiom(&self, i: &Idiom) -> Option<Value> {
		self.eval_idiom(i).map(|i| i.into())
	}

	fn eval_idiom(&self, i: &Idiom) -> Option<Idiom> {
		let mut new_i = Vec::with_capacity(i.0.len());
		for p in &i.0 {
			if let Some(p) = self.eval_part(p) {
				new_i.push(p);
			} else {
				return None;
			}
		}
		Some(new_i.into())
	}
	fn eval_part(&self, p: &Part) -> Option<Part> {
		match p {
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Index(_)
			| Part::Optional
			| Part::Recurse(_) => Some(p.clone()),
			Part::Where(v) => self.eval_value(v).map(Part::Where),
			Part::Graph(_) => None,
			Part::Value(v) => self.eval_value(v).map(Part::Value),
			Part::Start(v) => self.eval_value(v).map(Part::Start),
			Part::Method(n, p) => self.eval_values(p).map(|v| Part::Method(n.clone(), v)),
			Part::Destructure(p) => self.eval_destructure_parts(p).map(Part::Destructure),
			Part::Nest(v) => self.eval_idiom(v).map(Part::Nest),
		}
	}

	fn eval_value_cast(&self, c: &Cast) -> Option<Value> {
		self.eval_cast(c).map(|c| c.into())
	}

	fn eval_cast(&self, c: &Cast) -> Option<Cast> {
		self.eval_value(&c.1).map(|v| Cast(c.0.clone(), v))
	}

	fn eval_value_range(&self, r: &Range) -> Option<Value> {
		self.eval_range(r).map(|r| r.into())
	}

	fn eval_range(&self, r: &Range) -> Option<Range> {
		if let Some(beg) = self.eval_bound(&r.beg) {
			self.eval_bound(&r.end).map(|end| Range {
				beg,
				end,
			})
		} else {
			None
		}
	}

	fn eval_bound(&self, b: &Bound<Value>) -> Option<Bound<Value>> {
		match b {
			Bound::Included(v) => self.eval_value(v).map(Bound::Included),
			Bound::Excluded(v) => self.eval_value(v).map(Bound::Excluded),
			Bound::Unbounded => Some(Bound::Unbounded),
		}
	}

	fn eval_id_range(&self, r: &IdRange) -> Option<IdRange> {
		if let Some(beg) = self.eval_id_bound(&r.beg) {
			self.eval_id_bound(&r.end).map(|end| IdRange {
				beg,
				end,
			})
		} else {
			None
		}
	}

	fn eval_id_bound(&self, b: &Bound<Id>) -> Option<Bound<Id>> {
		match b {
			Bound::Included(v) => self.eval_id(v).map(Bound::Included),
			Bound::Excluded(v) => self.eval_id(v).map(Bound::Excluded),
			Bound::Unbounded => Some(Bound::Unbounded),
		}
	}

	fn eval_value_function(&self, f: &Function) -> Option<Value> {
		self.eval_function(f).map(|f| f.into())
	}

	fn eval_function(&self, f: &Function) -> Option<Function> {
		match f {
			Function::Normal(s, args) => {
				self.eval_values(args).map(|args| Function::Normal(s.clone(), args))
			}
			Function::Custom(s, args) => {
				self.eval_values(args).map(|args| Function::Custom(s.clone(), args))
			}
			Function::Script(s, args) => {
				self.eval_values(args).map(|args| Function::Script(s.clone(), args))
			}
			Function::Anonymous(p, args) => {
				self.eval_values(args).map(|args| Function::Anonymous(p.clone(), args))
			}
		}
	}

	fn eval_value_model(&self, m: &Model) -> Option<Value> {
		self.eval_model(m).map(|m| m.into())
	}

	fn eval_model(&self, m: &Model) -> Option<Model> {
		self.eval_values(&m.args).map(|args| Model {
			name: m.name.clone(),
			version: m.version.clone(),
			args,
		})
	}

	fn eval_value_expression(&self, e: &Expression) -> Option<Value> {
		if self.0.contains(e) {
			return Some(Value::Bool(true));
		}
		self.eval_expression(e).map(|e| e.into())
	}

	fn eval_expression(&self, e: &Expression) -> Option<Expression> {
		match e {
			Expression::Unary {
				o,
				v,
			} => self.eval_value(v).map(|v| Expression::Unary {
				o: o.clone(),
				v,
			}),
			Expression::Binary {
				l,
				o,
				r,
			} => {
				if let Some(l) = self.eval_value(l) {
					self.eval_value(r).map(|r| Expression::Binary {
						l,
						o: o.clone(),
						r,
					})
				} else {
					None
				}
			}
		}
	}
}
