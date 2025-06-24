use crate::expr::id::range::RecordIdKeyRangeLit;
use crate::expr::literal::ObjectEntry;
use crate::expr::part::DestructurePart;
use crate::expr::{
	Cond, Expr, Function, FunctionCall, Idiom, Literal, Model, Part, PrefixOperator,
	RecordIdKeyLit, RecordIdLit,
};
use crate::idx::planner::executor::KnnExpressions;
use crate::val::{Array, Number, Object, Range, RecordId, Value};

use std::collections::BTreeMap;
use std::ops::Bound;

pub(super) struct KnnConditionRewriter<'a>(&'a KnnExpressions);

impl<'a> KnnConditionRewriter<'a> {
	// This function rebuild the same condition, but replaces any KnnExpression by a `true` value
	#[expect(clippy::mutable_key_type)]
	pub(super) fn build(expressions: &'a KnnExpressions, cond: &Cond) -> Option<Cond> {
		let b = Self(expressions);
		b.rewrite_expr(&cond.0).map(Cond)
	}

	fn rewrite_expr(&self, v: &Expr) -> Option<Expr> {
		if self.0.contains(v) {
			return Some(Expr::Literal(Literal::Bool(true)));
		}

		match v {
			Expr::Literal(Literal::Array(a)) => self.eval_value_array(a),
			Expr::Literal(Literal::Object(o)) => self.eval_value_object(o),
			Expr::Literal(Literal::RecordId(r)) => self.eval_value_thing(r),
			Expr::Literal(l) => Some(Expr::Literal(l.clone())),
			Expr::Idiom(i) => self.eval_value_idiom(i),
			Expr::Binary {
				left,
				op,
				right,
			} => {
				let left = self.rewrite_expr(left)?;
				let right = self.rewrite_expr(right)?;
				Some(Expr::Binary {
					left,
					op: op.clone(),
					right,
				})
			}
			Expr::Prefix {
				op,
				expr,
			} => Some(Expr::Prefix {
				op: op.clone(),
				expr: self.rewrite_expr(expr)?,
			}),
			Expr::Postfix {
				op,
				expr,
			} => Some(Expr::Prefix {
				op: op.clone(),
				expr: self.rewrite_expr(expr)?,
			}),
			Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Forach(_)
			| Expr::Let(_) => Some(v.clone()),

			Expr::Block(_) | Expr::Future(_) => None,
			Expr::FunctionCall(function_call) => {
				let arguments = Vec::new();
				for arg in function_call.arguments.iter() {
					arguments.push(self.rewrite_expr(arg)?);
				}

				Some(Expr::FunctionCall(Box::new(FunctionCall {
					arguments,
					receiver: function_call.receiver.clone(),
				})))
			}
		}
	}

	fn eval_value_array(&self, a: &Vec<Expr>) -> Option<Value> {
		self.eval_array(a).map(|a| a.into())
	}

	fn eval_array(&self, a: &Array) -> Option<Array> {
		self.eval_values(&a.0).map(|v| v.into())
	}

	fn eval_values(&self, values: &[Value]) -> Option<Vec<Value>> {
		let mut new_vec = Vec::with_capacity(values.len());
		for v in values {
			if let Some(v) = self.rewrite_expr(v) {
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

	fn eval_value_object(&self, o: &[ObjectEntry]) -> Option<Value> {
		self.eval_object(o).map(|o| o.into())
	}

	fn eval_object(&self, o: &[ObjectEntry]) -> Option<Object> {
		let mut new_o = BTreeMap::new();
		for entry in o {
			if let Some(v) = self.rewrite_expr(&entry.value) {
				new_o.insert(entry.key.clone(), v);
			} else {
				return None;
			}
		}
		Some(new_o.into())
	}

	fn eval_value_thing(&self, t: &RecordIdLit) -> Option<Value> {
		self.eval_thing(t).map(|t| t.into())
	}

	fn eval_thing(&self, t: &RecordIdLit) -> Option<RecordId> {
		self.eval_id(&t.id).map(|key| RecordId {
			table: t.tb.clone(),
			key,
		})
	}

	fn eval_id(&self, id: &RecordIdKeyLit) -> Option<RecordIdKeyLit> {
		match id {
			RecordIdKeyLit::Number(_)
			| RecordIdKeyLit::String(_)
			| RecordIdKeyLit::Generate(_)
			| RecordIdKeyLit::Uuid(_) => Some(id.clone()),
			RecordIdKeyLit::Array(a) => self.eval_array(a).map(RecordIdKeyLit::Array),
			RecordIdKeyLit::Object(o) => self.eval_object(o).map(RecordIdKeyLit::Object),
			RecordIdKeyLit::Range(r) => {
				self.eval_id_range(r).map(|v| RecordIdKeyLit::Range(Box::new(v)))
			}
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
			| Part::Optional
			| Part::Recurse(_, None, _)
			| Part::Doc
			| Part::RepeatRecurse => Some(p.clone()),
			Part::Where(v) => self.rewrite_expr(v).map(Part::Where),
			Part::Graph(_) => None,
			Part::Value(v) => self.rewrite_expr(v).map(Part::Value),
			Part::Start(v) => self.rewrite_expr(v).map(Part::Start),
			Part::Method(n, p) => self.eval_values(p).map(|v| Part::Method(n.clone(), v)),
			Part::Destructure(p) => self.eval_destructure_parts(p).map(Part::Destructure),
			Part::Recurse(r, Some(v), instruction) => self
				.eval_idiom(v)
				.map(|v| Part::Recurse(r.to_owned(), Some(v), instruction.to_owned())),
		}
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
			Bound::Included(v) => self.rewrite_expr(v).map(Bound::Included),
			Bound::Excluded(v) => self.rewrite_expr(v).map(Bound::Excluded),
			Bound::Unbounded => Some(Bound::Unbounded),
		}
	}

	fn eval_id_range(&self, r: &RecordIdKeyRangeLit) -> Option<RecordIdKeyRangeLit> {
		if let Some(beg) = self.eval_id_bound(&r.start) {
			self.eval_id_bound(&r.end).map(|end| RecordIdKeyRangeLit {
				start: beg,
				end,
			})
		} else {
			None
		}
	}

	fn eval_id_bound(&self, b: &Bound<RecordIdKeyLit>) -> Option<Bound<RecordIdKeyLit>> {
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
			Function::Anonymous(p, args, args_computed) => self
				.eval_values(args)
				.map(|args| Function::Anonymous(p.clone(), args, args_computed.to_owned())),
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
}
