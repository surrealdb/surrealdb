use std::ops::Bound;

use crate::expr::literal::ObjectEntry;
use crate::expr::part::DestructurePart;
use crate::expr::{
	Cond, Expr, FunctionCall, Idiom, Literal, Part, RecordIdKeyLit, RecordIdKeyRangeLit,
	RecordIdLit,
};
use crate::idx::planner::executor::KnnExpressions;

pub(super) struct KnnConditionRewriter<'a>(&'a KnnExpressions);

impl<'a> KnnConditionRewriter<'a> {
	// This function rebuild the same condition, but replaces any KnnExpression by a
	// `true` value
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
			Expr::Idiom(i) => self.eval_expr_idiom(i),
			Expr::Binary {
				left,
				op,
				right,
			} => {
				let left = self.rewrite_expr(left)?;
				let right = self.rewrite_expr(right)?;
				Some(Expr::Binary {
					left: Box::new(left),
					op: op.clone(),
					right: Box::new(right),
				})
			}
			Expr::Prefix {
				op,
				expr,
			} => Some(Expr::Prefix {
				op: op.clone(),
				expr: Box::new(self.rewrite_expr(expr)?),
			}),
			Expr::Postfix {
				op,
				expr,
			} => Some(Expr::Postfix {
				op: op.clone(),
				expr: Box::new(self.rewrite_expr(expr)?),
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
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_) => Some(v.clone()),

			Expr::Block(_) => None,
			Expr::FunctionCall(function_call) => {
				let mut arguments = Vec::new();
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

	fn eval_value_array(&self, a: &[Expr]) -> Option<Expr> {
		self.eval_exprs(a).map(|x| Expr::Literal(Literal::Array(x)))
	}

	fn eval_exprs(&self, values: &[Expr]) -> Option<Vec<Expr>> {
		let mut new_vec = Vec::with_capacity(values.len());
		for v in values {
			new_vec.push(self.rewrite_expr(v)?);
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

	fn eval_value_object(&self, o: &[ObjectEntry]) -> Option<Expr> {
		self.eval_object(o).map(|o| Expr::Literal(Literal::Object(o)))
	}

	fn eval_object(&self, o: &[ObjectEntry]) -> Option<Vec<ObjectEntry>> {
		let mut new_o = Vec::with_capacity(o.len());
		for entry in o {
			new_o.push(ObjectEntry {
				key: entry.key.clone(),
				value: self.rewrite_expr(&entry.value)?,
			});
		}
		Some(new_o)
	}

	fn eval_value_thing(&self, t: &RecordIdLit) -> Option<Expr> {
		self.eval_thing(t).map(|t| Expr::Literal(Literal::RecordId(t)))
	}

	fn eval_thing(&self, t: &RecordIdLit) -> Option<RecordIdLit> {
		self.eval_id(&t.key).map(|id| RecordIdLit {
			table: t.table.clone(),
			key: id,
		})
	}

	fn eval_id(&self, id: &RecordIdKeyLit) -> Option<RecordIdKeyLit> {
		match id {
			RecordIdKeyLit::Number(_)
			| RecordIdKeyLit::String(_)
			| RecordIdKeyLit::Generate(_)
			| RecordIdKeyLit::Uuid(_) => Some(id.clone()),
			RecordIdKeyLit::Array(a) => self.eval_exprs(a).map(RecordIdKeyLit::Array),
			RecordIdKeyLit::Object(o) => self.eval_object(o).map(RecordIdKeyLit::Object),
			RecordIdKeyLit::Range(r) => {
				self.eval_id_range(r).map(|v| RecordIdKeyLit::Range(Box::new(v)))
			}
		}
	}

	fn eval_expr_idiom(&self, i: &Idiom) -> Option<Expr> {
		self.eval_idiom(i).map(Expr::Idiom)
	}

	fn eval_idiom(&self, i: &Idiom) -> Option<Idiom> {
		let mut new_i = Vec::with_capacity(i.0.len());
		for p in &i.0 {
			let p = self.eval_part(p)?;
			new_i.push(p);
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
			Part::Lookup(_) => None,
			Part::Value(v) => self.rewrite_expr(v).map(Part::Value),
			Part::Start(v) => self.rewrite_expr(v).map(Part::Start),
			Part::Method(n, p) => self.eval_exprs(p).map(|v| Part::Method(n.clone(), v)),
			Part::Destructure(p) => self.eval_destructure_parts(p).map(Part::Destructure),
			Part::Recurse(r, Some(v), instruction) => self
				.eval_idiom(v)
				.map(|v| Part::Recurse(r.to_owned(), Some(v), instruction.to_owned())),
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
}
