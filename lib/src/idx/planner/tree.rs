use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::{IndexOperator, IndexOption};
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Cond, Expression, Idiom, Operator, Subquery, Table, Value, With};
use async_recursion::async_recursion;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Tree {}

impl Tree {
	/// Traverse all the conditions and extract every expression
	/// that can be resolved by an index.
	pub(super) async fn build<'a>(
		ctx: &'a Context<'_>,
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		cond: &'a Option<Cond>,
		with: &'a Option<With>,
	) -> Result<Option<(Node, IndexMap, Vec<IndexRef>)>, Error> {
		let with_indexes = match with {
			Some(With::Index(ixs)) => Vec::with_capacity(ixs.len()),
			_ => vec![],
		};
		let mut b = TreeBuilder {
			ctx,
			opt,
			txn,
			table,
			with,
			indexes: None,
			index_lookup: Default::default(),
			index_map: IndexMap::default(),
			with_indexes,
		};
		let mut res = None;
		if let Some(cond) = cond {
			res = Some((b.eval_value(&cond.0).await?, b.index_map, b.with_indexes));
		}
		Ok(res)
	}
}

struct TreeBuilder<'a> {
	ctx: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	table: &'a Table,
	with: &'a Option<With>,
	indexes: Option<Arc<[DefineIndexStatement]>>,
	index_lookup: HashMap<Idiom, Option<Arc<Vec<IndexRef>>>>,
	index_map: IndexMap,
	with_indexes: Vec<IndexRef>,
}

impl<'a> TreeBuilder<'a> {
	async fn find_indexes(&mut self, i: &Idiom) -> Result<Option<Arc<Vec<IndexRef>>>, Error> {
		if let Some(irs) = self.index_lookup.get(i) {
			return Ok(irs.clone());
		}
		if self.indexes.is_none() {
			let indexes = self
				.txn
				.clone()
				.lock()
				.await
				.all_tb_indexes(self.opt.ns(), self.opt.db(), &self.table.0)
				.await?;
			self.indexes = Some(indexes);
		}
		let mut irs = Vec::new();
		if let Some(indexes) = &self.indexes {
			for ix in indexes.as_ref() {
				if ix.cols.len() == 1 && ix.cols[0].eq(i) {
					let ir = self.index_map.definitions.len() as IndexRef;
					if let Some(With::Index(ixs)) = self.with {
						if ixs.contains(&ix.name.0) {
							self.with_indexes.push(ir);
						}
					}
					self.index_map.definitions.insert(ir, ix.clone());
					irs.push(ir);
				}
			}
		}
		let irs = if irs.is_empty() {
			None
		} else {
			Some(Arc::new(irs))
		};
		self.index_lookup.insert(i.clone(), irs.clone());
		Ok(irs)
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn eval_value(&mut self, v: &Value) -> Result<Node, Error> {
		match v {
			Value::Expression(e) => self.eval_expression(e).await,
			Value::Idiom(i) => self.eval_idiom(i).await,
			Value::Strand(_) | Value::Number(_) | Value::Bool(_) | Value::Thing(_) => {
				Ok(Node::Scalar(v.to_owned()))
			}
			Value::Array(a) => Ok(self.eval_array(a)),
			Value::Subquery(s) => self.eval_subquery(s).await,
			Value::Param(p) => {
				let v = p.compute(self.ctx, self.opt, self.txn, None).await?;
				self.eval_value(&v).await
			}
			_ => Ok(Node::Unsupported(format!("Unsupported value: {}", v))),
		}
	}

	fn eval_array(&mut self, a: &Array) -> Node {
		// Check if it is a numeric vector
		for v in &a.0 {
			if !v.is_number() {
				return Node::Unsupported(format!("Unsupported array: {}", a));
			}
		}
		Node::Vector(a.to_owned())
	}

	async fn eval_idiom(&mut self, i: &Idiom) -> Result<Node, Error> {
		if let Some(irs) = self.find_indexes(i).await? {
			if !irs.is_empty() {
				return Ok(Node::IndexedField(i.to_owned(), irs));
			}
		}
		Ok(Node::NonIndexedField)
	}

	async fn eval_expression(&mut self, e: &Expression) -> Result<Node, Error> {
		match e {
			Expression::Unary {
				..
			} => Ok(Node::Unsupported("unary expressions not supported".to_string())),
			Expression::Binary {
				l,
				o,
				r,
			} => {
				let left = self.eval_value(l).await?;
				let right = self.eval_value(r).await?;
				if let Some(io) = self.index_map.options.get(e) {
					return Ok(Node::Expression {
						io: Some(io.clone()),
						left: Box::new(left),
						right: Box::new(right),
						exp: Arc::new(e.clone()),
					});
				}
				let mut io = None;
				if let Some((id, irs)) = left.is_indexed_field() {
					io = self.lookup_index_option(irs.as_slice(), o, id, &right, e);
				} else if let Some((id, irs)) = right.is_indexed_field() {
					io = self.lookup_index_option(irs.as_slice(), o, id, &left, e);
				};
				Ok(Node::Expression {
					io,
					left: Box::new(left),
					right: Box::new(right),
					exp: Arc::new(e.clone()),
				})
			}
		}
	}

	fn lookup_index_option(
		&mut self,
		irs: &[IndexRef],
		op: &Operator,
		id: &Idiom,
		n: &Node,
		e: &Expression,
	) -> Option<IndexOption> {
		for ir in irs {
			if let Some(ix) = self.index_map.definitions.get(ir) {
				let op = match &ix.index {
					Index::Idx => Self::eval_index_operator(op, n),
					Index::Uniq => Self::eval_index_operator(op, n),
					Index::Search {
						..
					} => {
						if let Some(v) = n.is_scalar() {
							if let Operator::Matches(mr) = op {
								Some(IndexOperator::Matches(v.clone().to_raw_string(), *mr))
							} else {
								None
							}
						} else {
							None
						}
					}
					Index::MTree(_) => {
						if let Operator::Knn(k) = op {
							if let Node::Vector(a) = n {
								Some(IndexOperator::Knn(a.clone(), *k))
							} else {
								None
							}
						} else {
							None
						}
					}
				};
				if let Some(op) = op {
					let io = IndexOption::new(*ir, id.clone(), op);
					self.index_map.options.insert(Arc::new(e.clone()), io.clone());
					return Some(io);
				}
			}
		}
		None
	}

	fn eval_index_operator(op: &Operator, n: &Node) -> Option<IndexOperator> {
		if let Some(v) = n.is_scalar() {
			match op {
				Operator::Equal => Some(IndexOperator::Equality(Array::from(v.clone()))),
				Operator::LessThan
				| Operator::LessThanOrEqual
				| Operator::MoreThan
				| Operator::MoreThanOrEqual => Some(IndexOperator::RangePart(op.clone(), v.clone())),
				_ => None,
			}
		} else {
			None
		}
	}

	async fn eval_subquery(&mut self, s: &Subquery) -> Result<Node, Error> {
		match s {
			Subquery::Value(v) => self.eval_value(v).await,
			_ => Ok(Node::Unsupported(format!("Unsupported subquery: {}", s))),
		}
	}
}

pub(super) type IndexRef = u16;

/// For each expression the a possible index option
#[derive(Default)]
pub(super) struct IndexMap {
	pub(super) options: HashMap<Arc<Expression>, IndexOption>,
	pub(super) definitions: HashMap<IndexRef, DefineIndexStatement>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) enum Node {
	Expression {
		io: Option<IndexOption>,
		left: Box<Node>,
		right: Box<Node>,
		exp: Arc<Expression>,
	},
	IndexedField(Idiom, Arc<Vec<IndexRef>>),
	NonIndexedField,
	Scalar(Value),
	Vector(Array),
	Unsupported(String),
}

impl Node {
	pub(super) fn is_scalar(&self) -> Option<&Value> {
		if let Node::Scalar(v) = self {
			Some(v)
		} else {
			None
		}
	}

	pub(super) fn is_indexed_field(&self) -> Option<(&Idiom, Arc<Vec<IndexRef>>)> {
		if let Node::IndexedField(id, irs) = self {
			Some((id, irs.clone()))
		} else {
			None
		}
	}
}
