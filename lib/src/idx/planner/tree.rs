use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::{IndexOption, Lookup};
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Cond, Expression, Idiom, Operator, Subquery, Table, Value};
use async_recursion::async_recursion;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Tree {}

impl Tree {
	/// Traverse condition and extract every expression
	/// that can be resolved by an index.
	pub(super) async fn build<'a>(
		ctx: &'a Context<'_>,
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		cond: &'a Option<Cond>,
	) -> Result<Option<(Node, IndexMap)>, Error> {
		let mut b = TreeBuilder {
			ctx,
			opt,
			txn,
			table,
			indexes: None,
			index_map: IndexMap::default(),
		};
		let mut res = None;
		if let Some(cond) = cond {
			res = Some((b.eval_value(&cond.0).await?, b.index_map));
		}
		Ok(res)
	}
}

struct TreeBuilder<'a> {
	ctx: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	table: &'a Table,
	indexes: Option<Arc<[DefineIndexStatement]>>,
	index_map: IndexMap,
}

impl<'a> TreeBuilder<'a> {
	async fn find_index(&mut self, i: &Idiom) -> Result<Option<DefineIndexStatement>, Error> {
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
		if let Some(indexes) = &self.indexes {
			for ix in indexes.as_ref() {
				if ix.cols.len() == 1 && ix.cols[0].eq(i) {
					return Ok(Some(ix.clone()));
				}
			}
		}
		Ok(None)
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
			Value::Subquery(s) => self.eval_subquery(s).await,
			Value::Param(p) => {
				let v = p.compute(self.ctx, self.opt, self.txn, None).await?;
				self.eval_value(&v).await?
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
		Ok(if let Some(ix) = self.find_index(i).await? {
			Node::IndexedField(i.to_owned(), ix)
		} else {
			Node::NonIndexedField
		})
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
				if let Some(io) = self.index_map.0.get(e) {
					return Ok(Node::Expression {
						io: Some(io.clone()),
						left: Box::new(left),
						right: Box::new(right),
						exp: e.clone(),
					});
				}
				let mut io = None;
				if let Some((id, ix)) = left.is_indexed_field() {
					io = self.lookup_index_option(ix, o, id, &right, e);
				} else if let Some((id, ix)) = right.is_indexed_field() {
					io = self.lookup_index_option(ix, o, id, &left, e);
				};
				Ok(Node::Expression {
					io,
					left: Box::new(left),
					right: Box::new(right),
					exp: e.clone(),
				})
			}
		}
	}

	fn lookup_index_option(
		&mut self,
		ix: &DefineIndexStatement,
		op: &Operator,
		id: &Idiom,
		v: &Node,
		e: &Expression,
	) -> Option<IndexOption> {
		let params = match &ix.index {
			Index::Idx => Self::lookup_idx_option(op, v),
			Index::Uniq => Self::lookup_uniq_option(op, v),
			Index::Search {
				..
			} => Self::lookup_ftindex_option(op, v),
			Index::MTree {
				..
			} => Self::lookup_mtree_option(op, v),
		};
		if let Some(p) = params {
			let io = IndexOption::new(ix.clone(), id.clone(), op.to_owned(), p);
			self.index_map.0.insert(e.clone(), io.clone());
			return Some(io);
		}
		None
	}

	fn lookup_idx_option(op: &Operator, n: &Node) -> Option<Lookup> {
		if Operator::Equal.eq(op) {
			if let Node::Scalar(v) = n {
				return Some(Lookup::IdxEqual(v.to_owned()));
			}
		}
		None
	}

	fn lookup_uniq_option(op: &Operator, n: &Node) -> Option<Lookup> {
		if Operator::Equal.eq(op) {
			if let Node::Scalar(v) = n {
				return Some(Lookup::UniqEqual(v.to_owned()));
			}
		}
		None
	}

	fn lookup_ftindex_option(op: &Operator, v: &Node) -> Option<Lookup> {
		if let Operator::Matches(mr) = op {
			if let Node::Scalar(v) = v {
				return Some(Lookup::FtMatches {
					qs: v.to_owned().to_raw_string(),
					mr: *mr,
				});
			}
		}
		None
	}

	fn lookup_mtree_option(op: &Operator, v: &Node) -> Option<Lookup> {
		if let Operator::Knn(k) = op {
			if let Node::Vector(a) = v {
				return Some(Lookup::MtKnn {
					a: a.to_owned(),
					k: *k,
				});
			}
		}
		None
	}

	async fn eval_subquery(&mut self, s: &Subquery) -> Result<Node, Error> {
		match s {
			Subquery::Value(v) => self.eval_value(v).await,
			_ => Ok(Node::Unsupported(format!("Unsupported subquery: {}", s))),
		}
	}
}

/// For each expression the a possible index option
#[derive(Default)]
pub(super) struct IndexMap(HashMap<Expression, IndexOption>);

impl IndexMap {
	pub(super) fn consume(self) -> HashMap<Expression, IndexOption> {
		self.0
	}
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) enum Node {
	Expression {
		io: Option<IndexOption>,
		left: Box<Node>,
		right: Box<Node>,
		exp: Expression,
	},
	IndexedField(Idiom, DefineIndexStatement),
	NonIndexedField,
	Scalar(Value),
	Vector(Array),
	Unsupported(String),
}

impl Node {
	pub(super) fn is_indexed_field(&self) -> Option<(&Idiom, &DefineIndexStatement)> {
		if let Node::IndexedField(id, ix) = self {
			Some((id, ix))
		} else {
			None
		}
	}
}
