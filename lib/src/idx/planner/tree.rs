use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::IndexOption;
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Cond, Expression, Idiom, Operator, Subquery, Table, Value};
use async_recursion::async_recursion;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Tree {}

impl Tree {
	/// Traverse the all the conditions and extract every expression
	/// that can be resolved by an index.
	pub(super) async fn build<'a>(
		ctx: &'a Context<'_>,
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		cond: &Option<Cond>,
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
				.all_ix(self.opt.ns(), self.opt.db(), &self.table.0)
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
		Ok(match v {
			Value::Expression(e) => self.eval_expression(e).await?,
			Value::Idiom(i) => self.eval_idiom(i).await?,
			Value::Strand(_) => Node::Scalar(v.to_owned()),
			Value::Number(_) => Node::Scalar(v.to_owned()),
			Value::Bool(_) => Node::Scalar(v.to_owned()),
			Value::Thing(_) => Node::Scalar(v.to_owned()),
			Value::Subquery(s) => self.eval_subquery(s).await?,
			Value::Param(p) => {
				let v = p.compute(self.ctx, self.opt, self.txn, None).await?;
				self.eval_value(&v).await?
			}
			_ => Node::Unsupported,
		})
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
			} => Err(Error::FeatureNotYetImplemented {
				feature: "unary expressions in index",
			}),
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
		if let Some(v) = v.is_scalar() {
			let (found, mr, qs) = match &ix.index {
				Index::Idx => (Operator::Equal.eq(op), None, None),
				Index::Uniq => (Operator::Equal.eq(op), None, None),
				Index::Search {
					..
				} => {
					if let Operator::Matches(mr) = op {
						(true, *mr, Some(v.clone().to_raw_string()))
					} else {
						(false, None, None)
					}
				}
			};
			if found {
				let io = IndexOption::new(ix.clone(), id.clone(), op.to_owned(), v.clone(), qs, mr);
				self.index_map.0.insert(e.clone(), io.clone());
				return Some(io);
			}
		}
		None
	}

	async fn eval_subquery(&mut self, s: &Subquery) -> Result<Node, Error> {
		Ok(match s {
			Subquery::Value(v) => self.eval_value(v).await?,
			_ => Node::Unsupported,
		})
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
	Unsupported,
}

impl Node {
	pub(super) fn is_scalar(&self) -> Option<&Value> {
		if let Node::Scalar(v) = self {
			Some(v)
		} else {
			None
		}
	}

	pub(super) fn is_indexed_field(&self) -> Option<(&Idiom, &DefineIndexStatement)> {
		if let Node::IndexedField(id, ix) = self {
			Some((id, ix))
		} else {
			None
		}
	}
}
