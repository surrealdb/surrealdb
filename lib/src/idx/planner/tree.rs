use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::IndexOption;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Cond, Expression, Idiom, Operator, Subquery, Table, Value};
use async_recursion::async_recursion;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct Tree {}

pub(super) type IndexMap = HashMap<Expression, HashSet<IndexOption>>;

impl Tree {
	pub(super) async fn build<'a>(
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		cond: &Option<Cond>,
	) -> Result<Option<(Node, IndexMap)>, Error> {
		let mut b = TreeBuilder {
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
			Value::Subquery(s) => self.eval_subquery(s).await?,
			_ => Node::Unsupported,
		})
	}

	async fn eval_idiom(&mut self, i: &Idiom) -> Result<Node, Error> {
		Ok(if let Some(ix) = self.find_index(i).await? {
			Node::IndexedField(ix)
		} else {
			Node::NonIndexedField
		})
	}

	async fn eval_expression(&mut self, e: &Expression) -> Result<Node, Error> {
		let left = self.eval_value(&e.l).await?;
		let right = self.eval_value(&e.r).await?;
		let mut index_option = None;
		if let Some(ix) = left.is_indexed_field() {
			if let Some(io) = IndexOption::found(ix, &e.o, &right, e) {
				index_option = Some(io.clone());
				self.add_index(e, io);
			}
		}
		if let Some(ix) = right.is_indexed_field() {
			if let Some(io) = IndexOption::found(ix, &e.o, &left, e) {
				index_option = Some(io.clone());
				self.add_index(e, io);
			}
		}
		Ok(Node::Expression {
			index_option,
			left: Box::new(left),
			right: Box::new(right),
			operator: e.o.to_owned(),
		})
	}

	fn add_index(&mut self, e: &Expression, io: IndexOption) {
		match self.index_map.entry(e.clone()) {
			Entry::Occupied(mut e) => {
				e.get_mut().insert(io);
			}
			Entry::Vacant(e) => {
				e.insert(HashSet::from([io]));
			}
		}
	}

	async fn eval_subquery(&mut self, s: &Subquery) -> Result<Node, Error> {
		Ok(match s {
			Subquery::Value(v) => self.eval_value(v).await?,
			_ => Node::Unsupported,
		})
	}
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) enum Node {
	Expression {
		index_option: Option<IndexOption>,
		left: Box<Node>,
		right: Box<Node>,
		operator: Operator,
	},
	IndexedField(DefineIndexStatement),
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

	pub(super) fn is_indexed_field(&self) -> Option<&DefineIndexStatement> {
		if let Node::IndexedField(ix) = self {
			Some(ix)
		} else {
			None
		}
	}
}
