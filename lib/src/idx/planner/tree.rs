use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Cond, Expression, Idiom, Operator, Subquery, Table, Value};
use async_recursion::async_recursion;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) enum Node {
	Expression {
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
	pub(super) fn is_scalar(&self) -> bool {
		if let Node::Scalar(_) = self {
			true
		} else {
			false
		}
	}

	pub(super) fn is_indexed_field(&self) -> Option<&DefineIndexStatement> {
		if let Node::IndexedField(index) = self {
			Some(index)
		} else {
			None
		}
	}

	pub(super) fn to_array(&self) -> Result<Array, Error> {
		let mut a = Array::with_capacity(1);
		if let Node::Scalar(v) = self {
			a.push(v.to_owned());
		}
		Ok(a)
	}

	pub(super) fn to_string(&self) -> Result<String, Error> {
		if let Node::Scalar(v) = self {
			Ok(v.to_string())
		} else {
			Err(Error::BypassQueryPlanner)
		}
	}

	pub(super) fn explain(&self) -> Value {
		match &self {
			Node::Expression {
				..
			} => Value::from("Expression"),
			Node::IndexedField(_) => Value::from("Indexed Field"),
			Node::NonIndexedField => Value::from("Non indexed Field"),
			Node::Scalar(v) => v.to_owned(),
			Node::Unsupported => Value::from("Not supported"),
		}
	}
}

pub(super) struct TreeBuilder<'a> {
	opt: &'a Options,
	txn: &'a Transaction,
	table: &'a Table,
	indexes: Option<Arc<[DefineIndexStatement]>>,
}

impl<'a> TreeBuilder<'a> {
	pub(super) async fn parse(
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		cond: &Option<Cond>,
	) -> Result<Option<Node>, Error> {
		let mut builder = TreeBuilder {
			opt,
			txn,
			table,
			indexes: None,
		};
		let mut root = None;
		if let Some(cond) = cond {
			root = Some(builder.eval_value(&cond.0).await?);
		}
		Ok(root)
	}

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

	#[async_recursion]
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
		Ok(if let Some(index) = self.find_index(i).await? {
			Node::IndexedField(index)
		} else {
			Node::NonIndexedField
		})
	}

	async fn eval_expression(&mut self, e: &Expression) -> Result<Node, Error> {
		let left = self.eval_value(&e.l).await?;
		let right = self.eval_value(&e.r).await?;
		Ok(Node::Expression {
			left: Box::new(left),
			right: Box::new(right),
			operator: e.o.to_owned(),
		})
	}

	async fn eval_subquery(&mut self, s: &Subquery) -> Result<Node, Error> {
		Ok(match s {
			Subquery::Value(v) => self.eval_value(v).await?,
			_ => Node::Unsupported,
		})
	}
}
