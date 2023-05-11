use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Cond, Expression, Idiom, Number, Operator, Strand, Subquery, Table, Value};
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
	Strand(Strand),
	Number(Number),
	Bool(bool),
	Unsupported,
}

impl Node {
	pub(super) fn is_scalar(&self) -> bool {
		match self {
			Node::Expression {
				..
			} => false,
			Node::IndexedField(_) => false,
			Node::NonIndexedField => false,
			Node::Strand(_) => true,
			Node::Number(_) => true,
			Node::Bool(_) => true,
			Node::Unsupported => false,
		}
	}

	pub(super) fn is_indexed_field(&self) -> Option<&DefineIndexStatement> {
		if let Node::IndexedField(index) = self {
			Some(index)
		} else {
			None
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
		println!("parse {:?}", table);
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
					println!("INDEX FOUND: {:?}", ix.name);
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
			Value::Strand(s) => Node::Strand(s.clone()),
			Value::Number(n) => Node::Number(n.clone()),
			Value::Bool(b) => Node::Bool(b.clone()),
			Value::Subquery(s) => self.eval_subquery(s).await?,
			_ => {
				println!("UNSUPPORTED VALUE {:?}", v);
				Node::Unsupported
			}
		})
	}

	async fn eval_idiom(&mut self, i: &Idiom) -> Result<Node, Error> {
		println!("eval_idiom {:?}", i);
		Ok(if let Some(index) = self.find_index(i).await? {
			Node::IndexedField(index)
		} else {
			Node::NonIndexedField
		})
	}

	async fn eval_expression(&mut self, e: &Expression) -> Result<Node, Error> {
		println!("eval_expression {:?}", e.o);
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
			_ => {
				println!("UNSUPPORTED SUBQUERY {:?}", s);
				Node::Unsupported
			}
		})
	}
}
