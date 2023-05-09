use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Cond, Expression, Idiom, Number, Operator, Strand, Table, Value};
use async_recursion::async_recursion;

#[derive(Debug)]
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

pub(super) struct TreeBuilder<'a> {
	opt: &'a Options,
	txn: &'a Transaction,
	table: &'a Table,
}

impl<'a> TreeBuilder<'a> {
	pub(super) async fn parse(
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		cond: &Option<Cond>,
	) -> Result<Option<Node>, Error> {
		let builder = TreeBuilder {
			opt,
			txn,
			table,
		};
		let mut root = None;
		if let Some(cond) = cond {
			root = Some(builder.eval_value(&cond.0).await?);
		}
		Ok(root)
	}

	#[async_recursion]
	async fn eval_value(&self, v: &Value) -> Result<Node, Error> {
		Ok(match v {
			Value::Expression(e) => self.eval_expression(e).await?,
			Value::Idiom(i) => self.eval_idiom(i).await?,
			Value::Strand(s) => Node::Strand(s.clone()),
			Value::Number(n) => Node::Number(n.clone()),
			Value::Bool(b) => Node::Bool(b.clone()),
			_ => Node::Unsupported,
		})
	}

	async fn eval_idiom(&self, i: &Idiom) -> Result<Node, Error> {
		let indexes = self
			.txn
			.clone()
			.lock()
			.await
			.all_ix(self.opt.ns(), self.opt.db(), &self.table.0)
			.await?;
		for ix in indexes.as_ref() {
			if ix.cols.len() == 1 && ix.cols[0].eq(i) {
				println!("INDEX FOUND: {:?}", ix.name);
				return Ok(Node::IndexedField(ix.clone()));
			}
		}
		Ok(Node::NonIndexedField)
	}

	async fn eval_expression(&self, e: &Expression) -> Result<Node, Error> {
		let left = self.eval_value(&e.l).await?;
		let right = self.eval_value(&e.r).await?;
		Ok(Node::Expression {
			left: Box::new(left),
			right: Box::new(right),
			operator: e.o.to_owned(),
		})
	}
}
