use crate::dbs::{Iterable, Options, Transaction};
use crate::err::Error;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Cond, Expression, Idiom, Number, Operator, Strand, Table, Value};
use async_recursion::async_recursion;

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	cond: &'a Option<Cond>,
}

#[derive(Debug)]
struct Node {
	operands: Vec<Operand>,
	operator: Operator,
	has_index: bool,
	has_idiom: bool,
}

impl From<&Operator> for Node {
	fn from(o: &Operator) -> Self {
		Self {
			operands: Vec::with_capacity(2),
			operator: o.to_owned(),
			has_index: false,
			has_idiom: false,
		}
	}
}

impl Node {
	fn is_suitable(&self) -> bool {
		for operand in &self.operands {
			if let Operand::Index(_) = operand {
				// TODO check operator depending on the index
				if self.operator != Operator::Equal {
					return false;
				}
			}
		}
		return !self.has_idiom;
	}

	fn add_operand(&mut self, operand: Operand) {
		match operand {
			Operand::Index(_) => self.has_index = true,
			Operand::Idiom => self.has_idiom = true,
			_ => {}
		}
		self.operands.push(operand);
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Operand {
	None,
	Index(DefineIndexStatement),
	Idiom,
	Array(Array),
	Strand(Strand),
	Number(Number),
	Bool(bool),
	Unsupported,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			cond,
			opt,
		}
	}

	pub(crate) async fn get_iterable(
		&self,
		txn: &Transaction,
		t: Table,
	) -> Result<Iterable, Error> {
		if let Some(cond) = self.cond {
			let mut nodes = Vec::new();
			self.eval_value(txn, &t, &cond.0, &mut Node::from(&Operator::Equal), &mut nodes)
				.await?;
			let mut index_nodes = Vec::new();
			for node in nodes {
				if !node.is_suitable() {
					return Ok(Iterable::Table(t));
				}
				if node.has_index {
					index_nodes.push(node);
				}
			}
			println!("INDEX FOUND: {:?}", index_nodes);
			return Ok(Iterable::Table(t));
		} else {
			Ok(Iterable::Table(t))
		}
	}

	#[async_recursion]
	async fn eval_value(
		&self,
		txn: &Transaction,
		t: &Table,
		v: &Value,
		node: &mut Node,
		nodes: &mut Vec<Node>,
	) -> Result<(), Error> {
		match v {
			Value::Expression(e) => {
				self.eval_expression(txn, t, e, nodes).await?;
			}
			Value::Idiom(i) => {
				self.eval_idiom(txn, t, i, node).await?;
			}
			Value::Strand(s) => {
				node.add_operand(Operand::Strand(s.clone()));
			}
			Value::Number(n) => {
				node.add_operand(Operand::Number(n.clone()));
			}
			Value::Bool(b) => {
				node.add_operand(Operand::Bool(b.clone()));
			}
			_ => {}
		};
		Ok(())
	}

	async fn eval_idiom(
		&self,
		txn: &Transaction,
		t: &Table,
		i: &Idiom,
		node: &mut Node,
	) -> Result<(), Error> {
		let indexes = txn.clone().lock().await.all_ix(self.opt.ns(), self.opt.db(), &t.0).await?;
		for ix in indexes.as_ref() {
			if ix.cols.len() == 1 && ix.cols[0].eq(i) {
				node.add_operand(Operand::Index(ix.clone()));
				return Ok(());
			}
		}
		node.add_operand(Operand::Idiom);
		Ok(())
	}

	async fn eval_expression(
		&self,
		txn: &Transaction,
		t: &Table,
		e: &Expression,
		nodes: &mut Vec<Node>,
	) -> Result<(), Error> {
		let mut node = Node::from(&e.o);
		self.eval_value(txn, t, &e.l, &mut node, nodes).await?;
		self.eval_value(txn, t, &e.r, &mut node, nodes).await?;
		nodes.push(node);
		Ok(())
	}
}
