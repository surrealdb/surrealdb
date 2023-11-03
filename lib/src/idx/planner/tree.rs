use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::{IndexOperator, IndexOption};
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Cond, Expression, Idiom, Operator, Part, Subquery, Table, Value, With};
use async_recursion::async_recursion;
use indexmap::IndexMap;
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
	) -> Result<Option<(Node, IndexesMap, Vec<IndexRef>, HashMap<IdiomRef, Idiom>)>, Error> {
		let mut b = TreeBuilder::new(ctx, opt, txn, table, with);
		if let Some(cond) = cond {
			let node = b.eval_value(&cond.0).await?;
			println!("COND: {}", cond.0);
			println!("idioms: {:?}", b.idioms);
			println!("idioms_indexes: {:?}", b.idioms_indexes);
			println!("idioms_filters: {:?}", b.idioms_filters);
			Ok(Some((
				node,
				b.index_map,
				b.with_indexes,
				b.idioms.into_iter().map(|(key, value)| (value, key)).collect(),
			)))
		} else {
			Ok(None)
		}
	}
}

struct TreeBuilder<'a> {
	ctx: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	table: &'a Table,
	with: &'a Option<With>,
	indexes: Option<Arc<[DefineIndexStatement]>>,
	idioms: IndexMap<Idiom, IdiomRef>,
	idioms_indexes: HashMap<IdiomRef, Option<Arc<Vec<IndexRef>>>>,
	idioms_filters: HashMap<IdiomRef, (IndexOption, Arc<Node>)>,
	index_map: IndexesMap,
	with_indexes: Vec<IndexRef>,
}

impl<'a> TreeBuilder<'a> {
	fn new(
		ctx: &'a Context<'_>,
		opt: &'a Options,
		txn: &'a Transaction,
		table: &'a Table,
		with: &'a Option<With>,
	) -> Self {
		let with_indexes = match with {
			Some(With::Index(ixs)) => Vec::with_capacity(ixs.len()),
			_ => vec![],
		};
		Self {
			ctx,
			opt,
			txn,
			table,
			with,
			indexes: None,
			idioms: Default::default(),
			idioms_indexes: Default::default(),
			idioms_filters: Default::default(),
			index_map: Default::default(),
			with_indexes,
		}
	}
	async fn lazy_cache_indexes(&mut self) -> Result<(), Error> {
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
		Ok(())
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn eval_value(&mut self, v: &Value) -> Result<Node, Error> {
		match v {
			Value::Expression(e) => self.eval_expression(e).await,
			Value::Idiom(i) => self.eval_idiom(i).await,
			Value::Strand(_) | Value::Number(_) | Value::Bool(_) | Value::Thing(_) => {
				Ok(Node::Computed(v.to_owned()))
			}
			Value::Array(a) => self.eval_array(a).await,
			Value::Subquery(s) => self.eval_subquery(s).await,
			Value::Param(p) => {
				let v = p.compute(self.ctx, self.opt, self.txn, None).await?;
				self.eval_value(&v).await
			}
			_ => Ok(Node::Unsupported(format!("Unsupported value: {}", v))),
		}
	}

	async fn eval_array(&mut self, a: &Array) -> Result<Node, Error> {
		let mut values = Vec::with_capacity(a.len());
		for v in &a.0 {
			values.push(v.compute(self.ctx, self.opt, self.txn, None).await?);
		}
		Ok(Node::Computed(Value::Array(Array::from(values))))
	}

	async fn eval_idiom(&mut self, i: &Idiom) -> Result<Node, Error> {
		let mut res = Node::NonIndexedField;
		// Check if the idiom has already been resolved
		if let Some(ir) = self.idioms.get(i) {
			if let Some((io, node)) = self.idioms_filters.get(ir).cloned() {
				return Ok(Node::IndexedFilter(io, node));
			}
			if let Some(Some(irs)) = self.idioms_indexes.get(ir).cloned() {
				return Ok(Node::IndexedField(*ir, irs));
			}
			return Ok(res);
		};

		// Compute the idiom value if it is a param
		if let Some(Part::Start(x)) = i.0.first() {
			if x.is_param() {
				let v = i.compute(self.ctx, self.opt, self.txn, None).await?;
				return self.eval_value(&v).await;
			}
		}

		self.lazy_cache_indexes().await?;

		let new_idr = self.idioms.len() as IdiomRef;
		self.idioms.insert(i.clone(), new_idr);

		// First we want to detect the form `field[WHERE subfield = ...]`
		if let Some((io, val)) = self.detect_indexed_filter(new_idr, i).await? {
			self.idioms_filters.insert(new_idr, (io.clone(), val.clone()));
			res = Node::IndexedFilter(io, val);
		// Otherwise we try to detect if it matches an index
		} else if let Some(irs) = self.resolve_indexes(new_idr, i) {
			res = Node::IndexedField(new_idr, irs);
		}

		Ok(res)
	}

	fn resolve_indexes(&mut self, ir: IdiomRef, i: &Idiom) -> Option<Arc<Vec<IndexRef>>> {
		let mut res = None;
		if let Some(indexes) = &self.indexes {
			let mut irs = Vec::new();
			for ix in indexes.as_ref() {
				if ix.cols.len() == 1 && ix.cols[0].eq(i) {
					let ixr = self.index_map.definitions.len() as IndexRef;
					if let Some(With::Index(ixs)) = self.with {
						if ixs.contains(&ix.name.0) {
							self.with_indexes.push(ixr);
						}
					}
					self.index_map.definitions.push(ix.clone());
					irs.push(ixr);
				}
			}
			if !irs.is_empty() {
				res = Some(Arc::new(irs));
			}
		}
		self.idioms_indexes.insert(ir, res.clone());
		res
	}
	async fn detect_indexed_filter(
		&mut self,
		idr: IdiomRef,
		i: &Idiom,
	) -> Result<Option<(IndexOption, Arc<Node>)>, Error> {
		if i.len() != 2 {
			return Ok(None);
		}
		let mut res = None;
		if let (Part::Field(id1), Part::Where(Value::Expression(e))) = (&i.0[0], &i.0[1]) {
			if let Expression::Binary {
				l: Value::Idiom(i),
				o,
				r,
			} = e.as_ref()
			{
				if i.len() == 1 {
					if let Part::Field(id2) = &i.0[0] {
						let translated_idiom = Idiom::from(vec![
							Part::Field(id1.clone()),
							Part::All,
							Part::Field(id2.clone()),
						]);
						let n = Arc::new(self.eval_value(r).await?);
						let new_idr = self.idioms.len() as IdiomRef;
						if let Some(irs) = self.resolve_indexes(new_idr, &translated_idiom) {
							if let Some(io) = self.lookup_index_option(
								irs.as_slice(),
								o,
								idr,
								n.as_ref(),
								None,
								IdiomPosition::Left,
							) {
								self.idioms_filters.insert(idr, (io.clone(), n.clone()));
								res = Some((io, n.clone()));
							}
						}
						self.idioms.insert(translated_idiom, new_idr);
					}
				}
			}
		}
		Ok(res)
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
					io = self.lookup_index_option(
						irs.as_slice(),
						o,
						id,
						&right,
						Some(e),
						IdiomPosition::Left,
					);
				} else if let Some((id, irs)) = right.is_indexed_field() {
					io = self.lookup_index_option(
						irs.as_slice(),
						o,
						id,
						&left,
						Some(e),
						IdiomPosition::Right,
					);
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
		id: IdiomRef,
		n: &Node,
		e: Option<&Expression>,
		p: IdiomPosition,
	) -> Option<IndexOption> {
		for ir in irs {
			if let Some(ix) = self.index_map.definitions.get(*ir as usize) {
				let op = match &ix.index {
					Index::Idx => Self::eval_index_operator(op, n, p),
					Index::Uniq => Self::eval_index_operator(op, n, p),
					Index::Search {
						..
					} => Self::eval_matches_operator(op, n),
					Index::MTree(_) => Self::eval_knn_operator(op, n),
				};
				if let Some(op) = op {
					let io = IndexOption::new(*ir, id, op);
					if let Some(e) = e {
						self.index_map.options.insert(Arc::new(e.clone()), io.clone());
					}
					return Some(io);
				}
			}
		}
		None
	}
	fn eval_matches_operator(op: &Operator, n: &Node) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			if let Operator::Matches(mr) = op {
				return Some(IndexOperator::Matches(v.clone().to_raw_string(), *mr));
			}
		}
		None
	}

	fn eval_knn_operator(op: &Operator, n: &Node) -> Option<IndexOperator> {
		if let Operator::Knn(k) = op {
			if let Node::Computed(Value::Array(a)) = n {
				return Some(IndexOperator::Knn(a.clone(), *k));
			}
		}
		None
	}

	fn eval_index_operator(op: &Operator, n: &Node, p: IdiomPosition) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			match (op, v, p) {
				(Operator::Equal, v, _) => Some(IndexOperator::Equality(v.clone())),
				(Operator::Contain, v, IdiomPosition::Left) => {
					Some(IndexOperator::Equality(v.clone()))
				}
				(Operator::ContainAny, Value::Array(a), IdiomPosition::Left) => {
					Some(IndexOperator::Union(a.clone()))
				}
				(Operator::ContainAll, Value::Array(a), IdiomPosition::Left) => {
					Some(IndexOperator::Union(a.clone()))
				}
				(
					Operator::LessThan
					| Operator::LessThanOrEqual
					| Operator::MoreThan
					| Operator::MoreThanOrEqual,
					v,
					p,
				) => Some(IndexOperator::RangePart(p.transform(op), v.clone())),
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
pub(super) type IdiomRef = u16;

/// For each expression the a possible index option
#[derive(Default)]
pub(super) struct IndexesMap {
	pub(super) options: HashMap<Arc<Expression>, IndexOption>,
	pub(super) definitions: Vec<DefineIndexStatement>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) enum Node {
	Expression {
		io: Option<IndexOption>,
		left: Box<Node>,
		right: Box<Node>,
		exp: Arc<Expression>,
	},
	IndexedField(IdiomRef, Arc<Vec<IndexRef>>),
	IndexedFilter(IndexOption, Arc<Node>),
	NonIndexedField,
	Computed(Value),
	Unsupported(String),
}

impl Node {
	pub(super) fn is_computed(&self) -> Option<&Value> {
		if let Node::Computed(v) = self {
			Some(v)
		} else {
			None
		}
	}

	pub(super) fn is_indexed_field(&self) -> Option<(IdiomRef, Arc<Vec<IndexRef>>)> {
		if let Node::IndexedField(ir, irs) = self {
			Some((*ir, irs.clone()))
		} else {
			None
		}
	}
}

#[derive(Clone, Copy)]
enum IdiomPosition {
	Left,
	Right,
}

impl IdiomPosition {
	// Reverses the operator for non commutative operators
	fn transform(&self, op: &Operator) -> Operator {
		match self {
			IdiomPosition::Left => op.clone(),
			IdiomPosition::Right => match op {
				Operator::LessThan => Operator::MoreThan,
				Operator::LessThanOrEqual => Operator::MoreThanOrEqual,
				Operator::MoreThan => Operator::LessThan,
				Operator::MoreThanOrEqual => Operator::LessThanOrEqual,
				_ => op.clone(),
			},
		}
	}
}
