use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::KnnExpressions;
use crate::idx::planner::plan::{IndexOperator, IndexOption};
use crate::kvs;
use crate::sql::index::{Distance, Index};
use crate::sql::statements::{DefineFieldStatement, DefineIndexStatement};
use crate::sql::{
	Array, Cond, Expression, Idiom, Kind, Number, Operator, Part, Subquery, Table, Value, With,
};
use async_recursion::async_recursion;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Tree {
	pub(super) root: Node,
	pub(super) index_map: IndexesMap,
	pub(super) with_indexes: Vec<IndexRef>,
	pub(super) knn_expressions: KnnExpressions,
}

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
	) -> Result<Option<Self>, Error> {
		let mut b = TreeBuilder::new(ctx, opt, txn, table, with);
		if let Some(cond) = cond {
			let root = b.eval_value(0, &cond.0).await?;
			Ok(Some(Self {
				root,
				index_map: b.index_map,
				with_indexes: b.with_indexes,
				knn_expressions: b.knn_expressions,
			}))
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
	fields_and_indexes: Option<(Arc<[DefineIndexStatement]>, Arc<[DefineFieldStatement]>)>,
	resolved_expressions: HashMap<Arc<Expression>, ResolvedExpression>,
	resolved_idioms: HashMap<Arc<Idiom>, Arc<Idiom>>,
	idioms_indexes: HashMap<Arc<Idiom>, Arc<Vec<IndexRef>>>,
	index_map: IndexesMap,
	with_indexes: Vec<IndexRef>,
	knn_expressions: KnnExpressions,
	idioms_record_options: HashMap<Arc<Idiom>, RecordOptions>,
	group_sequence: GroupRef,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct RecordOptions {
	irs: Arc<Vec<IndexRef>>,
	remotes: Arc<Vec<(Table, Vec<DefineIndexStatement>)>>,
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
			fields_and_indexes: None,
			resolved_expressions: Default::default(),
			resolved_idioms: Default::default(),
			idioms_indexes: Default::default(),
			index_map: Default::default(),
			with_indexes,
			knn_expressions: Default::default(),
			idioms_record_options: Default::default(),
			group_sequence: 0,
		}
	}

	async fn lazy_cache_fields_and_indexes(
		&mut self,
		tx: &mut kvs::Transaction,
	) -> Result<(), Error> {
		if self.fields_and_indexes.is_some() {
			return Ok(());
		}
		let indexes = tx.all_tb_indexes(self.opt.ns(), self.opt.db(), &self.table.0).await?;
		let fields = tx.all_tb_fields(self.opt.ns(), self.opt.db(), &self.table.0).await?;
		self.fields_and_indexes = Some((indexes, fields));
		Ok(())
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn eval_value(&mut self, group: GroupRef, v: &Value) -> Result<Node, Error> {
		match v {
			Value::Expression(e) => self.eval_expression(group, e).await,
			Value::Idiom(i) => self.eval_idiom(group, i).await,
			Value::Strand(_)
			| Value::Number(_)
			| Value::Bool(_)
			| Value::Thing(_)
			| Value::Duration(_)
			| Value::Uuid(_)
			| Value::Constant(_)
			| Value::Geometry(_)
			| Value::Datetime(_) => Ok(Node::Computed(Arc::new(v.to_owned()))),
			Value::Array(a) => self.eval_array(a).await,
			Value::Subquery(s) => self.eval_subquery(s).await,
			Value::Param(p) => {
				let v = p.compute(self.ctx, self.opt, self.txn, None).await?;
				self.eval_value(group, &v).await
			}
			_ => Ok(Node::Unsupported(format!("Unsupported value: {}", v))),
		}
	}

	async fn eval_array(&mut self, a: &Array) -> Result<Node, Error> {
		let mut values = Vec::with_capacity(a.len());
		for v in &a.0 {
			values.push(v.compute(self.ctx, self.opt, self.txn, None).await?);
		}
		Ok(Node::Computed(Arc::new(Value::Array(Array::from(values)))))
	}

	async fn eval_idiom(&mut self, group: GroupRef, i: &Idiom) -> Result<Node, Error> {
		// Check if the idiom has already been resolved
		if let Some(i) = self.resolved_idioms.get(i) {
			if let Some(irs) = self.idioms_indexes.get(i) {
				if !irs.is_empty() {
					return Ok(Node::IndexedField(i.clone(), irs.clone()));
				}
			}
			if let Some(ro) = self.idioms_record_options.get(i).cloned() {
				return Ok(Node::RecordField(i.clone(), ro));
			}
			return Ok(Node::NonIndexedField(i.clone()));
		};

		// Compute the idiom value if it is a param
		if let Some(Part::Start(x)) = i.0.first() {
			if x.is_param() {
				let v = i.compute(self.ctx, self.opt, self.txn, None).await?;
				return self.eval_value(group, &v).await;
			}
		}

		let mut tx = self.txn.lock().await;
		self.lazy_cache_fields_and_indexes(&mut tx).await?;

		let i = Arc::new(i.clone());
		self.resolved_idioms.insert(i.clone(), i.clone());

		// Try to detect if it matches an index
		let irs = self.resolve_indexes(i.clone());
		if !irs.is_empty() {
			return Ok(Node::IndexedField(i, irs));
		}
		// Try to detect an indexed record field
		if let Some(ro) = self.resolve_record_field(&mut tx, &i).await? {
			return Ok(Node::RecordField(i.clone(), ro));
		}
		Ok(Node::NonIndexedField(i))
	}

	fn resolve_indexes(&mut self, i: Arc<Idiom>) -> Arc<Vec<IndexRef>> {
		if let Some(irs) = self.idioms_indexes.get(&i).cloned() {
			return irs;
		}
		let mut irs = Vec::new();
		if let Some((indexes, _)) = &self.fields_and_indexes {
			for ix in indexes.iter() {
				if ix.cols.len() == 1 && ix.cols[0].eq(&i) {
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
		}
		let irs = Arc::new(irs);
		self.idioms_indexes.insert(i, irs.clone());
		irs
	}

	async fn resolve_record_field(
		&mut self,
		tx: &mut kvs::Transaction,
		idiom: &Arc<Idiom>,
	) -> Result<Option<RecordOptions>, Error> {
		if let Some((_, fields)) = &self.fields_and_indexes {
			for field in fields.iter() {
				if let Some(Kind::Record(tables)) = &field.kind {
					if idiom.starts_with(&field.name.0) {
						let (local_field, remote_field) = idiom.0.split_at(field.name.0.len());
						if remote_field.is_empty() {
							return Ok(None);
						}
						let local_field = Idiom::from(local_field);
						let remote_field = Idiom::from(remote_field);
						let mut remotes = vec![];
						for table in tables {
							let indexes =
								tx.all_tb_indexes(self.opt.ns(), self.opt.db(), &table.0).await?;
							let mut ix_res = vec![];
							for ix in indexes.iter() {
								if ix.cols.len() == 1 && ix.cols[0].eq(&remote_field) {
									ix_res.push(ix.clone());
								}
							}
							remotes.push((table.clone(), ix_res));
						}
						// TODO: compute IRS
						let irs = self.resolve_indexes(Arc::new(local_field));
						let ro = RecordOptions {
							irs,
							remotes: Arc::new(remotes),
						};
						self.idioms_record_options.insert(idiom.clone(), ro.clone());
						return Ok(Some(ro));
					}
				}
			}
		}
		Ok(None)
	}

	async fn eval_expression(&mut self, group: GroupRef, e: &Expression) -> Result<Node, Error> {
		match e {
			Expression::Unary {
				..
			} => Ok(Node::Unsupported("unary expressions not supported".to_string())),
			Expression::Binary {
				l,
				o,
				r,
			} => {
				// Did we already compute the same expression?
				if let Some(re) = self.resolved_expressions.get(e).cloned() {
					return Ok(re.into());
				}
				let exp = Arc::new(e.clone());
				let left = Arc::new(self.eval_value(group, l).await?);
				let right = Arc::new(self.eval_value(group, r).await?);
				let mut io = None;
				if let Some((id, irs)) = left.is_indexed_field() {
					io = self.lookup_index_option(
						irs.as_slice(),
						o,
						id,
						&right,
						&exp,
						IdiomPosition::Left,
					)?;
				} else if let Some((id, irs)) = right.is_indexed_field() {
					io = self.lookup_index_option(
						irs.as_slice(),
						o,
						id,
						&left,
						&exp,
						IdiomPosition::Right,
					)?;
				} else if let Some(id) = left.is_non_indexed_field() {
					self.eval_knn(id, &right, &exp)?;
				} else if let Some(id) = right.is_non_indexed_field() {
					self.eval_knn(id, &left, &exp)?;
				}
				let re = ResolvedExpression {
					group,
					exp: exp.clone(),
					io: io.clone(),
					left: left.clone(),
					right: right.clone(),
				};
				self.resolved_expressions.insert(exp, re.clone());
				Ok(re.into())
			}
		}
	}

	fn lookup_index_option(
		&mut self,
		irs: &[IndexRef],
		op: &Operator,
		id: Arc<Idiom>,
		n: &Node,
		e: &Arc<Expression>,
		p: IdiomPosition,
	) -> Result<Option<IndexOption>, Error> {
		for ir in irs {
			if let Some(ix) = self.index_map.definitions.get(*ir as usize) {
				let op = match &ix.index {
					Index::Idx => Self::eval_index_operator(op, n, p),
					Index::Uniq => Self::eval_index_operator(op, n, p),
					Index::Search {
						..
					} => Self::eval_matches_operator(op, n),
					Index::MTree(_) => self.eval_indexed_knn(e, op, n, id.clone())?,
				};
				if let Some(op) = op {
					let io = IndexOption::new(*ir, id, op);
					self.index_map.options.push((e.clone(), io.clone()));
					return Ok(Some(io));
				}
			}
		}
		Ok(None)
	}
	fn eval_matches_operator(op: &Operator, n: &Node) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			if let Operator::Matches(mr) = op {
				return Some(IndexOperator::Matches(v.clone().to_raw_string(), *mr));
			}
		}
		None
	}

	fn eval_indexed_knn(
		&mut self,
		exp: &Arc<Expression>,
		op: &Operator,
		n: &Node,
		id: Arc<Idiom>,
	) -> Result<Option<IndexOperator>, Error> {
		if let Operator::Knn(k, d) = op {
			if let Node::Computed(v) = n {
				let vec: Vec<Number> = v.as_ref().try_into()?;
				self.knn_expressions.insert(
					exp.clone(),
					(*k, id, Arc::new(vec), d.clone().unwrap_or(Distance::Euclidean)),
				);
				if let Value::Array(a) = v.as_ref() {
					match d {
						None | Some(Distance::Euclidean) | Some(Distance::Manhattan) => {
							return Ok(Some(IndexOperator::Knn(a.clone(), *k)))
						}
						_ => {}
					}
				}
			}
		}
		Ok(None)
	}

	fn eval_knn(&mut self, id: Arc<Idiom>, val: &Node, exp: &Arc<Expression>) -> Result<(), Error> {
		if let Operator::Knn(k, d) = exp.operator() {
			if let Node::Computed(v) = val {
				let vec: Vec<Number> = v.as_ref().try_into()?;
				self.knn_expressions.insert(
					exp.clone(),
					(*k, id, Arc::new(vec), d.clone().unwrap_or(Distance::Euclidean)),
				);
			}
		}
		Ok(())
	}

	fn eval_index_operator(op: &Operator, n: &Node, p: IdiomPosition) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			match (op, v, p) {
				(Operator::Equal, v, _) => Some(IndexOperator::Equality(v.clone())),
				(Operator::Contain, v, IdiomPosition::Left) => {
					Some(IndexOperator::Equality(v.clone()))
				}
				(
					Operator::ContainAny | Operator::ContainAll | Operator::Inside,
					Value::Array(a),
					IdiomPosition::Left,
				) => Some(IndexOperator::Union(a.clone())),
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
		self.group_sequence += 1;
		match s {
			Subquery::Value(v) => self.eval_value(self.group_sequence, v).await,
			_ => Ok(Node::Unsupported(format!("Unsupported subquery: {}", s))),
		}
	}
}

pub(super) type IndexRef = u16;
/// For each expression a possible index option
#[derive(Default)]
pub(super) struct IndexesMap {
	pub(super) options: Vec<(Arc<Expression>, IndexOption)>,
	pub(super) definitions: Vec<DefineIndexStatement>,
}

pub(super) type GroupRef = u16;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) enum Node {
	Expression {
		group: GroupRef,
		io: Option<IndexOption>,
		left: Arc<Node>,
		right: Arc<Node>,
		exp: Arc<Expression>,
	},
	IndexedField(Arc<Idiom>, Arc<Vec<IndexRef>>),
	RecordField(Arc<Idiom>, RecordOptions),
	NonIndexedField(Arc<Idiom>),
	Computed(Arc<Value>),
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

	pub(super) fn is_indexed_field(&self) -> Option<(Arc<Idiom>, Arc<Vec<IndexRef>>)> {
		match self {
			Node::IndexedField(id, irs) => Some((id.clone(), irs.clone())),
			Node::RecordField(id, ro) => {
				if ro.irs.is_empty() {
					None
				} else {
					Some((id.clone(), ro.irs.clone()))
				}
			}
			_ => None,
		}
	}

	pub(super) fn is_non_indexed_field(&self) -> Option<Arc<Idiom>> {
		if let Node::NonIndexedField(id) = self {
			Some(id.clone())
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
	// Reverses the operator for non-commutative operators
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

#[derive(Clone)]
struct ResolvedExpression {
	group: GroupRef,
	exp: Arc<Expression>,
	io: Option<IndexOption>,
	left: Arc<Node>,
	right: Arc<Node>,
}
impl From<ResolvedExpression> for Node {
	fn from(re: ResolvedExpression) -> Self {
		Node::Expression {
			group: re.group,
			io: re.io,
			left: re.left,
			right: re.right,
			exp: re.exp,
		}
	}
}
