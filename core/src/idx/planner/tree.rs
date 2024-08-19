use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::planner::executor::{
	KnnBruteForceExpression, KnnBruteForceExpressions, KnnExpressions,
};
use crate::idx::planner::plan::{IndexOperator, IndexOption};
use crate::idx::planner::rewriter::KnnConditionRewriter;
use crate::kvs::Transaction;
use crate::sql::index::Index;
use crate::sql::statements::{DefineFieldStatement, DefineIndexStatement};
use crate::sql::{
	Array, Cond, Expression, Idiom, Kind, Number, Operator, Order, Orders, Part, Subquery, Table,
	Value, With,
};
use reblessive::tree::Stk;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Tree {
	pub(super) root: Option<Node>,
	pub(super) index_map: IndexesMap,
	pub(super) with_indexes: Vec<IndexRef>,
	pub(super) knn_expressions: KnnExpressions,
	pub(super) knn_brute_force_expressions: KnnBruteForceExpressions,
	pub(super) knn_condition: Option<Cond>,
	pub(super) indexed_order: Option<(IndexRef, bool)>,
}

impl Tree {
	/// Traverse all the conditions and extract every expression
	/// that can be resolved by an index.
	pub(super) async fn build<'a>(
		stk: &mut Stk,
		ctx: &'a Context,
		opt: &'a Options,
		table: &'a Table,
		cond: Option<&Cond>,
		with: Option<&With>,
		order: Option<&Orders>,
	) -> Result<Self, Error> {
		let mut b = TreeBuilder::new(ctx, opt, table, with);
		if let Some(cond) = cond {
			b.eval_cond(stk, cond).await?;
		}
		if let Some(orders) = order {
			if let Some(order) = orders.0.get(0) {
				b.eval_indexed_order(order).await?;
			}
		}
		Ok(Self {
			root: b.root,
			index_map: b.index_map,
			with_indexes: b.with_indexes,
			knn_expressions: b.knn_expressions,
			knn_brute_force_expressions: b.knn_brute_force_expressions,
			knn_condition: b.knn_condition,
			indexed_order: b.indexed_order,
		})
	}
}

struct TreeBuilder<'a> {
	ctx: &'a Context,
	opt: &'a Options,
	table: &'a Table,
	with: Option<&'a With>,
	schemas: HashMap<Table, SchemaCache>,
	idioms_indexes: HashMap<Table, HashMap<Idiom, LocalIndexRefs>>,
	resolved_expressions: HashMap<Arc<Expression>, ResolvedExpression>,
	resolved_idioms: HashMap<Idiom, Node>,
	index_map: IndexesMap,
	with_indexes: Vec<IndexRef>,
	knn_brute_force_expressions: HashMap<Arc<Expression>, KnnBruteForceExpression>,
	knn_expressions: KnnExpressions,
	idioms_record_options: HashMap<Idiom, RecordOptions>,
	group_sequence: GroupRef,
	root: Option<Node>,
	knn_condition: Option<Cond>,
	indexed_order: Option<(IndexRef, bool)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct RecordOptions {
	locals: LocalIndexRefs,
	remotes: RemoteIndexRefs,
}

pub(super) type LocalIndexRefs = Vec<IndexRef>;
pub(super) type RemoteIndexRefs = Arc<Vec<(Idiom, LocalIndexRefs)>>;

impl<'a> TreeBuilder<'a> {
	fn new(ctx: &'a Context, opt: &'a Options, table: &'a Table, with: Option<&'a With>) -> Self {
		let with_indexes = match with {
			Some(With::Index(ixs)) => Vec::with_capacity(ixs.len()),
			_ => vec![],
		};
		Self {
			ctx,
			opt,
			table,
			with,
			schemas: Default::default(),
			idioms_indexes: Default::default(),
			resolved_expressions: Default::default(),
			resolved_idioms: Default::default(),
			index_map: Default::default(),
			with_indexes,
			knn_brute_force_expressions: Default::default(),
			knn_expressions: Default::default(),
			idioms_record_options: Default::default(),
			group_sequence: 0,
			root: None,
			knn_condition: None,
			indexed_order: None,
		}
	}

	async fn lazy_load_schema_resolver(
		&mut self,
		tx: &Transaction,
		table: &Table,
	) -> Result<(), Error> {
		if self.schemas.contains_key(table) {
			return Ok(());
		}
		let l = SchemaCache::new(self.opt, table, tx).await?;
		self.schemas.insert(table.clone(), l);
		Ok(())
	}

	async fn eval_cond(&mut self, stk: &mut Stk, cond: &Cond) -> Result<(), Error> {
		self.root = Some(self.eval_value(stk, 0, &cond.0).await?);
		self.knn_condition = if self.knn_expressions.is_empty() {
			None
		} else {
			KnnConditionRewriter::build(&self.knn_expressions, cond)
		};
		Ok(())
	}

	async fn eval_indexed_order(&mut self, order: &Order) -> Result<(), Error> {
		if order.random {
			return Ok(());
		}
		if let Node::IndexedField(_, irs) = self.resolve_idiom(order).await? {
			for ir in irs {
				let valid = if let Some(ix_def) = self.index_map.definitions.get(ir as usize) {
					match ix_def.index {
						Index::Idx => true,
						Index::Uniq => true,
						_ => false,
					}
				} else {
					false
				};
				if valid {
					self.indexed_order = Some((ir, order.direction));
					break;
				}
			}
		}
		Ok(())
	}

	async fn eval_value(
		&mut self,
		stk: &mut Stk,
		group: GroupRef,
		v: &Value,
	) -> Result<Node, Error> {
		match v {
			Value::Expression(e) => self.eval_expression(stk, group, e).await,
			Value::Idiom(i) => self.eval_idiom(stk, group, i).await,
			Value::Strand(_)
			| Value::Number(_)
			| Value::Bool(_)
			| Value::Thing(_)
			| Value::Duration(_)
			| Value::Uuid(_)
			| Value::Constant(_)
			| Value::Geometry(_)
			| Value::Datetime(_)
			| Value::Param(_)
			| Value::Function(_) => Ok(Node::Computable),
			Value::Array(a) => self.eval_array(stk, a).await,
			Value::Subquery(s) => self.eval_subquery(stk, s).await,
			_ => Ok(Node::Unsupported(format!("Unsupported value: {}", v))),
		}
	}

	async fn compute(&self, stk: &mut Stk, v: &Value, n: Node) -> Result<Node, Error> {
		Ok(if n == Node::Computable {
			match v.compute(stk, self.ctx, self.opt, None).await {
				Ok(v) => Node::Computed(Arc::new(v)),
				Err(_) => Node::Unsupported(format!("Unsupported value: {}", v)),
			}
		} else {
			n
		})
	}

	async fn eval_array(&mut self, stk: &mut Stk, a: &Array) -> Result<Node, Error> {
		let mut values = Vec::with_capacity(a.len());
		for v in &a.0 {
			values.push(stk.run(|stk| v.compute(stk, self.ctx, self.opt, None)).await?);
		}
		Ok(Node::Computed(Arc::new(Value::Array(Array::from(values)))))
	}

	async fn eval_idiom(
		&mut self,
		stk: &mut Stk,
		group: GroupRef,
		i: &Idiom,
	) -> Result<Node, Error> {
		// Check if the idiom has already been resolved
		if let Some(node) = self.resolved_idioms.get(i).cloned() {
			return Ok(node);
		};

		// Compute the idiom value if it is a param
		if let Some(Part::Start(x)) = i.0.first() {
			if x.is_param() {
				let v = stk.run(|stk| i.compute(stk, self.ctx, self.opt, None)).await?;
				return stk.run(|stk| self.eval_value(stk, group, &v)).await;
			}
		}

		let n = self.resolve_idiom(i).await?;
		Ok(n)
	}

	async fn resolve_idiom(&mut self, i: &Idiom) -> Result<Node, Error> {
		let tx = self.ctx.tx();
		self.lazy_load_schema_resolver(&tx, self.table).await?;

		// Try to detect if it matches an index
		let n = if let Some(schema) = self.schemas.get(self.table).cloned() {
			let irs = self.resolve_indexes(self.table, i, &schema);
			if !irs.is_empty() {
				Node::IndexedField(i.clone(), irs)
			} else if let Some(ro) =
				self.resolve_record_field(&tx, schema.fields.as_ref(), i).await?
			{
				// Try to detect an indexed record field
				Node::RecordField(i.clone(), ro)
			} else {
				Node::NonIndexedField(i.clone())
			}
		} else {
			Node::NonIndexedField(i.clone())
		};
		self.resolved_idioms.insert(i.clone(), n.clone());
		Ok(n)
	}

	fn resolve_indexes(&mut self, t: &Table, i: &Idiom, schema: &SchemaCache) -> Vec<IndexRef> {
		if let Some(m) = self.idioms_indexes.get(t) {
			if let Some(irs) = m.get(i).cloned() {
				return irs;
			}
		}
		let mut irs = Vec::new();
		for ix in schema.indexes.iter() {
			if ix.cols.len() == 1 && ix.cols[0].eq(i) {
				let ixr = self.index_map.definitions.len() as IndexRef;
				if let Some(With::Index(ixs)) = &self.with {
					if ixs.contains(&ix.name.0) {
						self.with_indexes.push(ixr);
					}
				}
				self.index_map.definitions.push(ix.clone());
				irs.push(ixr);
			}
		}
		if let Some(e) = self.idioms_indexes.get_mut(t) {
			e.insert(i.clone(), irs.clone());
		} else {
			self.idioms_indexes.insert(t.clone(), HashMap::from([(i.clone(), irs.clone())]));
		}
		irs
	}

	async fn resolve_record_field(
		&mut self,
		tx: &Transaction,
		fields: &[DefineFieldStatement],
		idiom: &Idiom,
	) -> Result<Option<RecordOptions>, Error> {
		for field in fields.iter() {
			if let Some(Kind::Record(tables)) = &field.kind {
				if idiom.starts_with(&field.name.0) {
					let (local_field, remote_field) = idiom.0.split_at(field.name.0.len());
					if remote_field.is_empty() {
						return Ok(None);
					}
					let local_field = Idiom::from(local_field);
					self.lazy_load_schema_resolver(tx, self.table).await?;
					let locals;
					if let Some(shema) = self.schemas.get(self.table).cloned() {
						locals = self.resolve_indexes(self.table, &local_field, &shema);
					} else {
						return Ok(None);
					}

					let remote_field = Idiom::from(remote_field);
					let mut remotes = vec![];
					for table in tables {
						self.lazy_load_schema_resolver(tx, table).await?;
						if let Some(shema) = self.schemas.get(table).cloned() {
							let remote_irs = self.resolve_indexes(table, &remote_field, &shema);
							remotes.push((remote_field.clone(), remote_irs));
						} else {
							return Ok(None);
						}
					}
					let ro = RecordOptions {
						locals,
						remotes: Arc::new(remotes),
					};
					self.idioms_record_options.insert(idiom.clone(), ro.clone());
					return Ok(Some(ro));
				}
			}
		}
		Ok(None)
	}

	async fn eval_expression(
		&mut self,
		stk: &mut Stk,
		group: GroupRef,
		e: &Expression,
	) -> Result<Node, Error> {
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
				let left = stk.run(|stk| self.eval_value(stk, group, l)).await?;
				let right = stk.run(|stk| self.eval_value(stk, group, r)).await?;
				// If both values are computable, then we can delegate the computation to the parent
				if left == Node::Computable && right == Node::Computable {
					return Ok(Node::Computable);
				}
				let exp = Arc::new(e.clone());
				let left = Arc::new(self.compute(stk, l, left).await?);
				let right = Arc::new(self.compute(stk, r, right).await?);
				let mut io = None;
				if let Some((id, local_irs, remote_irs)) = left.is_indexed_field() {
					io = self.lookup_index_options(
						o,
						id,
						&right,
						&exp,
						IdiomPosition::Left,
						local_irs,
						remote_irs,
					)?;
				} else if let Some((id, local_irs, remote_irs)) = right.is_indexed_field() {
					io = self.lookup_index_options(
						o,
						id,
						&left,
						&exp,
						IdiomPosition::Right,
						local_irs,
						remote_irs,
					)?;
				}
				if let Some(id) = left.is_field() {
					self.eval_bruteforce_knn(id, &right, &exp)?;
				} else if let Some(id) = right.is_field() {
					self.eval_bruteforce_knn(id, &left, &exp)?;
				}
				let re = ResolvedExpression {
					group,
					exp: exp.clone(),
					io,
					left: left.clone(),
					right: right.clone(),
				};
				self.resolved_expressions.insert(exp, re.clone());
				Ok(re.into())
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn lookup_index_options(
		&mut self,
		o: &Operator,
		id: &Idiom,
		node: &Node,
		exp: &Arc<Expression>,
		p: IdiomPosition,
		local_irs: LocalIndexRefs,
		remote_irs: Option<RemoteIndexRefs>,
	) -> Result<Option<IndexOption>, Error> {
		if let Some(remote_irs) = remote_irs {
			let mut remote_ios = Vec::with_capacity(remote_irs.len());
			for (id, irs) in remote_irs.iter() {
				if let Some(io) = self.lookup_index_option(irs.as_slice(), o, id, node, exp, p)? {
					remote_ios.push(io);
				} else {
					return Ok(None);
				}
			}
			if let Some(ir) = self.lookup_join_index_ref(local_irs.as_slice()) {
				let io = IndexOption::new(ir, id.clone(), p, IndexOperator::Join(remote_ios));
				return Ok(Some(io));
			}
			return Ok(None);
		}
		let io = self.lookup_index_option(local_irs.as_slice(), o, id, node, exp, p)?;
		Ok(io)
	}

	fn lookup_index_option(
		&mut self,
		irs: &[IndexRef],
		op: &Operator,
		id: &Idiom,
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
					Index::MTree(_) => self.eval_mtree_knn(e, op, n)?,
					Index::Hnsw(_) => self.eval_hnsw_knn(e, op, n)?,
				};
				if let Some(op) = op {
					let io = IndexOption::new(*ir, id.clone(), p, op);
					self.index_map.options.push((e.clone(), io.clone()));
					return Ok(Some(io));
				}
			}
		}
		Ok(None)
	}

	fn lookup_join_index_ref(&self, irs: &[IndexRef]) -> Option<IndexRef> {
		for ir in irs {
			if let Some(ix) = self.index_map.definitions.get(*ir as usize) {
				match &ix.index {
					Index::Idx | Index::Uniq => return Some(*ir),
					_ => {}
				};
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

	fn eval_mtree_knn(
		&mut self,
		exp: &Arc<Expression>,
		op: &Operator,
		n: &Node,
	) -> Result<Option<IndexOperator>, Error> {
		if let Operator::Knn(k, None) = op {
			if let Node::Computed(v) = n {
				let vec: Arc<Vec<Number>> = Arc::new(v.as_ref().try_into()?);
				self.knn_expressions.insert(exp.clone());
				return Ok(Some(IndexOperator::Knn(vec, *k)));
			}
		}
		Ok(None)
	}

	fn eval_hnsw_knn(
		&mut self,
		exp: &Arc<Expression>,
		op: &Operator,
		n: &Node,
	) -> Result<Option<IndexOperator>, Error> {
		if let Operator::Ann(k, ef) = op {
			if let Node::Computed(v) = n {
				let vec: Arc<Vec<Number>> = Arc::new(v.as_ref().try_into()?);
				self.knn_expressions.insert(exp.clone());
				return Ok(Some(IndexOperator::Ann(vec, *k, *ef)));
			}
		}
		Ok(None)
	}

	fn eval_bruteforce_knn(
		&mut self,
		id: &Idiom,
		val: &Node,
		exp: &Arc<Expression>,
	) -> Result<(), Error> {
		if let Operator::Knn(k, Some(d)) = exp.operator() {
			if let Node::Computed(v) = val {
				let vec: Arc<Vec<Number>> = Arc::new(v.as_ref().try_into()?);
				self.knn_expressions.insert(exp.clone());
				self.knn_brute_force_expressions.insert(
					exp.clone(),
					KnnBruteForceExpression::new(*k, id.clone(), vec, d.clone()),
				);
			}
		}
		Ok(())
	}

	fn eval_index_operator(op: &Operator, n: &Node, p: IdiomPosition) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			match (op, v, p) {
				(Operator::Equal, v, _) => Some(IndexOperator::Equality(v.clone())),
				(Operator::Exact, v, _) => Some(IndexOperator::Exactness(v.clone())),
				(Operator::Contain, v, IdiomPosition::Left) => {
					Some(IndexOperator::Equality(v.clone()))
				}
				(Operator::Inside, v, IdiomPosition::Right) => {
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

	async fn eval_subquery(&mut self, stk: &mut Stk, s: &Subquery) -> Result<Node, Error> {
		self.group_sequence += 1;
		match s {
			Subquery::Value(v) => stk.run(|stk| self.eval_value(stk, self.group_sequence, v)).await,
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

#[derive(Clone)]
struct SchemaCache {
	indexes: Arc<[DefineIndexStatement]>,
	fields: Arc<[DefineFieldStatement]>,
}

impl SchemaCache {
	async fn new(opt: &Options, table: &Table, tx: &Transaction) -> Result<Self, Error> {
		let indexes = tx.all_tb_indexes(opt.ns()?, opt.db()?, table).await?;
		let fields = tx.all_tb_fields(opt.ns()?, opt.db()?, table).await?;
		Ok(Self {
			indexes,
			fields,
		})
	}
}

pub(super) type GroupRef = u16;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum Node {
	Expression {
		group: GroupRef,
		io: Option<IndexOption>,
		left: Arc<Node>,
		right: Arc<Node>,
		exp: Arc<Expression>,
	},
	IndexedField(Idiom, Vec<IndexRef>),
	RecordField(Idiom, RecordOptions),
	NonIndexedField(Idiom),
	Computable,
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

	pub(super) fn is_indexed_field(
		&self,
	) -> Option<(&Idiom, LocalIndexRefs, Option<RemoteIndexRefs>)> {
		match self {
			Node::IndexedField(id, irs) => Some((id, irs.clone(), None)),
			Node::RecordField(id, ro) => Some((id, ro.locals.clone(), Some(ro.remotes.clone()))),
			_ => None,
		}
	}

	pub(super) fn is_field(&self) -> Option<&Idiom> {
		match self {
			Node::IndexedField(id, _) => Some(id),
			Node::RecordField(id, _) => Some(id),
			Node::NonIndexedField(id) => Some(id),
			_ => None,
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) enum IdiomPosition {
	/// The idiom is on the left of the condition clause
	Left,
	/// The idiom is on the right tf the condition clause
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
