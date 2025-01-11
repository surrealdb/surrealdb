use crate::dbs::Options;
use crate::err::Error;
use crate::idx::planner::executor::{
	KnnBruteForceExpression, KnnBruteForceExpressions, KnnExpressions,
};
use crate::idx::planner::plan::{IndexOperator, IndexOption};
use crate::idx::planner::rewriter::KnnConditionRewriter;
use crate::idx::planner::StatementContext;
use crate::kvs::Transaction;
use crate::sql::index::Index;
use crate::sql::statements::{DefineFieldStatement, DefineIndexStatement};
use crate::sql::{
	order::{OrderList, Ordering},
	Array, Cond, Expression, Idiom, Kind, Number, Operator, Order, Part, Subquery, Table, Value,
	With,
};
use reblessive::tree::Stk;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

pub(super) struct Tree {
	pub(super) root: Option<Node>,
	pub(super) index_map: IndexesMap,
	pub(super) with_indexes: Option<Vec<IndexReference>>,
	pub(super) knn_expressions: KnnExpressions,
	pub(super) knn_brute_force_expressions: KnnBruteForceExpressions,
	pub(super) knn_condition: Option<Cond>,
	/// Is every expression backed by an index?
	pub(super) all_expressions_with_index: bool,
	/// Does the whole query contain only AND relations?
	pub(super) all_and: bool,
	/// Does a group contain only AND relations?
	pub(super) all_and_groups: HashMap<GroupRef, bool>,
}

impl Tree {
	/// Traverse all the conditions and extract every expression
	/// that can be resolved by an index.
	pub(super) async fn build<'a>(
		stk: &mut Stk,
		stm_ctx: &'a StatementContext<'a>,
		table: &'a Table,
	) -> Result<Self, Error> {
		let mut b = TreeBuilder::new(stm_ctx, table);
		if let Some(cond) = stm_ctx.cond {
			b.eval_cond(stk, cond).await?;
		}
		b.eval_order().await?;
		Ok(Self {
			root: b.root,
			index_map: b.index_map,
			with_indexes: b.with_indexes,
			knn_expressions: b.knn_expressions,
			knn_brute_force_expressions: b.knn_brute_force_expressions,
			knn_condition: b.knn_condition,
			all_expressions_with_index: b.leaf_nodes_count > 0
				&& b.leaf_nodes_with_index_count == b.leaf_nodes_count,
			all_and: b.all_and.unwrap_or(true),
			all_and_groups: b.all_and_groups,
		})
	}
}

struct TreeBuilder<'a> {
	ctx: &'a StatementContext<'a>,
	table: &'a Table,
	first_order: Option<&'a Order>,
	schemas: HashMap<Table, SchemaCache>,
	idioms_indexes: HashMap<Table, HashMap<Arc<Idiom>, LocalIndexRefs>>,
	resolved_expressions: HashMap<Arc<Expression>, ResolvedExpression>,
	resolved_idioms: HashMap<Arc<Idiom>, Node>,
	index_map: IndexesMap,
	with_indexes: Option<Vec<IndexReference>>,
	knn_brute_force_expressions: HashMap<Arc<Expression>, KnnBruteForceExpression>,
	knn_expressions: KnnExpressions,
	idioms_record_options: HashMap<Arc<Idiom>, RecordOptions>,
	group_sequence: GroupRef,
	root: Option<Node>,
	knn_condition: Option<Cond>,
	leaf_nodes_count: usize,
	leaf_nodes_with_index_count: usize,
	all_and: Option<bool>,
	all_and_groups: HashMap<GroupRef, bool>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct RecordOptions {
	locals: LocalIndexRefs,
	remotes: RemoteIndexRefs,
}

pub(super) type IdiomCol = usize;
pub(super) type LocalIndexRefs = Vec<(IndexReference, IdiomCol)>;
pub(super) type RemoteIndexRefs = Arc<Vec<(Arc<Idiom>, LocalIndexRefs)>>;

impl<'a> TreeBuilder<'a> {
	fn new(ctx: &'a StatementContext<'a>, table: &'a Table) -> Self {
		let with_indexes = match ctx.with {
			Some(With::Index(ixs)) => Some(Vec::with_capacity(ixs.len())),
			_ => None,
		};
		let first_order = if let Some(Ordering::Order(OrderList(o))) = ctx.order {
			o.first()
		} else {
			None
		};
		Self {
			ctx,
			table,
			first_order,
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
			all_and: None,
			all_and_groups: Default::default(),
			leaf_nodes_count: 0,
			leaf_nodes_with_index_count: 0,
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
		let l = SchemaCache::new(self.ctx.opt, table, tx).await?;
		self.schemas.insert(table.clone(), l);
		Ok(())
	}

	async fn eval_order(&mut self) -> Result<(), Error> {
		if let Some(o) = self.first_order {
			if let Value::Idiom(idiom) = &o.value {
				match o.direction {
					Value::Bool(true) => {
						if let Node::IndexedField(id, irf) = self.resolve_idiom(idiom).await? {
							for (ixr, id_col) in &irf {
								if *id_col == 0 {
									self.index_map.order_limit = Some(IndexOption::new(
										ixr.clone(),
										Some(id),
										IdiomPosition::None,
										IndexOperator::Order,
									));
									break;
								}
							}
						}
					}
					Value::Param(_) => {
						// For parameters, we can't determine the direction at planning time
						// so we don't use index optimization
					}
					_ => {}
				}
			}
		}
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
			| Value::Null
			| Value::None
			| Value::Function(_) => {
				self.leaf_nodes_count += 1;
				Ok(Node::Computable)
			}
			Value::Array(a) => self.eval_array(stk, a).await,
			Value::Subquery(s) => self.eval_subquery(stk, s).await,
			_ => Ok(Node::Unsupported(format!("Unsupported value: {}", v))),
		}
	}

	async fn compute(&self, stk: &mut Stk, v: &Value, n: Node) -> Result<Node, Error> {
		Ok(if n == Node::Computable {
			match v.compute(stk, self.ctx.ctx, self.ctx.opt, None).await {
				Ok(v) => Node::Computed(v.into()),
				Err(_) => Node::Unsupported(format!("Unsupported value: {}", v)),
			}
		} else {
			n
		})
	}

	async fn eval_array(&mut self, stk: &mut Stk, a: &Array) -> Result<Node, Error> {
		self.leaf_nodes_count += 1;
		let mut values = Vec::with_capacity(a.len());
		for v in &a.0 {
			values.push(stk.run(|stk| v.compute(stk, self.ctx.ctx, self.ctx.opt, None)).await?);
		}
		Ok(Node::Computed(Arc::new(Value::Array(Array::from(values)))))
	}

	async fn eval_idiom(&mut self, stk: &mut Stk, gr: GroupRef, i: &Idiom) -> Result<Node, Error> {
		self.leaf_nodes_count += 1;
		// Check if the idiom has already been resolved
		if let Some(node) = self.resolved_idioms.get(i).cloned() {
			return Ok(node);
		};

		// Compute the idiom value if it is a param
		if let Some(Part::Start(x)) = i.0.first() {
			if x.is_param() {
				let v = stk.run(|stk| i.compute(stk, self.ctx.ctx, self.ctx.opt, None)).await?;
				return stk.run(|stk| self.eval_value(stk, gr, &v)).await;
			}
		}

		let n = self.resolve_idiom(i).await?;
		Ok(n)
	}

	async fn resolve_idiom(&mut self, i: &Idiom) -> Result<Node, Error> {
		let tx = self.ctx.ctx.tx();
		self.lazy_load_schema_resolver(&tx, self.table).await?;
		let i = Arc::new(i.clone());
		// Try to detect if it matches an index
		let n = if let Some(schema) = self.schemas.get(self.table).cloned() {
			let irs = self.resolve_indexes(self.table, &i, &schema);
			if !irs.is_empty() {
				Node::IndexedField(i.clone(), irs)
			} else if let Some(ro) =
				self.resolve_record_field(&tx, schema.fields.as_ref(), &i).await?
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

	fn resolve_indexes(&mut self, t: &Table, i: &Idiom, schema: &SchemaCache) -> LocalIndexRefs {
		// Did we already resolve this idiom?
		if let Some(m) = self.idioms_indexes.get(t) {
			if let Some(irs) = m.get(i).cloned() {
				return irs;
			}
		}
		let mut irs = Vec::new();
		for (idx, ix) in schema.indexes.iter().enumerate() {
			if let Some(idiom_index) = ix.cols.iter().position(|p| p.eq(i)) {
				let ixr = schema.new_reference(idx);
				// Check if the WITH clause allow the index to be used
				if let Some(With::Index(ixs)) = &self.ctx.with {
					if ixs.contains(&ix.name.0) {
						if let Some(wi) = &mut self.with_indexes {
							wi.push(ixr.clone());
						} else {
							self.with_indexes = Some(vec![ixr.clone()]);
						}
					}
				}
				irs.push((ixr, idiom_index));
			}
		}
		let i = Arc::new(i.clone());
		if let Some(e) = self.idioms_indexes.get_mut(t) {
			e.insert(i, irs.clone());
		} else {
			self.idioms_indexes.insert(t.clone(), HashMap::from([(i, irs.clone())]));
		}
		irs
	}

	async fn resolve_record_field(
		&mut self,
		tx: &Transaction,
		fields: &[DefineFieldStatement],
		idiom: &Arc<Idiom>,
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

					let remote_field = Arc::new(Idiom::from(remote_field));
					let mut remotes = vec![];
					for table in tables {
						self.lazy_load_schema_resolver(tx, table).await?;
						if let Some(schema) = self.schemas.get(table).cloned() {
							let remote_irs = self.resolve_indexes(table, &remote_field, &schema);
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
			} => {
				self.leaf_nodes_count += 1;
				Ok(Node::Unsupported("unary expressions not supported".to_string()))
			}
			Expression::Binary {
				l,
				o,
				r,
			} => {
				// Did we already compute the same expression?
				if let Some(re) = self.resolved_expressions.get(e).cloned() {
					return Ok(re.into());
				}
				self.check_boolean_operator(group, o);
				let left = stk.run(|stk| self.eval_value(stk, group, l)).await?;
				let right = stk.run(|stk| self.eval_value(stk, group, r)).await?;
				// If both values are computable, then we can delegate the computation to the parent
				if left == Node::Computable && right == Node::Computable {
					return Ok(Node::Computable);
				}
				let exp = Arc::new(e.clone());
				let left = Arc::new(self.compute(stk, l, left).await?);
				let right = Arc::new(self.compute(stk, r, right).await?);
				let io = if let Some((id, local_irs, remote_irs)) = left.is_indexed_field() {
					self.lookup_index_options(
						o,
						id,
						&right,
						&exp,
						IdiomPosition::Left,
						local_irs,
						remote_irs,
					)?
				} else if let Some((id, local_irs, remote_irs)) = right.is_indexed_field() {
					self.lookup_index_options(
						o,
						id,
						&left,
						&exp,
						IdiomPosition::Right,
						local_irs,
						remote_irs,
					)?
				} else {
					None
				};
				if let Some(id) = left.is_field() {
					self.eval_bruteforce_knn(id, &right, &exp)?;
				} else if let Some(id) = right.is_field() {
					self.eval_bruteforce_knn(id, &left, &exp)?;
				}
				self.check_leaf_node_with_index(io.as_ref());
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

	fn check_boolean_operator(&mut self, gr: GroupRef, op: &Operator) {
		match op {
			Operator::Neg | Operator::Or => {
				if self.all_and != Some(false) {
					self.all_and = Some(false);
				}
				self.all_and_groups.entry(gr).and_modify(|b| *b = false).or_insert(false);
			}
			Operator::And => {
				if self.all_and.is_none() {
					self.all_and = Some(true);
				}
				self.all_and_groups.entry(gr).or_insert(true);
			}
			_ => {
				self.all_and_groups.entry(gr).or_insert(true);
			}
		}
	}

	fn check_leaf_node_with_index(&mut self, io: Option<&IndexOption>) {
		if let Some(io) = io {
			if let Some(wi) = &self.with_indexes {
				if !wi.contains(io.ix_ref()) {
					return;
				}
			}
			self.leaf_nodes_with_index_count += 2;
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn lookup_index_options(
		&mut self,
		o: &Operator,
		id: &Arc<Idiom>,
		node: &Node,
		exp: &Arc<Expression>,
		p: IdiomPosition,
		local_irs: &LocalIndexRefs,
		remote_irs: Option<&RemoteIndexRefs>,
	) -> Result<Option<IndexOption>, Error> {
		if let Some(remote_irs) = remote_irs {
			let mut remote_ios = Vec::with_capacity(remote_irs.len());
			for (id, irs) in remote_irs.iter() {
				if let Some(io) = self.lookup_index_option(irs, o, id, node, exp, p)? {
					remote_ios.push(io);
				} else {
					return Ok(None);
				}
			}
			if let Some((irf, _)) = self.lookup_join_index_ref(local_irs) {
				let io =
					IndexOption::new(irf, Some(id.clone()), p, IndexOperator::Join(remote_ios));
				return Ok(Some(io));
			}
			return Ok(None);
		}
		let io = self.lookup_index_option(local_irs, o, id, node, exp, p)?;
		Ok(io)
	}

	fn lookup_index_option(
		&mut self,
		irs: &LocalIndexRefs,
		op: &Operator,
		id: &Arc<Idiom>,
		n: &Node,
		e: &Arc<Expression>,
		p: IdiomPosition,
	) -> Result<Option<IndexOption>, Error> {
		let mut res = None;
		for (ixr, col) in irs.iter() {
			let op = match &ixr.index {
				Index::Idx => self.eval_index_operator(ixr, op, n, p, *col),
				Index::Uniq => self.eval_index_operator(ixr, op, n, p, *col),
				Index::Search {
					..
				} if *col == 0 => Self::eval_matches_operator(op, n),
				Index::MTree(_) if *col == 0 => self.eval_mtree_knn(e, op, n)?,
				Index::Hnsw(_) if *col == 0 => self.eval_hnsw_knn(e, op, n)?,
				_ => None,
			};
			if res.is_none() {
				if let Some(op) = op {
					let io = IndexOption::new(ixr.clone(), Some(id.clone()), p, op);
					self.index_map.options.push((e.clone(), io.clone()));
					res = Some(io);
				}
			}
		}
		Ok(res)
	}

	fn lookup_join_index_ref(&self, irs: &LocalIndexRefs) -> Option<(IndexReference, IdiomCol)> {
		for (ixr, id_col) in irs.iter().filter(|(_, id_col)| 0.eq(id_col)) {
			match &ixr.index {
				Index::Idx | Index::Uniq => return Some((ixr.clone(), *id_col)),
				_ => {}
			};
		}
		None
	}

	fn eval_matches_operator(op: &Operator, n: &Node) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			if let Operator::Matches(mr) = op {
				return Some(IndexOperator::Matches(v.to_raw_string(), *mr));
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

	fn eval_index_operator(
		&mut self,
		ixr: &IndexReference,
		op: &Operator,
		n: &Node,
		p: IdiomPosition,
		col: IdiomCol,
	) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			match (op, v, p) {
				(Operator::Equal | Operator::Exact, v, _) => {
					self.index_map.check_compound(ixr, col, &v);
					if col == 0 {
						return Some(IndexOperator::Equality(vec![v]));
					}
				}
				(Operator::Contain, v, IdiomPosition::Left) => {
					if col == 0 {
						return Some(IndexOperator::Equality(vec![v]));
					}
				}
				(Operator::Inside, v, IdiomPosition::Right) => {
					if col == 0 {
						return Some(IndexOperator::Equality(vec![v]));
					}
				}
				(
					Operator::ContainAny | Operator::ContainAll | Operator::Inside,
					v,
					IdiomPosition::Left,
				) => {
					if col == 0 {
						if let Value::Array(_) = v.as_ref() {
							return Some(IndexOperator::Union(v));
						}
					}
				}
				(
					Operator::LessThan
					| Operator::LessThanOrEqual
					| Operator::MoreThan
					| Operator::MoreThanOrEqual,
					v,
					p,
				) => {
					if col == 0 {
						return Some(IndexOperator::RangePart(p.transform(op), v));
					}
				}
				_ => {}
			}
		}
		None
	}

	async fn eval_subquery(&mut self, stk: &mut Stk, s: &Subquery) -> Result<Node, Error> {
		self.group_sequence += 1;
		match s {
			Subquery::Value(v) => stk.run(|stk| self.eval_value(stk, self.group_sequence, v)).await,
			_ => Ok(Node::Unsupported(format!("Unsupported subquery: {}", s))),
		}
	}
}

pub(super) type CompoundIndexes = HashMap<IndexReference, Vec<Option<Arc<Value>>>>;

/// For each expression a possible index option
#[derive(Default)]
pub(super) struct IndexesMap {
	pub(super) options: Vec<(Arc<Expression>, IndexOption)>,
	/// For each index, tells if the columns are requested
	pub(super) compound_indexes: CompoundIndexes,
	pub(super) order_limit: Option<IndexOption>,
}

impl IndexesMap {
	pub(crate) fn check_compound(&mut self, ixr: &IndexReference, col: usize, val: &Arc<Value>) {
		let cols = ixr.cols.len();
		let values = self.compound_indexes.entry(ixr.clone()).or_insert(vec![None; cols]);
		values[col] = Some(val.clone());
	}
}

#[derive(Debug, Clone)]
pub(super) struct IndexReference {
	indexes: Arc<[DefineIndexStatement]>,
	idx: usize,
}

impl IndexReference {
	pub(super) fn new(indexes: Arc<[DefineIndexStatement]>, idx: usize) -> Self {
		Self {
			indexes,
			idx,
		}
	}
}

impl Hash for IndexReference {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		state.write_usize(self.idx);
	}
}

impl PartialEq for IndexReference {
	fn eq(&self, other: &Self) -> bool {
		self.idx == other.idx
	}
}

impl Eq for IndexReference {}

impl Deref for IndexReference {
	type Target = DefineIndexStatement;

	fn deref(&self) -> &Self::Target {
		&self.indexes[self.idx]
	}
}

#[derive(Clone)]
struct SchemaCache {
	indexes: Arc<[DefineIndexStatement]>,
	fields: Arc<[DefineFieldStatement]>,
}

impl SchemaCache {
	async fn new(opt: &Options, table: &Table, tx: &Transaction) -> Result<Self, Error> {
		let indexes = tx.all_tb_indexes(opt.ns()?, opt.db()?, table).await?;
		let fields = tx.all_tb_fields(opt.ns()?, opt.db()?, table, None).await?;
		Ok(Self {
			indexes,
			fields,
		})
	}

	fn new_reference(&self, idx: usize) -> IndexReference {
		IndexReference::new(self.indexes.clone(), idx)
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
	IndexedField(Arc<Idiom>, LocalIndexRefs),
	RecordField(Arc<Idiom>, RecordOptions),
	NonIndexedField(Arc<Idiom>),
	Computable,
	Computed(Arc<Value>),
	Unsupported(String),
}

impl Node {
	pub(super) fn is_computed(&self) -> Option<Arc<Value>> {
		if let Self::Computed(v) = self {
			Some(v.clone())
		} else {
			None
		}
	}

	pub(super) fn is_indexed_field(
		&self,
	) -> Option<(&Arc<Idiom>, &LocalIndexRefs, Option<&RemoteIndexRefs>)> {
		match self {
			Self::IndexedField(id, irs) => Some((id, irs, None)),
			Self::RecordField(id, ro) => Some((id, &ro.locals, Some(&ro.remotes))),
			_ => None,
		}
	}

	pub(super) fn is_field(&self) -> Option<&Idiom> {
		match self {
			Self::IndexedField(id, _) => Some(id),
			Self::RecordField(id, _) => Some(id),
			Self::NonIndexedField(id) => Some(id),
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
	/// Eg. ORDER LIMIT
	None,
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
			IdiomPosition::None => op.clone(),
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
