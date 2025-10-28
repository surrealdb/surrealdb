use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, DatabaseId, Index, IndexDefinition, IndexId, NamespaceId};
use crate::expr::operator::NearestNeighbor;
use crate::expr::order::{OrderList, Ordering};
use crate::expr::visit::MutVisitor;
use crate::expr::{
	BinaryOperator, Cond, Expr, FlowResultExt as _, Idiom, Kind, Literal, Order, Part, With,
};
use crate::idx::planner::StatementContext;
use crate::idx::planner::executor::{
	KnnBruteForceExpression, KnnBruteForceExpressions, KnnExpressions,
};
use crate::idx::planner::plan::{IndexOperator, IndexOption};
use crate::idx::planner::rewriter::KnnConditionRewriter;
use crate::kvs::Transaction;
use crate::val::{Array, Number, Value};

pub(super) struct Tree {
	pub(super) root: Option<Node>,
	pub(super) index_map: IndexesMap,
	pub(super) with_indexes: WithIndexes,
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
		table: &'a str,
	) -> Result<Self> {
		let mut b = TreeBuilder::new(stm_ctx, table);
		if let Some(cond) = stm_ctx.cond {
			b.eval_cond(stk, cond).await?;
		}
		b.eval_order().await?;
		b.eval_count(table).await?;
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
	table: &'a str,
	first_order: Option<&'a Order>,
	schemas: HashMap<String, SchemaCache>,
	idioms_indexes: HashMap<String, HashMap<Arc<Idiom>, LocalIndexRefs>>,
	resolved_expressions: HashMap<Arc<Expr>, ResolvedExpression>,
	resolved_idioms: HashMap<Arc<Idiom>, Node>,
	index_map: IndexesMap,
	with_indexes: WithIndexes,
	knn_brute_force_expressions: HashMap<Arc<Expr>, KnnBruteForceExpression>,
	knn_expressions: KnnExpressions,
	idioms_record_options: HashMap<Arc<Idiom>, RecordOptions>,
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
	fn new(ctx: &'a StatementContext<'a>, table: &'a str) -> Self {
		let with_indexes = WithIndexes::with_capacity(ctx.with);
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
			//group_sequence: 0,
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
		table: &str,
	) -> Result<SchemaCache> {
		if let Some(sc) = self.schemas.get(table).cloned() {
			return Ok(sc);
		}
		let (ns, db) = self.ctx.ctx.expect_ns_db_ids(self.ctx.opt).await?;
		let sc = SchemaCache::new(ns, db, table, tx).await?;
		self.schemas.insert(table.to_owned(), sc.clone());
		Ok(sc)
	}

	async fn eval_order(&mut self) -> Result<()> {
		if let Some(o) = self.first_order {
			if let Node::IndexedField(id, irf) = self.resolve_idiom(&o.value).await? {
				for (index_reference, id_col) in &irf {
					if *id_col == 0 {
						self.index_map.order_limit = Some(IndexOption::new(
							index_reference.clone(),
							Some(id),
							IdiomPosition::None,
							IndexOperator::Order(!o.direction),
						));
						break;
					}
				}
			}
		}
		Ok(())
	}

	async fn eval_cond(&mut self, stk: &mut Stk, cond: &Cond) -> Result<()> {
		self.root = Some(self.eval_value(stk, 0, &cond.0).await?);
		self.knn_condition = if self.knn_expressions.is_empty() {
			None
		} else {
			let mut cond = cond.0.clone();
			let _ = KnnConditionRewriter(&self.knn_expressions).visit_mut_expr(&mut cond);
			Some(Cond(cond))
		};
		Ok(())
	}

	async fn eval_count(&mut self, table: &str) -> Result<()> {
		if let Some(f) = self.ctx.fields {
			if f.is_count_all_only() {
				if let Some(g) = self.ctx.group {
					if g.is_group_all_only() {
						let tx = self.ctx.ctx.tx();
						let schema = self.lazy_load_schema_resolver(&tx, table).await?;
						for (pos, ix) in schema.indexes.iter().enumerate() {
							if let Index::Count(cond) = &ix.index {
								if self.ctx.cond.eq(&cond.as_ref()) {
									let index_reference = schema.new_reference(pos);
									if self.check_allowed_by_with_indexes(&index_reference) {
										self.index_map.index_count = Some(IndexOption::new(
											index_reference,
											None,
											IdiomPosition::None,
											IndexOperator::Count,
										));
										break;
									}
								}
							}
						}
					}
				}
			}
		}
		Ok(())
	}

	async fn eval_value(&mut self, stk: &mut Stk, group: GroupRef, v: &Expr) -> Result<Node> {
		match v {
			Expr::Binary {
				left,
				op,
				right,
			} => {
				// Did we already compute the same expression?
				if let Some(re) = self.resolved_expressions.get(v).cloned() {
					return Ok(re.into());
				}
				self.check_boolean_operator(group, op);
				let left_node = stk.run(|stk| self.eval_value(stk, group, left)).await?;
				let right_node = stk.run(|stk| self.eval_value(stk, group, right)).await?;
				// If both values are computable, then we can delegate the computation to the parent
				if left_node == Node::Computable && right_node == Node::Computable {
					return Ok(Node::Computable);
				}
				let exp = Arc::new(v.clone());
				let left = Arc::new(self.compute(stk, left, left_node).await?);
				let right = Arc::new(self.compute(stk, right, right_node).await?);
				let io = if let Some((id, local_irs, remote_irs)) = left.is_indexed_field() {
					self.lookup_index_options(
						op,
						id,
						&right,
						&exp,
						IdiomPosition::Left,
						local_irs,
						remote_irs,
					)?
				} else if let Some((id, local_irs, remote_irs)) = right.is_indexed_field() {
					self.lookup_index_options(
						op,
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
			Expr::Idiom(i) => self.eval_idiom(stk, group, i).await,
			Expr::Literal(
				Literal::Integer(_)
				| Literal::Bool(_)
				| Literal::String(_)
				| Literal::RecordId(_)
				| Literal::Duration(_)
				| Literal::Uuid(_)
				| Literal::Datetime(_)
				| Literal::None
				| Literal::Null
				| Literal::Decimal(_)
				| Literal::Float(_),
			)
			| Expr::Param(_)
			| Expr::FunctionCall(_) => {
				self.leaf_nodes_count += 1;
				Ok(Node::Computable)
			}
			Expr::Literal(Literal::Array(a)) => self.eval_array(stk, a).await,
			_ => Ok(Node::Unsupported(format!("Unsupported expression: {}", v))),
		}
	}

	async fn compute(&self, stk: &mut Stk, v: &Expr, n: Node) -> Result<Node> {
		Ok(if n == Node::Computable {
			match stk.run(|stk| v.compute(stk, self.ctx.ctx, self.ctx.opt, None)).await {
				Ok(v) => Node::Computed(v.into()),
				Err(_) => Node::Unsupported(format!("Unsupported expression: {}", v)),
			}
		} else {
			n
		})
	}

	async fn eval_array(&mut self, stk: &mut Stk, a: &[Expr]) -> Result<Node> {
		self.leaf_nodes_count += 1;
		let mut values = Vec::with_capacity(a.len());
		for v in a {
			values.push(
				stk.run(|stk| v.compute(stk, self.ctx.ctx, self.ctx.opt, None))
					.await
					.catch_return()?,
			);
		}
		Ok(Node::Computed(Arc::new(Value::Array(Array(values)))))
	}

	async fn eval_idiom(&mut self, stk: &mut Stk, gr: GroupRef, i: &Idiom) -> Result<Node> {
		self.leaf_nodes_count += 1;
		// Check if the idiom has already been resolved
		if let Some(node) = self.resolved_idioms.get(i).cloned() {
			return Ok(node);
		};

		// Compute the idiom value if it is a param
		if let Some(Part::Start(x)) = i.0.first() {
			if matches!(x, Expr::Param(_)) {
				let v = stk
					.run(|stk| i.compute(stk, self.ctx.ctx, self.ctx.opt, None))
					.await
					.catch_return()?;
				let v = v.into_literal();
				return stk.run(|stk| self.eval_value(stk, gr, &v)).await;
			}
		}

		let n = self.resolve_idiom(i).await?;
		Ok(n)
	}

	async fn resolve_idiom(&mut self, i: &Idiom) -> Result<Node> {
		let tx = self.ctx.ctx.tx();
		let schema = self.lazy_load_schema_resolver(&tx, self.table).await?;
		let i = Arc::new(i.clone());
		// Try to detect if it matches an index
		let n = {
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
		};
		self.resolved_idioms.insert(i.clone(), n.clone());
		Ok(n)
	}

	fn resolve_indexes(&mut self, t: &str, i: &Idiom, schema: &SchemaCache) -> LocalIndexRefs {
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
				// Check if the WITH clause allows the index to be used
				if self.check_allowed_by_with_indexes(&ixr) {
					irs.push((ixr, idiom_index));
				}
			}
		}
		let i = Arc::new(i.clone());
		if let Some(e) = self.idioms_indexes.get_mut(t) {
			e.insert(i, irs.clone());
		} else {
			self.idioms_indexes.insert(t.to_owned(), HashMap::from([(i, irs.clone())]));
		}
		irs
	}

	/// Check if the index is allowed by the WITH clause
	fn check_allowed_by_with_indexes(&mut self, ixr: &IndexReference) -> bool {
		// Is it already allowed?
		if self.with_indexes.allowed_index(ixr.index_id) {
			return true;
		}
		// If not, let's check the list
		if let Some(With::Index(ixs)) = &self.ctx.with {
			if ixs.iter().any(|x| x == ixr.name.as_str()) {
				// It is explicitly mentioned in the WITH clause
				self.with_indexes.push(ixr.index_id);
				return true;
			}
			// Not found in the WITH clause, so disallow the index
			false
		} else {
			// There is no WITH clause, so we allow all indexes
			true
		}
	}

	async fn resolve_record_field(
		&mut self,
		tx: &Transaction,
		fields: &[catalog::FieldDefinition],
		idiom: &Arc<Idiom>,
	) -> Result<Option<RecordOptions>> {
		for field in fields.iter() {
			if let Some(Kind::Record(tables)) = &field.field_kind {
				if idiom.starts_with(&field.name.0) {
					let (local_field, remote_field) = idiom.0.split_at(field.name.0.len());
					if remote_field.is_empty() {
						return Ok(None);
					}
					let local_field = Idiom(local_field.to_vec());
					let schema = self.lazy_load_schema_resolver(tx, self.table).await?;
					let locals = self.resolve_indexes(self.table, &local_field, &schema);
					let remote_field = Arc::new(Idiom(remote_field.to_vec()));
					let mut remotes = vec![];
					for table in tables {
						let schema = self.lazy_load_schema_resolver(tx, table).await?;
						let remote_irs = self.resolve_indexes(table, &remote_field, &schema);
						remotes.push((remote_field.clone(), remote_irs));
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

	fn check_boolean_operator(&mut self, gr: GroupRef, op: &BinaryOperator) {
		match op {
			BinaryOperator::Or => {
				if self.all_and != Some(false) {
					self.all_and = Some(false);
				}
				self.all_and_groups.entry(gr).and_modify(|b| *b = false).or_insert(false);
			}
			BinaryOperator::And => {
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
			if self.with_indexes.allowed_index(io.index_reference().index_id) {
				self.leaf_nodes_with_index_count += 2;
			}
		}
	}

	#[expect(clippy::too_many_arguments)]
	fn lookup_index_options(
		&mut self,
		o: &BinaryOperator,
		id: &Arc<Idiom>,
		node: &Node,
		exp: &Arc<Expr>,
		p: IdiomPosition,
		local_irs: &LocalIndexRefs,
		remote_irs: Option<&RemoteIndexRefs>,
	) -> Result<Option<IndexOption>> {
		if let Some(remote_irs) = remote_irs {
			let mut remote_ios = Vec::with_capacity(remote_irs.len());
			for (id, irs) in remote_irs.iter() {
				if let Some(io) = self.lookup_index_option(irs, o, id, node, exp, p)? {
					remote_ios.push(io);
				} else {
					return Ok(None);
				}
			}
			if let Some((index_reference, _)) = self.lookup_join_index_ref(local_irs) {
				let io = IndexOption::new(
					index_reference,
					Some(id.clone()),
					p,
					IndexOperator::Join(remote_ios),
				);
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
		op: &BinaryOperator,
		id: &Arc<Idiom>,
		n: &Node,
		e: &Arc<Expr>,
		p: IdiomPosition,
	) -> Result<Option<IndexOption>> {
		let mut res = None;
		for (index_reference, col) in irs.iter() {
			let op = match &index_reference.index {
				Index::Idx => self.eval_index_operator(index_reference, op, n, p, *col),
				Index::Uniq => self.eval_index_operator(index_reference, op, n, p, *col),
				Index::FullText {
					..
				} if *col == 0 => Self::eval_matches_operator(op, n),
				Index::MTree(_) if *col == 0 => self.eval_mtree_knn(e, op, n)?,
				Index::Hnsw(_) if *col == 0 => self.eval_hnsw_knn(e, op, n)?,
				_ => None,
			};
			if res.is_none() {
				if let Some(op) = op {
					let io = IndexOption::new(index_reference.clone(), Some(id.clone()), p, op);
					self.index_map.options.push((e.clone(), io.clone()));
					res = Some(io);
				}
			}
		}
		Ok(res)
	}

	fn lookup_join_index_ref(&self, irs: &LocalIndexRefs) -> Option<(IndexReference, IdiomCol)> {
		for (index_reference, id_col) in irs.iter().filter(|(_, id_col)| 0.eq(id_col)) {
			match index_reference.index {
				Index::Idx | Index::Uniq => return Some((index_reference.clone(), *id_col)),
				_ => {}
			};
		}
		None
	}

	fn eval_matches_operator(op: &BinaryOperator, n: &Node) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			if let BinaryOperator::Matches(mr) = op {
				return Some(IndexOperator::Matches(v.to_raw_string(), mr.clone()));
			}
		}
		None
	}

	fn eval_mtree_knn(
		&mut self,
		exp: &Arc<Expr>,
		op: &BinaryOperator,
		n: &Node,
	) -> Result<Option<IndexOperator>> {
		let BinaryOperator::NearestNeighbor(nn) = op else {
			return Ok(None);
		};
		let NearestNeighbor::KTree(k) = &**nn else {
			return Ok(None);
		};

		if let Node::Computed(v) = n {
			let vec: Arc<Vec<Number>> = Arc::new(v.as_ref().clone().coerce_to()?);
			self.knn_expressions.insert(exp.clone());
			return Ok(Some(IndexOperator::Knn(vec, *k)));
		}
		Ok(None)
	}

	fn eval_hnsw_knn(
		&mut self,
		exp: &Arc<Expr>,
		op: &BinaryOperator,
		n: &Node,
	) -> Result<Option<IndexOperator>> {
		let BinaryOperator::NearestNeighbor(nn) = op else {
			return Ok(None);
		};
		let NearestNeighbor::Approximate(k, ef) = &**nn else {
			return Ok(None);
		};

		if let Node::Computed(v) = n {
			let vec: Arc<Vec<Number>> = Arc::new(v.as_ref().clone().coerce_to()?);
			self.knn_expressions.insert(exp.clone());
			return Ok(Some(IndexOperator::Ann(vec, *k, *ef)));
		}

		Ok(None)
	}

	fn eval_bruteforce_knn(&mut self, id: &Idiom, val: &Node, exp: &Arc<Expr>) -> Result<()> {
		let Expr::Binary {
			op,
			..
		} = &**exp
		else {
			return Ok(());
		};

		let BinaryOperator::NearestNeighbor(nn) = op else {
			return Ok(());
		};
		let NearestNeighbor::K(k, d) = &**nn else {
			return Ok(());
		};

		if let Node::Computed(v) = val {
			let vec: Arc<Vec<Number>> = Arc::new(v.as_ref().clone().coerce_to()?);
			self.knn_expressions.insert(exp.clone());
			self.knn_brute_force_expressions
				.insert(exp.clone(), KnnBruteForceExpression::new(*k, id.clone(), vec, d.clone()));
		}
		Ok(())
	}

	fn eval_index_operator(
		&mut self,
		ixr: &IndexReference,
		op: &BinaryOperator,
		n: &Node,
		p: IdiomPosition,
		col: IdiomCol,
	) -> Option<IndexOperator> {
		if let Some(v) = n.is_computed() {
			match (op, v, p) {
				(BinaryOperator::Equal | BinaryOperator::ExactEqual, v, _) => {
					let iop = IndexOperator::Equality(v);
					self.index_map.check_compound(ixr, col, &iop);
					if col == 0 {
						return Some(iop);
					}
				}
				(BinaryOperator::Contain, v, IdiomPosition::Left) => {
					if col == 0 && ixr.cols[0].contains(&Part::All) {
						return Some(IndexOperator::Equality(v));
					}
				}
				(BinaryOperator::Inside, v, IdiomPosition::Right) => {
					if col == 0 && ixr.cols[0].contains(&Part::All) {
						return Some(IndexOperator::Equality(v));
					}
				}
				(BinaryOperator::Inside, v, IdiomPosition::Left) => {
					if let Value::Array(a) = v.as_ref() {
						self.index_map.check_compound_array(ixr, col, a);
						if col == 0 {
							return Some(IndexOperator::Union(v));
						}
					}
				}
				(
					BinaryOperator::ContainAny | BinaryOperator::ContainAll,
					v,
					IdiomPosition::Left,
				)
				| (
					BinaryOperator::AnyInside | BinaryOperator::AllInside,
					v,
					IdiomPosition::Right,
				) => {
					if v.is_array() && col == 0 {
						return Some(IndexOperator::Union(v));
					}
				}
				(
					BinaryOperator::LessThan
					| BinaryOperator::LessThanEqual
					| BinaryOperator::MoreThan
					| BinaryOperator::MoreThanEqual,
					v,
					p,
				) => {
					let iop = IndexOperator::RangePart(p.transform(op), v);
					self.index_map.check_compound(ixr, col, &iop);
					if col == 0 {
						return Some(iop);
					}
				}
				_ => {}
			}
		}
		None
	}
}

/// Store the list of indexes that can be used for a given expression
/// Use a Vector rather than a Set because small vectors are faster than HashSet.
/// We don't expect to have more than a few indexes here
pub(super) struct WithIndexes(Option<Vec<IndexId>>);

impl WithIndexes {
	fn with_capacity(with: Option<&With>) -> Self {
		let with_indexes = match with {
			Some(With::Index(ixs)) => Some(Vec::with_capacity(ixs.len())),
			_ => None,
		};
		Self(with_indexes)
	}

	fn push(&mut self, index_id: IndexId) {
		if let Some(wi) = &mut self.0 {
			wi.push(index_id);
		} else {
			self.0 = Some(vec![index_id]);
		}
	}

	/// Check if an index is allowed to be used
	pub(super) fn allowed_index(&self, index_id: IndexId) -> bool {
		if let Some(wi) = &self.0 {
			if !wi.contains(&index_id) {
				return false;
			}
		}
		true
	}
}

pub(super) type CompoundIndexes = HashMap<IndexReference, Vec<Vec<IndexOperator>>>;

#[derive(Default)]
pub(super) struct IndexesMap {
	/// For each expression a possible index option
	pub(super) options: Vec<(Arc<Expr>, IndexOption)>,
	/// For each index, tells if the columns are requested
	pub(super) compound_indexes: CompoundIndexes,
	/// Is there an index candidate that matches order/limit?
	pub(super) order_limit: Option<IndexOption>,
	/// Is there an index candidate for index count?
	pub(super) index_count: Option<IndexOption>,
}

impl IndexesMap {
	pub(crate) fn check_compound(&mut self, ixr: &IndexReference, col: usize, iop: &IndexOperator) {
		let cols = ixr.cols.len();
		let values = self.compound_indexes.entry(ixr.clone()).or_insert(vec![vec![]; cols]);
		if let Some(a) = values.get_mut(col) {
			a.push(iop.clone());
		}
	}

	pub(crate) fn check_compound_array(&mut self, ixr: &IndexReference, col: usize, a: &Array) {
		for v in a.iter() {
			let iop = IndexOperator::Equality(Arc::new(v.clone()));
			self.check_compound(ixr, col, &iop)
		}
	}
}

#[derive(Debug, Clone)]
pub(super) struct IndexReference {
	indexes: Arc<[IndexDefinition]>,
	idx: usize,
}

impl IndexReference {
	pub(super) fn new(indexes: Arc<[IndexDefinition]>, idx: usize) -> Self {
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
	type Target = IndexDefinition;

	fn deref(&self) -> &Self::Target {
		&self.indexes[self.idx]
	}
}

#[derive(Clone)]
struct SchemaCache {
	indexes: Arc<[IndexDefinition]>,
	fields: Arc<[catalog::FieldDefinition]>,
}

impl SchemaCache {
	async fn new(ns: NamespaceId, db: DatabaseId, table: &str, tx: &Transaction) -> Result<Self> {
		let indexes = tx.all_tb_indexes(ns, db, table).await?;
		let fields = tx.all_tb_fields(ns, db, table, None).await?;
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

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Node {
	Expression {
		group: GroupRef,
		io: Option<IndexOption>,
		left: Arc<Node>,
		right: Arc<Node>,
		exp: Arc<Expr>,
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
	fn transform(&self, op: &BinaryOperator) -> BinaryOperator {
		match self {
			IdiomPosition::Left => op.clone(),
			IdiomPosition::Right => match op {
				BinaryOperator::LessThan => BinaryOperator::MoreThan,
				BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
				BinaryOperator::MoreThan => BinaryOperator::LessThan,
				BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
				_ => op.clone(),
			},
			IdiomPosition::None => op.clone(),
		}
	}
}

#[derive(Clone)]
struct ResolvedExpression {
	group: GroupRef,
	exp: Arc<Expr>,
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
