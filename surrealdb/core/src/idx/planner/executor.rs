use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::{Result, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseDefinition, DatabaseId, Distance, Index, IndexDefinition, NamespaceId,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, NsDbTbCtx};
use crate::err::Error;
use crate::expr::operator::{BooleanOperator, MatchesOperator};
use crate::expr::{Cond, Expr, FlowResultExt as _, Idiom};
use crate::idx::IndexKeyBase;
use crate::idx::ft::MatchRef;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms, Scorer};
use crate::idx::ft::highlighter::HighlightParams;
use crate::idx::planner::iterators::{
	IndexCountThingIterator, IndexEqualThingIterator, IndexJoinThingIterator,
	IndexRangeReverseThingIterator, IndexRangeThingIterator, IndexUnionThingIterator,
	IteratorRecord, IteratorRef, KnnIterator, KnnIteratorResult, MatchesThingIterator,
	RecordIterator, UniqueEqualThingIterator, UniqueJoinThingIterator,
	UniqueRangeReverseThingIterator, UniqueRangeThingIterator, UniqueUnionThingIterator,
};
use crate::idx::planner::knn::{KnnBruteForceResult, KnnPriorityList};
use crate::idx::planner::plan::IndexOperator::Matches;
use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
use crate::idx::planner::tree::{IdiomPosition, IndexReference};
use crate::idx::planner::{IterationStage, ScanDirection};
use crate::idx::trees::store::hnsw::SharedHnswIndex;
use crate::val::{Array, Number, Object, RecordId, TableName, Value};

pub(super) type KnnBruteForceEntry = (KnnPriorityList, Idiom, Arc<Vec<Number>>, Distance);

pub(super) struct KnnBruteForceExpression {
	k: u32,
	id: Idiom,
	obj: Arc<Vec<Number>>,
	d: Distance,
}

impl KnnBruteForceExpression {
	pub(super) fn new(k: u32, id: Idiom, obj: Arc<Vec<Number>>, d: Distance) -> Self {
		Self {
			k,
			id,
			obj,
			d,
		}
	}
}

pub(super) type KnnBruteForceExpressions = HashMap<Arc<Expr>, KnnBruteForceExpression>;

pub(super) type KnnExpressions = HashSet<Arc<Expr>>;

#[derive(Clone)]
pub(crate) struct QueryExecutor(Arc<InnerQueryExecutor>);

/// Concrete index handle stored per IndexReference.
/// This maps an abstract IndexReference to the actual index implementation
/// that will be used at execution time.
enum PerIndexReferenceIndex {
	FullText(FullTextIndex),
	Hnsw(SharedHnswIndex),
}

/// Execution-time entry per expression. Associates a parsed expression with
/// the prepared execution structure (per index type) used to iterate results.
enum PerExpressionEntry {
	FullText(FullTextEntry),
	Hnsw(HnswEntry),
	KnnBruteForce(KnnBruteForceEntry),
}

/// Entry keyed by MatchRef for MATCHES queries, decoupling expression identity
/// from the underlying search/full-text index preparation.
enum PerMatchRefEntry {
	FullText(FullTextEntry),
}

pub(super) struct InnerQueryExecutor {
	table: TableName,
	ir_map: HashMap<IndexReference, PerIndexReferenceIndex>,
	mr_entries: HashMap<MatchRef, PerMatchRefEntry>,
	exp_entries: HashMap<Arc<Expr>, PerExpressionEntry>,
	it_entries: Vec<IteratorEntry>,
	knn_bruteforce_len: usize, // Count of brute-force KNN expressions aggregated for later merging
}

impl From<InnerQueryExecutor> for QueryExecutor {
	fn from(value: InnerQueryExecutor) -> Self {
		Self(Arc::new(value))
	}
}

pub(super) enum IteratorEntry {
	Single(Option<Arc<Expr>>, IndexOption),
	Range(HashSet<Arc<Expr>>, IndexReference, RangeValue, RangeValue, ScanDirection),
}

impl IteratorEntry {
	pub(super) fn explain(&self) -> Value {
		match self {
			Self::Single(_, io) => io.explain(),
			Self::Range(_, ir, from, to, sc) => {
				let mut e = HashMap::default();
				e.insert("index", Value::from(ir.name.clone()));
				e.insert("from", Value::from(from));
				e.insert("to", Value::from(to));
				e.insert("direction", Value::from(sc.to_string()));
				Value::from(Object::from(e))
			}
		}
	}
}

impl InnerQueryExecutor {
	#[expect(clippy::mutable_key_type)]
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn new(
		doc_ctx: &NsDbTbCtx,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		table: TableName,
		ios: Vec<(Arc<Expr>, IndexOption)>,
		kbtes: KnnBruteForceExpressions,
		knn_condition: Option<Cond>,
	) -> Result<Self> {
		let mut mr_entries = HashMap::default();
		let mut exp_entries = HashMap::default();
		let mut ir_map = HashMap::default();
		let knn_condition = knn_condition.map(Arc::new);

		// Create all the instances of index entries.
		// Map them to Idioms and MatchRef
		for (exp, io) in ios {
			let index_reference = io.index_reference();
			match &index_reference.index {
				Index::FullText(p) => {
					let fulltext_entry: Option<FullTextEntry> = match ir_map
						.entry(index_reference.clone())
					{
						Entry::Occupied(e) => {
							if let PerIndexReferenceIndex::FullText(fti) = e.get() {
								FullTextEntry::new(stk, ctx, opt, fti, io).await?
							} else {
								None
							}
						}
						Entry::Vacant(e) => {
							let ix: &IndexDefinition = e.key();
							let ikb = IndexKeyBase::new(
								doc_ctx.ns.namespace_id,
								doc_ctx.db.database_id,
								ix.table_name.clone(),
								ix.index_id,
							);
							let ft = FullTextIndex::new(ctx.get_index_stores(), &ctx.tx(), ikb, p)
								.await?;
							let fte = FullTextEntry::new(stk, ctx, opt, &ft, io).await?;
							e.insert(PerIndexReferenceIndex::FullText(ft));
							fte
						}
					};
					if let Some(e) = fulltext_entry {
						if let Matches(
							_,
							MatchesOperator {
								rf: Some(mr),
								..
							},
						) = e.0.io.op()
						{
							let mr_entry = PerMatchRefEntry::FullText(e.clone());
							ensure!(
								mr_entries.insert(*mr, mr_entry).is_none(),
								Error::DuplicatedMatchRef {
									mr: *mr,
								}
							);
						}
						exp_entries.insert(exp, PerExpressionEntry::FullText(e));
					}
				}
				Index::Hnsw(p) => {
					if let IndexOperator::Ann(a, k, ef) = io.op() {
						let he = match ir_map.entry(index_reference.clone()) {
							Entry::Occupied(e) => {
								if let PerIndexReferenceIndex::Hnsw(hi) = e.get() {
									Some(
										HnswEntry::new(
											&doc_ctx.db,
											stk,
											ctx,
											opt,
											hi.clone(),
											a,
											*k,
											*ef,
											knn_condition.clone(),
										)
										.await?,
									)
								} else {
									None
								}
							}
							Entry::Vacant(e) => {
								let tb = ctx
									.tx()
									.expect_tb(
										doc_ctx.ns.namespace_id,
										doc_ctx.db.database_id,
										&index_reference.table_name,
									)
									.await?;
								let hi = ctx
									.get_index_stores()
									.get_index_hnsw(
										doc_ctx.ns.namespace_id,
										doc_ctx.db.database_id,
										ctx,
										tb.table_id,
										index_reference,
										p,
									)
									.await?;
								// Ensure the local HNSW index is up to date with the KVS
								hi.check_state(ctx).await?;
								// Now we can execute the request
								let entry = HnswEntry::new(
									&doc_ctx.db,
									stk,
									ctx,
									opt,
									hi.clone(),
									a,
									*k,
									*ef,
									knn_condition.clone(),
								)
								.await?;
								e.insert(PerIndexReferenceIndex::Hnsw(hi));
								Some(entry)
							}
						};
						if let Some(he) = he {
							exp_entries.insert(exp, PerExpressionEntry::Hnsw(he));
						}
					}
				}
				_ => {}
			}
		}

		let knn_bruteforce_len = kbtes.len();
		for (exp, knn) in kbtes {
			let ke = (KnnPriorityList::new(knn.k as usize), knn.id, knn.obj, knn.d);
			exp_entries.insert(exp, PerExpressionEntry::KnnBruteForce(ke));
		}

		Ok(Self {
			table,
			ir_map,
			mr_entries,
			exp_entries,
			it_entries: Vec::new(),
			knn_bruteforce_len,
		})
	}

	pub(super) fn add_iterator(&mut self, it_entry: IteratorEntry) -> IteratorRef {
		let ir = self.it_entries.len();
		self.it_entries.push(it_entry);
		ir as IteratorRef
	}
}

impl QueryExecutor {
	pub(crate) async fn knn(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		thg: &RecordId,
		doc: Option<&CursorDoc>,
		exp: &Expr,
	) -> Result<Value> {
		if let Some(IterationStage::Iterate(e)) = ctx.get_iteration_stage() {
			if let Some(results) = e {
				return Ok(results.contains(exp, thg).into());
			}
			Ok(Value::Bool(false))
		} else {
			if let Some(PerExpressionEntry::KnnBruteForce((p, id, val, dist))) =
				self.0.exp_entries.get(exp)
			{
				let v = id.compute(stk, ctx, opt, doc).await.catch_return()?;
				if let Ok(v) = v.coerce_to()
					&& let Ok(dist) = dist.compute(&v, val.as_ref())
				{
					p.add(dist, thg).await;
					return Ok(Value::Bool(true));
				}
			}
			Ok(Value::Bool(false))
		}
	}

	pub(super) async fn build_bruteforce_knn_result(&self) -> KnnBruteForceResult {
		let mut result = KnnBruteForceResult::with_capacity(self.0.knn_bruteforce_len);
		for (exp, entry) in self.0.exp_entries.iter() {
			if let PerExpressionEntry::KnnBruteForce((p, _, _, _)) = entry {
				result.insert(exp.clone(), p.build().await);
			}
		}
		result
	}

	pub(crate) fn is_table(&self, tb: &TableName) -> bool {
		self.0.table.eq(tb)
	}

	pub(crate) fn has_bruteforce_knn(&self) -> bool {
		self.0.knn_bruteforce_len != 0
	}

	/// Returns `true` if the expression is matching the current iterator.
	pub(crate) fn is_iterator_expression(&self, ir: IteratorRef, exp: &Expr) -> bool {
		match self.0.it_entries.get(ir) {
			Some(IteratorEntry::Single(Some(e), ..)) => exp.eq(e.as_ref()),
			Some(IteratorEntry::Range(es, ..)) => es.contains(exp),
			_ => false,
		}
	}

	pub(crate) fn explain(&self, ir: IteratorRef) -> Value {
		match self.0.it_entries.get(ir) {
			Some(ie) => ie.explain(),
			None => Value::None,
		}
	}

	fn get_match_ref(match_ref: &Value) -> Option<MatchRef> {
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			Some(m)
		} else {
			None
		}
	}

	pub(crate) async fn new_iterator(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ir: IteratorRef,
	) -> Result<Option<RecordIterator>> {
		if let Some(it_entry) = self.0.it_entries.get(ir) {
			match it_entry {
				IteratorEntry::Single(_, io) => self.new_single_iterator(ns, db, ir, io).await,
				IteratorEntry::Range(_, index_reference, from, to, sc) => Ok(self
					.new_range_iterator(
						ir,
						ns,
						db,
						index_reference,
						from.clone(),
						to.clone(),
						*sc,
					)?),
			}
		} else {
			Ok(None)
		}
	}

	async fn new_single_iterator(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		irf: IteratorRef,
		io: &IndexOption,
	) -> Result<Option<RecordIterator>> {
		match io.index_reference().index {
			Index::Idx | Index::Count(_) => {
				Ok(self.new_index_iterator(ns, db, irf, io.clone()).await?)
			}
			Index::Uniq => Ok(self.new_unique_index_iterator(ns, db, irf, io.clone()).await?),
			Index::FullText {
				..
			} => self.new_fulltext_index_iterator(irf, io.clone()).await,
			Index::Hnsw(_) => Ok(self.new_hnsw_index_ann_iterator(irf)),
		}
	}

	/// Converts a value from an IndexOperator to a `fd`.
	/// Values from `IndexOperator::Equality` can be either single values or arrays.
	/// When it is an array id describe the composite values of one item in the compound index.
	/// When it is not an array, it is the first column of the compound index.
	fn equality_to_fd(value: &Value) -> Array {
		if let Value::Array(a) = value {
			a.clone()
		} else {
			Array::from(vec![value.clone()])
		}
	}

	/// Converts a value from an `IndexOperator::Union` to a vector of `fd`.
	/// Values fron IndexOperator can be either single values or arrays.
	/// When it is an array it is different possible values. Each of then needs to be converted to
	/// an fd. When it is not an array, it is a unique value.
	fn union_to_fds(value: &Value) -> Vec<Array> {
		if let Value::Array(a) = value {
			a.iter().map(Self::equality_to_fd).collect()
		} else {
			vec![Self::equality_to_fd(value)]
		}
	}

	async fn new_index_iterator(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<RecordIterator>> {
		let ix = io.index_reference();
		Ok(match io.op() {
			IndexOperator::Equality(value) => {
				let fd = Self::equality_to_fd(value);
				Some(Self::new_index_equal_iterator(ir, ns, db, ix, &fd)?)
			}
			IndexOperator::Union(values) => {
				let fds = Self::union_to_fds(values);
				Some(RecordIterator::IndexUnion(IndexUnionThingIterator::new(
					ir, ns, db, ix, &fds,
				)?))
			}
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(ns, db, ir, ios).await?;
				let index_join =
					Box::new(IndexJoinThingIterator::new(ir, ns, db, ix.clone(), iterators)?);
				Some(RecordIterator::IndexJoin(index_join))
			}
			IndexOperator::Order(reverse) => {
				if *reverse {
					Some(RecordIterator::IndexRangeReverse(
						IndexRangeReverseThingIterator::full_range(ir, ns, db, ix)?,
					))
				} else {
					Some(RecordIterator::IndexRange(IndexRangeThingIterator::full_range(
						ir, ns, db, ix,
					)?))
				}
			}
			IndexOperator::Range(prefix, ranges) => Some(RecordIterator::IndexRange(
				IndexRangeThingIterator::compound_range(ir, ns, db, ix, prefix, ranges)?,
			)),
			IndexOperator::Count => Some(RecordIterator::IndexCount(IndexCountThingIterator::new(
				ns,
				db,
				&ix.table_name,
				ix.index_id,
			)?)),
			_ => None,
		})
	}

	fn new_index_equal_iterator(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &Array,
	) -> Result<RecordIterator> {
		Ok(RecordIterator::IndexEqual(IndexEqualThingIterator::new(irf, ns, db, ix, fd)?))
	}

	#[expect(clippy::too_many_arguments)]
	fn new_range_iterator(
		&self,
		ir: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: RangeValue,
		to: RangeValue,
		sc: ScanDirection,
	) -> Result<Option<RecordIterator>> {
		match ix.index {
			Index::Idx => {
				return Ok(Some(Self::new_index_range_iterator(ir, ns, db, ix, from, to, sc)?));
			}
			Index::Uniq => {
				return Ok(Some(Self::new_unique_range_iterator(ir, ns, db, ix, from, to, sc)?));
			}
			_ => {}
		}
		Ok(None)
	}

	fn new_index_range_iterator(
		ir: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: RangeValue,
		to: RangeValue,
		sc: ScanDirection,
	) -> Result<RecordIterator> {
		Ok(match sc {
			ScanDirection::Forward => {
				RecordIterator::IndexRange(IndexRangeThingIterator::new(ir, ns, db, ix, from, to)?)
			}
			ScanDirection::Backward => RecordIterator::IndexRangeReverse(
				IndexRangeReverseThingIterator::new(ir, ns, db, ix, from, to)?,
			),
		})
	}

	fn new_unique_range_iterator(
		ir: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: RangeValue,
		to: RangeValue,
		sc: ScanDirection,
	) -> Result<RecordIterator> {
		Ok(match sc {
			ScanDirection::Forward => RecordIterator::UniqueRange(UniqueRangeThingIterator::new(
				ir, ns, db, ix, from, to,
			)?),
			ScanDirection::Backward => RecordIterator::UniqueRangeReverse(
				UniqueRangeReverseThingIterator::new(ir, ns, db, ix, from, to)?,
			),
		})
	}

	async fn new_unique_index_iterator(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		irf: IteratorRef,
		io: IndexOption,
	) -> Result<Option<RecordIterator>> {
		Ok(match io.op() {
			IndexOperator::Equality(value) => {
				let fd = Self::equality_to_fd(value);
				Some(Self::new_unique_equal_iterator(irf, ns, db, io.index_reference(), &fd)?)
			}
			IndexOperator::Union(values) => {
				let fds = Self::union_to_fds(values);
				Some(RecordIterator::UniqueUnion(UniqueUnionThingIterator::new(
					irf,
					ns,
					db,
					io.index_reference(),
					&fds,
				)?))
			}
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(ns, db, irf, ios).await?;
				let unique_join = Box::new(UniqueJoinThingIterator::new(
					irf,
					ns,
					db,
					io.index_reference().clone(),
					iterators,
				)?);
				Some(RecordIterator::UniqueJoin(unique_join))
			}
			IndexOperator::Order(reverse) => {
				if *reverse {
					Some(RecordIterator::UniqueRangeReverse(
						UniqueRangeReverseThingIterator::full_range(
							irf,
							ns,
							db,
							io.index_reference(),
						)?,
					))
				} else {
					Some(RecordIterator::UniqueRange(UniqueRangeThingIterator::full_range(
						irf,
						ns,
						db,
						io.index_reference(),
					)?))
				}
			}
			IndexOperator::Range(prefix, ranges) => {
				Some(RecordIterator::UniqueRange(UniqueRangeThingIterator::compound_range(
					irf,
					ns,
					db,
					io.index_reference(),
					prefix,
					ranges,
				)?))
			}
			_ => None,
		})
	}

	fn new_unique_equal_iterator(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &Array,
	) -> Result<RecordIterator> {
		if ix.cols.len() > 1 {
			// If the index is unique and the index is a composite index,
			// then we have the opportunity to iterate on the first column of the index
			// and consider it as a standard index (rather than a unique one)
			Ok(RecordIterator::IndexEqual(IndexEqualThingIterator::new(irf, ns, db, ix, fd)?))
		} else {
			Ok(RecordIterator::UniqueEqual(UniqueEqualThingIterator::new(irf, ns, db, ix, fd)?))
		}
	}

	async fn new_fulltext_index_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<RecordIterator>> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir)
			&& let Matches(
				_,
				MatchesOperator {
					operator,
					..
				},
			) = io.op()
			&& let Some(PerIndexReferenceIndex::FullText(fti)) =
				self.0.ir_map.get(io.index_reference())
			&& let Some(PerExpressionEntry::FullText(fte)) = self.0.exp_entries.get(exp)
		{
			let hits = fti.new_hits_iterator(&fte.0.qt, *operator);
			let it = MatchesThingIterator::new(ir, hits);
			return Ok(Some(RecordIterator::FullTextMatches(it)));
		}
		Ok(None)
	}

	fn new_hnsw_index_ann_iterator(&self, ir: IteratorRef) -> Option<RecordIterator> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir)
			&& let Some(PerExpressionEntry::Hnsw(he)) = self.0.exp_entries.get(exp)
		{
			let it = KnnIterator::new(ir, he.res.clone());
			return Some(RecordIterator::Knn(it));
		}
		None
	}

	async fn build_iterators(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		irf: IteratorRef,
		ios: &[IndexOption],
	) -> Result<VecDeque<RecordIterator>> {
		let mut iterators = VecDeque::with_capacity(ios.len());
		for io in ios {
			if let Some(it) = Box::pin(self.new_single_iterator(ns, db, irf, io)).await? {
				iterators.push_back(it);
			}
		}
		Ok(iterators)
	}

	#[expect(clippy::too_many_arguments)]
	pub(crate) async fn matches(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		thg: &RecordId,
		exp: &Expr,
		l: Value,
		r: Value,
	) -> Result<bool> {
		if let Some(PerExpressionEntry::FullText(fte)) = self.0.exp_entries.get(exp) {
			let ix = fte.0.io.index_reference();
			if let Some(PerIndexReferenceIndex::FullText(fti)) = self.0.ir_map.get(ix) {
				if self.0.table == ix.table_name.as_str() {
					return self.fulltext_matches_with_doc_id(ctx, thg, fti, fte).await;
				}
				return self.fulltext_matches_with_value(stk, ctx, opt, fti, fte, l, r).await;
			}
		}

		// If no previous case were successful, we end up with a user error
		Err(anyhow::Error::new(Error::NoIndexFoundForMatch {
			exp: exp.to_sql(),
		}))
	}

	async fn fulltext_matches_with_doc_id(
		&self,
		ctx: &FrozenContext,
		thg: &RecordId,
		fti: &FullTextIndex,
		fte: &FullTextEntry,
	) -> Result<bool> {
		if fte.0.qt.is_empty() {
			return Ok(false);
		}
		let tx = ctx.tx();
		if let Some(doc_id) = fti.get_doc_id(&tx, thg).await?
			&& fte.0.qt.contains_doc(doc_id)
		{
			return Ok(true);
		}
		Ok(false)
	}

	#[expect(clippy::too_many_arguments)]
	async fn fulltext_matches_with_value(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		fti: &FullTextIndex,
		fte: &FullTextEntry,
		l: Value,
		r: Value,
	) -> Result<bool> {
		// If the query terms contains terms that are unknown in the index
		// of if there are no terms in the query
		// we are sure that it does not match any document
		if fte.0.qt.is_empty() {
			return Ok(false);
		}
		let v = match fte.0.io.idiom_position() {
			IdiomPosition::Left => r,
			IdiomPosition::Right => l,
			IdiomPosition::None => return Ok(false),
		};
		// Check if the value matches the query terms
		fti.matches_value(stk, ctx, opt, &fte.0.qt, fte.0.bo, v).await
	}

	fn get_match_ref_entry(&self, match_ref: &Value) -> Option<&PerMatchRefEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			return self.0.mr_entries.get(&mr);
		}
		None
	}

	fn get_fulltext_index(&self, fe: &FullTextEntry) -> Option<&FullTextIndex> {
		if let Some(PerIndexReferenceIndex::FullText(si)) =
			self.0.ir_map.get(fe.0.io.index_reference())
		{
			Some(si)
		} else {
			None
		}
	}

	pub(crate) async fn highlight(
		&self,
		ctx: &FrozenContext,
		thg: &RecordId,
		hlp: HighlightParams,
		doc: &Value,
	) -> Result<Value> {
		if let Some(PerMatchRefEntry::FullText(fte)) = self.get_match_ref_entry(hlp.match_ref())
			&& let Some(fti) = self.get_fulltext_index(fte)
			&& let Some(id) = fte.0.io.idiom_ref()
		{
			let tx = ctx.tx();
			let res = fti.highlight(&tx, thg, &fte.0.qt, hlp, id, doc).await;
			return res;
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		ctx: &FrozenContext,
		thg: &RecordId,
		match_ref: Value,
		partial: bool,
	) -> Result<Value> {
		if let Some(mre) = self.get_match_ref_entry(&match_ref) {
			match mre {
				PerMatchRefEntry::FullText(fte) => {
					if let Some(fti) = self.get_fulltext_index(fte) {
						let tx = ctx.tx();
						let res = fti.read_offsets(&tx, thg, &fte.0.qt, partial).await;
						return res;
					}
				}
			}
		}
		Ok(Value::None)
	}

	pub(crate) async fn score(
		&self,
		ctx: &FrozenContext,
		match_ref: &Value,
		rid: &RecordId,
		ir: Option<&Arc<IteratorRecord>>,
	) -> Result<Value> {
		if let Some(mre) = self.get_match_ref_entry(match_ref) {
			let mut doc_id = if let Some(ir) = ir {
				ir.doc_id()
			} else {
				None
			};
			match mre {
				PerMatchRefEntry::FullText(fte) => {
					if let Some(scorer) = &fte.0.scorer
						&& let Some(fti) = self.get_fulltext_index(fte)
					{
						let tx = ctx.tx();
						if doc_id.is_none() {
							doc_id = fti.get_doc_id(&tx, rid).await?;
						}
						if let Some(doc_id) = doc_id {
							let score = scorer.score(fti, &tx, &fte.0.qt, doc_id).await?;
							return Ok(Value::from(score));
						}
					}
				}
			}
		}
		Ok(Value::None)
	}
}

#[derive(Clone)]
struct FullTextEntry(Arc<InnerFullTextEntry>);

struct InnerFullTextEntry {
	io: IndexOption,
	qt: QueryTerms,
	bo: BooleanOperator,
	scorer: Option<Scorer>,
}

impl FullTextEntry {
	async fn new(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		fti: &FullTextIndex,
		io: IndexOption,
	) -> Result<Option<Self>> {
		if let Matches(qs, mo) = io.op() {
			let qt = fti.extract_querying_terms(stk, ctx, opt, qs.to_owned()).await?;
			let scorer = fti.new_scorer(ctx).await?;
			Ok(Some(Self(Arc::new(InnerFullTextEntry {
				bo: mo.operator,
				io,
				qt,
				scorer,
			}))))
		} else {
			Ok(None)
		}
	}
}

#[derive(Clone)]
pub(super) struct HnswEntry {
	res: VecDeque<KnnIteratorResult>,
}

impl HnswEntry {
	#[expect(clippy::too_many_arguments)]
	async fn new(
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		h: SharedHnswIndex,
		v: &[Number],
		n: u32,
		ef: u32,
		cond: Option<Arc<Cond>>,
	) -> Result<Self> {
		let cond_filter = cond.map(|cond| (opt, cond));
		let res = h.knn_search(db, ctx, stk, v, n as usize, ef as usize, cond_filter).await?;
		Ok(Self {
			res,
		})
	}
}
