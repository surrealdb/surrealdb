use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use tokio::sync::RwLock;

use crate::catalog::{
	DatabaseDefinition, DatabaseId, Distance, Index, IndexDefinition, NamespaceId,
};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::operator::{BooleanOperator, MatchesOperator};
use crate::expr::{Cond, Expr, FlowResultExt as _, Ident, Idiom};
use crate::idx::IndexKeyBase;
use crate::idx::docids::btdocids::BTreeDocIds;
use crate::idx::ft::MatchRef;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms, Scorer};
use crate::idx::ft::highlighter::HighlightParams;
use crate::idx::ft::search::scorer::BM25Scorer;
use crate::idx::ft::search::termdocs::SearchTermsDocs;
use crate::idx::ft::search::terms::SearchTerms;
use crate::idx::ft::search::{SearchIndex, TermIdList, TermIdSet};
use crate::idx::planner::checker::{HnswConditionChecker, MTreeConditionChecker};
use crate::idx::planner::iterators::{
	IndexEqualThingIterator, IndexJoinThingIterator, IndexRangeThingIterator,
	IndexUnionThingIterator, IteratorRecord, IteratorRef, KnnIterator, KnnIteratorResult,
	MatchesThingIterator, ThingIterator, UniqueEqualThingIterator, UniqueJoinThingIterator,
	UniqueRangeThingIterator, UniqueUnionThingIterator,
};
#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
use crate::idx::planner::iterators::{
	IndexRangeReverseThingIterator, UniqueRangeReverseThingIterator,
};
use crate::idx::planner::knn::{KnnBruteForceResult, KnnPriorityList};
use crate::idx::planner::plan::IndexOperator::Matches;
use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue, StoreRangeValue};
use crate::idx::planner::tree::{IdiomPosition, IndexReference};
use crate::idx::planner::{IterationStage, ScanDirection};
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::hnsw::SharedHnswIndex;
use crate::key::value::{StoreKeyArray, StoreKeyValue};
use crate::kvs::TransactionType;
use crate::val::{Number, Object, RecordId, Value};

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
	Search(SearchIndex),
	FullText(FullTextIndex),
	MTree(MTreeIndex),
	Hnsw(SharedHnswIndex),
}

/// Execution-time entry per expression. Associates a parsed expression with
/// the prepared execution structure (per index type) used to iterate results.
enum PerExpressionEntry {
	Search(SearchEntry),
	FullText(FullTextEntry),
	MTree(MtEntry),
	Hnsw(HnswEntry),
	KnnBruteForce(KnnBruteForceEntry),
}

/// Entry keyed by MatchRef for MATCHES queries, decoupling expression identity
/// from the underlying search/full-text index preparation.
enum PerMatchRefEntry {
	Search(SearchEntry),
	FullText(FullTextEntry),
}

pub(super) struct InnerQueryExecutor {
	table: String,
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
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		table: &Ident,
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
			let ixr = io.ix_ref();
			match &ixr.index {
				Index::Search(p) => {
					let search_entry: Option<SearchEntry> = match ir_map.entry(ixr.clone()) {
						Entry::Occupied(e) => {
							if let PerIndexReferenceIndex::Search(si) = e.get() {
								SearchEntry::new(stk, ctx, opt, si, io).await?
							} else {
								None
							}
						}
						Entry::Vacant(e) => {
							let ix: &IndexDefinition = e.key();
							let ikb = IndexKeyBase::new(
								db.namespace_id,
								db.database_id,
								&ix.what,
								&ix.name,
							);
							let si = SearchIndex::new(
								ctx,
								db.namespace_id,
								db.database_id,
								p.az.as_str(),
								ikb,
								p,
								TransactionType::Read,
							)
							.await?;
							let fte = SearchEntry::new(stk, ctx, opt, &si, io).await?;
							e.insert(PerIndexReferenceIndex::Search(si));
							fte
						}
					};
					if let Some(e) = search_entry {
						if let Matches(
							_,
							MatchesOperator {
								rf: Some(mr),
								..
							},
						) = e.0.index_option.op()
						{
							let mr_entry = PerMatchRefEntry::Search(e.clone());
							ensure!(
								mr_entries.insert(*mr, mr_entry).is_none(),
								Error::DuplicatedMatchRef {
									mr: *mr,
								}
							);
						}
						exp_entries.insert(exp, PerExpressionEntry::Search(e));
					}
				}
				Index::FullText(p) => {
					let fulltext_entry: Option<FullTextEntry> = match ir_map.entry(ixr.clone()) {
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
								db.namespace_id,
								db.database_id,
								&ix.what,
								&ix.name,
							);
							let ft = FullTextIndex::new(
								opt.id()?,
								ctx.get_index_stores(),
								&ctx.tx(),
								ikb,
								p,
							)
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
				Index::MTree(p) => {
					if let IndexOperator::Knn(a, k) = io.op() {
						let mte = match ir_map.entry(ixr.clone()) {
							Entry::Occupied(e) => {
								if let PerIndexReferenceIndex::MTree(mti) = e.get() {
									Some(
										MtEntry::new(
											db,
											stk,
											ctx,
											opt,
											mti,
											a,
											*k,
											knn_condition.clone(),
										)
										.await?,
									)
								} else {
									None
								}
							}
							Entry::Vacant(e) => {
								let ix: &IndexDefinition = e.key();
								let ikb = IndexKeyBase::new(
									db.namespace_id,
									db.database_id,
									&ix.what,
									&ix.name,
								);
								let tx = ctx.tx();
								let mti =
									MTreeIndex::new(&tx, ikb, p, TransactionType::Read).await?;
								drop(tx);
								let entry = MtEntry::new(
									db,
									stk,
									ctx,
									opt,
									&mti,
									a,
									*k,
									knn_condition.clone(),
								)
								.await?;
								e.insert(PerIndexReferenceIndex::MTree(mti));
								Some(entry)
							}
						};
						if let Some(mte) = mte {
							exp_entries.insert(exp, PerExpressionEntry::MTree(mte));
						}
					}
				}
				Index::Hnsw(p) => {
					if let IndexOperator::Ann(a, k, ef) = io.op() {
						let he = match ir_map.entry(ixr.clone()) {
							Entry::Occupied(e) => {
								if let PerIndexReferenceIndex::Hnsw(hi) = e.get() {
									Some(
										HnswEntry::new(
											db,
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
								let hi = ctx
									.get_index_stores()
									.get_index_hnsw(db.namespace_id, db.database_id, ctx, ixr, p)
									.await?;
								// Ensure the local HNSW index is up to date with the KVS
								hi.write().await.check_state(&ctx.tx()).await?;
								// Now we can execute the request
								let entry = HnswEntry::new(
									db,
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
			table: table.clone().into_string(),
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
		ctx: &Context,
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
				if let Ok(v) = v.coerce_to() {
					if let Ok(dist) = dist.compute(&v, val.as_ref()) {
						p.add(dist, thg).await;
						return Ok(Value::Bool(true));
					}
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

	pub(crate) fn is_table(&self, tb: &str) -> bool {
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
	) -> Result<Option<ThingIterator>> {
		if let Some(it_entry) = self.0.it_entries.get(ir) {
			match it_entry {
				IteratorEntry::Single(_, io) => self.new_single_iterator(ns, db, ir, io).await,
				IteratorEntry::Range(_, ixr, from, to, sc) => {
					Ok(self.new_range_iterator(ir, ns, db, ixr, from.into(), to.into(), *sc)?)
				}
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
	) -> Result<Option<ThingIterator>> {
		let ixr = io.ix_ref();
		match ixr.index {
			Index::Idx => Ok(self.new_index_iterator(ns, db, irf, ixr, io.clone()).await?),
			Index::Uniq => Ok(self.new_unique_index_iterator(ns, db, irf, ixr, io.clone()).await?),
			Index::Search {
				..
			} => self.new_search_index_iterator(irf, io.clone()).await,
			Index::FullText {
				..
			} => self.new_fulltext_index_iterator(irf, io.clone()).await,
			Index::MTree(_) => Ok(self.new_mtree_index_knn_iterator(irf)),
			Index::Hnsw(_) => Ok(self.new_hnsw_index_ann_iterator(irf)),
		}
	}

	/// Converts a value from an IndexOperator to a `fd`.
	/// Values from `IndexOperator::Equality` can be either single values or arrays.
	/// When it is an array id describe the composite values of one item in the compound index.
	/// When it is not an array, it is the first column of the compound index.
	fn equality_to_fd(value: &Value) -> StoreKeyArray {
		if let Value::Array(a) = value {
			let a: Vec<_> = a.iter().map(|v| StoreKeyValue::from(v.clone())).collect();
			StoreKeyArray(a)
		} else {
			StoreKeyArray::from(StoreKeyValue::from(value.clone()))
		}
	}

	/// Converts a value from an `IndexOperator::Union` to a vector of `fd`.
	/// Values fron IndexOperator can be either single values or arrays.
	/// When it is an array it is different possible values. Each of then needs to be converted to
	/// an fd. When it is not an array, it is a unique value.
	fn union_to_fds(value: &Value) -> Vec<StoreKeyArray> {
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
		ix: &IndexReference,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		Ok(match io.op() {
			IndexOperator::Equality(value) => {
				let fd = Self::equality_to_fd(value);
				Some(Self::new_index_equal_iterator(ir, ns, db, ix, &fd)?)
			}
			IndexOperator::Union(values) => {
				let fds = Self::union_to_fds(values);
				Some(ThingIterator::IndexUnion(IndexUnionThingIterator::new(ir, ns, db, ix, &fds)?))
			}
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(ns, db, ir, ios).await?;
				let index_join =
					Box::new(IndexJoinThingIterator::new(ir, ns, db, ix.clone(), iterators)?);
				Some(ThingIterator::IndexJoin(index_join))
			}
			IndexOperator::Order(reverse) => {
				if *reverse {
					#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
					{
						Some(ThingIterator::IndexRangeReverse(
							IndexRangeReverseThingIterator::full_range(ir, ns, db, ix)?,
						))
					}
					#[cfg(not(any(feature = "kv-rocksdb", feature = "kv-tikv")))]
					None
				} else {
					Some(ThingIterator::IndexRange(IndexRangeThingIterator::full_range(
						ir, ns, db, ix,
					)?))
				}
			}
			IndexOperator::Range(prefix, ranges) => Some(ThingIterator::IndexRange(
				IndexRangeThingIterator::compound_range(ir, ns, db, ix, prefix, ranges)?,
			)),
			_ => None,
		})
	}

	fn new_index_equal_iterator(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &StoreKeyArray,
	) -> Result<ThingIterator> {
		Ok(ThingIterator::IndexEqual(IndexEqualThingIterator::new(irf, ns, db, ix, fd)?))
	}

	#[expect(clippy::too_many_arguments)]
	fn new_range_iterator(
		&self,
		ir: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
		sc: ScanDirection,
	) -> Result<Option<ThingIterator>> {
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
		from: StoreRangeValue,
		to: StoreRangeValue,
		sc: ScanDirection,
	) -> Result<ThingIterator> {
		Ok(match sc {
			ScanDirection::Forward => {
				ThingIterator::IndexRange(IndexRangeThingIterator::new(ir, ns, db, ix, from, to)?)
			}
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			ScanDirection::Backward => ThingIterator::IndexRangeReverse(
				IndexRangeReverseThingIterator::new(ir, ns, db, ix, from, to)?,
			),
		})
	}

	fn new_unique_range_iterator(
		ir: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
		sc: ScanDirection,
	) -> Result<ThingIterator> {
		Ok(match sc {
			ScanDirection::Forward => {
				ThingIterator::UniqueRange(UniqueRangeThingIterator::new(ir, ns, db, ix, from, to)?)
			}
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			ScanDirection::Backward => ThingIterator::UniqueRangeReverse(
				UniqueRangeReverseThingIterator::new(ir, ns, db, ix, from, to)?,
			),
		})
	}

	async fn new_unique_index_iterator(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		irf: IteratorRef,
		ixr: &IndexReference,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		Ok(match io.op() {
			IndexOperator::Equality(value) => {
				let fd = Self::equality_to_fd(value);
				Some(Self::new_unique_equal_iterator(irf, ns, db, ixr, &fd)?)
			}
			IndexOperator::Union(values) => {
				let fds = Self::union_to_fds(values);
				Some(ThingIterator::UniqueUnion(UniqueUnionThingIterator::new(
					irf, ns, db, ixr, &fds,
				)?))
			}
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(ns, db, irf, ios).await?;
				let unique_join =
					Box::new(UniqueJoinThingIterator::new(irf, ns, db, ixr.clone(), iterators)?);
				Some(ThingIterator::UniqueJoin(unique_join))
			}
			IndexOperator::Order(reverse) => {
				if *reverse {
					#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
					{
						Some(ThingIterator::UniqueRangeReverse(
							UniqueRangeReverseThingIterator::full_range(irf, ns, db, ixr)?,
						))
					}
					#[cfg(not(any(feature = "kv-rocksdb", feature = "kv-tikv")))]
					None
				} else {
					Some(ThingIterator::UniqueRange(UniqueRangeThingIterator::full_range(
						irf, ns, db, ixr,
					)?))
				}
			}
			IndexOperator::Range(prefix, ranges) => Some(ThingIterator::UniqueRange(
				UniqueRangeThingIterator::compound_range(irf, ns, db, ixr, prefix, ranges)?,
			)),
			_ => None,
		})
	}

	fn new_unique_equal_iterator(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &StoreKeyArray,
	) -> Result<ThingIterator> {
		if ix.cols.len() > 1 {
			// If the index is unique and the index is a composite index,
			// then we have the opportunity to iterate on the first column of the index
			// and consider it as a standard index (rather than a unique one)
			Ok(ThingIterator::IndexEqual(IndexEqualThingIterator::new(irf, ns, db, ix, fd)?))
		} else {
			Ok(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(irf, ns, db, ix, fd)?))
		}
	}

	async fn new_search_index_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir) {
			if let Matches(_, _) = io.op() {
				if let Some(PerIndexReferenceIndex::Search(si)) = self.0.ir_map.get(io.ix_ref()) {
					if let Some(PerExpressionEntry::Search(se)) = self.0.exp_entries.get(exp) {
						let hits = si.new_hits_iterator(&se.0.terms_docs)?;
						let it = MatchesThingIterator::new(ir, hits);
						return Ok(Some(ThingIterator::SearchMatches(it)));
					}
				}
			}
		}
		Ok(None)
	}

	async fn new_fulltext_index_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir) {
			if let Matches(
				_,
				MatchesOperator {
					operator,
					..
				},
			) = io.op()
			{
				if let Some(PerIndexReferenceIndex::FullText(fti)) = self.0.ir_map.get(io.ix_ref())
				{
					if let Some(PerExpressionEntry::FullText(fte)) = self.0.exp_entries.get(exp) {
						let hits = fti.new_hits_iterator(&fte.0.qt, operator.clone());
						let it = MatchesThingIterator::new(ir, hits);
						return Ok(Some(ThingIterator::FullTextMatches(it)));
					}
				}
			}
		}
		Ok(None)
	}

	fn new_mtree_index_knn_iterator(&self, ir: IteratorRef) -> Option<ThingIterator> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir) {
			if let Some(PerExpressionEntry::MTree(mte)) = self.0.exp_entries.get(exp) {
				let it = KnnIterator::new(ir, mte.res.clone());
				return Some(ThingIterator::Knn(it));
			}
		}
		None
	}

	fn new_hnsw_index_ann_iterator(&self, ir: IteratorRef) -> Option<ThingIterator> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir) {
			if let Some(PerExpressionEntry::Hnsw(he)) = self.0.exp_entries.get(exp) {
				let it = KnnIterator::new(ir, he.res.clone());
				return Some(ThingIterator::Knn(it));
			}
		}
		None
	}

	async fn build_iterators(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		irf: IteratorRef,
		ios: &[IndexOption],
	) -> Result<VecDeque<ThingIterator>> {
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
		ctx: &Context,
		opt: &Options,
		thg: &RecordId,
		exp: &Expr,
		l: Value,
		r: Value,
	) -> Result<bool> {
		match self.0.exp_entries.get(exp) {
			Some(PerExpressionEntry::Search(se)) => {
				let ix = se.0.index_option.ix_ref();
				if self.0.table == ix.what.as_str() {
					return self.search_matches_with_doc_id(ctx, thg, se).await;
				}
				if let Some(PerIndexReferenceIndex::Search(si)) = self.0.ir_map.get(ix) {
					return self.search_matches_with_value(stk, ctx, opt, si, se, l, r).await;
				}
			}
			Some(PerExpressionEntry::FullText(fte)) => {
				let ix = fte.0.io.ix_ref();
				if let Some(PerIndexReferenceIndex::FullText(fti)) = self.0.ir_map.get(ix) {
					if self.0.table == ix.what.as_str() {
						return self.fulltext_matches_with_doc_id(ctx, thg, fti, fte).await;
					}
					return self.fulltext_matches_with_value(stk, ctx, opt, fti, fte, l, r).await;
				}
			}
			_ => {}
		}
		// If no previous case were successful, we end up with a user error
		Err(anyhow::Error::new(Error::NoIndexFoundForMatch {
			exp: exp.to_string(),
		}))
	}

	async fn search_matches_with_doc_id(
		&self,
		ctx: &Context,
		thg: &RecordId,
		se: &SearchEntry,
	) -> Result<bool> {
		// If there is no terms, it can't be a match
		if se.0.terms_docs.is_empty() {
			return Ok(false);
		}
		let doc_key = revision::to_vec(thg)?;
		let tx = ctx.tx();
		let di = se.0.doc_ids.read().await;
		let doc_id = di.get_doc_id(&tx, doc_key).await?;
		drop(di);
		if let Some(doc_id) = doc_id {
			for opt_td in se.0.terms_docs.iter() {
				if let Some((_, docs)) = opt_td {
					if !docs.contains(doc_id) {
						return Ok(false);
					}
				} else {
					// If one of the term is missing, it can't be a match
					return Ok(false);
				}
			}
			return Ok(true);
		}
		Ok(false)
	}

	async fn fulltext_matches_with_doc_id(
		&self,
		ctx: &Context,
		thg: &RecordId,
		fti: &FullTextIndex,
		fte: &FullTextEntry,
	) -> Result<bool> {
		if fte.0.qt.is_empty() {
			return Ok(false);
		}
		let tx = ctx.tx();
		if let Some(doc_id) = fti.get_doc_id(&tx, thg).await? {
			if fte.0.qt.contains_doc(doc_id) {
				return Ok(true);
			}
		}
		Ok(false)
	}

	#[expect(clippy::too_many_arguments)]
	async fn search_matches_with_value(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		si: &SearchIndex,
		se: &SearchEntry,
		l: Value,
		r: Value,
	) -> Result<bool> {
		// If the query terms contains terms that are unknown in the index
		// of if there are no terms in the query
		// we are sure that it does not match any document
		if !se.0.query_terms_set.is_matchable() {
			return Ok(false);
		}
		let v = match se.0.index_option.id_pos() {
			IdiomPosition::Left => r,
			IdiomPosition::Right => l,
			IdiomPosition::None => return Ok(false),
		};
		let terms = se.0.terms.read().await;
		// Extract the terms set from the record
		let t = si.extract_indexing_terms(stk, ctx, opt, v).await?;
		drop(terms);
		Ok(se.0.query_terms_set.is_subset(&t))
	}

	#[expect(clippy::too_many_arguments)]
	async fn fulltext_matches_with_value(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_fti: &FullTextIndex,
		_fte: &FullTextEntry,
		_l: Value,
		_r: Value,
	) -> Result<bool> {
		todo!()
	}

	fn get_match_ref_entry(&self, match_ref: &Value) -> Option<&PerMatchRefEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			return self.0.mr_entries.get(&mr);
		}
		None
	}

	fn get_search_index(&self, se: &SearchEntry) -> Option<&SearchIndex> {
		if let Some(PerIndexReferenceIndex::Search(si)) =
			self.0.ir_map.get(se.0.index_option.ix_ref())
		{
			Some(si)
		} else {
			None
		}
	}

	fn get_fulltext_index(&self, fe: &FullTextEntry) -> Option<&FullTextIndex> {
		if let Some(PerIndexReferenceIndex::FullText(si)) = self.0.ir_map.get(fe.0.io.ix_ref()) {
			Some(si)
		} else {
			None
		}
	}

	pub(crate) async fn highlight(
		&self,
		ctx: &Context,
		thg: &RecordId,
		hlp: HighlightParams,
		doc: &Value,
	) -> Result<Value> {
		match self.get_match_ref_entry(hlp.match_ref()) {
			Some(PerMatchRefEntry::Search(se)) => {
				if let Some(si) = self.get_search_index(se) {
					if let Some(id) = se.0.index_option.id_ref() {
						let tx = ctx.tx();
						let res =
							si.highlight(&tx, thg, &se.0.query_terms_list, hlp, id, doc).await;
						return res;
					}
				}
			}
			Some(PerMatchRefEntry::FullText(fte)) => {
				if let Some(fti) = self.get_fulltext_index(fte) {
					if let Some(id) = fte.0.io.id_ref() {
						let tx = ctx.tx();
						let res = fti.highlight(&tx, thg, &fte.0.qt, hlp, id, doc).await;
						return res;
					}
				}
			}
			_ => {}
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		ctx: &Context,
		thg: &RecordId,
		match_ref: Value,
		partial: bool,
	) -> Result<Value> {
		if let Some(mre) = self.get_match_ref_entry(&match_ref) {
			match mre {
				PerMatchRefEntry::Search(se) => {
					if let Some(si) = self.get_search_index(se) {
						let tx = ctx.tx();
						let res = si.read_offsets(&tx, thg, &se.0.query_terms_list, partial).await;
						return res;
					}
				}
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
		ctx: &Context,
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
				PerMatchRefEntry::Search(se) => {
					if let Some(scorer) = &se.0.scorer {
						let tx = ctx.tx();
						if doc_id.is_none() {
							let key = revision::to_vec(rid)?;
							let di = se.0.doc_ids.read().await;
							doc_id = di.get_doc_id(&tx, key).await?;
							drop(di);
						}
						if let Some(doc_id) = doc_id {
							let score = scorer.score(&tx, doc_id).await?;
							return Ok(Value::from(score));
						}
					}
				}
				PerMatchRefEntry::FullText(fte) => {
					if let Some(scorer) = &fte.0.scorer {
						if let Some(fti) = self.get_fulltext_index(fte) {
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
		}
		Ok(Value::None)
	}
}

#[derive(Clone)]
struct SearchEntry(Arc<InnerSearchEntry>);

struct InnerSearchEntry {
	index_option: IndexOption,
	doc_ids: Arc<RwLock<BTreeDocIds>>,
	terms: Arc<RwLock<SearchTerms>>,
	query_terms_set: TermIdSet,
	query_terms_list: TermIdList,
	terms_docs: Arc<SearchTermsDocs>,
	scorer: Option<BM25Scorer>,
}

impl SearchEntry {
	async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		si: &SearchIndex,
		io: IndexOption,
	) -> Result<Option<Self>> {
		if let Matches(
			qs,
			MatchesOperator {
				operator,
				..
			},
		) = io.op()
		{
			if !matches!(operator, BooleanOperator::And) {
				bail!(Error::Unimplemented(
					"SEARCH indexes only support AND operations".to_string()
				))
			}
			let (terms_list, terms_set, terms_docs) =
				si.extract_querying_terms(stk, ctx, opt, qs.to_owned()).await?;
			let terms_docs = Arc::new(terms_docs);
			Ok(Some(Self(Arc::new(InnerSearchEntry {
				index_option: io,
				doc_ids: si.doc_ids(),
				query_terms_set: terms_set,
				query_terms_list: terms_list,
				scorer: si.new_scorer(terms_docs.clone())?,
				terms: si.terms(),
				terms_docs,
			}))))
		} else {
			Ok(None)
		}
	}
}

#[derive(Clone)]
struct FullTextEntry(Arc<InnerFullTextEntry>);

struct InnerFullTextEntry {
	io: IndexOption,
	qt: QueryTerms,
	scorer: Option<Scorer>,
}

impl FullTextEntry {
	async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		fti: &FullTextIndex,
		io: IndexOption,
	) -> Result<Option<Self>> {
		if let Matches(qs, _) = io.op() {
			let qt = fti.extract_querying_terms(stk, ctx, opt, qs.to_owned()).await?;
			let scorer = fti.new_scorer(ctx).await?;
			Ok(Some(Self(Arc::new(InnerFullTextEntry {
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
pub(super) struct MtEntry {
	res: VecDeque<KnnIteratorResult>,
}

impl MtEntry {
	#[expect(clippy::too_many_arguments)]
	async fn new(
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		mt: &MTreeIndex,
		o: &[Number],
		k: u32,
		cond: Option<Arc<Cond>>,
	) -> Result<Self> {
		let cond_checker = if let Some(cond) = cond {
			MTreeConditionChecker::new_cond(ctx, opt, cond)
		} else {
			MTreeConditionChecker::new(ctx)
		};
		let res = mt.knn_search(db, stk, ctx, o, k as usize, cond_checker).await?;
		Ok(Self {
			res,
		})
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
		ctx: &Context,
		opt: &Options,
		h: SharedHnswIndex,
		v: &[Number],
		n: u32,
		ef: u32,
		cond: Option<Arc<Cond>>,
	) -> Result<Self> {
		let cond_checker = if let Some(cond) = cond {
			HnswConditionChecker::new_cond(ctx, opt, cond)
		} else {
			HnswConditionChecker::new()
		};
		let res = h
			.read()
			.await
			.knn_search(db, &ctx.tx(), stk, v, n as usize, ef as usize, cond_checker)
			.await?;
		Ok(Self {
			res,
		})
	}
}
