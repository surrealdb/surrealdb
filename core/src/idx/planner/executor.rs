use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::docids::DocIds;
use crate::idx::ft::analyzer::{Analyzer, TermsList, TermsSet};
use crate::idx::ft::highlighter::HighlightParams;
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::terms::Terms;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::checker::{HnswConditionChecker, MTreeConditionChecker};
use crate::idx::planner::iterators::{
	IndexEqualThingIterator, IndexJoinThingIterator, IndexRangeThingIterator,
	IndexUnionThingIterator, IteratorRecord, IteratorRef, KnnIterator, KnnIteratorResult,
	MatchesThingIterator, MultipleIterators, ThingIterator, UniqueEqualThingIterator,
	UniqueJoinThingIterator, UniqueRangeThingIterator, UniqueUnionThingIterator,
};
use crate::idx::planner::knn::{KnnBruteForceResult, KnnPriorityList};
use crate::idx::planner::plan::IndexOperator::Matches;
use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
use crate::idx::planner::tree::{IdiomPosition, IndexRef, IndexesMap};
use crate::idx::planner::IterationStage;
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::hnsw::SharedHnswIndex;
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, TransactionType};
use crate::sql::index::{Distance, Index};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Cond, Expression, Idiom, Number, Object, Table, Thing, Value};
use num_traits::{FromPrimitive, ToPrimitive};
use reblessive::tree::Stk;
use rust_decimal::Decimal;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

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

pub(super) type KnnBruteForceExpressions = HashMap<Arc<Expression>, KnnBruteForceExpression>;

pub(super) type KnnExpressions = HashSet<Arc<Expression>>;

#[derive(Clone)]
pub(crate) struct QueryExecutor(Arc<InnerQueryExecutor>);

pub(super) struct InnerQueryExecutor {
	table: String,
	ft_map: HashMap<IndexRef, FtIndex>,
	mr_entries: HashMap<MatchRef, FtEntry>,
	exp_entries: HashMap<Arc<Expression>, FtEntry>,
	it_entries: Vec<IteratorEntry>,
	index_definitions: Vec<DefineIndexStatement>,
	mt_entries: HashMap<Arc<Expression>, MtEntry>,
	hnsw_entries: HashMap<Arc<Expression>, HnswEntry>,
	knn_bruteforce_entries: HashMap<Arc<Expression>, KnnBruteForceEntry>,
}

impl From<InnerQueryExecutor> for QueryExecutor {
	fn from(value: InnerQueryExecutor) -> Self {
		Self(Arc::new(value))
	}
}

pub(super) enum IteratorEntry {
	Single(Option<Arc<Expression>>, IndexOption),
	Range(HashSet<Arc<Expression>>, IndexRef, RangeValue, RangeValue),
}

impl IteratorEntry {
	pub(super) fn explain(&self, ix_def: &[DefineIndexStatement]) -> Value {
		match self {
			Self::Single(_, io) => io.explain(ix_def),
			Self::Range(_, ir, from, to) => {
				let mut e = HashMap::default();
				if let Some(ix) = ix_def.get(*ir as usize) {
					e.insert("index", Value::from(ix.name.0.to_owned()));
				}
				e.insert("from", Value::from(from));
				e.insert("to", Value::from(to));
				Value::from(Object::from(e))
			}
		}
	}
}
impl InnerQueryExecutor {
	#[allow(clippy::too_many_arguments)]
	#[allow(clippy::mutable_key_type)]
	pub(super) async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		table: &Table,
		im: IndexesMap,
		knns: KnnExpressions,
		kbtes: KnnBruteForceExpressions,
		knn_condition: Option<Cond>,
	) -> Result<Self, Error> {
		let mut mr_entries = HashMap::default();
		let mut exp_entries = HashMap::default();
		let mut ft_map = HashMap::default();
		let mut mt_map: HashMap<IndexRef, MTreeIndex> = HashMap::default();
		let mut mt_entries = HashMap::default();
		let mut hnsw_map: HashMap<IndexRef, SharedHnswIndex> = HashMap::default();
		let mut hnsw_entries = HashMap::default();
		let mut knn_bruteforce_entries = HashMap::with_capacity(knns.len());
		let knn_condition = knn_condition.map(Arc::new);

		// Create all the instances of FtIndex
		// Build the FtEntries and map them to Idioms and MatchRef
		for (exp, io) in im.options {
			let ix_ref = io.ix_ref();
			if let Some(idx_def) = im.definitions.get(ix_ref as usize) {
				match &idx_def.index {
					Index::Search(p) => {
						let ft_entry = match ft_map.entry(ix_ref) {
							Entry::Occupied(e) => FtEntry::new(stk, ctx, opt, e.get(), io).await?,
							Entry::Vacant(e) => {
								let ikb = IndexKeyBase::new(opt.ns()?, opt.db()?, idx_def)?;
								let ft = FtIndex::new(
									ctx,
									opt,
									p.az.as_str(),
									ikb,
									p,
									TransactionType::Read,
								)
								.await?;
								let fte = FtEntry::new(stk, ctx, opt, &ft, io).await?;
								e.insert(ft);
								fte
							}
						};
						if let Some(e) = ft_entry {
							if let Matches(_, Some(mr)) = e.0.index_option.op() {
								if mr_entries.insert(*mr, e.clone()).is_some() {
									return Err(Error::DuplicatedMatchRef {
										mr: *mr,
									});
								}
							}
							exp_entries.insert(exp, e);
						}
					}
					Index::MTree(p) => {
						if let IndexOperator::Knn(a, k) = io.op() {
							let entry = match mt_map.entry(ix_ref) {
								Entry::Occupied(e) => {
									MtEntry::new(
										stk,
										ctx,
										opt,
										e.get(),
										a,
										*k,
										knn_condition.clone(),
									)
									.await?
								}
								Entry::Vacant(e) => {
									let ikb = IndexKeyBase::new(opt.ns()?, opt.db()?, idx_def)?;
									let tx = ctx.tx();
									let mt = MTreeIndex::new(
										ctx.get_index_stores(),
										&tx,
										ikb,
										p,
										TransactionType::Read,
									)
									.await?;
									drop(tx);
									let entry = MtEntry::new(
										stk,
										ctx,
										opt,
										&mt,
										a,
										*k,
										knn_condition.clone(),
									)
									.await?;
									e.insert(mt);
									entry
								}
							};
							mt_entries.insert(exp, entry);
						}
					}
					Index::Hnsw(p) => {
						if let IndexOperator::Ann(a, k, ef) = io.op() {
							let entry = match hnsw_map.entry(ix_ref) {
								Entry::Occupied(e) => {
									HnswEntry::new(
										stk,
										ctx,
										opt,
										e.get().clone(),
										a,
										*k,
										*ef,
										knn_condition.clone(),
									)
									.await?
								}
								Entry::Vacant(e) => {
									let hnsw = ctx
										.get_index_stores()
										.get_index_hnsw(ctx, opt, idx_def, p)
										.await?;
									// Ensure the local HNSW index is up to date with the KVS
									hnsw.write().await.check_state(&ctx.tx()).await?;
									// Now we can execute the request
									let entry = HnswEntry::new(
										stk,
										ctx,
										opt,
										hnsw.clone(),
										a,
										*k,
										*ef,
										knn_condition.clone(),
									)
									.await?;
									e.insert(hnsw);
									entry
								}
							};
							hnsw_entries.insert(exp, entry);
						}
					}
					_ => {}
				}
			}
		}

		for (exp, knn) in kbtes {
			knn_bruteforce_entries
				.insert(exp, (KnnPriorityList::new(knn.k as usize), knn.id, knn.obj, knn.d));
		}

		Ok(Self {
			table: table.0.clone(),
			ft_map,
			mr_entries,
			exp_entries,
			it_entries: Vec::new(),
			index_definitions: im.definitions,
			mt_entries,
			hnsw_entries,
			knn_bruteforce_entries,
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
		thg: &Thing,
		doc: Option<&CursorDoc>,
		exp: &Expression,
	) -> Result<Value, Error> {
		if let Some(IterationStage::Iterate(e)) = ctx.get_iteration_stage() {
			if let Some(results) = e {
				return Ok(results.contains(exp, thg).into());
			}
			Ok(Value::Bool(false))
		} else {
			if let Some((p, id, val, dist)) = self.0.knn_bruteforce_entries.get(exp) {
				let v: Vec<Number> = id.compute(stk, ctx, opt, doc).await?.try_into()?;
				let dist = dist.compute(&v, val.as_ref())?;
				p.add(dist, thg).await;
			}
			Ok(Value::Bool(true))
		}
	}

	pub(super) async fn build_bruteforce_knn_result(&self) -> KnnBruteForceResult {
		let mut result = KnnBruteForceResult::with_capacity(self.0.knn_bruteforce_entries.len());
		for (e, (p, _, _, _)) in &self.0.knn_bruteforce_entries {
			result.insert(e.clone(), p.build().await);
		}
		result
	}

	pub(crate) fn is_table(&self, tb: &str) -> bool {
		self.0.table.eq(tb)
	}

	pub(crate) fn has_bruteforce_knn(&self) -> bool {
		!self.0.knn_bruteforce_entries.is_empty()
	}

	/// Returns `true` if the expression is matching the current iterator.
	pub(crate) fn is_iterator_expression(&self, irf: IteratorRef, exp: &Expression) -> bool {
		match self.0.it_entries.get(irf as usize) {
			Some(IteratorEntry::Single(Some(e), ..)) => exp.eq(e.as_ref()),
			Some(IteratorEntry::Range(es, ..)) => es.contains(exp),
			_ => false,
		}
	}

	pub(crate) fn explain(&self, itr: IteratorRef) -> Value {
		match self.0.it_entries.get(itr as usize) {
			Some(ie) => ie.explain(self.0.index_definitions.as_slice()),
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
		opt: &Options,
		irf: IteratorRef,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(it_entry) = self.0.it_entries.get(irf as usize) {
			match it_entry {
				IteratorEntry::Single(_, io) => self.new_single_iterator(opt, irf, io).await,
				IteratorEntry::Range(_, ixr, from, to) => {
					Ok(self.new_range_iterator(opt, *ixr, from, to)?)
				}
			}
		} else {
			Ok(None)
		}
	}

	async fn new_single_iterator(
		&self,
		opt: &Options,
		irf: IteratorRef,
		io: &IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(ix) = self.get_index_def(io.ix_ref()) {
			match ix.index {
				Index::Idx => Ok(self.new_index_iterator(opt, irf, ix, io.clone()).await?),
				Index::Uniq => Ok(self.new_unique_index_iterator(opt, irf, ix, io.clone()).await?),
				Index::Search {
					..
				} => self.new_search_index_iterator(irf, io.clone()).await,
				Index::MTree(_) => Ok(self.new_mtree_index_knn_iterator(irf)),
				Index::Hnsw(_) => Ok(self.new_hnsw_index_ann_iterator(irf)),
			}
		} else {
			Ok(None)
		}
	}

	async fn new_index_iterator(
		&self,
		opt: &Options,
		irf: IteratorRef,
		ix: &DefineIndexStatement,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		Ok(match io.op() {
			IndexOperator::Equality(value) | IndexOperator::Exactness(value) => {
				if let Value::Number(n) = value.as_ref() {
					let values = Self::get_equal_number_variants(n);
					if values.len() == 1 {
						Some(Self::new_index_equal_iterator(irf, opt, ix, &values[0])?)
					} else {
						Some(Self::new_multiple_index_equal_iterators(irf, opt, ix, values)?)
					}
				} else {
					Some(Self::new_index_equal_iterator(irf, opt, ix, value)?)
				}
			}
			IndexOperator::Union(value) => Some(ThingIterator::IndexUnion(
				IndexUnionThingIterator::new(irf, opt.ns()?, opt.db()?, &ix.what, &ix.name, value),
			)),
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(opt, irf, ios).await?;
				let index_join = Box::new(IndexJoinThingIterator::new(irf, opt, ix, iterators)?);
				Some(ThingIterator::IndexJoin(index_join))
			}
			IndexOperator::Order(asc) => {
				if *asc {
					Some(ThingIterator::IndexRange(IndexRangeThingIterator::full_range(
						irf,
						opt.ns()?,
						opt.db()?,
						&ix.what,
						&ix.name,
					)))
				} else {
					None
				}
			}
			_ => None,
		})
	}

	fn new_index_equal_iterator(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		value: &Value,
	) -> Result<ThingIterator, Error> {
		Ok(ThingIterator::IndexEqual(IndexEqualThingIterator::new(
			irf,
			opt.ns()?,
			opt.db()?,
			&ix.what,
			&ix.name,
			value,
		)))
	}

	fn new_multiple_index_equal_iterators(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		values: Vec<Value>,
	) -> Result<ThingIterator, Error> {
		let mut iterators = VecDeque::with_capacity(values.len());
		for value in values {
			iterators.push_back(Self::new_index_equal_iterator(irf, opt, ix, &value)?);
		}
		Ok(ThingIterator::Multiples(Box::new(MultipleIterators::new(iterators))))
	}

	/// This function takes a reference to a `Number` enum and returns a vector of `Value` enum.
	/// The `Number` enum can be either an `Int`, `Float`, or `Decimal`.
	/// The function first initializes an empty vector with a capacity of 3 to store the converted values.
	/// It then matches on the input number and performs the appropriate conversions.
	/// For `Int`, it pushes the original `Int` value, the equivalent `Float` value, and if possible, the equivalent `Decimal` value.
	/// For `Float`, it pushes the original `Float` value, the truncated `Int` value if it is a whole number, and if possible, the equivalent `Decimal` value.
	/// For `Decimal`, it pushes the equivalent `Int` value if it is representable as an `i64`, and the equivalent `Float` value if it is representable as an `f64`.
	/// Finally, it returns the vector of converted values.
	fn get_number_variants<F>(
		n: &Number,
		float_to_int: F,
	) -> (Option<i64>, Option<f64>, Option<Decimal>)
	where
		F: Fn(&f64) -> Option<i64>,
	{
		let oi;
		let of;
		let od;
		match n {
			Number::Int(i) => {
				oi = Some(*i);
				of = Some(*i as f64);
				od = Decimal::from_i64(*i);
			}
			Number::Float(f) => {
				oi = float_to_int(f);
				of = Some(*f);
				od = Decimal::from_f64(*f);
			}
			Number::Decimal(d) => {
				oi = d.to_i64();
				of = d.to_f64();
				od = Some(*d);
			}
		};
		(oi, of, od)
	}
	fn get_equal_number_variants(n: &Number) -> Vec<Value> {
		let (oi, of, od) = Self::get_number_variants(n, |f| {
			if f.trunc().eq(f) {
				f.to_i64()
			} else {
				None
			}
		});
		let mut values = Vec::with_capacity(3);
		if let Some(i) = oi {
			values.push(Number::Int(i).into());
		}
		if let Some(f) = of {
			values.push(Number::Float(f).into());
		}
		if let Some(d) = od {
			values.push(Number::Decimal(d).into());
		}
		values
	}

	fn get_range_number_from_variants(n: &Number) -> (Option<i64>, Option<f64>, Option<Decimal>) {
		Self::get_number_variants(n, |f| f.floor().to_i64())
	}

	fn get_range_number_to_variants(n: &Number) -> (Option<i64>, Option<f64>, Option<Decimal>) {
		Self::get_number_variants(n, |f| f.ceil().to_i64())
	}

	fn get_range_number_variants(
		from: &Number,
		from_inc: bool,
		to: &Number,
		to_inc: bool,
	) -> Vec<(RangeValue, RangeValue)> {
		let (from_i, from_f, from_d) = Self::get_range_number_from_variants(from);
		let (to_i, to_f, to_d) = Self::get_range_number_to_variants(to);
		let mut vec = Vec::with_capacity(3);
		if let (Some(from), Some(to)) = (from_i, to_i) {
			vec.push((
				RangeValue {
					value: Number::Int(from).into(),
					inclusive: from_inc,
				},
				RangeValue {
					value: Number::Int(to).into(),
					inclusive: to_inc,
				},
			));
		}
		if let (Some(from), Some(to)) = (from_f, to_f) {
			vec.push((
				RangeValue {
					value: Number::Float(from).into(),
					inclusive: from_inc,
				},
				RangeValue {
					value: Number::Float(to).into(),
					inclusive: to_inc,
				},
			));
		}
		if let (Some(from), Some(to)) = (from_d, to_d) {
			vec.push((
				RangeValue {
					value: Number::Decimal(from).into(),
					inclusive: from_inc,
				},
				RangeValue {
					value: Number::Decimal(to).into(),
					inclusive: to_inc,
				},
			));
		}
		vec
	}

	fn new_range_iterator(
		&self,
		opt: &Options,
		ir: IndexRef,
		from: &RangeValue,
		to: &RangeValue,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(ix) = self.get_index_def(ir) {
			match ix.index {
				Index::Idx => {
					return if let (Value::Number(from_n), Value::Number(to_n)) =
						(&from.value, &to.value)
					{
						let ranges = Self::get_range_number_variants(
							from_n,
							from.inclusive,
							to_n,
							to.inclusive,
						);
						if ranges.len() == 1 {
							let range = &ranges[0];
							Ok(Some(Self::new_index_range_iterator(
								ir, opt, ix, &range.0, &range.1,
							)?))
						} else {
							Ok(Some(Self::new_multiple_index_range_iterator(ir, opt, ix, &ranges)?))
						}
					} else {
						Ok(Some(Self::new_index_range_iterator(ir, opt, ix, from, to)?))
					}
				}
				Index::Uniq => {
					return Ok(Some(ThingIterator::UniqueRange(UniqueRangeThingIterator::new(
						ir,
						opt.ns()?,
						opt.db()?,
						&ix.what,
						&ix.name,
						from,
						to,
					))))
				}
				_ => {}
			}
		}
		Ok(None)
	}

	fn new_index_range_iterator(
		ir: IndexRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		from: &RangeValue,
		to: &RangeValue,
	) -> Result<ThingIterator, Error> {
		Ok(ThingIterator::IndexRange(IndexRangeThingIterator::new(
			ir,
			opt.ns()?,
			opt.db()?,
			&ix.what,
			&ix.name,
			from,
			to,
		)))
	}

	fn new_multiple_index_range_iterator(
		ir: IndexRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		ranges: &[(RangeValue, RangeValue)],
	) -> Result<ThingIterator, Error> {
		let mut iterators = VecDeque::with_capacity(ranges.len());
		for range in ranges {
			iterators.push_back(Self::new_index_range_iterator(ir, opt, ix, &range.0, &range.1)?);
		}
		Ok(ThingIterator::Multiples(Box::new(MultipleIterators::new(iterators))))
	}

	async fn new_unique_index_iterator(
		&self,
		opt: &Options,
		irf: IteratorRef,
		ix: &DefineIndexStatement,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		Ok(match io.op() {
			IndexOperator::Equality(value) | IndexOperator::Exactness(value) => {
				if let Value::Number(n) = value.as_ref() {
					let values = Self::get_equal_number_variants(n);
					if values.len() == 1 {
						Some(Self::new_unique_equal_iterator(irf, opt, ix, &values[0])?)
					} else {
						Some(Self::new_multiple_unique_equal_iterators(irf, opt, ix, values)?)
					}
				} else {
					Some(Self::new_unique_equal_iterator(irf, opt, ix, value)?)
				}
			}
			IndexOperator::Union(value) => Some(ThingIterator::UniqueUnion(
				UniqueUnionThingIterator::new(irf, opt, ix, value)?,
			)),
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(opt, irf, ios).await?;
				let unique_join = Box::new(UniqueJoinThingIterator::new(irf, opt, ix, iterators)?);
				Some(ThingIterator::UniqueJoin(unique_join))
			}
			IndexOperator::Order(asc) => {
				if *asc {
					Some(ThingIterator::UniqueRange(UniqueRangeThingIterator::full_range(
						irf,
						opt.ns()?,
						opt.db()?,
						&ix.what,
						&ix.name,
					)))
				} else {
					None
				}
			}
			_ => None,
		})
	}

	fn new_unique_equal_iterator(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		value: &Value,
	) -> Result<ThingIterator, Error> {
		Ok(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(
			irf,
			opt.ns()?,
			opt.db()?,
			&ix.what,
			&ix.name,
			value,
		)))
	}

	fn new_multiple_unique_equal_iterators(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		values: Vec<Value>,
	) -> Result<ThingIterator, Error> {
		let mut iterators = VecDeque::with_capacity(values.len());
		for value in values {
			iterators.push_back(Self::new_unique_equal_iterator(irf, opt, ix, &value)?);
		}
		Ok(ThingIterator::Multiples(Box::new(MultipleIterators::new(iterators))))
	}

	async fn new_search_index_iterator(
		&self,
		irf: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(irf as usize) {
			if let Matches(_, _) = io.op() {
				if let Some(fti) = self.0.ft_map.get(&io.ix_ref()) {
					if let Some(fte) = self.0.exp_entries.get(exp) {
						let it =
							MatchesThingIterator::new(irf, fti, fte.0.terms_docs.clone()).await?;
						return Ok(Some(ThingIterator::Matches(it)));
					}
				}
			}
		}
		Ok(None)
	}

	fn new_mtree_index_knn_iterator(&self, irf: IteratorRef) -> Option<ThingIterator> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(irf as usize) {
			if let Some(mte) = self.0.mt_entries.get(exp) {
				let it = KnnIterator::new(irf, mte.res.clone());
				return Some(ThingIterator::Knn(it));
			}
		}
		None
	}

	fn new_hnsw_index_ann_iterator(&self, irf: IteratorRef) -> Option<ThingIterator> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(irf as usize) {
			if let Some(he) = self.0.hnsw_entries.get(exp) {
				let it = KnnIterator::new(irf, he.res.clone());
				return Some(ThingIterator::Knn(it));
			}
		}
		None
	}

	async fn build_iterators(
		&self,
		opt: &Options,
		irf: IteratorRef,
		ios: &[IndexOption],
	) -> Result<VecDeque<ThingIterator>, Error> {
		let mut iterators = VecDeque::with_capacity(ios.len());
		for io in ios {
			if let Some(it) = Box::pin(self.new_single_iterator(opt, irf, io)).await? {
				iterators.push_back(it);
			}
		}
		Ok(iterators)
	}

	fn get_index_def(&self, ir: IndexRef) -> Option<&DefineIndexStatement> {
		self.0.index_definitions.get(ir as usize)
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn matches(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		thg: &Thing,
		exp: &Expression,
		l: Value,
		r: Value,
	) -> Result<bool, Error> {
		if let Some(ft) = self.0.exp_entries.get(exp) {
			if let Some(ix_def) = self.get_index_def(ft.0.index_option.ix_ref()) {
				if self.0.table.eq(&ix_def.what.0) {
					return self.matches_with_doc_id(ctx, thg, ft).await;
				}
			}
			return self.matches_with_value(stk, ctx, opt, ft, l, r).await;
		}

		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	async fn matches_with_doc_id(
		&self,
		ctx: &Context,
		thg: &Thing,
		ft: &FtEntry,
	) -> Result<bool, Error> {
		let doc_key: Key = thg.into();
		let tx = ctx.tx();
		let di = ft.0.doc_ids.read().await;
		let doc_id = di.get_doc_id(&tx, doc_key).await?;
		drop(di);
		if let Some(doc_id) = doc_id {
			let term_goals = ft.0.terms_docs.len();
			// If there is no terms, it can't be a match
			if term_goals == 0 {
				return Ok(false);
			}
			for opt_td in ft.0.terms_docs.iter() {
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

	async fn matches_with_value(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		ft: &FtEntry,
		l: Value,
		r: Value,
	) -> Result<bool, Error> {
		// If the query terms contains terms that are unknown in the index
		// of if there are no terms in the query
		// we are sure that it does not match any document
		if !ft.0.query_terms_set.is_matchable() {
			return Ok(false);
		}
		let v = match ft.0.index_option.id_pos() {
			IdiomPosition::Left => r,
			IdiomPosition::Right => l,
			IdiomPosition::None => return Ok(false),
		};
		let terms = ft.0.terms.read().await;
		// Extract the terms set from the record
		let t = ft.0.analyzer.extract_indexing_terms(stk, ctx, opt, &terms, v).await?;
		drop(terms);
		Ok(ft.0.query_terms_set.is_subset(&t))
	}

	fn get_ft_entry(&self, match_ref: &Value) -> Option<&FtEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			self.0.mr_entries.get(&mr)
		} else {
			None
		}
	}

	fn get_ft_entry_and_index(&self, match_ref: &Value) -> Option<(&FtEntry, &FtIndex)> {
		if let Some(e) = self.get_ft_entry(match_ref) {
			if let Some(ft) = self.0.ft_map.get(&e.0.index_option.ix_ref()) {
				return Some((e, ft));
			}
		}
		None
	}

	pub(crate) async fn highlight(
		&self,
		ctx: &Context,
		thg: &Thing,
		hlp: HighlightParams,
		doc: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_ft_entry_and_index(hlp.match_ref()) {
			let tx = ctx.tx();
			let res = ft
				.highlight(&tx, thg, &e.0.query_terms_list, hlp, e.0.index_option.id_ref(), doc)
				.await;
			return res;
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		ctx: &Context,
		thg: &Thing,
		match_ref: Value,
		partial: bool,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_ft_entry_and_index(&match_ref) {
			let tx = ctx.tx();
			let res = ft.extract_offsets(&tx, thg, &e.0.query_terms_list, partial).await;
			return res;
		}
		Ok(Value::None)
	}

	pub(crate) async fn score(
		&self,
		ctx: &Context,
		match_ref: &Value,
		rid: &Thing,
		ir: Option<&Arc<IteratorRecord>>,
	) -> Result<Value, Error> {
		if let Some(e) = self.get_ft_entry(match_ref) {
			if let Some(scorer) = &e.0.scorer {
				let tx = ctx.tx();
				let mut doc_id = if let Some(ir) = ir {
					ir.doc_id()
				} else {
					None
				};
				if doc_id.is_none() {
					let key: Key = rid.into();
					let di = e.0.doc_ids.read().await;
					doc_id = di.get_doc_id(&tx, key).await?;
					drop(di);
				}
				if let Some(doc_id) = doc_id {
					let score = scorer.score(&tx, doc_id).await?;
					if let Some(score) = score {
						return Ok(Value::from(score));
					}
				}
			}
		}
		Ok(Value::None)
	}
}

#[derive(Clone)]
struct FtEntry(Arc<Inner>);

struct Inner {
	index_option: IndexOption,
	doc_ids: Arc<RwLock<DocIds>>,
	analyzer: Analyzer,
	query_terms_set: TermsSet,
	query_terms_list: TermsList,
	terms: Arc<RwLock<Terms>>,
	terms_docs: TermsDocs,
	scorer: Option<BM25Scorer>,
}

impl FtEntry {
	async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		ft: &FtIndex,
		io: IndexOption,
	) -> Result<Option<Self>, Error> {
		if let Matches(qs, _) = io.op() {
			let (terms_list, terms_set) =
				ft.extract_querying_terms(stk, ctx, opt, qs.to_owned()).await?;
			let tx = ctx.tx();
			let terms_docs = Arc::new(ft.get_terms_docs(&tx, &terms_list).await?);
			drop(tx);
			Ok(Some(Self(Arc::new(Inner {
				index_option: io,
				doc_ids: ft.doc_ids(),
				analyzer: ft.analyzer(),
				query_terms_set: terms_set,
				query_terms_list: terms_list,
				scorer: ft.new_scorer(terms_docs.clone())?,
				terms: ft.terms(),
				terms_docs,
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
	async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		mt: &MTreeIndex,
		o: &[Number],
		k: u32,
		cond: Option<Arc<Cond>>,
	) -> Result<Self, Error> {
		let cond_checker = if let Some(cond) = cond {
			MTreeConditionChecker::new_cond(ctx, opt, cond)
		} else {
			MTreeConditionChecker::new(ctx)
		};
		let res = mt.knn_search(stk, ctx, o, k as usize, cond_checker).await?;
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
	#[allow(clippy::too_many_arguments)]
	async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		h: SharedHnswIndex,
		v: &[Number],
		n: u32,
		ef: u32,
		cond: Option<Arc<Cond>>,
	) -> Result<Self, Error> {
		let cond_checker = if let Some(cond) = cond {
			HnswConditionChecker::new_cond(ctx, opt, cond)
		} else {
			HnswConditionChecker::new()
		};
		let res = h
			.read()
			.await
			.knn_search(&ctx.tx(), stk, v, n as usize, ef as usize, cond_checker)
			.await?;
		Ok(Self {
			res,
		})
	}
}
