use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::index::{Distance, Index};
use crate::expr::statements::DefineIndexStatement;
use crate::expr::{
	Array, BooleanOperation, Cond, Expression, FlowResultExt as _, Idiom, Number, Object, Table,
	Thing, Value,
};
use crate::idx::IndexKeyBase;
use crate::idx::docids::btdocids::BTreeDocIds;
use crate::idx::ft::MatchRef;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms, Scorer};
use crate::idx::ft::highlighter::HighlightParams;
use crate::idx::ft::search::scorer::BM25Scorer;
use crate::idx::ft::search::termdocs::SearchTermsDocs;
use crate::idx::ft::search::terms::SearchTerms;
use crate::idx::ft::search::{SearchIndex, TermIdList, TermIdSet};
use crate::idx::planner::IterationStage;
use crate::idx::planner::checker::{HnswConditionChecker, MTreeConditionChecker};
use crate::idx::planner::iterators::{
	IndexEqualThingIterator, IndexJoinThingIterator, IndexRangeThingIterator,
	IndexUnionThingIterator, IteratorRange, IteratorRecord, IteratorRef, KnnIterator,
	KnnIteratorResult, MatchesThingIterator, MultipleIterators, ThingIterator,
	UniqueEqualThingIterator, UniqueJoinThingIterator, UniqueRangeThingIterator,
	UniqueUnionThingIterator, ValueType,
};
#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
use crate::idx::planner::iterators::{
	IndexRangeReverseThingIterator, UniqueRangeReverseThingIterator,
};
use crate::idx::planner::knn::{KnnBruteForceResult, KnnPriorityList};
use crate::idx::planner::plan::IndexOperator::Matches;
use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
use crate::idx::planner::tree::{IdiomPosition, IndexReference};
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::hnsw::SharedHnswIndex;
use crate::kvs::TransactionType;
use anyhow::{Result, bail, ensure};
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

enum PerIndexReferenceIndex {
	Search(SearchIndex),
	FullText(FullTextIndex),
	MTree(MTreeIndex),
	Hnsw(SharedHnswIndex),
}

enum PerExpressionEntry {
	Search(SearchEntry),
	FullText(FullTextEntry),
	MTree(MtEntry),
	Hnsw(HnswEntry),
	KnnBruteForce(KnnBruteForceEntry),
}

enum PerMatchRefEntry {
	Search(SearchEntry),
	FullText(FullTextEntry),
}

pub(super) struct InnerQueryExecutor {
	table: String,
	ir_map: HashMap<IndexReference, PerIndexReferenceIndex>,
	mr_entries: HashMap<MatchRef, PerMatchRefEntry>,
	exp_entries: HashMap<Arc<Expression>, PerExpressionEntry>,
	it_entries: Vec<IteratorEntry>,
	knn_bruteforce_len: usize,
}

impl From<InnerQueryExecutor> for QueryExecutor {
	fn from(value: InnerQueryExecutor) -> Self {
		Self(Arc::new(value))
	}
}

pub(super) enum IteratorEntry {
	Single(Option<Arc<Expression>>, IndexOption),
	Range(HashSet<Arc<Expression>>, IndexReference, RangeValue, RangeValue),
}

impl IteratorEntry {
	pub(super) fn explain(&self) -> Value {
		match self {
			Self::Single(_, io) => io.explain(),
			Self::Range(_, ir, from, to) => {
				let mut e = HashMap::default();
				e.insert("index", Value::from(ir.name.0.clone()));
				e.insert("from", Value::from(from));
				e.insert("to", Value::from(to));
				Value::from(Object::from(e))
			}
		}
	}
}

impl InnerQueryExecutor {
	#[expect(clippy::mutable_key_type)]
	pub(super) async fn new(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		table: &Table,
		ios: Vec<(Arc<Expression>, IndexOption)>,
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
							let (ns, db) = opt.ns_db()?;
							let ix: &DefineIndexStatement = e.key();
							let ikb = IndexKeyBase::new(ns, db, &ix.what, &ix.name);
							let si = SearchIndex::new(
								ctx,
								opt,
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
						if let Matches(_, Some(mr), _) = e.0.index_option.op() {
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
							let (ns, db) = opt.ns_db()?;
							let ix: &DefineIndexStatement = e.key();
							let ikb = IndexKeyBase::new(ns, db, &ix.what, &ix.name);
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
						if let Matches(_, Some(mr), _) = e.0.io.op() {
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
								let (ns, db) = opt.ns_db()?;
								let ix: &DefineIndexStatement = e.key();
								let ikb = IndexKeyBase::new(ns, db, &ix.what, &ix.name);
								let tx = ctx.tx();
								let mti =
									MTreeIndex::new(&tx, ikb, p, TransactionType::Read).await?;
								drop(tx);
								let entry =
									MtEntry::new(stk, ctx, opt, &mti, a, *k, knn_condition.clone())
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
								let hi =
									ctx.get_index_stores().get_index_hnsw(ctx, opt, ixr, p).await?;
								// Ensure the local HNSW index is up to date with the KVS
								hi.write().await.check_state(&ctx.tx()).await?;
								// Now we can execute the request
								let entry = HnswEntry::new(
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
			table: table.0.clone(),
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
		thg: &Thing,
		doc: Option<&CursorDoc>,
		exp: &Expression,
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
	pub(crate) fn is_iterator_expression(&self, ir: IteratorRef, exp: &Expression) -> bool {
		match self.0.it_entries.get(ir) {
			Some(IteratorEntry::Single(Some(e), ..)) => exp.eq(e.as_ref()),
			Some(IteratorEntry::Range(es, ..)) => es.contains(exp),
			_ => false,
		}
	}

	pub(crate) fn is_iterator_condition(&self, ir: IteratorRef, cond: &Cond) -> bool {
		if let Value::Expression(e) = &cond.0 {
			self.is_iterator_expression(ir, e)
		} else {
			false
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
		opt: &Options,
		ir: IteratorRef,
	) -> Result<Option<ThingIterator>> {
		if let Some(it_entry) = self.0.it_entries.get(ir) {
			match it_entry {
				IteratorEntry::Single(_, io) => self.new_single_iterator(opt, ir, io).await,
				IteratorEntry::Range(_, ixr, from, to) => {
					Ok(self.new_range_iterator(ir, opt, ixr, from, to)?)
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
	) -> Result<Option<ThingIterator>> {
		let ixr = io.ix_ref();
		match ixr.index {
			Index::Idx => Ok(self.new_index_iterator(opt, irf, ixr, io.clone()).await?),
			Index::Uniq => Ok(self.new_unique_index_iterator(opt, irf, ixr, io.clone()).await?),
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

	async fn new_index_iterator(
		&self,
		opt: &Options,
		ir: IteratorRef,
		ix: &IndexReference,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		Ok(match io.op() {
			IndexOperator::Equality(value) => {
				let variants = Self::get_equal_variants_from_value(value);
				if variants.len() == 1 {
					Some(Self::new_index_equal_iterator(ir, opt, ix, &variants[0])?)
				} else {
					let (ns, db) = opt.ns_db()?;
					Some(ThingIterator::IndexUnion(IndexUnionThingIterator::new(
						ir, ns, db, ix, &variants,
					)?))
				}
			}
			IndexOperator::Union(values) => {
				let variants = Self::get_equal_variants_from_values(values);
				let (ns, db) = opt.ns_db()?;
				Some(ThingIterator::IndexUnion(IndexUnionThingIterator::new(
					ir, ns, db, ix, &variants,
				)?))
			}
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(opt, ir, ios).await?;
				let index_join =
					Box::new(IndexJoinThingIterator::new(ir, opt, ix.clone(), iterators)?);
				Some(ThingIterator::IndexJoin(index_join))
			}
			IndexOperator::Order(reverse) => {
				if *reverse {
					#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
					{
						Some(ThingIterator::IndexRangeReverse(
							IndexRangeReverseThingIterator::full_range(
								ir,
								opt.ns()?,
								opt.db()?,
								ix,
							)?,
						))
					}
					#[cfg(not(any(feature = "kv-rocksdb", feature = "kv-tikv")))]
					None
				} else {
					Some(ThingIterator::IndexRange(IndexRangeThingIterator::full_range(
						ir,
						opt.ns()?,
						opt.db()?,
						ix,
					)?))
				}
			}
			_ => None,
		})
	}

	fn get_equal_variants_from_value(value: &Value) -> Vec<Array> {
		let mut variants = Vec::with_capacity(1);
		Self::generate_variants_from_value(value, &mut variants);
		variants
	}

	fn get_equal_variants_from_values(values: &Value) -> Vec<Array> {
		if let Value::Array(a) = values {
			let mut variants = Vec::with_capacity(a.len());
			for v in &a.0 {
				Self::generate_variants_from_value(v, &mut variants);
			}
			variants
		} else {
			vec![]
		}
	}

	fn generate_variants_from_value(value: &Value, variants: &mut Vec<Array>) {
		if let Value::Array(a) = value {
			Self::generate_variants_from_array(a, variants);
		} else {
			let a = Array(vec![value.clone()]);
			Self::generate_variants_from_array(&a, variants)
		}
	}

	fn generate_variants_from_array(array: &Array, variants: &mut Vec<Array>) {
		let col_count = array.len();
		let mut cols_values = Vec::with_capacity(col_count);
		for value in array.iter() {
			let value_variants = if let Value::Number(n) = value {
				Self::get_equal_number_variants(n)
			} else {
				vec![value.clone()]
			};
			cols_values.push(value_variants);
		}
		Self::generate_variant(0, vec![], &cols_values, variants);
	}

	fn generate_variant(
		col: usize,
		variant: Vec<Value>,
		cols_values: &[Vec<Value>],
		variants: &mut Vec<Array>,
	) {
		if let Some(values) = cols_values.get(col) {
			let col = col + 1;
			for value in values {
				let mut current_variant = variant.clone();
				current_variant.push(value.clone());
				Self::generate_variant(col, current_variant, cols_values, variants);
			}
		} else {
			variants.push(Array(variant));
		}
	}

	fn new_index_equal_iterator(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		array: &Array,
	) -> Result<ThingIterator> {
		let (ns, db) = opt.ns_db()?;
		Ok(ThingIterator::IndexEqual(IndexEqualThingIterator::new(irf, ns, db, ix, array)?))
	}

	/// This function takes a reference to a `Number` enum and a conversion function `float_to_int`.
	/// It returns a tuple containing the variants of the `Number` as `Option<i64>`, `Option<f64>`, and `Option<Decimal>`.
	///
	/// The `Number` enum can be one of the following:
	/// - `Int(i64)`: Integer value.
	/// - `Float(f64)`: Floating point value.
	/// - `Decimal(Decimal)`: Decimal value.
	///
	/// The function performs the following conversions based on the type of the `Number`:
	/// - For `Int`, it returns the original `Int` value as `Option<i64>`, the equivalent `Float` value as `Option<f64>`, and the equivalent `Decimal` value as `Option<Decimal>`.
	/// - For `Float`, it uses the provided `float_to_int` function to convert the `Float` to `Option<i64>`, returns the original `Float` value as `Option<f64>`, and the equivalent `Decimal` value as `Option<Decimal>`.
	/// - For `Decimal`, it converts the `Decimal` to `Option<i64>` (if representable as `i64`), returns the equivalent `Float` value as `Option<f64>` (if representable as `f64`), and the original `Decimal` value as `Option<Decimal>`.
	///
	/// # Parameters
	/// - `n`: A reference to a `Number` enum.
	/// - `float_to_int`: A function that converts a reference to `f64` to `Option<i64>`.
	///
	/// # Returns
	/// A tuple of `(Option<i64>, Option<f64>, Option<Decimal>)` representing the converted variants of the input `Number`.
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
				od = Decimal::from_f64(*f).map(|d| d.normalize());
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

	fn get_from_range_number_variants<'a>(from: &Number, from_inc: bool) -> Vec<IteratorRange<'a>> {
		let (from_i, from_f, from_d) = Self::get_range_number_from_variants(from);
		let mut vec = Vec::with_capacity(3);
		if let Some(from) = from_i {
			vec.push(IteratorRange::new(
				ValueType::NumberInt,
				RangeValue {
					value: Number::Int(from).into(),
					inclusive: from_inc,
				},
				RangeValue {
					value: Value::None,
					inclusive: false,
				},
			));
		}
		if let Some(from) = from_f {
			vec.push(IteratorRange::new(
				ValueType::NumberFloat,
				RangeValue {
					value: Number::Float(from).into(),
					inclusive: from_inc,
				},
				RangeValue {
					value: Value::None,
					inclusive: false,
				},
			));
		}
		if let Some(from) = from_d {
			vec.push(IteratorRange::new(
				ValueType::NumberDecimal,
				RangeValue {
					value: Number::Decimal(from).into(),
					inclusive: from_inc,
				},
				RangeValue {
					value: Value::None,
					inclusive: false,
				},
			));
		}
		vec
	}

	fn get_to_range_number_variants<'a>(to: &Number, to_inc: bool) -> Vec<IteratorRange<'a>> {
		let (from_i, from_f, from_d) = Self::get_range_number_to_variants(to);
		let mut vec = Vec::with_capacity(3);
		if let Some(to) = from_i {
			vec.push(IteratorRange::new(
				ValueType::NumberInt,
				RangeValue {
					value: Value::None,
					inclusive: false,
				},
				RangeValue {
					value: Number::Int(to).into(),
					inclusive: to_inc,
				},
			));
		}
		if let Some(to) = from_f {
			vec.push(IteratorRange::new(
				ValueType::NumberFloat,
				RangeValue {
					value: Value::None,
					inclusive: false,
				},
				RangeValue {
					value: Number::Float(to).into(),
					inclusive: to_inc,
				},
			));
		}
		if let Some(to) = from_d {
			vec.push(IteratorRange::new(
				ValueType::NumberDecimal,
				RangeValue {
					value: Value::None,
					inclusive: false,
				},
				RangeValue {
					value: Number::Decimal(to).into(),
					inclusive: to_inc,
				},
			));
		}
		vec
	}

	fn get_ranges_number_variants<'a>(
		from: &Number,
		from_inc: bool,
		to: &Number,
		to_inc: bool,
	) -> Vec<IteratorRange<'a>> {
		let (from_i, from_f, from_d) = Self::get_range_number_from_variants(from);
		let (to_i, to_f, to_d) = Self::get_range_number_to_variants(to);
		let mut vec = Vec::with_capacity(3);
		if let (Some(from), Some(to)) = (from_i, to_i) {
			vec.push(IteratorRange::new(
				ValueType::NumberInt,
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
			vec.push(IteratorRange::new(
				ValueType::NumberFloat,
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
			vec.push(IteratorRange::new(
				ValueType::NumberDecimal,
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
		ir: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		from: &RangeValue,
		to: &RangeValue,
	) -> Result<Option<ThingIterator>> {
		match ix.index {
			Index::Idx => {
				let ranges = Self::get_ranges_variants(from, to);
				if let Some(ranges) = ranges {
					if ranges.len() == 1 {
						return Ok(Some(Self::new_index_range_iterator(ir, opt, ix, &ranges[0])?));
					} else {
						return Ok(Some(Self::new_multiple_index_range_iterator(
							ir, opt, ix, &ranges,
						)?));
					}
				}
				return Ok(Some(Self::new_index_range_iterator(
					ir,
					opt,
					ix,
					&IteratorRange::new_ref(ValueType::None, from, to),
				)?));
			}
			Index::Uniq => {
				let ranges = Self::get_ranges_variants(from, to);
				if let Some(ranges) = ranges {
					if ranges.len() == 1 {
						return Ok(Some(Self::new_unique_range_iterator(ir, opt, ix, &ranges[0])?));
					} else {
						return Ok(Some(Self::new_multiple_unique_range_iterator(
							ir, opt, ix, &ranges,
						)?));
					}
				}
				return Ok(Some(Self::new_unique_range_iterator(
					ir,
					opt,
					ix,
					&IteratorRange::new_ref(ValueType::None, from, to),
				)?));
			}
			_ => {}
		}
		Ok(None)
	}

	fn get_ranges_variants<'a>(
		from: &'a RangeValue,
		to: &'a RangeValue,
	) -> Option<Vec<IteratorRange<'a>>> {
		match (&from.value, &to.value) {
			(Value::Number(from_n), Value::Number(to_n)) => {
				Some(Self::get_ranges_number_variants(from_n, from.inclusive, to_n, to.inclusive))
			}
			(Value::Number(from_n), Value::None) => {
				Some(Self::get_from_range_number_variants(from_n, from.inclusive))
			}
			(Value::None, Value::Number(to_n)) => {
				Some(Self::get_to_range_number_variants(to_n, to.inclusive))
			}
			_ => None,
		}
	}

	fn new_index_range_iterator(
		ir: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		range: &IteratorRange,
	) -> Result<ThingIterator> {
		let (ns, db) = opt.ns_db()?;
		Ok(ThingIterator::IndexRange(IndexRangeThingIterator::new(ir, ns, db, ix, range)?))
	}

	fn new_unique_range_iterator(
		ir: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		range: &IteratorRange<'_>,
	) -> Result<ThingIterator> {
		let (ns, db) = opt.ns_db()?;
		Ok(ThingIterator::UniqueRange(UniqueRangeThingIterator::new(ir, ns, db, ix, range)?))
	}

	fn new_multiple_index_range_iterator(
		ir: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		ranges: &[IteratorRange],
	) -> Result<ThingIterator> {
		let mut iterators = VecDeque::with_capacity(ranges.len());
		for range in ranges {
			iterators.push_back(Self::new_index_range_iterator(ir, opt, ix, range)?);
		}
		Ok(ThingIterator::Multiples(Box::new(MultipleIterators::new(iterators))))
	}

	fn new_multiple_unique_range_iterator(
		ir: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		ranges: &[IteratorRange<'_>],
	) -> Result<ThingIterator> {
		let mut iterators = VecDeque::with_capacity(ranges.len());
		for range in ranges {
			iterators.push_back(Self::new_unique_range_iterator(ir, opt, ix, range)?);
		}
		Ok(ThingIterator::Multiples(Box::new(MultipleIterators::new(iterators))))
	}

	async fn new_unique_index_iterator(
		&self,
		opt: &Options,
		irf: IteratorRef,
		ixr: &IndexReference,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		Ok(match io.op() {
			IndexOperator::Equality(values) => {
				let variants = Self::get_equal_variants_from_value(values);
				if variants.len() == 1 {
					Some(Self::new_unique_equal_iterator(irf, opt, ixr, &variants[0])?)
				} else {
					Some(ThingIterator::UniqueUnion(UniqueUnionThingIterator::new(
						irf, opt, ixr, &variants,
					)?))
				}
			}
			IndexOperator::Union(values) => {
				let variants = Self::get_equal_variants_from_values(values);
				Some(ThingIterator::UniqueUnion(UniqueUnionThingIterator::new(
					irf, opt, ixr, &variants,
				)?))
			}
			IndexOperator::Join(ios) => {
				let iterators = self.build_iterators(opt, irf, ios).await?;
				let unique_join =
					Box::new(UniqueJoinThingIterator::new(irf, opt, ixr.clone(), iterators)?);
				Some(ThingIterator::UniqueJoin(unique_join))
			}
			IndexOperator::Order(reverse) => {
				if *reverse {
					#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
					{
						Some(ThingIterator::UniqueRangeReverse(
							UniqueRangeReverseThingIterator::full_range(
								irf,
								opt.ns()?,
								opt.db()?,
								ixr,
							)?,
						))
					}
					#[cfg(not(any(feature = "kv-rocksdb", feature = "kv-tikv")))]
					None
				} else {
					Some(ThingIterator::UniqueRange(UniqueRangeThingIterator::full_range(
						irf,
						opt.ns()?,
						opt.db()?,
						ixr,
					)?))
				}
			}
			_ => None,
		})
	}

	fn new_unique_equal_iterator(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		array: &Array,
	) -> Result<ThingIterator> {
		let (ns, db) = opt.ns_db()?;
		if ix.cols.len() > 1 {
			// If the index is unique and the index is a composite index,
			// then we have the opportunity to iterate on the first column of the index
			// and consider it as a standard index (rather than a unique one)
			Ok(ThingIterator::IndexEqual(IndexEqualThingIterator::new(irf, ns, db, ix, array)?))
		} else {
			Ok(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(irf, ns, db, ix, array)?))
		}
	}

	async fn new_search_index_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>> {
		if let Some(IteratorEntry::Single(Some(exp), ..)) = self.0.it_entries.get(ir) {
			if let Matches(_, _, _) = io.op() {
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
			if let Matches(_, _, bo) = io.op() {
				if let Some(PerIndexReferenceIndex::FullText(fti)) = self.0.ir_map.get(io.ix_ref())
				{
					if let Some(PerExpressionEntry::FullText(fte)) = self.0.exp_entries.get(exp) {
						let hits = fti.new_hits_iterator(&fte.0.qt, bo.clone());
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
		opt: &Options,
		irf: IteratorRef,
		ios: &[IndexOption],
	) -> Result<VecDeque<ThingIterator>> {
		let mut iterators = VecDeque::with_capacity(ios.len());
		for io in ios {
			if let Some(it) = Box::pin(self.new_single_iterator(opt, irf, io)).await? {
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
		thg: &Thing,
		exp: &Expression,
		l: Value,
		r: Value,
	) -> Result<bool> {
		match self.0.exp_entries.get(exp) {
			Some(PerExpressionEntry::Search(se)) => {
				let ix = se.0.index_option.ix_ref();
				if self.0.table.eq(&ix.what.0) {
					return self.search_matches_with_doc_id(ctx, thg, se).await;
				}
				if let Some(PerIndexReferenceIndex::Search(si)) = self.0.ir_map.get(ix) {
					return self.search_matches_with_value(stk, ctx, opt, si, se, l, r).await;
				}
			}
			Some(PerExpressionEntry::FullText(fte)) => {
				let ix = fte.0.io.ix_ref();
				if let Some(PerIndexReferenceIndex::FullText(fti)) = self.0.ir_map.get(ix) {
					if self.0.table.eq(&ix.what.0) {
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
		thg: &Thing,
		se: &SearchEntry,
	) -> Result<bool> {
		// TODO ask Emmanuel
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
		thg: &Thing,
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

	#[allow(clippy::too_many_arguments)]
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

	#[allow(clippy::too_many_arguments)]
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
		thg: &Thing,
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
		thg: &Thing,
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
		rid: &Thing,
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
		if let Matches(qs, _, bo) = io.op() {
			if !matches!(bo, BooleanOperation::And) {
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
		if let Matches(qs, _, _) = io.op() {
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
	async fn new(
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
	#[expect(clippy::too_many_arguments)]
	async fn new(
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
			.knn_search(&ctx.tx(), stk, v, n as usize, ef as usize, cond_checker)
			.await?;
		Ok(Self {
			res,
		})
	}
}
