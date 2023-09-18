use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::iterators::{
	IndexEqualThingIterator, IndexRangeThingIterator, KnnThingIterator, MatchesThingIterator,
	ThingIterator, UniqueEqualThingIterator, UniqueRangeThingIterator,
};
use crate::idx::planner::plan::IndexOperator::Matches;
use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
use crate::idx::planner::tree::{IndexMap, IndexRef};
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Expression, Object, Table, Thing, Value};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct QueryExecutor {
	table: String,
	ft_map: HashMap<IndexRef, FtIndex>,
	mr_entries: HashMap<MatchRef, FtEntry>,
	exp_entries: HashMap<Arc<Expression>, FtEntry>,
	it_entries: Vec<IteratorEntry>,
	index_definitions: HashMap<IndexRef, DefineIndexStatement>,
	mt_exp: HashMap<Arc<Expression>, MtEntry>,
}

pub(crate) type IteratorRef = u16;

pub(super) enum IteratorEntry {
	Single(Arc<Expression>, IndexOption),
	Range(HashSet<Arc<Expression>>, IndexRef, RangeValue, RangeValue),
}

impl IteratorEntry {
	pub(super) fn explain(&self, e: &mut HashMap<&str, Value>) -> IndexRef {
		match self {
			Self::Single(_, io) => {
				io.explain(e);
				io.ir()
			}
			Self::Range(_, ir, from, to) => {
				e.insert("from", Value::from(from));
				e.insert("to", Value::from(to));
				*ir
			}
		}
	}
}
impl QueryExecutor {
	pub(super) async fn new(
		opt: &Options,
		txn: &Transaction,
		table: &Table,
		im: IndexMap,
	) -> Result<Self, Error> {
		let mut run = txn.lock().await;

		let mut mr_entries = HashMap::default();
		let mut exp_entries = HashMap::default();
		let mut ft_map = HashMap::default();
		let mut mt_map: HashMap<IndexRef, MTreeIndex> = HashMap::default();
		let mut mt_exp = HashMap::default();

		// Create all the instances of FtIndex
		// Build the FtEntries and map them to Expressions and MatchRef
		for (exp, io) in im.options {
			let ir = io.ir();
			if let Some(idx_def) = im.definitions.get(&ir) {
				match &idx_def.index {
					Index::Search(p) => {
						let mut ft_entry = None;
						if let Some(ft) = ft_map.get(&ir) {
							if ft_entry.is_none() {
								ft_entry = FtEntry::new(&mut run, ft, io).await?;
							}
						} else {
							let ikb = IndexKeyBase::new(opt, idx_def);
							let az = run.get_db_analyzer(opt.ns(), opt.db(), p.az.as_str()).await?;
							let ft =
								FtIndex::new(&mut run, az, ikb, p, TreeStoreType::Read).await?;
							if ft_entry.is_none() {
								ft_entry = FtEntry::new(&mut run, &ft, io).await?;
							}
							ft_map.insert(ir, ft);
						}
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
							let entry = if let Some(mt) = mt_map.get(&ir) {
								MtEntry::new(&mut run, mt, a.clone(), *k).await?
							} else {
								let ikb = IndexKeyBase::new(opt, idx_def);
								let mt =
									MTreeIndex::new(&mut run, ikb, p, TreeStoreType::Read).await?;
								let entry = MtEntry::new(&mut run, &mt, a.clone(), *k).await?;
								mt_map.insert(ir, mt);
								entry
							};
							mt_exp.insert(exp, entry);
						}
					}
					_ => {}
				}
			}
		}

		Ok(Self {
			table: table.0.clone(),
			ft_map,
			mr_entries,
			exp_entries,
			it_entries: Vec::new(),
			index_definitions: im.definitions,
			mt_exp,
		})
	}

	pub(crate) async fn knn(
		&self,
		_txn: &Transaction,
		_thg: &Thing,
		exp: &Expression,
	) -> Result<Value, Error> {
		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	pub(super) fn add_iterator(&mut self, it_entry: IteratorEntry) -> IteratorRef {
		let ir = self.it_entries.len();
		self.it_entries.push(it_entry);
		ir as IteratorRef
	}

	pub(crate) fn is_distinct(&self, ir: IteratorRef) -> bool {
		(ir as usize) < self.it_entries.len()
	}

	pub(crate) fn is_iterator_expression(&self, ir: IteratorRef, exp: &Expression) -> bool {
		match self.it_entries.get(ir as usize) {
			Some(IteratorEntry::Single(e, ..)) => exp.eq(e.as_ref()),
			Some(IteratorEntry::Range(es, ..)) => es.contains(exp),
			_ => false,
		}
	}

	pub(crate) fn explain(&self, itr: IteratorRef) -> Value {
		match self.it_entries.get(itr as usize) {
			Some(ie) => {
				let mut e = HashMap::default();
				let ir = ie.explain(&mut e);
				if let Some(ix) = self.index_definitions.get(&ir) {
					e.insert("index", Value::from(ix.name.0.to_owned()));
				}
				Value::from(Object::from(e))
			}
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
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(it_entry) = self.it_entries.get(ir as usize) {
			match it_entry {
				IteratorEntry::Single(_, io) => {
					if let Some(ix) = self.index_definitions.get(&io.ir()) {
						match ix.index {
							Index::Idx => Self::new_index_iterator(opt, ix, io.clone()),
							Index::Uniq => Self::new_unique_index_iterator(opt, ix, io.clone()),
							Index::Search {
								..
							} => self.new_search_index_iterator(ir, io.clone()).await,
							Index::MTree(_) => Ok(self.new_mtree_index_knn_iterator(ir)),
						}
					} else {
						Ok(None)
					}
				}
				IteratorEntry::Range(_, ir, from, to) => {
					Ok(self.new_range_iterator(opt, *ir, from, to)?)
				}
			}
		} else {
			Ok(None)
		}
	}

	fn new_index_iterator(
		opt: &Options,
		ix: &DefineIndexStatement,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		match io.op() {
			IndexOperator::Equality(array) => {
				Ok(Some(ThingIterator::IndexEqual(IndexEqualThingIterator::new(opt, ix, array)?)))
			}
			IndexOperator::RangePart(_, _) => Ok(None), // TODO
			_ => Ok(None),
		}
	}

	fn new_range_iterator(
		&self,
		opt: &Options,
		ir: IndexRef,
		from: &RangeValue,
		to: &RangeValue,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(ix) = self.index_definitions.get(&ir) {
			match ix.index {
				Index::Idx => {
					return Ok(Some(ThingIterator::IndexRange(IndexRangeThingIterator::new(
						opt, ix, from, to,
					))))
				}
				Index::Uniq => {
					return Ok(Some(ThingIterator::UniqueRange(UniqueRangeThingIterator::new(
						opt, ix, from, to,
					))))
				}
				_ => {}
			}
		}
		Ok(None)
	}

	fn new_unique_index_iterator(
		opt: &Options,
		ix: &DefineIndexStatement,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		match io.op() {
			IndexOperator::Equality(array) => {
				Ok(Some(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(opt, ix, array)?)))
			}
			IndexOperator::RangePart(_, _) => {
				todo!()
			}
			_ => Ok(None),
		}
	}

	async fn new_search_index_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(IteratorEntry::Single(exp, ..)) = self.it_entries.get(ir as usize) {
			if let Matches(_, _) = io.op() {
				if let Some(fti) = self.ft_map.get(&io.ir()) {
					if let Some(fte) = self.exp_entries.get(exp.as_ref()) {
						let it = MatchesThingIterator::new(fti, fte.0.terms_docs.clone()).await?;
						return Ok(Some(ThingIterator::Matches(it)));
					}
				}
			}
		}
		Ok(None)
	}

	fn new_mtree_index_knn_iterator(&self, ir: IteratorRef) -> Option<ThingIterator> {
		if let Some(IteratorEntry::Single(exp, ..)) = self.it_entries.get(ir as usize) {
			if let Some(mte) = self.mt_exp.get(exp.as_ref()) {
				let it = KnnThingIterator::new(mte.doc_ids.clone(), mte.res.clone());
				return Some(ThingIterator::Knn(it));
			}
		}
		None
	}

	pub(crate) async fn matches(
		&self,
		txn: &Transaction,
		thg: &Thing,
		exp: &Expression,
	) -> Result<Value, Error> {
		// Otherwise, we look for the first possible index options, and evaluate the expression
		// Does the record id match this executor's table?
		if thg.tb.eq(&self.table) {
			if let Some(ft) = self.exp_entries.get(exp) {
				let mut run = txn.lock().await;
				let doc_key: Key = thg.into();
				if let Some(doc_id) =
					ft.0.doc_ids.read().await.get_doc_id(&mut run, doc_key).await?
				{
					let term_goals = ft.0.terms_docs.len();
					// If there is no terms, it can't be a match
					if term_goals == 0 {
						return Ok(Value::Bool(false));
					}
					for opt_td in ft.0.terms_docs.iter() {
						if let Some((_, docs)) = opt_td {
							if !docs.contains(doc_id) {
								return Ok(Value::Bool(false));
							}
						} else {
							// If one of the term is missing, it can't be a match
							return Ok(Value::Bool(false));
						}
					}
					return Ok(Value::Bool(true));
				}
				return Ok(Value::Bool(false));
			}
		}

		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	fn get_ft_entry(&self, match_ref: &Value) -> Option<&FtEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			self.mr_entries.get(&mr)
		} else {
			None
		}
	}

	fn get_ft_entry_and_index(&self, match_ref: &Value) -> Option<(&FtEntry, &FtIndex)> {
		if let Some(e) = self.get_ft_entry(match_ref) {
			if let Some(ft) = self.ft_map.get(&e.0.index_option.ir()) {
				return Some((e, ft));
			}
		}
		None
	}

	pub(crate) async fn highlight(
		&self,
		txn: &Transaction,
		thg: &Thing,
		prefix: Value,
		suffix: Value,
		match_ref: &Value,
		doc: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_ft_entry_and_index(match_ref) {
			let mut run = txn.lock().await;
			return ft
				.highlight(&mut run, thg, &e.0.terms, prefix, suffix, e.0.index_option.id(), doc)
				.await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		txn: &Transaction,
		thg: &Thing,
		match_ref: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_ft_entry_and_index(match_ref) {
			let mut run = txn.lock().await;
			return ft.extract_offsets(&mut run, thg, &e.0.terms).await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn score(
		&self,
		txn: &Transaction,
		match_ref: &Value,
		rid: &Thing,
		mut doc_id: Option<DocId>,
	) -> Result<Value, Error> {
		if let Some(e) = self.get_ft_entry(match_ref) {
			if let Some(scorer) = &e.0.scorer {
				let mut run = txn.lock().await;
				if doc_id.is_none() {
					let key: Key = rid.into();
					doc_id = e.0.doc_ids.read().await.get_doc_id(&mut run, key).await?;
				};
				if let Some(doc_id) = doc_id {
					let score = scorer.score(&mut run, doc_id).await?;
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
	terms: Vec<Option<TermId>>,
	terms_docs: TermsDocs,
	scorer: Option<BM25Scorer>,
}

impl FtEntry {
	async fn new(
		tx: &mut kvs::Transaction,
		ft: &FtIndex,
		io: IndexOption,
	) -> Result<Option<Self>, Error> {
		if let Matches(qs, _) = io.op() {
			let terms = ft.extract_terms(tx, qs.to_owned()).await?;
			let terms_docs = Arc::new(ft.get_terms_docs(tx, &terms).await?);
			Ok(Some(Self(Arc::new(Inner {
				index_option: io,
				doc_ids: ft.doc_ids(),
				scorer: ft.new_scorer(terms_docs.clone())?,
				terms,
				terms_docs,
			}))))
		} else {
			Ok(None)
		}
	}
}

#[derive(Clone)]
pub(super) struct MtEntry {
	doc_ids: Arc<RwLock<DocIds>>,
	res: VecDeque<RoaringTreemap>,
}

impl MtEntry {
	async fn new(
		tx: &mut kvs::Transaction,
		mt: &MTreeIndex,
		a: Array,
		k: u32,
	) -> Result<Self, Error> {
		let res = mt.knn_search(tx, a, k as usize).await?;
		Ok(Self {
			res,
			doc_ids: mt.doc_ids(),
		})
	}
}
