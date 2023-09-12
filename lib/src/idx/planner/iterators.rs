use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds, NO_DOC_ID};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::key::index::Index;
use crate::kvs::Key;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Thing, Value};
use roaring::RoaringTreemap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) enum ThingIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
	Matches(MatchesThingIterator),
	Knn(KnnThingIterator),
}

impl ThingIterator {
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		size: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		match self {
			ThingIterator::IndexEqual(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueEqual(i) => i.next_batch(tx).await,
			ThingIterator::IndexRange(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueRange(i) => i.next_batch(tx, size).await,
			ThingIterator::Matches(i) => i.next_batch(tx, size).await,
			ThingIterator::Knn(i) => i.next_batch(tx, size).await,
		}
	}
}

pub(crate) struct IndexEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl IndexEqualThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, v: &Array) -> Result<Self, Error> {
		let beg = Index::prefix_ids_beg(opt.ns(), opt.db(), &ix.what, &ix.name, v);
		let end = Index::prefix_ids_end(opt.ns(), opt.db(), &ix.what, &ix.name, v);
		Ok(Self {
			beg,
			end,
		})
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		let min = self.beg.clone();
		let max = self.end.clone();
		let res = txn.lock().await.scan(min..max, limit).await?;
		if let Some((key, _)) = res.last() {
			self.beg = key.clone();
			self.beg.push(0x00);
		}
		let res = res.iter().map(|(_, val)| (val.into(), NO_DOC_ID)).collect();
		Ok(res)
	}
}

struct RangeScan {
	beg: Vec<u8>,
	end: Vec<u8>,
	beg_excl: Option<Vec<u8>>,
	end_excl: Option<Vec<u8>>,
}

impl RangeScan {
	fn new(beg: Vec<u8>, beg_incl: bool, end: Vec<u8>, end_incl: bool) -> Self {
		let beg_excl = if !beg_incl {
			Some(beg.clone())
		} else {
			None
		};
		let end_excl = if !end_incl {
			Some(end.clone())
		} else {
			None
		};
		Self {
			beg,
			end,
			beg_excl,
			end_excl,
		}
	}

	fn matches(&mut self, k: &Key) -> bool {
		if let Some(b) = &self.beg_excl {
			if b.eq(k) {
				self.beg_excl = None;
				return false;
			}
		}
		if let Some(e) = &self.end_excl {
			if e.eq(k) {
				self.end_excl = None;
				return false;
			}
		}
		true
	}
}

pub(crate) struct IndexRangeThingIterator {
	r: RangeScan,
}

impl IndexRangeThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		from: &RangeValue,
		to: &RangeValue,
	) -> Self {
		let beg = Self::compute_beg(opt, ix, from);
		let end = Self::compute_end(opt, ix, to);
		Self {
			r: RangeScan::new(beg, from.inclusive, end, to.inclusive),
		}
	}

	fn compute_beg(opt: &Options, ix: &DefineIndexStatement, from: &RangeValue) -> Vec<u8> {
		if from.value == Value::None {
			return Index::prefix_beg(opt.ns(), opt.db(), &ix.what, &ix.name);
		}
		let fd = Array::from(from.value.to_owned());
		if from.inclusive {
			Index::prefix_ids_beg(opt.ns(), opt.db(), &ix.what, &ix.name, &fd)
		} else {
			Index::prefix_ids_end(opt.ns(), opt.db(), &ix.what, &ix.name, &fd)
		}
	}

	fn compute_end(opt: &Options, ix: &DefineIndexStatement, to: &RangeValue) -> Vec<u8> {
		if to.value == Value::None {
			return Index::prefix_end(opt.ns(), opt.db(), &ix.what, &ix.name);
		}
		let fd = Array::from(to.value.to_owned());
		if to.inclusive {
			Index::prefix_ids_end(opt.ns(), opt.db(), &ix.what, &ix.name, &fd)
		} else {
			Index::prefix_ids_beg(opt.ns(), opt.db(), &ix.what, &ix.name, &fd)
		}
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		let res = txn.lock().await.scan(min..max, limit).await?;
		if let Some((key, _)) = res.last() {
			self.r.beg = key.clone();
			self.r.beg.push(0x00);
		}
		let mut r = Vec::with_capacity(res.len());
		for (k, v) in res {
			if self.r.matches(&k) {
				r.push((v.into(), NO_DOC_ID));
			}
		}
		Ok(r)
	}
}

pub(crate) struct UniqueEqualThingIterator {
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, a: &Array) -> Result<Self, Error> {
		let key = Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, a, None).into();
		Ok(Self {
			key: Some(key),
		})
	}

	async fn next_batch(&mut self, txn: &Transaction) -> Result<Vec<(Thing, DocId)>, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = txn.lock().await.get(key).await? {
				return Ok(vec![(val.into(), NO_DOC_ID)]);
			}
		}
		Ok(vec![])
	}
}

pub(crate) struct UniqueRangeThingIterator {
	r: RangeScan,
	done: bool,
}

impl UniqueRangeThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		from: &RangeValue,
		to: &RangeValue,
	) -> Self {
		let beg = Self::compute_beg(opt, ix, from);
		let end = Self::compute_end(opt, ix, to);
		Self {
			r: RangeScan::new(beg, from.inclusive, end, to.inclusive),
			done: false,
		}
	}

	fn compute_beg(opt: &Options, ix: &DefineIndexStatement, from: &RangeValue) -> Vec<u8> {
		if from.value == Value::None {
			return Index::prefix_beg(opt.ns(), opt.db(), &ix.what, &ix.name);
		}
		Index::new(
			opt.ns(),
			opt.db(),
			&ix.what,
			&ix.name,
			&Array::from(from.value.to_owned()),
			None,
		)
		.encode()
		.unwrap()
	}

	fn compute_end(opt: &Options, ix: &DefineIndexStatement, to: &RangeValue) -> Vec<u8> {
		if to.value == Value::None {
			return Index::prefix_end(opt.ns(), opt.db(), &ix.what, &ix.name);
		}
		Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, &Array::from(to.value.to_owned()), None)
			.encode()
			.unwrap()
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		mut limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		if self.done {
			return Ok(vec![]);
		}
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		limit += 1;
		let mut tx = txn.lock().await;
		let res = tx.scan(min..max, limit).await?;
		let mut r = Vec::with_capacity(res.len());
		for (k, v) in res {
			limit -= 1;
			if limit == 0 {
				self.r.beg = k;
				return Ok(r);
			}
			if self.r.matches(&k) {
				r.push((v.into(), NO_DOC_ID));
			}
		}
		let end = self.r.end.clone();
		if self.r.matches(&end) {
			if let Some(v) = tx.get(end).await? {
				r.push((v.into(), NO_DOC_ID));
			}
		}
		self.done = true;
		Ok(r)
	}
}

pub(crate) struct MatchesThingIterator {
	hits: Option<HitsIterator>,
}

impl MatchesThingIterator {
	pub(super) async fn new(fti: &FtIndex, terms_docs: TermsDocs) -> Result<Self, Error> {
		let hits = fti.new_hits_iterator(terms_docs)?;
		Ok(Self {
			hits,
		})
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		mut limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		let mut res = vec![];
		if let Some(hits) = &mut self.hits {
			let mut run = txn.lock().await;
			while limit > 0 {
				if let Some(hit) = hits.next(&mut run).await? {
					res.push(hit);
				} else {
					break;
				}
				limit -= 1;
			}
		}
		Ok(res)
	}
}

pub(crate) struct KnnThingIterator {
	doc_ids: Arc<RwLock<DocIds>>,
	res: VecDeque<RoaringTreemap>,
	current: Option<RoaringTreemap>,
	skip: RoaringTreemap,
}

impl KnnThingIterator {
	pub(super) fn new(doc_ids: Arc<RwLock<DocIds>>, mut res: VecDeque<RoaringTreemap>) -> Self {
		let current = res.pop_front();
		Self {
			doc_ids,
			res,
			current,
			skip: RoaringTreemap::new(),
		}
	}
	async fn next_batch(
		&mut self,
		txn: &Transaction,
		mut limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		let mut res = vec![];
		let mut tx = txn.lock().await;
		while self.current.is_some() && limit > 0 {
			if let Some(docs) = &mut self.current {
				if let Some(doc_id) = docs.iter().next() {
					docs.remove(doc_id);
					if self.skip.insert(doc_id) {
						if let Some(doc_key) =
							self.doc_ids.read().await.get_doc_key(&mut tx, doc_id).await?
						{
							res.push((doc_key.into(), doc_id));
							limit -= 1;
						}
					}
					if docs.is_empty() {
						self.current = None;
					}
				}
			}
			if self.current.is_none() {
				self.current = self.res.pop_front();
			}
		}
		Ok(res)
	}
}
