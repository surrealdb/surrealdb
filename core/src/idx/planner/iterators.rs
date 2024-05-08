use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::key::index::Index;
use crate::kvs::{Key, Limit, ScanPage};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Thing, Value};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) enum ThingIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	IndexUnion(IndexUnionThingIterator),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
	UniqueUnion(UniqueUnionThingIterator),
	Matches(MatchesThingIterator),
	Knn(DocIdsIterator),
	Things(ThingsIterator),
}

impl ThingIterator {
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		size: u32,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		match self {
			ThingIterator::IndexEqual(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueEqual(i) => i.next_batch(tx).await,
			ThingIterator::IndexRange(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueRange(i) => i.next_batch(tx, size).await,
			ThingIterator::IndexUnion(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueUnion(i) => i.next_batch(tx, size).await,
			ThingIterator::Matches(i) => i.next_batch(tx, size).await,
			ThingIterator::Knn(i) => i.next_batch(tx, size).await,
			ThingIterator::Things(i) => i.next_batch(size),
		}
	}
}

pub(crate) struct IndexEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl IndexEqualThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, v: &Value) -> Self {
		let a = Array::from(v.clone());
		let beg = Index::prefix_ids_beg(opt.ns(), opt.db(), &ix.what, &ix.name, &a);
		let end = Index::prefix_ids_end(opt.ns(), opt.db(), &ix.what, &ix.name, &a);
		Self {
			beg,
			end,
		}
	}

	async fn next_scan(
		txn: &Transaction,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		let min = beg.clone();
		let max = end.to_owned();
		let res = txn
			.lock()
			.await
			.scan_paged(
				ScanPage {
					range: min..max,
					limit: Limit::Limited(limit),
				},
				limit,
			)
			.await?;
		let res = res.values;
		if let Some((key, _)) = res.last() {
			let mut key = key.clone();
			key.push(0x00);
			*beg = key;
		}
		let res = res.iter().map(|(_, val)| (val.into(), None)).collect();
		Ok(res)
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: u32,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		Self::next_scan(txn, &mut self.beg, &self.end, limit).await
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
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		let res = txn
			.lock()
			.await
			.scan_paged(
				ScanPage {
					range: min..max,
					limit: Limit::Limited(limit),
				},
				limit,
			)
			.await?;
		let res = res.values;
		if let Some((key, _)) = res.last() {
			self.r.beg.clone_from(key);
			self.r.beg.push(0x00);
		}
		let mut r = Vec::with_capacity(res.len());
		for (k, v) in res {
			if self.r.matches(&k) {
				r.push((v.into(), None));
			}
		}
		Ok(r)
	}
}

pub(crate) struct IndexUnionThingIterator {
	values: VecDeque<(Vec<u8>, Vec<u8>)>,
	current: Option<(Vec<u8>, Vec<u8>)>,
}

impl IndexUnionThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, a: &Array) -> Self {
		// We create a VecDeque to hold the prefix keys (begin and end) for each value in the array.
		let mut values: VecDeque<(Vec<u8>, Vec<u8>)> =
			a.0.iter()
				.map(|v| {
					let a = Array::from(v.clone());
					let beg = Index::prefix_ids_beg(opt.ns(), opt.db(), &ix.what, &ix.name, &a);
					let end = Index::prefix_ids_end(opt.ns(), opt.db(), &ix.what, &ix.name, &a);
					(beg, end)
				})
				.collect();
		let current = values.pop_front();
		Self {
			values,
			current,
		}
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: u32,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		while let Some(r) = &mut self.current {
			let res = IndexEqualThingIterator::next_scan(txn, &mut r.0, &r.1, limit).await?;
			if !res.is_empty() {
				return Ok(res);
			}
			self.current = self.values.pop_front();
		}
		Ok(vec![])
	}
}

pub(crate) struct UniqueEqualThingIterator {
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, v: &Value) -> Self {
		let a = Array::from(v.to_owned());
		let key = Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, &a, None).into();
		Self {
			key: Some(key),
		}
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = txn.lock().await.get(key).await? {
				return Ok(vec![(val.into(), None)]);
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
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		if self.done {
			return Ok(vec![]);
		}
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		limit += 1;
		let mut tx = txn.lock().await;
		let res = tx
			.scan_paged(
				ScanPage {
					range: min..max,
					limit: Limit::Limited(limit),
				},
				limit,
			)
			.await?;
		let res = res.values;
		let mut r = Vec::with_capacity(res.len());
		for (k, v) in res {
			limit -= 1;
			if limit == 0 {
				self.r.beg = k;
				return Ok(r);
			}
			if self.r.matches(&k) {
				r.push((v.into(), None));
			}
		}
		let end = self.r.end.clone();
		if self.r.matches(&end) {
			if let Some(v) = tx.get(end).await? {
				r.push((v.into(), None));
			}
		}
		self.done = true;
		Ok(r)
	}
}

pub(crate) struct UniqueUnionThingIterator {
	keys: VecDeque<Key>,
}

impl UniqueUnionThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, a: &Array) -> Self {
		// We create a VecDeque to hold the key for each value in the array.
		let keys: VecDeque<Key> =
			a.0.iter()
				.map(|v| {
					let a = Array::from(v.clone());
					let key = Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, &a, None).into();
					key
				})
				.collect();
		Self {
			keys,
		}
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: u32,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		let mut run = txn.lock().await;
		let mut res = vec![];
		while let Some(key) = self.keys.pop_front() {
			if let Some(val) = run.get(key).await? {
				res.push((val.into(), None));
			}
			if res.len() >= limit as usize {
				return Ok(res);
			}
		}
		Ok(res)
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
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		let mut res = vec![];
		if let Some(hits) = &mut self.hits {
			let mut run = txn.lock().await;
			while limit > 0 {
				if let Some((thg, doc_id)) = hits.next(&mut run).await? {
					res.push((thg, Some(doc_id)));
				} else {
					break;
				}
				limit -= 1;
			}
		}
		Ok(res)
	}
}

pub(crate) struct DocIdsIterator {
	doc_ids: Arc<RwLock<DocIds>>,
	res: VecDeque<DocId>,
}

impl DocIdsIterator {
	pub(super) fn new(doc_ids: Arc<RwLock<DocIds>>, res: VecDeque<DocId>) -> Self {
		Self {
			doc_ids,
			res,
		}
	}
	async fn next_batch(
		&mut self,
		txn: &Transaction,
		mut limit: u32,
	) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		let mut res = vec![];
		let mut tx = txn.lock().await;
		while limit > 0 {
			if let Some(doc_id) = self.res.pop_front() {
				if let Some(doc_key) =
					self.doc_ids.read().await.get_doc_key(&mut tx, doc_id).await?
				{
					res.push((doc_key.into(), Some(doc_id)));
					limit -= 1;
				}
			} else {
				break;
			}
		}
		Ok(res)
	}
}

pub(crate) struct ThingsIterator {
	res: VecDeque<Thing>,
}

impl ThingsIterator {
	pub(super) fn new(res: VecDeque<Thing>) -> Self {
		Self {
			res,
		}
	}
	fn next_batch(&mut self, limit: u32) -> Result<Vec<(Thing, Option<DocId>)>, Error> {
		let mut res = vec![];
		let mut count = 0;
		while limit > count {
			if let Some(thg) = self.res.pop_front() {
				res.push((thg, None));
				count += 1;
			} else {
				break;
			}
		}
		Ok(res)
	}
}
