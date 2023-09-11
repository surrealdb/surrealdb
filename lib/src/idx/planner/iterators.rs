use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::docids::{DocId, NO_DOC_ID};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::key;
use crate::kvs::Key;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Thing};

pub(crate) enum ThingIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
	Matches(MatchesThingIterator),
}

impl ThingIterator {
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		size: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		match self {
			ThingIterator::IndexEqual(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueEqual(i) => i.next_batch(tx, size).await,
			ThingIterator::IndexRange(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueRange(i) => i.next_batch(tx, size).await,
			ThingIterator::Matches(i) => i.next_batch(tx, size).await,
		}
	}
}

pub(crate) struct IndexEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl IndexEqualThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, v: &Array) -> Result<Self, Error> {
		let (beg, end) = key::index::Index::range_all_ids(
			opt.ns(),
			opt.db(),
			&ix.what,
			&ix.name,
			v,
			true,
			v,
			true,
		);
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

pub(crate) struct IndexRangeThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
	beg_excl: Option<Vec<u8>>,
	end_excl: Option<Vec<u8>>,
}

impl IndexRangeThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		from: &RangeValue,
		to: &RangeValue,
	) -> Self {
		let (beg, end) = key::index::Index::range_all_ids(
			opt.ns(),
			opt.db(),
			&ix.what,
			&ix.name,
			&Array::from(from.value.to_owned()),
			from.inclusive,
			&Array::from(to.value.to_owned()),
			to.inclusive,
		);
		let beg_excl = if from.inclusive {
			Some(beg.clone())
		} else {
			None
		};
		let end_excl = if to.inclusive {
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
		let mut r = Vec::with_capacity(res.len());
		for (k, v) in res {
			if let Some(b) = &self.beg_excl {
				if b.eq(&k) {
					self.beg_excl = None;
					continue;
				}
			}
			if let Some(e) = &self.end_excl {
				if e.eq(&k) {
					self.end_excl = None;
					continue;
				}
			}
			r.push((v.into(), NO_DOC_ID));
		}
		Ok(r)
	}
}

pub(crate) struct UniqueEqualThingIterator {
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, a: &Array) -> Result<Self, Error> {
		let key = key::index::Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, a, None).into();
		Ok(Self {
			key: Some(key),
		})
	}

	async fn next_batch(
		&mut self,
		txn: &Transaction,
		_limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = txn.lock().await.get(key).await? {
				return Ok(vec![(val.into(), NO_DOC_ID)]);
			}
		}
		Ok(vec![])
	}
}

pub(crate) struct UniqueRangeThingIterator {}

impl UniqueRangeThingIterator {
	pub(super) fn new(
		_opt: &Options,
		_ix: &DefineIndexStatement,
		_from: &RangeValue,
		_to: &RangeValue,
	) -> Self {
		todo!()
	}
	async fn next_batch(
		&mut self,
		_txn: &Transaction,
		_limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		todo!()
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
