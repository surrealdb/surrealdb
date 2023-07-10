use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::docids::{DocId, NO_DOC_ID};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator, MatchRef};
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::IndexKeyBase;
use crate::key;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Expression, Ident, Idiom, Object, Operator, Thing, Value};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Default)]
pub(super) struct PlanBuilder {
	indexes: Vec<(Expression, IndexOption)>,
}

impl PlanBuilder {
	pub(super) fn add_index_option(&mut self, e: Expression, i: IndexOption) {
		self.indexes.push((e, i));
	}

	pub(super) fn build(mut self) -> Result<Plan, Error> {
		// TODO select the best option if there are several (cost based)
		if let Some((e, i)) = self.indexes.pop() {
			Ok(Plan::new(e, i))
		} else {
			Err(Error::BypassQueryPlanner)
		}
	}
}

pub(crate) struct Plan {
	pub(super) e: Expression,
	pub(super) i: IndexOption,
}

impl Plan {
	pub(super) fn new(e: Expression, i: IndexOption) -> Self {
		Self {
			e,
			i,
		}
	}

	pub(crate) async fn new_iterator(
		&self,
		opt: &Options,
		txn: &Transaction,
		exe: &QueryExecutor,
	) -> Result<ThingIterator, Error> {
		self.i.new_iterator(opt, txn, exe).await
	}

	pub(crate) fn explain(&self) -> Value {
		Value::Object(Object::from(HashMap::from([
			("index", Value::from(self.i.ix().name.0.to_owned())),
			("operator", Value::from(self.i.op().to_string())),
			("value", self.i.value().clone()),
		])))
	}
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ix: DefineIndexStatement,
	id: Idiom,
	v: Value,
	qs: Option<String>,
	op: Operator,
	mr: Option<MatchRef>,
}

impl IndexOption {
	pub(super) fn new(
		ix: DefineIndexStatement,
		id: Idiom,
		op: Operator,
		v: Value,
		qs: Option<String>,
		mr: Option<MatchRef>,
	) -> Self {
		Self(Arc::new(Inner {
			ix,
			id,
			op,
			v,
			qs,
			mr,
		}))
	}

	pub(super) fn ix(&self) -> &DefineIndexStatement {
		&self.0.ix
	}

	pub(super) fn op(&self) -> &Operator {
		&self.0.op
	}

	pub(super) fn value(&self) -> &Value {
		&self.0.v
	}

	pub(super) fn qs(&self) -> Option<&String> {
		self.0.qs.as_ref()
	}

	pub(super) fn id(&self) -> &Idiom {
		&self.0.id
	}

	pub(super) fn match_ref(&self) -> Option<&MatchRef> {
		self.0.mr.as_ref()
	}

	async fn new_iterator(
		&self,
		opt: &Options,
		txn: &Transaction,
		exe: &QueryExecutor,
	) -> Result<ThingIterator, Error> {
		match &self.ix().index {
			Index::Idx => {
				if self.op() == &Operator::Equal {
					return Ok(ThingIterator::NonUniqueEqual(NonUniqueEqualThingIterator::new(
						opt,
						self.ix(),
						self.value(),
					)?));
				}
			}
			Index::Uniq => {
				if self.op() == &Operator::Equal {
					return Ok(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(
						opt,
						self.ix(),
						self.value(),
					)?));
				}
			}
			Index::Search {
				az,
				hl,
				sc,
				order,
			} => {
				if let Operator::Matches(_) = self.op() {
					let td = exe.pre_match_terms_docs();
					return Ok(ThingIterator::Matches(
						MatchesThingIterator::new(opt, txn, self.ix(), az, *hl, sc, *order, td)
							.await?,
					));
				}
			}
		}
		Err(Error::BypassQueryPlanner)
	}
}

pub(crate) enum ThingIterator {
	NonUniqueEqual(NonUniqueEqualThingIterator),
	UniqueEqual(UniqueEqualThingIterator),
	Matches(MatchesThingIterator),
}

impl ThingIterator {
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		size: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		match self {
			ThingIterator::NonUniqueEqual(i) => i.next_batch(tx, size).await,
			ThingIterator::UniqueEqual(i) => i.next_batch(tx, size).await,
			ThingIterator::Matches(i) => i.next_batch(tx, size).await,
		}
	}
}

pub(crate) struct NonUniqueEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl NonUniqueEqualThingIterator {
	fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		v: &Value,
	) -> Result<NonUniqueEqualThingIterator, Error> {
		let v = Array::from(v.clone());
		let (beg, end) =
			key::index::Index::range_all_ids(opt.ns(), opt.db(), &ix.what, &ix.name, &v);
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

pub(crate) struct UniqueEqualThingIterator {
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	fn new(opt: &Options, ix: &DefineIndexStatement, v: &Value) -> Result<Self, Error> {
		let v = Array::from(v.clone());
		let key = key::index::Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, v, None).into();
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

pub(crate) struct MatchesThingIterator {
	hits: Option<HitsIterator>,
}

impl MatchesThingIterator {
	#[allow(clippy::too_many_arguments)]
	async fn new(
		opt: &Options,
		txn: &Transaction,
		ix: &DefineIndexStatement,
		az: &Ident,
		hl: bool,
		sc: &Scoring,
		order: u32,
		terms_docs: Option<TermsDocs>,
	) -> Result<Self, Error> {
		let ikb = IndexKeyBase::new(opt, ix);
		if let Scoring::Bm {
			..
		} = sc
		{
			let mut run = txn.lock().await;
			let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
			let fti = FtIndex::new(&mut run, az, ikb, order, sc, hl).await?;
			if let Some(terms_docs) = terms_docs {
				let hits = fti.new_hits_iterator(&mut run, terms_docs).await?;
				Ok(Self {
					hits,
				})
			} else {
				Ok(Self {
					hits: None,
				})
			}
		} else {
			Err(Error::FeatureNotYetImplemented {
				feature: "Vector Search",
			})
		}
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

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::IndexOption;
	use crate::sql::statements::DefineIndexStatement;
	use crate::sql::{Idiom, Operator, Value};
	use std::collections::HashSet;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			Operator::Equal,
			Value::from("test"),
			None,
			None,
		);

		let io2 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			Operator::Equal,
			Value::from("test"),
			None,
			None,
		);

		set.insert(io1);
		set.insert(io2.clone());
		set.insert(io2);

		assert_eq!(set.len(), 1);
	}
}
