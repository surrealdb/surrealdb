use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::FtIndex;
use crate::idx::planner::plan::IndexOption;
use crate::idx::planner::tree::IndexMap;
use crate::idx::IndexKeyBase;
use crate::sql::index::Index;
use crate::sql::{Expression, Thing, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct QueryExecutor {
	inner: Arc<Inner>,
}

struct Inner {
	index_map: IndexMap,
	pre_match: Option<Expression>,
	ft_map: HashMap<String, FtIndex>,
}

impl QueryExecutor {
	pub(super) async fn new(
		opt: &Options,
		txn: &Transaction,
		index_map: IndexMap,
		pre_match: Option<Expression>,
	) -> Result<Self, Error> {
		let mut run = txn.lock().await;
		let mut ft_map = HashMap::new();
		for (_, ios) in &index_map {
			for io in ios {
				if let Index::Search {
					order,
					..
				} = &io.ix.index
				{
					if !ft_map.contains_key(&io.ix.name.0) {
						let ikb = IndexKeyBase::new(opt, &io.ix);
						let ft = FtIndex::new(&mut run, ikb, order.to_usize()).await?;
						ft_map.insert(io.ix.name.0.clone(), ft);
					}
				}
			}
		}
		Ok(Self {
			inner: Arc::new(Inner {
				index_map,
				pre_match,
				ft_map,
			}),
		})
	}

	pub(crate) fn matches(&self, rid: Option<&Thing>, exp: &Expression) -> Result<Value, Error> {
		// If we find the expression in `pre_match`,
		// it means that we are using an Iterator::Index
		// and we are iterating over document that already matches the expression.
		if let Some(pre_match) = &self.inner.pre_match {
			if pre_match.eq(exp) {
				return Ok(Value::Bool(true));
			}
		}
		// Otherwise, we look for the first possible index options, and evaluate the expression
		if let Some(ios) = self.inner.index_map.get(exp) {
			for io in ios {
				return Ok(Value::Bool(self.matches_index(io, rid)));
			}
		}
		// If not previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundOnMatch {
			value: exp.to_string(),
		})
	}

	fn matches_index(&self, _io: &IndexOption, rid: Option<&Thing>) -> bool {
		if let Some(rid) = rid {
			if let Some(_fti) = self.inner.ft_map.get(&rid.tb) {
				todo!()
			}
		}
		false
	}
}
