use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::tree::IndexMap;
use crate::idx::IndexKeyBase;
use crate::sql::index::Index;
use crate::sql::{Expression, Table, Thing, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct QueryExecutor {
	inner: Arc<Inner>,
}

struct Inner {
	table: String,
	index_map: IndexMap,
	pre_match: Option<Expression>,
	ft_map: HashMap<String, FtIndex>,
	terms: HashMap<MatchRef, (String, Vec<TermId>)>,
}

impl QueryExecutor {
	pub(super) async fn new(
		opt: &Options,
		txn: &Transaction,
		table: &Table,
		index_map: IndexMap,
		pre_match: Option<Expression>,
	) -> Result<Self, Error> {
		let mut run = txn.lock().await;
		let mut ft_map = HashMap::new();
		for ios in index_map.index.values() {
			for io in ios {
				if let Index::Search {
					az,
					order,
					sc,
					hl,
				} = &io.ix.index
				{
					if !ft_map.contains_key(&io.ix.name.0) {
						let ikb = IndexKeyBase::new(opt, &io.ix);
						let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
						let ft = FtIndex::new(&mut run, az, ikb, *order, sc, *hl).await?;
						ft_map.insert(io.ix.name.0.clone(), ft);
					}
				}
			}
		}
		let mut terms = HashMap::with_capacity(index_map.terms.len());
		for (mr, (ix, qs)) in &index_map.terms {
			if let Some(ft) = ft_map.get(ix) {
				let term_ids = ft.extract_terms(&mut run, qs.clone()).await?;
				terms.insert(*mr, (ix.to_owned(), term_ids));
			}
		}
		Ok(Self {
			inner: Arc::new(Inner {
				table: table.0.clone(),
				index_map,
				pre_match,
				ft_map,
				terms,
			}),
		})
	}

	pub(crate) async fn matches(
		&self,
		txn: &Transaction,
		thg: Option<&Thing>,
		exp: &Expression,
	) -> Result<Value, Error> {
		// If we find the expression in `pre_match`,
		// it means that we are using an Iterator::Index
		// and we are iterating over document that already matches the expression.
		if let Some(pre_match) = &self.inner.pre_match {
			if pre_match.eq(exp) {
				return Ok(Value::Bool(true));
			}
		}

		// Otherwise, we look for the first possible index options, and evaluate the expression
		if let Some(thg) = thg {
			// Does the record id match this executor's table?
			if thg.tb.eq(&self.inner.table) {
				if let Some(ios) = self.inner.index_map.index.get(exp) {
					for io in ios {
						if let Some(fti) = self.inner.ft_map.get(&io.ix.name.0) {
							let mut run = txn.lock().await;
							// TODO The query string could be extracted when IndexOptions are created
							let query_string = io.v.clone().convert_to_string()?;
							return Ok(Value::Bool(
								fti.match_id_value(&mut run, thg, &query_string).await?,
							));
						}
					}
				}
			}
		}

		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	pub(crate) async fn highlight(
		&self,
		txn: &Transaction,
		thg: Option<&Thing>,
		prefix: Value,
		suffix: Value,
		match_ref: Value,
		doc: Value,
	) -> Result<Value, Error> {
		let mut tx = txn.lock().await;
		if let Some(thg) = thg {
			if let Value::Number(n) = match_ref {
				let m = n.as_int() as u8;
				if let Some((ix, terms)) = self.inner.terms.get(&m) {
					if let Some(ft) = self.inner.ft_map.get(ix) {
						return ft.highlight(&mut tx, thg, terms, prefix, suffix, doc).await;
					}
				}
			}
		}
		Ok(doc)
	}
}
