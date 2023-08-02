use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::executor::fulltext::FtEntry;
use crate::idx::planner::executor::mtree::MtEntry;
use crate::idx::planner::iterators::{
	NonUniqueEqualThingIterator, ThingIterator, UniqueEqualThingIterator,
};
use crate::idx::planner::plan::IndexOption;
use crate::idx::planner::tree::IndexMap;
use crate::idx::trees::mtree::MTreeIndex;
use crate::sql::index::Index;
use crate::sql::{Expression, Operator, Table, Thing, Value};
use std::collections::HashMap;

mod fulltext;
mod mtree;

pub(crate) type IteratorRef = u16;

pub(crate) struct QueryExecutor {
	table: String,
	// FullText
	ft_map: HashMap<String, FtIndex>,
	ft_mr: HashMap<MatchRef, FtEntry>,
	ft_exp: HashMap<Expression, FtEntry>,
	// MTrees
	mt_map: HashMap<String, MTreeIndex>,
	mt_exp: HashMap<Expression, MtEntry>,
	// Iterators
	iterators: Vec<Expression>,
}

impl QueryExecutor {
	pub(super) async fn new(
		opt: &Options,
		txn: &Transaction,
		table: &Table,
		index_map: IndexMap,
	) -> Result<Self, Error> {
		let mut run = txn.lock().await;

		let mut exe = Self {
			table: table.0.clone(),
			ft_map: HashMap::default(),
			ft_mr: HashMap::default(),
			ft_exp: HashMap::default(),
			mt_map: HashMap::default(),
			mt_exp: HashMap::default(),
			iterators: Vec::new(),
		};

		// Create all the instances of FtIndex
		// Build the FtEntries and map them to Expressions and MatchRef
		for (exp, io) in index_map.consume() {
			match &io.ix().index {
				Index::Search(p) => exe.check_search_entry(opt, &mut run, &io, exp, p).await?,
				Index::MTree(p) => exe.check_mtree_entry(opt, &mut run, &io, exp, p).await?,
				_ => {}
			}
		}
		Ok(exe)
	}

	pub(super) fn add_iterator(&mut self, exp: Expression) -> IteratorRef {
		let ir = self.iterators.len();
		self.iterators.push(exp);
		ir as IteratorRef
	}

	pub(crate) fn is_distinct(&self, ir: IteratorRef) -> bool {
		(ir as usize) < self.iterators.len()
	}

	pub(crate) fn get_iterator_expression(&self, ir: IteratorRef) -> Option<&Expression> {
		self.iterators.get(ir as usize)
	}

	pub(crate) async fn new_iterator(
		&self,
		opt: &Options,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		match &io.ix().index {
			Index::Idx => Self::new_index_iterator(opt, io),
			Index::Uniq => Self::new_unique_index_iterator(opt, io),
			Index::Search {
				..
			} => self.new_search_index_iterator(ir, io).await,
			Index::MTree {
				..
			} => self.new_mtree_index_iterator(ir, io).await,
		}
	}

	fn new_index_iterator(opt: &Options, io: IndexOption) -> Result<Option<ThingIterator>, Error> {
		if io.op() == &Operator::Equal {
			return Ok(Some(ThingIterator::NonUniqueEqual(NonUniqueEqualThingIterator::new(
				opt,
				io.ix(),
				io.array(),
			)?)));
		}
		Ok(None)
	}

	fn new_unique_index_iterator(
		opt: &Options,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if io.op() == &Operator::Equal {
			return Ok(Some(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(
				opt,
				io.ix(),
				io.array(),
			)?)));
		}
		Ok(None)
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
}
