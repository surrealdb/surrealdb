use crate::idx::planner::tree::IndexMap;
use crate::idx::planner::QueryPlanner;
use crate::sql::Table;
use std::collections::HashMap;

pub(crate) struct QueryExecutor {
	indexes: HashMap<Table, IndexMap>,
}

impl<'a> From<QueryPlanner<'a>> for QueryExecutor {
	fn from(value: QueryPlanner<'a>) -> Self {
		Self {
			indexes: value.indexes,
		}
	}
}
