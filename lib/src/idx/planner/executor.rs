use crate::err::Error;
use crate::idx::planner::tree::IndexMap;
use crate::sql::{Expression, Value};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct QueryExecutor {
	inner: Arc<Inner>,
}

struct Inner {
	index_map: IndexMap,
	pre_match: Option<Expression>,
}

impl QueryExecutor {
	pub(super) fn new(index_map: IndexMap, pre_match: Option<Expression>) -> Self {
		Self {
			inner: Arc::new(Inner {
				index_map,
				pre_match,
			}),
		}
	}

	pub(crate) fn matches(&self, exp: &Expression) -> Result<Value, Error> {
		if let Some(pre_match) = &self.inner.pre_match {
			if pre_match.eq(exp) {
				return Ok(Value::Bool(true));
			}
		}
		// TODO - check with index
		todo!()
	}
}
