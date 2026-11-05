//! Limit operator - applies LIMIT and OFFSET (START) to a stream.

use crate::val::{Number, Value};

/// Which paging clause a count was written for. Only the error it raises
/// depends on this; the contract below is the same for both.
#[derive(Clone, Copy, Debug)]
pub(crate) enum PagingClause {
	Limit,
	Start,
}

impl PagingClause {
	fn rejects(self, value: String) -> anyhow::Error {
		match self {
			PagingClause::Limit => anyhow::Error::new(crate::exec::Error::InvalidLimit {
				value,
			}),
			PagingClause::Start => anyhow::Error::new(crate::exec::Error::InvalidStart {
				value,
			}),
		}
	}
}

/// The row count a paging clause names.
///
/// A count is written as a non-negative integer that fits in a `u32`, and
/// every other value is rejected rather than read as one: a float or decimal
/// is not truncated, `NONE` and `NULL` are not read as zero, and a count too
/// large to fit is an error rather than a wrap — so the answer does not depend
/// on the width of a pointer on the machine running the query.
pub(crate) fn paging_count(value: &Value, clause: PagingClause) -> Result<u32, anyhow::Error> {
	if let Value::Number(Number::Int(count)) = value
		&& *count >= 0
	{
		return u32::try_from(*count).map_err(|_| clause.rejects(count.to_string()));
	}
	Err(clause.rejects(value.clone().into_raw_string()))
}
