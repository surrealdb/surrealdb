use super::RecordIdKeyLit;
use crate::err::Error;
use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdKeyRangeLit {
	pub start: Bound<RecordIdKeyLit>,
	pub end: Bound<RecordIdKeyLit>,
}

impl From<RecordIdKeyRangeLit> for crate::expr::RecordIdKeyRangeLit {
	fn from(value: RecordIdKeyRangeLit) -> Self {
		crate::expr::RecordIdKeyRangeLit {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl From<crate::expr::RecordIdKeyRangeLit> for RecordIdKeyRangeLit {
	fn from(value: crate::expr::RecordIdKeyRangeLit) -> Self {
		crate::expr::RecordIdKeyRangeLit {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl fmt::Display for RecordIdKeyRangeLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.start {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(v) => write!(f, "{v}"),
			Bound::Excluded(v) => write!(f, "{v}>"),
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(v) => write!(f, "..{v}"),
			Bound::Included(v) => write!(f, "..={v}"),
		}?;
		Ok(())
	}
}
