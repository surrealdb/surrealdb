use super::SqlValue;
use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{
		Array, FlowResultExt as _, Part,
		part::{RecurseInstruction, RecursionPlan},
	},
};
use anyhow::{Result, bail};
use reblessive::tree::Stk;

#[derive(Clone, Copy, Debug)]
pub struct Recursion<'a> {
	pub min: u32,
	pub max: Option<u32>,
	pub iterated: u32,
	pub current: &'a SqlValue,
	pub path: &'a [Part],
	pub plan: Option<&'a RecursionPlan>,
	pub instruction: Option<&'a RecurseInstruction>,
}

impl<'a> Recursion<'a> {
	pub fn with_iterated(self, iterated: u32) -> Self {
		Self {
			iterated,
			..self
		}
	}

	pub fn with_current(self, current: &'a SqlValue) -> Self {
		Self {
			current,
			..self
		}
	}
}

// Method used to check if the value
// inside a recursed idiom path is final
pub(crate) fn is_final(v: &SqlValue) -> bool {
	match v {
		SqlValue::None => true,
		SqlValue::Null => true,
		SqlValue::Array(v) => v.is_empty() || v.is_all_none_or_null(),
		_ => false,
	}
}

pub(crate) fn get_final(v: &SqlValue) -> SqlValue {
	match v {
		SqlValue::Array(_) => SqlValue::Array(Array(vec![])),
		SqlValue::Null => SqlValue::Null,
		_ => SqlValue::None,
	}
}

pub(crate) fn clean_iteration(v: SqlValue) -> SqlValue {
	if let SqlValue::Array(v) = v {
		SqlValue::from(v.0.into_iter().filter(|v| !is_final(v)).collect::<Vec<SqlValue>>()).flatten()
	} else {
		v
	}
}
