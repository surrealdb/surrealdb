use crate::sql::idiom::Idiom;
use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Operation {
	pub op: Op,
	pub path: Idiom,
	pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub enum Op {
	None,
	Add,
	Remove,
	Replace,
	Change,
}

impl Default for Op {
	fn default() -> Op {
		Op::Add
	}
}

impl From<&Value> for Op {
	fn from(v: &Value) -> Self {
		match v.to_strand().as_str() {
			"add" => Op::Add,
			"remove" => Op::Remove,
			"replace" => Op::Replace,
			"change" => Op::Change,
			_ => Op::None,
		}
	}
}
