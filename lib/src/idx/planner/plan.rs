use crate::idx::planner::tree::Node;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Operator;

pub(crate) struct Plan {
	indexes: Vec<(DefineIndexStatement, Node, Operator)>,
}

impl Plan {
	pub(super) fn new() -> Self {
		Self {
			indexes: vec![],
		}
	}

	pub(super) fn add(&mut self, index: DefineIndexStatement, value: Node, operator: Operator) {
		self.indexes.push((index, value, operator));
	}
}
