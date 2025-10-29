use common::ids::IdSet;

use super::{Node, NodeCollection, NodeSet, NodeVec, UniqueNode};

impl<T> NodeCollection<T> for Vec<T> {
	fn get_node(&self, idx: u32) -> Option<&T> {
		self.get(idx as usize)
	}

	fn get_mut_node(&mut self, idx: u32) -> Option<&mut T> {
		self.get_mut(idx as usize)
	}
}

impl<T: Node> NodeVec<T> for Vec<T> {
	fn insert_node(&mut self, value: T) -> u32 {
		let len = u32::try_from(self.len()).expect("Too many nodes");
		self.push(value);
		len
	}
}

impl<T> NodeCollection<T> for IdSet<u32, T> {
	fn get_node(&self, idx: u32) -> Option<&T> {
		self.get(idx)
	}

	fn get_mut_node(&mut self, idx: u32) -> Option<&mut T> {
		self.get_mut(idx)
	}
}

impl<T: UniqueNode> NodeSet<T> for IdSet<u32, T> {
	fn insert_node(&mut self, value: T) -> u32 {
		self.push(value).expect("Too many nodes")
	}
}
