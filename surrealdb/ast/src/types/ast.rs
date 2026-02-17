use std::any::Any;
use std::ops::{Index, IndexMut};

use crate::types::{NodeLibrary, UniqueNode};
use crate::{Node, NodeId, NodeList, NodeListId};

#[derive(Debug)]
pub struct Ast<L> {
	library: L,
}

impl<L: NodeLibrary> Ast<L> {
	pub fn empty() -> Self {
		Ast {
			library: L::empty(),
		}
	}

	pub fn push<T: Node>(&mut self, value: T) -> NodeId<T> {
		self.library.insert(value)
	}

	pub fn push_set<T: UniqueNode>(&mut self, value: T) -> NodeId<T> {
		self.library.insert_set(value)
	}

	pub fn push_list<T: Node>(
		&mut self,
		value: T,
		head: &mut Option<NodeListId<T>>,
		tail: &mut Option<NodeListId<T>>,
	) {
		let node = self.push(value);
		let list_entry = NodeListId(self.push(NodeList {
			cur: node,
			next: None,
		}));

		if head.is_none() {
			*head = Some(list_entry)
		}

		if let Some(prev) = tail.replace(list_entry) {
			self[prev].next = Some(list_entry);
		}
	}

	pub fn iter_list<'a, T: Any>(&'a self, first: Option<NodeListId<T>>) -> ListIter<'a, T, L> {
		ListIter {
			ast: self,
			cur: first,
		}
	}

	pub fn clear(&mut self) {
		self.library.clear();
	}
}

impl<T: Any, L: NodeLibrary> Index<NodeId<T>> for Ast<L> {
	type Output = T;

	fn index(&self, index: NodeId<T>) -> &Self::Output {
		self.library.get(index).expect("Tried to access node in ast which did not exist")
	}
}

impl<T: Any, L: NodeLibrary> IndexMut<NodeId<T>> for Ast<L> {
	fn index_mut(&mut self, index: NodeId<T>) -> &mut Self::Output {
		self.library.get_mut(index).expect("Tried to access node in ast which did not exist")
	}
}

impl<T: Any, L: NodeLibrary> Index<NodeListId<T>> for Ast<L> {
	type Output = NodeList<T>;

	fn index(&self, index: NodeListId<T>) -> &Self::Output {
		self.library.get(index.0).expect("Tried to access node in ast which did not exist")
	}
}

impl<T: Any, L: NodeLibrary> IndexMut<NodeListId<T>> for Ast<L> {
	fn index_mut(&mut self, index: NodeListId<T>) -> &mut Self::Output {
		self.library.get_mut(index.0).expect("Tried to access node in ast which did not exist")
	}
}

pub struct ListIter<'a, T, L> {
	ast: &'a Ast<L>,
	cur: Option<NodeListId<T>>,
}

impl<'a, T, L> Iterator for ListIter<'a, T, L>
where
	T: Any,
	L: NodeLibrary,
{
	type Item = NodeId<T>;

	fn next(&mut self) -> Option<Self::Item> {
		let n = &self.ast[self.cur?];
		self.cur = n.next;
		Some(n.cur)
	}
}
