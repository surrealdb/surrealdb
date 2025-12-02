use common::id;
use std::{
	any::Any,
	hash::Hash,
	ops::{Index, IndexMut},
};

mod collections;

id!(NodeId<T>);

pub trait NodeCollection<T> {
	fn get_node(&self, idx: u32) -> Option<&T>;

	// TODO: Move this method out of this trait.
	fn get_mut_node(&mut self, idx: u32) -> Option<&mut T>;
}

pub trait NodeVec<T: Node>: NodeCollection<T> {
	fn insert_node(&mut self, value: T) -> u32;
}

pub trait NodeSet<T: UniqueNode>: NodeCollection<T> {
	fn insert_node(&mut self, value: T) -> u32;
}

pub trait Node: Any {}
pub trait UniqueNode: Any + Eq + Hash {}

#[derive(Debug)]
pub struct NodeList<T> {
	pub cur: NodeId<T>,
	pub next: Option<NodeListId<T>>,
}

impl<T: Node> Node for NodeList<T> {}

/// An id for a linked list of nodes.
#[derive(Eq, PartialEq, Hash, Debug)]
pub struct NodeListId<T>(pub NodeId<NodeList<T>>);
impl<T> Clone for NodeListId<T> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<T> Copy for NodeListId<T> {}

pub trait NodeLibrary {
	fn empty() -> Self;

	fn get<T: Any>(&self, id: NodeId<T>) -> Option<&T>;

	fn get_mut<T: Any>(&mut self, id: NodeId<T>) -> Option<&mut T>;

	fn insert<T: Node>(&mut self, value: T) -> NodeId<T>;

	fn insert_set<T: UniqueNode>(&mut self, value: T) -> NodeId<T>;

	fn clear(&mut self);
}

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

		if tail.is_none() {
			*tail = Some(list_entry)
		}

		if let Some(prev) = head.replace(list_entry) {
			self[prev].next = Some(list_entry);
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

#[macro_export]
macro_rules! library {
    (
        $(#[$m:meta])*
        $name:ident {
			$(
				$(#[$field_meta:ident])*
				$field:ident: $container:ident<$ty:ty>
			),*
			$(,)?
		}
    ) => {

        $(#[$m])*
        pub struct $name{
            $(
                pub $field: $container<$ty>
            ),*
        }

        impl $crate::types::NodeLibrary for $name{
            fn empty() -> Self{
                $name{
                    $($field: $container::new()),*
                }
            }

			fn get<T: ::std::any::Any>(&self, id: NodeId<T>) -> Option<&T>{
                let type_id = std::any::TypeId::of::<T>();
                $(
                    if std::any::TypeId::of::<$ty>() == type_id{
                        unsafe{
                            let cntr = std::mem::transmute::<&$container<$ty>,&$container<T>>(&self.$field);
                            return $crate::types::NodeCollection::<T>::get_node(cntr,id.into_u32());
                        }
                    }

                )*

                panic!("type '{}' not part of node library",std::any::type_name::<T>());
            }

            fn get_mut<T: ::std::any::Any>(&mut self, idx: NodeId<T>) -> Option<&mut T>{
                let type_id = std::any::TypeId::of::<T>();
                $(
                    if std::any::TypeId::of::<$ty>() == type_id{
                        unsafe{
                            let cntr = std::mem::transmute::<&mut $container<$ty>,&mut $container<T>>(&mut self.$field);
                            return $crate::types::NodeCollection::<T>::get_mut_node(cntr,idx.into_u32());
                        }
                    }
                )*
                panic!("type '{}' not part of node library",std::any::type_name::<T>());
            }

            fn insert<T: crate::types::Node>(&mut self, value: T) -> NodeId<T>{
                let type_id = std::any::TypeId::of::<T>();
                $(
					library!{@push $($field_meta)?, $ty, $container,self.$field = type_id <= value}
                )*
                panic!("type '{}' not part of node library",std::any::type_name::<T>());
            }

            fn insert_set<T: crate::types::UniqueNode>(&mut self, value: T) -> NodeId<T>{
                let type_id = std::any::TypeId::of::<T>();
                $(
					library!{@push_set $($field_meta)?, $ty, $container,self.$field = type_id <= value}
                )*
                panic!("type '{}' not part of node library",std::any::type_name::<T>());
            }

            fn clear(&mut self){
                $(
                    self.$field.clear();
                )*
            }

        }

    };

	(@push , $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
		if std::any::TypeId::of::<$ty>() == $ty_id{
			unsafe{
				let cntr = std::mem::transmute::<&mut $container<$ty>,&mut $container<T>>(&mut $this.$field);
				let idx = <$container<T> as $crate::types::NodeVec::<T>>::insert_node(cntr,$value);
				return NodeId::<T>::from_u32(idx).expect("Too many ast nodes")
			}
		}
	};

	(@push set, $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
	};

	(@push_set, $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
	};

	(@push_set set, $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
		if std::any::TypeId::of::<$ty>() == $ty_id{
			unsafe{
				let cntr = std::mem::transmute::<&mut $container<$ty>,&mut $container<T>>(&mut $this.$field);
				let idx = <$container<T> as $crate::types::NodeSet::<T>>::insert_node(cntr,$value);
				return NodeId::<T>::from_u32(idx).expect("Too many ast nodes")
			}
		}
	};
}
