use common::id;
use std::{any::Any, hash::Hash};

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

pub struct NodeList<T> {
	cur: NodeId<T>,
	next: Option<NodeId<T>>,
}

pub struct NodeListId<T>(pub NodeId<NodeList<T>>);

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

	pub fn clear(&mut self) {
		self.library.clear();
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
