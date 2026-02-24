use std::any::Any;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use common::id;
use common::span::Span;

mod ast;
mod collections;

pub use ast::Ast;

id!(NodeId<T>);

impl<T: Any> NodeId<T> {
	fn index<'a, L: NodeLibrary>(self, ast: &'a Ast<L>) -> &'a T {
		&ast[self]
	}
}

/// Trait for types which contain ast nodes.
pub trait NodeCollection<T> {
	fn get_node(&self, idx: u32) -> Option<&T>;

	// TODO: Move this method out of this trait.
	fn get_mut_node(&mut self, idx: u32) -> Option<&mut T>;
}

/// Trait for types which contain ast nodes as a vector.
pub trait NodeVec<T: Node>: NodeCollection<T> {
	fn insert_node(&mut self, value: T) -> u32;
}

/// Trait for types which contain ast nodes as hash-consed set.
pub trait NodeSet<T: UniqueNode>: NodeCollection<T> {
	fn insert_node(&mut self, value: T) -> u32;
}

/// Trait for types which can be part of the ast.
pub trait Node: Any {}
/// Trait for types which can be part of the ast in a hash-consed set.
pub trait UniqueNode: Any + Eq + Hash {}

#[derive(Debug, Clone, Copy)]
pub struct Spanned<T> {
	pub value: T,
	pub span: Span,
}

impl<T: Node> Node for Spanned<T> {}

impl<T> Deref for Spanned<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.value
	}
}
impl<T> DerefMut for Spanned<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.value
	}
}

/// A struct for a linked list of nodes.
#[derive(Debug)]
pub struct NodeList<T> {
	pub cur: NodeId<T>,
	pub next: Option<NodeListId<T>>,
}

impl<T: Node> Node for NodeList<T> {}

/// An id for a linked list of nodes.
#[derive(Eq, PartialEq, Hash, Debug)]
pub struct NodeListId<T>(pub NodeId<NodeList<T>>);

impl<T: Any> NodeListId<T> {
	fn index<'a, L: NodeLibrary>(self, ast: &'a Ast<L>) -> &'a NodeList<T> {
		&ast[self]
	}
}

impl<T> Clone for NodeListId<T> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<T> Copy for NodeListId<T> {}
impl<T: Node> Node for NodeListId<T> {}

/// Trait for types which have an ast, possibly needing access to the full ast to calculate.
pub trait AstSpan {
	fn ast_span<L: NodeLibrary>(&self, ast: &Ast<L>) -> Span;
}

impl AstSpan for Span {
	fn ast_span<L: NodeLibrary>(&self, _: &Ast<L>) -> Span {
		self.clone()
	}
}

impl<T> AstSpan for Spanned<T> {
	fn ast_span<L: NodeLibrary>(&self, _: &Ast<L>) -> Span {
		self.span
	}
}

impl<T: AstSpan + Node> AstSpan for NodeId<T> {
	fn ast_span<L: NodeLibrary>(&self, ast: &Ast<L>) -> Span {
		ast[*self].ast_span(ast)
	}
}

impl<T: AstSpan + Node> AstSpan for NodeListId<T> {
	fn ast_span<L: NodeLibrary>(&self, ast: &Ast<L>) -> Span {
		let start = self.index(ast).cur.index(ast).ast_span(ast);
		let end = ast.iter_list(Some(*self)).last().unwrap();
		start.extend(end.ast_span(ast))
	}
}

pub trait NodeLibrary {
	fn empty() -> Self;

	fn get<T: Any>(&self, id: NodeId<T>) -> Option<&T>;

	fn get_mut<T: Any>(&mut self, id: NodeId<T>) -> Option<&mut T>;

	fn insert<T: Node>(&mut self, value: T) -> NodeId<T>;

	fn insert_set<T: UniqueNode>(&mut self, value: T) -> NodeId<T>;

	fn clear(&mut self);
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
                let type_id = const { std::any::TypeId::of::<T>() };
                $(
                    if const{ std::any::TypeId::of::<$ty>() } == type_id{
                        unsafe{
                            let cntr = std::mem::transmute::<&$container<$ty>,&$container<T>>(&self.$field);
                            return $crate::types::NodeCollection::<T>::get_node(cntr,id.into_u32());
                        }
                    } else

                )*
				{
					panic!("type '{}' not part of node library",std::any::type_name::<T>());
				}
            }

            fn get_mut<T: ::std::any::Any>(&mut self, idx: NodeId<T>) -> Option<&mut T>{
                let type_id = const { std::any::TypeId::of::<T>() };
                $(
                    if const{ std::any::TypeId::of::<$ty>() } == type_id{
                        unsafe{
                            let cntr = std::mem::transmute::<&mut $container<$ty>,&mut $container<T>>(&mut self.$field);
                            return $crate::types::NodeCollection::<T>::get_mut_node(cntr,idx.into_u32());
                        }
                    } else
                )*
				{
					panic!("type '{}' not part of node library",std::any::type_name::<T>());
				}
            }

            fn insert<T: crate::types::Node>(&mut self, value: T) -> NodeId<T>{
                let type_id = const { std::any::TypeId::of::<T>() };
                $(
					library!{@push $($field_meta)?, $ty, $container,self.$field = type_id <= value}
                )*
				{
					panic!("type '{}' not part of node library",std::any::type_name::<T>());
				}
            }

            fn insert_set<T: crate::types::UniqueNode>(&mut self, value: T) -> NodeId<T>{
                let type_id = const { std::any::TypeId::of::<T>() };
                $(
					library!{@push_set $($field_meta)?, $ty, $container,self.$field = type_id <= value}
                )*
				{
					panic!("type '{}' not part of node library",std::any::type_name::<T>());
				}
            }

            fn clear(&mut self){
                $(
                    self.$field.clear();
                )*
            }

        }

    };

	(@push , $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
		if const{ std::any::TypeId::of::<$ty>() } == $ty_id{
			unsafe{
				let cntr = std::mem::transmute::<&mut $container<$ty>,&mut $container<T>>(&mut $this.$field);
				let idx = <$container<T> as $crate::types::NodeVec::<T>>::insert_node(cntr,$value);
				return NodeId::<T>::from_u32(idx).expect("Too many ast nodes")
			}
		}
	};

	(@push set, $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
		if const{ std::any::TypeId::of::<$ty>() } == $ty_id{
			panic!("tried to push type `{}` as part of a set which is not a unique node",std::any::type_name::<$ty>())
		}
	};

	(@push_set, $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
		if const{ std::any::TypeId::of::<$ty>() } == $ty_id{
			panic!("tried to push type `{}` as part of a vector which is a unique node",std::any::type_name::<$ty>())
		}
	};

	(@push_set set, $ty:ty, $container:ident, $this:ident.$field:ident = $ty_id:ident <= $value:ident) => {
		if const{ std::any::TypeId::of::<$ty>() } == $ty_id{
			unsafe{
				let cntr = std::mem::transmute::<&mut $container<$ty>,&mut $container<T>>(&mut $this.$field);
				let idx = <$container<T> as $crate::types::NodeSet::<T>>::insert_node(cntr,$value);
				return NodeId::<T>::from_u32(idx).expect("Too many ast nodes")
			}
		}
	};
}
