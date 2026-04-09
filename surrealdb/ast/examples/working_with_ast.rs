use common::span::Span;
use surrealdb_ast::{Ast, Builtin, Ident, NodeId, NodeList, NodeListId, Spanned};

fn new_ident(ast: &mut Ast, ident: &str) -> Ident {
	let str = ast.push_set_entry(ident);
	Ident {
		text: str,
		span: Span::empty(),
	}
}

fn push_ident(ast: &mut Ast, txt: &str) -> NodeId<Ident> {
	let ident = new_ident(ast, txt);
	ast.push(ident)
}

pub fn main() {
	let mut ast = Ast::empty();

	// Working with single nodes.
	// Type annotation for visibilty, not required
	let int_index: NodeId<Spanned<f64>> = ast.push(Spanned {
		value: 1.0,
		span: Span::empty(),
	});

	println!("{:?}", int_index); // returns `NodeId{ index: 0 }`

	println!("{:?}", ast[int_index]); // actually debug prints the value of the node.

	// Transparently works with any pushed type.
	let bool_index = ast.push(Builtin::True(Span::empty()));
	println!("{:?}", ast[bool_index]);

	// Strings are hash-consed and need a different method.
	// These methods require an annotation.
	let string_idx_a: NodeId<String> = ast.push_set_entry("a string");
	let string_idx_b = ast.push_set_entry("a string");

	// Pushing the same string returns the same index.
	assert_eq!(string_idx_a, string_idx_b);

	// accessing is no different.
	println!("{}", ast[string_idx_a]);

	// Sequences of nodes use `NodeListId` and `NodeList`
	// Essentially creating a linked list of nodes.
	let ident = push_ident(&mut ast, "an ident");
	let head: NodeListId<Ident> = ast.push_list_item(NodeList {
		cur: ident,
		next: None,
	});

	let ident = push_ident(&mut ast, "an other ident");
	let next = ast.push_list_item(NodeList {
		cur: ident,
		next: None,
	});
	ast[head].next = Some(next);

	// accessing all entries in a list
	let mut cur = Some(head);
	while let Some(c) = cur {
		// Could also be written as `println!("{}",ast[ast[ast[c].cur].txt])"
		// But this is more readable.
		println!("{}", c.index(&ast).cur.index(&ast).text.index(&ast));
		cur = ast[c].next;
	}

	// Ast implements convenience functions for both patterns.
	// the 'head' is the index which points to the first entry
	let mut head = None;
	// the 'tail' is the index which points to last entry which would need to be update when
	// pushing a new item.
	let mut tail = None;

	let ident = new_ident(&mut ast, "yet another ident");
	ast.push_list(ident, &mut head, &mut tail);

	let ident = new_ident(&mut ast, "a final ident");
	ast.push_list(ident, &mut head, &mut tail);

	for n in ast.iter_list(head) {
		println!("{}", n.index(&ast).text.index(&ast));
	}
}
