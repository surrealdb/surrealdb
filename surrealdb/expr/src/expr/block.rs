use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{BinaryOperator, Expr, Literal, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Block(pub Vec<Expr>);

impl Deref for Block {
	type Target = [Expr];
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Block {
	/// Check if this block does only reads.
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl ToSql for Block {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.0.len() {
			0 => f.push_str("{;}"),
			1 => {
				let v = &self.0[0];
				if fmt.is_pretty() {
					// Pretty mode: use expanded format even for single element
					f.push('{');
					f.push('\n');
					f.push('\n');
					let fmt = fmt.increment();
					fmt.write_indent(f);
					if let Expr::Literal(Literal::RecordId(_)) = v {
						write_sql!(f, fmt, "({v})");
					} else if let Expr::Binary {
						left,
						op: BinaryOperator::Equal,
						..
					} = v && let Expr::Param(_) = **left
					{
						write_sql!(f, fmt, "({v})");
					} else {
						v.fmt_sql(f, fmt);
					}
					f.push('\n');
					// Write indent at the block's level
					if let SqlFormat::Indented(level) = fmt
						&& level > 0
					{
						for _ in 0..(level - 1) {
							f.push('\t');
						}
					}
					f.push('}')
				} else {
					// Non-pretty: compact format
					f.push_str("{ ");
					if let Expr::Literal(Literal::RecordId(_)) = v {
						write_sql!(f, fmt, "({v})");
					} else if let Expr::Binary {
						left,
						op: BinaryOperator::Equal,
						..
					} = v && let Expr::Param(_) = **left
					{
						write_sql!(f, fmt, "({v})");
					} else {
						v.fmt_sql(f, fmt);
					}
					f.push_str(" }");
				}
			}
			_ => {
				// Multi-element blocks
				if fmt.is_pretty() {
					f.push('{');
					f.push('\n');
					f.push('\n');
					let fmt = fmt.increment();
					for (i, v) in self.0.iter().enumerate() {
						if i > 0 {
							f.push('\n');
							f.push('\n');
						}
						fmt.write_indent(f);
						if i == 0
							&& let Expr::Literal(Literal::RecordId(_)) = v
						{
							write_sql!(f, fmt, "({v})");
						} else if let Expr::Binary {
							left,
							op: BinaryOperator::Equal,
							..
						} = v && let Expr::Param(_) = **left
						{
							write_sql!(f, fmt, "({v})");
						} else {
							v.fmt_sql(f, fmt);
						}
						f.push(';');
					}
					f.push('\n');
					// Write indent at the block's level (not the content level)
					// The content was at fmt (incremented), so block's level is one less
					if let SqlFormat::Indented(level) = fmt
						&& level > 0
					{
						for _ in 0..(level - 1) {
							f.push('\t');
						}
					}
					f.push('}')
				} else {
					// Non-pretty: all on one line with space separation
					f.push_str("{ ");
					for (i, v) in self.0.iter().enumerate() {
						if i > 0 {
							f.push(' ');
						}

						if i == 0
							&& let Expr::Literal(Literal::RecordId(_)) = v
						{
							write_sql!(f, fmt, "({v})");
						} else if let Expr::Binary {
							left,
							op: BinaryOperator::Equal,
							..
						} = v && let Expr::Param(_) = **left
						{
							write_sql!(f, fmt, "({v})");
						} else {
							v.fmt_sql(f, fmt);
						}

						f.push(';');
					}
					f.push_str(" }")
				}
			}
		}
	}
}

impl InfoStructure for Block {
	fn structure(self) -> Value {
		Value::String(self.to_sql().into())
	}
}
