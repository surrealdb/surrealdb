use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmtsSql, Fmt, fmt_separated_by};
use crate::sql::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of
	/// else if's
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub exprs: Vec<(Expr, Expr)>,
	/// the final else body, if there is one
	pub close: Option<Expr>,
}

impl IfelseStatement {
	/// Check if the statement is bracketed
	pub(crate) fn bracketed(&self) -> bool {
		self.exprs.iter().all(|(_, v)| matches!(v, Expr::Block(_)))
			&& self.close.as_ref().map(|v| matches!(v, Expr::Block(_))).unwrap_or(true)
	}
}

impl From<IfelseStatement> for crate::expr::statements::IfelseStatement {
	fn from(v: IfelseStatement) -> Self {
		crate::expr::statements::IfelseStatement {
			exprs: v.exprs.into_iter().map(|(a, b)| (From::from(a), From::from(b))).collect(),
			close: v.close.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::IfelseStatement> for IfelseStatement {
	fn from(v: crate::expr::statements::IfelseStatement) -> Self {
		IfelseStatement {
			exprs: v.exprs.into_iter().map(|(a, b)| (From::from(a), From::from(b))).collect(),
			close: v.close.map(Into::into),
		}
	}
}

impl ToSql for IfelseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.bracketed() {
			// Helper to check if a block contains only simple expressions
			let is_simple_block = |expr: &Expr| -> bool {
				if let Expr::Block(block) = expr {
					block.0.iter().all(|stmt| {
						matches!(stmt, Expr::Literal(_) | Expr::Param(_) | Expr::Idiom(_))
					})
				} else {
					false
				}
			};

			// Check if this IF statement has any complex multi-statement blocks (determines overall
			// formatting style)
			let has_complex_multi = self.exprs.iter().any(
				|(_, expr)| matches!(expr, Expr::Block(block) if block.0.len() > 1 && !is_simple_block(expr)),
			) || self
				.close
				.as_ref()
				.map(
					|expr| matches!(expr, Expr::Block(block) if block.0.len() > 1 && !is_simple_block(expr)),
				)
				.unwrap_or(false);

			// Helper to format block contents specially for IF statements
			let fmt_block = |f: &mut String, fmt: SqlFormat, expr: &Expr, use_separated: bool| {
				if let Expr::Block(block) = expr {
					match block.0.len() {
						0 => f.push_str("{;}"),
						1 if !use_separated => {
							// Single statement in inline mode: always compact
							f.push_str("{ ");
							block.0[0].fmt_sql(f, SqlFormat::SingleLine);
							f.push_str(" }");
						}
						1 => {
							// Single statement in separated mode (for complex IF statements)
							f.push('{');
							f.push(' ');
							block.0[0].fmt_sql(f, SqlFormat::SingleLine);
							f.push(' ');
							f.push('}');
						}
						_ => {
							// Multi-statement blocks
							let needs_indent = is_simple_block(expr);

							if fmt.is_pretty() && !needs_indent {
								// Pretty mode with complex statements: custom formatting with
								// double indent
								f.push_str("{\n\n");
								let inner_fmt = fmt.increment();
								for (i, stmt) in block.0.iter().enumerate() {
									if i > 0 {
										f.push('\n');
										f.push('\n');
									}
									inner_fmt.write_indent(f);
									stmt.fmt_sql(f, SqlFormat::SingleLine);
									f.push(';');
								}
								f.push('\n');
								// Write indent at the block's level (outer fmt), not the content
								// level
								fmt.write_indent(f);
								f.push('\n');
								f.push('}');
							} else if fmt.is_pretty() {
								// Pretty mode with simple statements: custom simple formatting
								f.push_str("{\n\n");
								for (i, stmt) in block.0.iter().enumerate() {
									if i > 0 {
										f.push('\n');
									}
									f.push('\t');
									stmt.fmt_sql(f, SqlFormat::SingleLine);
									f.push(';');
								}
								f.push_str("\n}");
							} else {
								// Non-pretty mode
								f.push_str("{\n");
								for (i, stmt) in block.0.iter().enumerate() {
									if i > 0 {
										f.push('\n');
									}
									if needs_indent {
										f.push('\t');
									}
									stmt.fmt_sql(f, SqlFormat::SingleLine);
									f.push(';');
								}
								f.push_str("\n}");
							}
						}
					}
				} else {
					expr.fmt_sql(f, fmt);
				}
			};

			// In pretty mode: use separated format if we have complex multi-statement blocks,
			// OR if we're nested (already indented)
			let is_nested = matches!(fmt, SqlFormat::Indented(level) if level > 0);
			let use_separated = fmt.is_pretty() && (has_complex_multi || is_nested);

			write_sql!(
				f,
				fmt,
				"{}",
				&Fmt::new(
					self.exprs.iter().map(|args| {
						Fmt::new(args, |(cond, then), f, fmt| {
							if use_separated {
								// Separated format: condition and block on different lines
								write_sql!(f, fmt, "IF {}", CoverStmtsSql(cond));
								f.push('\n');
								// For nested IFs, use same indent level; for top-level complex IFs,
								// increment
								if is_nested {
									fmt.write_indent(f);
									fmt_block(f, fmt, then, true);
								} else {
									let fmt = fmt.increment();
									fmt.write_indent(f);
									fmt_block(f, fmt, then, true);
								}
							} else {
								// Inline format: condition and block on same line
								write_sql!(f, fmt, "IF {} ", CoverStmtsSql(cond));
								fmt_block(f, fmt, then, false);
							}
						})
					}),
					if use_separated {
						fmt_separated_by("\nELSE ")
					} else {
						fmt_separated_by(" ELSE ")
					},
				),
			);
			if let Some(ref v) = self.close {
				if use_separated {
					// Separated format
					f.push('\n');
					write_sql!(f, fmt, "ELSE");
					f.push('\n');
					// For nested IFs, use same indent level; for top-level complex IFs, increment
					if is_nested {
						fmt.write_indent(f);
						fmt_block(f, fmt, v, true);
					} else {
						let fmt = fmt.increment();
						fmt.write_indent(f);
						fmt_block(f, fmt, v, true);
					}
				} else {
					write_sql!(f, fmt, " ELSE ");
					fmt_block(f, fmt, v, false);
				}
			}
		} else {
			write_sql!(
				f,
				fmt,
				"{}",
				&Fmt::new(
					self.exprs.iter().map(|args| {
						Fmt::new(args, |(cond, then), f, fmt| {
							if fmt.is_pretty() {
								write_sql!(f, fmt, "IF {} THEN", CoverStmtsSql(cond));
								f.push('\n');
								let fmt = fmt.increment();
								fmt.write_indent(f);
								write_sql!(f, fmt, "{then}");
							} else {
								write_sql!(f, fmt, "IF {} THEN {then}", CoverStmtsSql(cond));
							}
						})
					}),
					if fmt.is_pretty() {
						fmt_separated_by("\nELSE ")
					} else {
						fmt_separated_by(" ELSE ")
					},
				),
			);
			if let Some(ref v) = self.close {
				if fmt.is_pretty() {
					f.push('\n');
					write_sql!(f, fmt, "ELSE");
					f.push('\n');
					let fmt = fmt.increment();
					fmt.write_indent(f);
					write_sql!(f, fmt, "{v}");
				} else {
					write_sql!(f, fmt, " ELSE {v}");
				}
			}
			if fmt.is_pretty() {
				write_sql!(f, fmt, "END");
			} else {
				write_sql!(f, fmt, " END");
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;

	#[test]
	fn format_pretty() {
		let query = syn::parse("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(query.to_sql(), "IF 1 { 1 } ELSE IF 2 { 2 };");
		// Single-statement blocks stay inline even in pretty mode
		assert_eq!(query.to_sql_pretty(), "IF 1 { 1 } ELSE IF 2 { 2 };");
	}
}
