//! PostgreSQL Expression printing and precedence formatting.

use core::fmt;

use crate::ast::expr::{
	ColumnRef, Expr, QualifiedName, TypeName, WhenClause, WindowFrame, WindowFrameBound,
	WindowFrameExclusion, WindowFrameUnits, WindowSpec,
};
use crate::ast::literal::Literal;
use crate::ast::op::{BinaryOperator, UnaryOperator};
use crate::lexer::keyword::{Keyword, KeywordCategory};
use crate::printer::ToPgSql;

/// Check if an identifier can be safely printed without double quotes.
pub fn is_safe_unquoted_ident(s: &str) -> bool {
	if s.is_empty() {
		return false;
	}
	let bytes = s.as_bytes();
	let Some(&first) = bytes.first() else {
		return false;
	};
	if !first.is_ascii_lowercase() && first != b'_' {
		return false;
	}
	for &b in bytes {
		if !b.is_ascii_lowercase() && !b.is_ascii_digit() && b != b'_' {
			return false;
		}
	}
	if matches!(Keyword::from_str_ignore_ascii_case(s), Some(kw) if kw.category() == KeywordCategory::Reserved)
	{
		return false;
	}
	true
}

/// Print an identifier, adding double quotes and escaping if necessary.
pub fn print_ident(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
	if s == "*" {
		f.write_str("*")
	} else if is_safe_unquoted_ident(s) {
		f.write_str(s)
	} else {
		f.write_str("\"")?;
		for ch in s.chars() {
			if ch == '"' {
				f.write_str("\"\"")?;
			} else {
				write!(f, "{ch}")?;
			}
		}
		f.write_str("\"")
	}
}

/// Print a string literal with single quotes and doubled quotes.
pub fn print_string_literal(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
	f.write_str("'")?;
	for ch in s.chars() {
		if ch == '\'' {
			f.write_str("''")?;
		} else {
			write!(f, "{ch}")?;
		}
	}
	f.write_str("'")
}

impl ToPgSql for Literal {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{self}")
	}
}

impl ToPgSql for ColumnRef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (i, field) in self.fields.iter().enumerate() {
			if i > 0 {
				f.write_str(".")?;
			}
			print_ident(f, field)?;
		}
		Ok(())
	}
}

impl ToPgSql for QualifiedName {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (i, part) in self.parts.iter().enumerate() {
			if i > 0 {
				f.write_str(".")?;
			}
			print_ident(f, part)?;
		}
		Ok(())
	}
}

impl ToPgSql for TypeName {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.name.to_pgsql(f)?;
		if !self.modifiers.is_empty() {
			f.write_str("(")?;
			for (i, m) in self.modifiers.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				f.write_str(m)?;
			}
			f.write_str(")")?;
		}
		for bound in &self.array_bounds {
			match bound {
				Some(n) => write!(f, "[{n}]")?,
				None => f.write_str("[]")?,
			}
		}
		Ok(())
	}
}

impl ToPgSql for UnaryOperator {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{self}")
	}
}

impl ToPgSql for BinaryOperator {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{self}")
	}
}

/// Precedence level of an expression for parenthesization.
pub fn expr_precedence(expr: &Expr) -> u8 {
	match expr {
		Expr::BinaryOp {
			op,
			..
		} => match op {
			BinaryOperator::Or => 10,
			BinaryOperator::And => 20,
			BinaryOperator::Eq
			| BinaryOperator::NotEq
			| BinaryOperator::Lt
			| BinaryOperator::LtEq
			| BinaryOperator::Gt
			| BinaryOperator::GtEq
			| BinaryOperator::Like
			| BinaryOperator::NotLike
			| BinaryOperator::ILike
			| BinaryOperator::NotILike
			| BinaryOperator::SimilarTo
			| BinaryOperator::NotSimilarTo => 40,
			BinaryOperator::ShiftLeft | BinaryOperator::ShiftRight | BinaryOperator::BitXor => 55,
			BinaryOperator::Concat | BinaryOperator::BitOr | BinaryOperator::BitAnd => 60,
			BinaryOperator::Add | BinaryOperator::Sub => 70,
			BinaryOperator::Mul | BinaryOperator::Div | BinaryOperator::Mod => 80,
			BinaryOperator::Exp => 90,
			BinaryOperator::RegexMatch
			| BinaryOperator::RegexMatchCi
			| BinaryOperator::RegexNotMatch
			| BinaryOperator::RegexNotMatchCi
			| BinaryOperator::Arrow
			| BinaryOperator::LongArrow
			| BinaryOperator::HashArrow
			| BinaryOperator::HashLongArrow
			| BinaryOperator::Contains
			| BinaryOperator::ContainedBy
			| BinaryOperator::Custom(_) => 65,
		},
		Expr::UnaryOp {
			op,
			..
		} => match op {
			UnaryOperator::Not => 25,
			UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::BitNot => 110,
			UnaryOperator::Custom(_) => 110,
		},
		Expr::IsNull {
			..
		}
		| Expr::IsTrue {
			..
		}
		| Expr::IsFalse {
			..
		}
		| Expr::IsUnknown {
			..
		}
		| Expr::IsDistinctFrom {
			..
		}
		| Expr::IsBound {
			..
		} => 30,
		Expr::Between {
			..
		}
		| Expr::InList {
			..
		} => 40,
		Expr::Collate {
			..
		} => 100,
		Expr::TypeCast {
			..
		} => 120,
		Expr::Subscript {
			..
		}
		| Expr::Slice {
			..
		} => 130,
		Expr::Literal {
			..
		}
		| Expr::ColumnRef(_)
		| Expr::Param {
			..
		}
		| Expr::FunctionCall {
			..
		}
		| Expr::Case {
			..
		}
		| Expr::ArrayConstructor {
			..
		}
		| Expr::Row {
			..
		}
		| Expr::Grouping {
			..
		}
		| Expr::JsonObject {
			..
		}
		| Expr::JsonArray {
			..
		}
		| Expr::JsonExists {
			..
		}
		| Expr::JsonValue {
			..
		}
		| Expr::JsonQuery {
			..
		} => 255,
	}
}

/// Format a sub-expression, adding parentheses if its precedence is lower than `parent_prec`.
fn print_sub_expr(
	f: &mut fmt::Formatter<'_>,
	sub: &Expr,
	parent_prec: u8,
	is_right: bool,
	is_right_assoc: bool,
) -> fmt::Result {
	let sub_prec = expr_precedence(sub);
	let needs_parens = if is_right {
		if is_right_assoc {
			sub_prec < parent_prec
		} else {
			sub_prec <= parent_prec
		}
	} else {
		sub_prec < parent_prec
	};

	if needs_parens {
		f.write_str("(")?;
		sub.to_pgsql(f)?;
		f.write_str(")")
	} else {
		sub.to_pgsql(f)
	}
}

impl ToPgSql for WhenClause {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("WHEN ")?;
		self.when.to_pgsql(f)?;
		f.write_str(" THEN ")?;
		self.then.to_pgsql(f)
	}
}

impl ToPgSql for Expr {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Literal {
				lit,
				..
			} => lit.to_pgsql(f),
			Self::ColumnRef(col) => col.to_pgsql(f),
			Self::Param {
				id,
				..
			} => write!(f, "${id}"),

			Self::UnaryOp {
				op,
				expr,
				..
			} => {
				op.to_pgsql(f)?;
				let parent_prec = expr_precedence(self);
				print_sub_expr(f, expr, parent_prec, true, false)
			}

			Self::BinaryOp {
				left,
				op,
				right,
				..
			} => {
				let parent_prec = expr_precedence(self);
				let is_right_assoc = matches!(op, BinaryOperator::Exp);

				print_sub_expr(f, left, parent_prec, false, is_right_assoc)?;
				f.write_str(" ")?;
				op.to_pgsql(f)?;
				f.write_str(" ")?;
				print_sub_expr(f, right, parent_prec, true, is_right_assoc)
			}

			Self::TypeCast {
				expr,
				data_type,
				..
			} => {
				let parent_prec = 120;
				print_sub_expr(f, expr, parent_prec, false, false)?;
				f.write_str("::")?;
				data_type.to_pgsql(f)
			}

			Self::Subscript {
				expr,
				index,
				..
			} => {
				let parent_prec = 130;
				print_sub_expr(f, expr, parent_prec, false, false)?;
				f.write_str("[")?;
				index.to_pgsql(f)?;
				f.write_str("]")
			}

			Self::Slice {
				expr,
				lower,
				upper,
				..
			} => {
				let parent_prec = 130;
				print_sub_expr(f, expr, parent_prec, false, false)?;
				f.write_str("[")?;
				if let Some(l) = lower {
					l.to_pgsql(f)?;
				}
				f.write_str(":")?;
				if let Some(u) = upper {
					u.to_pgsql(f)?;
				}
				f.write_str("]")
			}

			Self::IsNull {
				expr,
				negated,
				..
			} => {
				print_sub_expr(f, expr, 30, false, false)?;
				if *negated {
					f.write_str(" IS NOT NULL")
				} else {
					f.write_str(" IS NULL")
				}
			}
			Self::IsTrue {
				expr,
				negated,
				..
			} => {
				print_sub_expr(f, expr, 30, false, false)?;
				if *negated {
					f.write_str(" IS NOT TRUE")
				} else {
					f.write_str(" IS TRUE")
				}
			}
			Self::IsFalse {
				expr,
				negated,
				..
			} => {
				print_sub_expr(f, expr, 30, false, false)?;
				if *negated {
					f.write_str(" IS NOT FALSE")
				} else {
					f.write_str(" IS FALSE")
				}
			}
			Self::IsUnknown {
				expr,
				negated,
				..
			} => {
				print_sub_expr(f, expr, 30, false, false)?;
				if *negated {
					f.write_str(" IS NOT UNKNOWN")
				} else {
					f.write_str(" IS UNKNOWN")
				}
			}

			Self::IsDistinctFrom {
				left,
				right,
				negated,
				..
			} => {
				print_sub_expr(f, left, 30, false, false)?;
				if *negated {
					f.write_str(" IS NOT DISTINCT FROM ")?;
				} else {
					f.write_str(" IS DISTINCT FROM ")?;
				}
				print_sub_expr(f, right, 30, true, false)
			}

			Self::Between {
				expr,
				low,
				high,
				symmetric,
				negated,
				..
			} => {
				print_sub_expr(f, expr, 40, false, false)?;
				if *negated {
					f.write_str(" NOT BETWEEN ")?;
				} else {
					f.write_str(" BETWEEN ")?;
				}
				if *symmetric {
					f.write_str("SYMMETRIC ")?;
				}
				print_sub_expr(f, low, 40, false, false)?;
				f.write_str(" AND ")?;
				print_sub_expr(f, high, 40, true, false)
			}

			Self::InList {
				expr,
				list,
				negated,
				..
			} => {
				print_sub_expr(f, expr, 40, false, false)?;
				if *negated {
					f.write_str(" NOT IN (")?;
				} else {
					f.write_str(" IN (")?;
				}
				for (i, item) in list.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					item.to_pgsql(f)?;
				}
				f.write_str(")")
			}

			Self::FunctionCall {
				name,
				args,
				distinct,
				over,
				..
			} => {
				name.to_pgsql(f)?;
				f.write_str("(")?;
				if *distinct {
					f.write_str("DISTINCT ")?;
				}
				for (i, arg) in args.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					arg.to_pgsql(f)?;
				}
				f.write_str(")")?;
				if let Some(spec) = over {
					f.write_str(" OVER ")?;
					spec.to_pgsql(f)?;
				}
				Ok(())
			}

			Self::Case {
				operand,
				branches,
				else_expr,
				..
			} => {
				f.write_str("CASE")?;
				if let Some(op) = operand {
					f.write_str(" ")?;
					op.to_pgsql(f)?;
				}
				for branch in branches {
					f.write_str(" ")?;
					branch.to_pgsql(f)?;
				}
				if let Some(el) = else_expr {
					f.write_str(" ELSE ")?;
					el.to_pgsql(f)?;
				}
				f.write_str(" END")
			}

			Self::Collate {
				expr,
				collation,
				..
			} => {
				let parent_prec = 100;
				print_sub_expr(f, expr, parent_prec, false, false)?;
				f.write_str(" COLLATE ")?;
				collation.to_pgsql(f)
			}

			Self::ArrayConstructor {
				elements,
				..
			} => {
				f.write_str("ARRAY[")?;
				for (i, elem) in elements.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					elem.to_pgsql(f)?;
				}
				f.write_str("]")
			}

			Self::Row {
				fields,
				..
			} => {
				f.write_str("ROW(")?;
				for (i, item) in fields.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					item.to_pgsql(f)?;
				}
				f.write_str(")")
			}

			Self::Grouping {
				expr,
				..
			} => {
				f.write_str("(")?;
				expr.to_pgsql(f)?;
				f.write_str(")")
			}

			Self::IsBound {
				expr,
				negated,
				..
			} => {
				expr.to_pgsql(f)?;
				if *negated {
					f.write_str(" IS NOT BOUND")
				} else {
					f.write_str(" IS BOUND")
				}
			}

			Self::JsonObject {
				fields,
				..
			} => {
				f.write_str("JSON_OBJECT(")?;
				for (i, (k, v)) in fields.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					k.to_pgsql(f)?;
					f.write_str(": ")?;
					v.to_pgsql(f)?;
				}
				f.write_str(")")
			}

			Self::JsonArray {
				elements,
				..
			} => {
				f.write_str("JSON_ARRAY(")?;
				for (i, el) in elements.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					el.to_pgsql(f)?;
				}
				f.write_str(")")
			}

			Self::JsonExists {
				context,
				path,
				..
			} => {
				f.write_str("JSON_EXISTS(")?;
				context.to_pgsql(f)?;
				f.write_str(", '")?;
				f.write_str(path)?;
				f.write_str("')")
			}

			Self::JsonValue {
				context,
				path,
				returning,
				..
			} => {
				f.write_str("JSON_VALUE(")?;
				context.to_pgsql(f)?;
				f.write_str(", '")?;
				f.write_str(path)?;
				f.write_str("'")?;
				if let Some(ret) = returning {
					f.write_str(" RETURNING ")?;
					ret.to_pgsql(f)?;
				}
				f.write_str(")")
			}

			Self::JsonQuery {
				context,
				path,
				..
			} => {
				f.write_str("JSON_QUERY(")?;
				context.to_pgsql(f)?;
				f.write_str(", '")?;
				f.write_str(path)?;
				f.write_str("')")
			}
		}
	}
}

impl ToPgSql for WindowSpec {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("(")?;
		let mut has_prev = false;

		if let Some(ref name) = self.window_name {
			print_ident(f, name)?;
			has_prev = true;
		}

		if !self.partition_by.is_empty() {
			if has_prev {
				f.write_str(" ")?;
			}
			f.write_str("PARTITION BY ")?;
			for (i, expr) in self.partition_by.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				expr.to_pgsql(f)?;
			}
			has_prev = true;
		}

		if !self.order_by.is_empty() {
			if has_prev {
				f.write_str(" ")?;
			}
			f.write_str("ORDER BY ")?;
			for (i, item) in self.order_by.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				item.to_pgsql(f)?;
			}
			has_prev = true;
		}

		if let Some(ref frame) = self.frame {
			if has_prev {
				f.write_str(" ")?;
			}
			frame.to_pgsql(f)?;
		}

		f.write_str(")")
	}
}

impl ToPgSql for WindowFrame {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.units {
			WindowFrameUnits::Rows => f.write_str("ROWS ")?,
			WindowFrameUnits::Range => f.write_str("RANGE ")?,
			WindowFrameUnits::Groups => f.write_str("GROUPS ")?,
		}

		if let Some(ref end) = self.end {
			f.write_str("BETWEEN ")?;
			self.start.to_pgsql(f)?;
			f.write_str(" AND ")?;
			end.to_pgsql(f)?;
		} else {
			self.start.to_pgsql(f)?;
		}

		if let Some(ref excl) = self.exclusion {
			f.write_str(" ")?;
			match excl {
				WindowFrameExclusion::CurrentRow => f.write_str("EXCLUDE CURRENT ROW")?,
				WindowFrameExclusion::Group => f.write_str("EXCLUDE GROUP")?,
				WindowFrameExclusion::Ties => f.write_str("EXCLUDE TIES")?,
				WindowFrameExclusion::NoOthers => f.write_str("EXCLUDE NO OTHERS")?,
			}
		}

		Ok(())
	}
}

impl ToPgSql for WindowFrameBound {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::UnboundedPreceding => f.write_str("UNBOUNDED PRECEDING"),
			Self::UnboundedFollowing => f.write_str("UNBOUNDED FOLLOWING"),
			Self::CurrentRow => f.write_str("CURRENT ROW"),
			Self::Preceding(expr) => {
				expr.to_pgsql(f)?;
				f.write_str(" PRECEDING")
			}
			Self::Following(expr) => {
				expr.to_pgsql(f)?;
				f.write_str(" FOLLOWING")
			}
		}
	}
}
