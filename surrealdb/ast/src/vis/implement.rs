use core::fmt;
use std::any::Any;
use std::ops::Bound;
use std::time::Duration;

use common::span::Span;
use rust_decimal::Decimal;
use uuid::Uuid;

use super::AstFormatter;
use crate::mac::impl_vis_debug;
use crate::types::{Ast, NodeLibrary};
use crate::vis::AstVis;
use crate::{
	Base, BinaryOperator, DateTime, DestructureOperator, IdiomOperator, InfoKind, Integer, NodeId,
	NodeListId, PostfixOperator, RecordIdKeyGenerate, Sign, Spanned, UseKind,
};

impl<L, W> AstVis<L, W> for InfoKind
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "InfoKind", |ast, fmt| match self {
			InfoKind::Root => fmt.unit_variant("Root"),
			InfoKind::Namespace => fmt.unit_variant("Namespace"),
			InfoKind::Database {
				version,
			} => fmt
				.variant(ast, "Database", |ast, fmt| fmt.field(ast, "version", version)?.finish()),
			InfoKind::Table {
				name,
				version,
			} => fmt.variant(ast, "Table", |ast, fmt| {
				fmt.field(ast, "name", name)?.field(ast, "version", version)?.finish()
			}),
			InfoKind::User {
				name,
				base,
			} => fmt.variant(ast, "Table", |ast, fmt| {
				fmt.field(ast, "name", name)?.field(ast, "base", base)?.finish()
			}),
			InfoKind::Index {
				name,
				table,
			} => fmt.variant(ast, "Table", |ast, fmt| {
				fmt.field(ast, "name", name)?.field(ast, "table", table)?.finish()
			}),
		})
	}
}

impl<L, W> AstVis<L, W> for UseKind
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "UseStatementKind", |ast, fmt| match self {
			UseKind::Namespace(ns) => {
				fmt.variant(ast, "Namespace", |ast, fmt| fmt.tuple(ast, ns)?.finish())
			}
			UseKind::NamespaceDatabase(ns, db) => {
				fmt.variant(ast, "NamespaceDatabase", |ast, fmt| {
					fmt.tuple(ast, ns)?.tuple(ast, db)?.finish()
				})
			}
			UseKind::Database(db) => {
				fmt.variant(ast, "Database", |ast, fmt| fmt.tuple(ast, db)?.finish())
			}
		})
	}
}

impl<L, W> AstVis<L, W> for PostfixOperator
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "PostfixOperator", |ast, fmt| match self {
			PostfixOperator::Range => fmt.unit_variant("Range"),
			PostfixOperator::RangeSkip => fmt.unit_variant("RangeSkip"),
			PostfixOperator::MethodCall(receiver, arguments) => {
				fmt.variant(ast, "MethodCall", |ast, fmt| {
					fmt.field(ast, "receiver", receiver)?
						.field(ast, "arguments", arguments)?
						.finish()
				})
			}
			PostfixOperator::Call(arguments) => fmt
				.variant(ast, "Call", |ast, fmt| fmt.field(ast, "arguments", arguments)?.finish()),
		})
	}
}

impl<L, W> AstVis<L, W> for IdiomOperator
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "PostfixOperator", |ast, fmt| match self {
			IdiomOperator::All => fmt.unit_variant("All"),
			IdiomOperator::Last => fmt.unit_variant("Last"),
			IdiomOperator::Option => fmt.unit_variant("Option"),
			IdiomOperator::Repeat => fmt.unit_variant("Repeat"),
			IdiomOperator::Field(n) => {
				fmt.variant(ast, "Field", |ast, fmt| fmt.field(ast, "name", n)?.finish())
			}
			IdiomOperator::Index(i) => {
				fmt.variant(ast, "Index", |ast, fmt| fmt.field(ast, "expr", i)?.finish())
			}
			IdiomOperator::Where(i) => {
				fmt.variant(ast, "Where", |ast, fmt| fmt.field(ast, "expr", i)?.finish())
			}
			IdiomOperator::Destructure(x) => {
				fmt.variant(ast, "Destructure", |ast, fmt| fmt.field(ast, "op", x)?.finish())
			}
			IdiomOperator::Call(x) => {
				fmt.variant(ast, "Call", |ast, fmt| fmt.field(ast, "args", x)?.finish())
			}
		})
	}
}

impl<L, W> AstVis<L, W> for DestructureOperator
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "DestructureOperator", |ast, fmt| match self {
			DestructureOperator::All => fmt.unit_variant("All"),
			DestructureOperator::Expr(x) => {
				fmt.variant(ast, "Expr", |ast, fmt| fmt.field(ast, "expr", x)?.finish())
			}
			DestructureOperator::Destructure(x) => {
				fmt.variant(ast, "Destructure", |ast, fmt| fmt.field(ast, "op", x)?.finish())
			}
		})
	}
}

impl<L, W> AstVis<L, W> for RecordIdKeyGenerate
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "DestructureOperator", |_, fmt| match self {
			RecordIdKeyGenerate::Ulid => fmt.unit_variant("Ulid"),
			RecordIdKeyGenerate::Uuid => fmt.unit_variant("Uuid"),
			RecordIdKeyGenerate::Rand => fmt.unit_variant("Rand"),
		})
	}
}

impl<N, L, W> AstVis<L, W> for NodeId<N>
where
	N: AstVis<L, W> + Any,
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		ast[*self].fmt(ast, fmt)
	}
}

impl<N, L, W> AstVis<L, W> for NodeListId<N>
where
	N: AstVis<L, W> + Any,
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		for n in ast.iter_list(Some(*self)) {
			fmt.write_str("-")?;
			n.fmt(ast, fmt)?;
		}
		Ok(())
	}
}

impl<N, L, W> AstVis<L, W> for Spanned<N>
where
	N: AstVis<L, W>,
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		self.value.fmt(ast, fmt)
	}
}

impl<N, L, W> AstVis<L, W> for Option<N>
where
	N: AstVis<L, W>,
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		if let Some(x) = self.as_ref() {
			fmt.write_str("Some")?;
			fmt.new_line();
			fmt.indent(ast, |ast, fmt| x.fmt(ast, fmt))
		} else {
			fmt.write_str("None")
		}
	}
}

impl<N, L, W> AstVis<L, W> for Bound<N>
where
	N: AstVis<L, W>,
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		fmt.fmt_enum(ast, "Bound", |ast, fmt| match self {
			Bound::Included(x) => {
				fmt.variant(ast, "Included", |ast, fmt| fmt.tuple(ast, x)?.finish())
			}
			Bound::Excluded(x) => {
				fmt.variant(ast, "Excluded", |ast, fmt| fmt.tuple(ast, x)?.finish())
			}
			Bound::Unbounded => fmt.unit_variant("Unbounded"),
		})
	}
}

impl<L, W> AstVis<L, W> for String
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, _: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		write!(fmt.writer, "{:?}", self)
	}
}

impl<L, W> AstVis<L, W> for Span
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, _: &Ast<L>, _fmt: &mut AstFormatter<W>) -> fmt::Result {
		Ok(())
	}
}

impl<L, W> AstVis<L, W> for Integer
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, _: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result {
		match self.sign {
			Sign::Plus => fmt.fmt_args(format_args!("+{:?}", self.value)),
			Sign::Minus => fmt.fmt_args(format_args!("-{:?}", self.value)),
		}
	}
}

impl_vis_debug!(BinaryOperator);
impl_vis_debug!(Base);

impl_vis_debug!(Decimal);
impl_vis_debug!(Uuid);
impl_vis_debug!(DateTime);
impl_vis_debug!(Duration);

impl_vis_debug!(bool);
impl_vis_debug!(usize);
impl_vis_debug!(isize);
impl_vis_debug!(f64);
impl_vis_debug!(f32);
impl_vis_debug!(u64);
impl_vis_debug!(i64);
impl_vis_debug!(u32);
impl_vis_debug!(i32);
impl_vis_debug!(u16);
impl_vis_debug!(i16);
impl_vis_debug!(u8);
impl_vis_debug!(i8);
impl_vis_debug!(());
