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
	AlterKind, Base, BinaryOperator, DateTime, Integer, NodeId, NodeListId, Sign, Spanned,
};

impl<N, L> AstVis<L> for AlterKind<N>
where
	N: AstVis<L>,
	L: NodeLibrary,
{
	fn fmt<W>(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		fmt.fmt_enum(ast, "AlterKind", |ast, fmt| match self {
			AlterKind::Drop(_) => fmt.unit_variant("Drop"),
			AlterKind::Set(s) => fmt.variant(ast, "Set", |ast, fmt| fmt.tuple(ast, s)?.finish()),
		})
	}
}

impl<N, L> AstVis<L> for NodeId<N>
where
	N: AstVis<L> + Any,
	L: NodeLibrary,
{
	fn fmt<W>(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		ast[*self].fmt(ast, fmt)
	}
}

impl<N, L> AstVis<L> for NodeListId<N>
where
	N: AstVis<L> + Any,
	L: NodeLibrary,
{
	fn fmt<W>(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		fmt.new_line();
		fmt.indent(ast, |ast, fmt| {
			for n in ast.iter_list(Some(*self)) {
				fmt.write_str("-")?;
				n.fmt(ast, fmt)?;
			}
			Ok(())
		})
	}
}

impl<N, L> AstVis<L> for Spanned<N>
where
	N: AstVis<L>,
	L: NodeLibrary,
{
	fn fmt<W>(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		self.value.fmt(ast, fmt)
	}
}

impl<N, L> AstVis<L> for Option<N>
where
	N: AstVis<L>,
	L: NodeLibrary,
{
	fn fmt<W>(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		if let Some(x) = self.as_ref() {
			fmt.write_str("Some")?;
			fmt.new_line();
			x.fmt(ast, fmt)
		} else {
			fmt.write_str("None")
		}
	}
}

impl<N, L> AstVis<L> for Bound<N>
where
	N: AstVis<L>,
	L: NodeLibrary,
{
	fn fmt<W>(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
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

impl<L> AstVis<L> for String
where
	L: NodeLibrary,
{
	fn fmt<W>(&self, _: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		write!(fmt.writer, "{:?}", self)
	}
}

impl<L> AstVis<L> for Span
where
	L: NodeLibrary,
{
	fn fmt<W>(&self, _: &Ast<L>, _: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
		Ok(())
	}
}

impl<L> AstVis<L> for Integer
where
	L: NodeLibrary,
{
	fn fmt<W>(&self, _: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result
	where
		W: fmt::Write,
	{
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

impl_vis_debug!(Vec<u8>);

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
