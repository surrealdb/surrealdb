#![cfg(feature = "visualize")]
//! Module implementing ast visualization utilities.

mod implement;

use std::fmt::{self, Arguments};

use crate::mac::impl_vis_debug;
use crate::types::{Ast, NodeLibrary};
use crate::{Node, NodeId, Spanned};

pub fn visualize_ast<N, L, W>(root: NodeId<N>, ast: &Ast<L>, writer: W) -> fmt::Result
where
	N: Node + AstVis<L, W>,
	L: NodeLibrary,
	W: fmt::Write,
{
	let mut fmt = AstFormatter::new(writer);
	ast[root].fmt(ast, &mut fmt)
}

pub trait AstVis<L, W>
where
	L: NodeLibrary,
	W: fmt::Write,
{
	fn fmt(&self, ast: &Ast<L>, fmt: &mut AstFormatter<W>) -> fmt::Result;
}

pub struct AstFormatter<W> {
	writer: W,
	indent: usize,
	new_line: bool,
}

impl<W> AstFormatter<W>
where
	W: fmt::Write,
{
	pub fn new(w: W) -> Self {
		AstFormatter {
			writer: w,
			indent: 0,
			new_line: false,
		}
	}

	pub fn fmt_args(&mut self, args: Arguments<'_>) -> fmt::Result {
		if self.new_line {
			self.writer.write_char('\n')?;
			for _ in 0..self.indent {
				self.writer.write_char(' ')?;
			}
			self.new_line = false;
		}
		self.writer.write_fmt(args)
	}

	pub fn fmt_debug<D: fmt::Debug>(&mut self, dbg: &D) -> fmt::Result {
		if self.new_line {
			self.writer.write_char('\n')?;
			for _ in 0..self.indent {
				self.writer.write_char(' ')?;
			}
			self.new_line = false;
		}
		write!(self.writer, "{:?}", dbg)
	}

	pub fn fmt_struct<L, F>(&mut self, ast: &Ast<L>, name: &str, f: F) -> fmt::Result
	where
		L: NodeLibrary,
		F: FnOnce(&Ast<L>, &mut AstStructFormatter<W>) -> fmt::Result,
	{
		self.write_str(name)?;
		self.indent += 4;
		self.new_line();
		let fmt = unsafe { std::mem::transmute::<&mut Self, &mut AstStructFormatter<W>>(self) };
		f(ast, fmt)?;
		self.indent -= 4;
		Ok(())
	}

	pub fn fmt_enum<L, F>(&mut self, ast: &Ast<L>, name: &str, f: F) -> fmt::Result
	where
		L: NodeLibrary,
		F: FnOnce(&Ast<L>, &mut AstEnumFormatter<W>) -> fmt::Result,
	{
		self.write_str(name)?;
		let fmt = unsafe { std::mem::transmute::<&mut Self, &mut AstEnumFormatter<W>>(self) };
		f(ast, fmt)?;
		Ok(())
	}

	pub fn indent<L, F>(&mut self, ast: &Ast<L>, f: F) -> fmt::Result
	where
		L: NodeLibrary,
		F: FnOnce(&Ast<L>, &mut AstFormatter<W>) -> fmt::Result,
	{
		self.indent += 4;
		let res = f(ast, self);
		self.indent -= 4;
		res
	}

	fn write_str(&mut self, s: &str) -> fmt::Result {
		if self.new_line {
			self.writer.write_char('\n')?;
			for _ in 0..self.indent {
				self.writer.write_char(' ')?;
			}
			self.new_line = false;
		}
		for l in s.split("\n") {
			self.writer.write_str(l)?;
		}
		Ok(())
	}

	fn new_line(&mut self) {
		self.new_line = true;
	}
}

#[repr(transparent)]
pub struct AstStructFormatter<W>(AstFormatter<W>);

impl<W> AstStructFormatter<W>
where
	W: fmt::Write,
{
	pub fn tuple<L, N>(&mut self, ast: &Ast<L>, n: &N) -> Result<&mut Self, fmt::Error>
	where
		L: NodeLibrary,
		N: AstVis<L, W>,
	{
		let fmt = unsafe { std::mem::transmute::<&mut Self, &mut AstFormatter<W>>(self) };
		n.fmt(ast, fmt)?;
		Ok(self)
	}

	pub fn field<L, N>(&mut self, ast: &Ast<L>, name: &str, n: &N) -> Result<&mut Self, fmt::Error>
	where
		L: NodeLibrary,
		N: AstVis<L, W>,
	{
		self.0.write_str(".")?;
		self.0.write_str(name)?;
		self.0.write_str(": ")?;
		let fmt = unsafe { std::mem::transmute::<&mut Self, &mut AstFormatter<W>>(self) };
		n.fmt(ast, fmt)?;
		self.0.new_line();
		Ok(self)
	}

	pub fn finish(&mut self) -> Result<(), fmt::Error> {
		self.0.new_line();
		Ok(())
	}
}

#[repr(transparent)]
pub struct AstEnumFormatter<W>(AstFormatter<W>);

impl<W> AstEnumFormatter<W>
where
	W: fmt::Write,
{
	pub fn variant<L, F>(&mut self, ast: &Ast<L>, name: &str, cb: F) -> Result<(), fmt::Error>
	where
		L: NodeLibrary,
		F: FnOnce(&Ast<L>, &mut AstStructFormatter<W>) -> fmt::Result,
	{
		self.0.write_str("::")?;
		self.0.write_str(name)?;
		self.0.indent += 4;
		self.0.new_line();
		let fmt = unsafe { std::mem::transmute::<&mut Self, &mut AstStructFormatter<W>>(self) };
		cb(ast, fmt)?;
		self.0.indent -= 4;
		self.0.new_line();
		Ok(())
	}

	pub fn unit_variant(&mut self, name: &str) -> Result<(), fmt::Error> {
		self.0.write_str("::")?;
		self.0.write_str(name)?;
		Ok(())
	}
}
