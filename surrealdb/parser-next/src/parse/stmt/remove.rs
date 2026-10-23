//! `REMOVE` statements.

use crate::ast::{DefineKind, ext, remove};
use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	/// `REMOVE …` after `REMOVE`, as a [`Tag::Remove`] node whose `op` is the
	/// [`DefineKind`] of what is removed.
	pub(crate) fn remove(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let t = self.tok;
		let kind = if t.kind != T::Ident {
			return Err(self.unexpected("a remove statement keyword"));
		} else if t.kw == Kw::Namespace {
			DefineKind::Namespace
		} else if t.kw == Kw::Database {
			DefineKind::Database
		} else if t.kw == Kw::Function {
			DefineKind::Function
		} else if t.kw == Kw::Extension || t.kw == Kw::Module {
			DefineKind::Module
		} else if t.kw == Kw::Access {
			DefineKind::Access
		} else if t.kw == Kw::Param {
			DefineKind::Param
		} else if t.kw == Kw::Table || t.kw == Kw::Tb {
			DefineKind::Table
		} else if t.kw == Kw::Event {
			DefineKind::Event
		} else if t.kw == Kw::Field {
			DefineKind::Field
		} else if t.kw == Kw::Index {
			DefineKind::Index
		} else if t.kw == Kw::Analyzer {
			DefineKind::Analyzer
		} else if t.kw == Kw::Sequence {
			DefineKind::Sequence
		} else if t.kw == Kw::User {
			DefineKind::User
		} else if t.kw == Kw::Api {
			DefineKind::Api
		} else if t.kw == Kw::Bucket {
			DefineKind::Bucket
		} else if t.kw == Kw::Config {
			DefineKind::Config
		} else {
			return Err(self.unexpected("a remove statement keyword"));
		};
		self.bump();
		let mut deprecated = 0;
		if kind == DefineKind::Module {
			if t.kw == Kw::Module {
				deprecated |= ext::MODULE_KEYWORD;
			}
			if self.limits.no_surrealism {
				let label = if t.kw == Kw::Module {
					"Use of `REMOVE MODULE` requires Surrealism extensions"
				} else {
					"Use of `REMOVE EXTENSION` requires Surrealism extensions"
				};
				return Err(self.surrealism_off(t, label));
			}
		}
		let mut rec = [NONE; remove::LEN];
		let mut flags = 0;
		if (kind == DefineKind::Namespace
			|| kind == DefineKind::Database
			|| kind == DefineKind::Table)
			&& self.eat_kw(Kw::And)
		{
			self.expect_kw(Kw::Expunge, "EXPUNGE")?;
			flags |= 2;
		}
		if self.eat_kw(Kw::If) {
			self.expect_kw(Kw::Exists, "EXISTS")?;
			flags |= 1;
		}
		match kind {
			DefineKind::Function => {
				rec[remove::NAME] = self.custom_function_name()?;
				if self.eat(T::LParen) {
					self.expect(T::RParen, ")")?;
				}
			}
			DefineKind::Module => {
				let (name, silo, name_deprecated) = self.extension_name()?;
				rec[remove::NAME] = name;
				if silo {
					rec[remove::ON] = 1;
				}
				deprecated |= name_deprecated;
				if self.eat(T::LParen) {
					self.expect(T::RParen, ")")?;
				}
			}
			DefineKind::Param => {
				let p = self.expect(T::Param, "a parameter")?;
				rec[remove::NAME] = self.leaf(Tag::Param, 0, p);
			}
			DefineKind::Config => {
				let c = self.tok;
				rec[remove::ON] = if c.kind == T::Ident && c.kw == Kw::Graphql {
					0
				} else if c.kind == T::Ident && c.kw == Kw::Api {
					1
				} else if c.kind == T::Ident && c.kw == Kw::Default {
					2
				} else {
					return Err(self.unexpected("a type of config"));
				};
				self.bump();
			}
			DefineKind::Access | DefineKind::User => {
				rec[remove::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "ON")?;
				rec[remove::ON] = self.base()?;
			}
			DefineKind::Event | DefineKind::Field | DefineKind::Index => {
				rec[remove::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "ON")?;
				self.eat_kw(Kw::Table);
				rec[remove::ON] = self.expr(true)?;
			}
			DefineKind::Namespace
			| DefineKind::Database
			| DefineKind::Table
			| DefineKind::Api
			| DefineKind::Analyzer
			| DefineKind::Bucket
			| DefineKind::Sequence => {
				rec[remove::NAME] = self.expr(true)?;
			}
		}
		rec[remove::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Remove, kind as u8, idx, deprecated, start, first)
	}
}
