//! `REMOVE` statements.

use crate::ast::{DefineKind, ext, remove};
use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	/// `REMOVE …` after `REMOVE`, as a [`Tag::Remove`] node whose `op` is the
	/// [`DefineKind`] of what is removed.
	pub(crate) fn remove(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let t = self.tok;
		let kind = match t.kw {
			_ if t.kind != T::Ident => {
				return Err(self.unexpected("a remove statement keyword"));
			}
			Kw::Namespace => DefineKind::Namespace,
			Kw::Database => DefineKind::Database,
			Kw::Function => DefineKind::Function,
			Kw::Extension | Kw::Module => DefineKind::Module,
			Kw::Access => DefineKind::Access,
			Kw::Param => DefineKind::Param,
			Kw::Table | Kw::Tb => DefineKind::Table,
			Kw::Event => DefineKind::Event,
			Kw::Field => DefineKind::Field,
			Kw::Index => DefineKind::Index,
			Kw::Analyzer => DefineKind::Analyzer,
			Kw::Sequence => DefineKind::Sequence,
			Kw::User => DefineKind::User,
			Kw::Api => DefineKind::Api,
			Kw::Bucket => DefineKind::Bucket,
			Kw::Config => DefineKind::Config,
			_ => return Err(self.unexpected("a remove statement keyword")),
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
		if matches!(kind, DefineKind::Namespace | DefineKind::Database | DefineKind::Table)
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
				rec[remove::ON] = match c.kw {
					Kw::Graphql if c.kind == T::Ident => 0,
					Kw::Api if c.kind == T::Ident => 1,
					Kw::Default if c.kind == T::Ident => 2,
					_ => return Err(self.unexpected("a type of config")),
				};
				self.bump();
			}
			_ => {
				rec[remove::NAME] = self.expr(true)?;
				match kind {
					DefineKind::Access | DefineKind::User => {
						self.expect_kw(Kw::On, "ON")?;
						rec[remove::ON] = self.base()?;
					}
					DefineKind::Event | DefineKind::Field | DefineKind::Index => {
						self.expect_kw(Kw::On, "ON")?;
						self.eat_kw(Kw::Table);
						rec[remove::ON] = self.expr(true)?;
					}
					_ => {}
				}
			}
		}
		rec[remove::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Remove, kind as u8, idx, deprecated, start, first)
	}
}
