//! `REMOVE` statements.

use crate::ast::{DefineKind, remove};
use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	/// `REMOVE …` after `REMOVE`, as a [`Tag::Remove`] node whose `op` is the
	/// [`DefineKind`] of what is removed.
	pub(crate) fn remove(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let t = self.tok;
		let kind = match t.kw {
			_ if t.kind != T::Ident => {
				return Err(self.unexpected("expected a remove statement keyword"));
			}
			Kw::Namespace => DefineKind::Namespace,
			Kw::Database => DefineKind::Database,
			Kw::Function => DefineKind::Function,
			Kw::Module => DefineKind::Module,
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
			_ => return Err(self.unexpected("expected a remove statement keyword")),
		};
		self.bump();
		let mut rec = [NONE; remove::LEN];
		let mut flags = 0;
		if matches!(kind, DefineKind::Namespace | DefineKind::Database | DefineKind::Table)
			&& self.eat_kw(Kw::And)
		{
			self.expect_kw(Kw::Expunge, "expected `EXPUNGE`")?;
			flags |= 2;
		}
		if self.eat_kw(Kw::If) {
			self.expect_kw(Kw::Exists, "expected `EXISTS`")?;
			flags |= 1;
		}
		match kind {
			DefineKind::Function => {
				rec[remove::NAME] = self.custom_function_name()?;
				if self.eat(T::LParen) {
					self.expect(T::RParen, "expected `)`")?;
				}
			}
			DefineKind::Module => {
				let m = self.tok;
				rec[remove::NAME] = match m.kw {
					Kw::Mod if m.kind == T::Ident => {
						self.bump();
						if !self.at(T::PathSep) || self.tok.ws() {
							return Err(self.unexpected("expected `::`"));
						}
						self.bump();
						self.name()?
					}
					Kw::Silo if m.kind == T::Ident => {
						self.bump();
						self.expect(T::PathSep, "expected `::`")?;
						let mark = self.vals.len() as u32;
						let org = self.name()?;
						self.vals.push(org);
						self.expect(T::PathSep, "expected `::`")?;
						let pkg = self.name()?;
						self.vals.push(pkg);
						self.expect(T::PathSep, "expected `::`")?;
						if !self.at(T::Lt) || self.tok.ws() {
							return Err(self.unexpected("expected `<`"));
						}
						self.model_version()?;
						let list = self.list_from(mark).0;
						rec[remove::ON] = 1;
						list
					}
					_ => return Err(self.unexpected("expected a module name")),
				};
				if self.eat(T::LParen) {
					self.expect(T::RParen, "expected `)`")?;
				}
			}
			DefineKind::Param => {
				let p = self.expect(T::Param, "expected a parameter")?;
				rec[remove::NAME] = self.leaf(Tag::Param, 0, p);
			}
			DefineKind::Config => {
				let c = self.tok;
				rec[remove::ON] = match c.kw {
					Kw::Graphql if c.kind == T::Ident => 0,
					Kw::Api if c.kind == T::Ident => 1,
					Kw::Default if c.kind == T::Ident => 2,
					_ => return Err(self.unexpected("expected a type of config")),
				};
				self.bump();
			}
			_ => {
				rec[remove::NAME] = self.expr(true)?;
				match kind {
					DefineKind::Access | DefineKind::User => {
						self.expect_kw(Kw::On, "expected `ON`")?;
						rec[remove::ON] = self.base()?;
					}
					DefineKind::Event | DefineKind::Field | DefineKind::Index => {
						self.expect_kw(Kw::On, "expected `ON`")?;
						self.eat_kw(Kw::Table);
						rec[remove::ON] = self.expr(true)?;
					}
					_ => {}
				}
			}
		}
		rec[remove::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Remove, kind as u8, idx, 0, start, first)
	}
}
