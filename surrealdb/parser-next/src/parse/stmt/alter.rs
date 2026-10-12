//! `ALTER` statements, as clause lists like `DEFINE`'s.

use crate::ast::{ALTER_SYSTEM, Cl, DefineKind};
use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	/// `ALTER …` after `ALTER`.
	pub(crate) fn alter(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let t = self.tok;
		let kind = match t.kw {
			_ if t.kind != T::Ident => {
				return Err(self.unexpected("a alter statement keyword"));
			}
			Kw::System => ALTER_SYSTEM,
			Kw::Namespace => DefineKind::Namespace as u8,
			Kw::Database => DefineKind::Database as u8,
			Kw::Table | Kw::Tb => DefineKind::Table as u8,
			Kw::Event => DefineKind::Event as u8,
			Kw::Index => DefineKind::Index as u8,
			Kw::Field => DefineKind::Field as u8,
			Kw::Param => DefineKind::Param as u8,
			Kw::Sequence => DefineKind::Sequence as u8,
			Kw::Bucket => DefineKind::Bucket as u8,
			Kw::Analyzer => DefineKind::Analyzer as u8,
			Kw::Function => DefineKind::Function as u8,
			Kw::User => DefineKind::User as u8,
			Kw::Access => DefineKind::Access as u8,
			Kw::Config => DefineKind::Config as u8,
			Kw::Api => DefineKind::Api as u8,
			Kw::Module => DefineKind::Module as u8,
			_ => return Err(self.unexpected("a alter statement keyword")),
		};
		if self.limits.no_files && kind == DefineKind::Bucket as u8 {
			return Err(self.files_off(t));
		}
		self.bump();
		if self.limits.no_surrealism && kind == DefineKind::Module as u8 {
			return Err(self.surrealism_off(t, "Use of `ALTER MODULE` requires Surrealism modules"));
		}
		let mut rec = [0, NONE, NONE, NONE];
		let mark = self.vals.len() as u32;
		if kind == ALTER_SYSTEM {
			self.alter_system()?;
		} else if kind == DefineKind::Namespace as u8 || kind == DefineKind::Database as u8 {
			if !self.at_kw(Kw::Compact) {
				return Err(self.unexpected("`COMPACT`"));
			}
			self.flag_clause(Cl::Compact)?;
		} else {
			if self.eat_kw(Kw::If) {
				self.expect_kw(Kw::Exists, "EXISTS")?;
				rec[0] = 1;
			}
			self.alter_target(kind, &mut rec)?;
			let field_is_id = kind == DefineKind::Field as u8 && self.is_bare_id(rec[1]);
			if kind == DefineKind::Config as u8 {
				// One optional `COMMENT` or `DROP COMMENT`.
				if self.at_kw(Kw::Comment) {
					self.string_clause(Cl::Comment)?;
				} else if self.at_kw(Kw::Drop) {
					self.alter_drop(&[(Kw::Comment, Cl::Comment)], "COMMENT")?;
				}
			} else {
				let clauses = self.vals.len();
				self.alter_clauses(kind, field_is_id)?;
				// `ALTER INDEX` must change something.
				if kind == DefineKind::Index as u8 && self.vals.len() == clauses {
					return Err(self.unexpected("`PREPARE`, `DROP` or `COMMENT`"));
				}
			}
		}
		rec[3] = self.list_from(mark).0;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Alter, kind, idx, 0, start, first)
	}

	fn alter_system(&mut self) -> R<()> {
		let mut any = false;
		loop {
			let t = self.tok;
			match t.kw {
				Kw::Drop => {
					self.bump();
					let n = self.tok;
					if n.kw != Kw::None || !self.ident_is(n, "QUERY_TIMEOUT", false) {
						return Err(self.unexpected("`QUERY_TIMEOUT`"));
					}
					self.bump();
					self.push_clause(
						Cl::AlterDrop,
						Cl::QueryTimeout as u32,
						0,
						t.start,
						self.ast.nodes.len(),
					)?;
				}
				Kw::Compact => self.flag_clause(Cl::Compact)?,
				Kw::None if t.kind == T::Ident || t.kind == T::EscIdent => {
					if self.ident_is(t, "QUERY_TIMEOUT", false) {
						self.expr_clause(Cl::QueryTimeout)?;
					} else {
						return Err(self.unexpected("`QUERY_TIMEOUT`"));
					}
				}
				_ => break,
			}
			any = true;
		}
		if !any {
			return Err(self.unexpected("`COMPACT`, `DROP` or `QUERY_TIMEOUT`"));
		}
		Ok(())
	}

	/// The name and `ON` part of an altered definition.
	fn alter_target(&mut self, kind: u8, rec: &mut [u32]) -> R<()> {
		const TABLE: u8 = DefineKind::Table as u8;
		const EVENT: u8 = DefineKind::Event as u8;
		const INDEX: u8 = DefineKind::Index as u8;
		const FIELD: u8 = DefineKind::Field as u8;
		const PARAM: u8 = DefineKind::Param as u8;
		const FUNCTION: u8 = DefineKind::Function as u8;
		const USER: u8 = DefineKind::User as u8;
		const ACCESS: u8 = DefineKind::Access as u8;
		const CONFIG: u8 = DefineKind::Config as u8;
		const MODULE: u8 = DefineKind::Module as u8;
		match kind {
			TABLE => rec[1] = self.expr(false)?,
			EVENT | INDEX | FIELD => {
				rec[1] = self.expr(true)?;
				self.expect_kw(Kw::On, "ON")?;
				self.eat_kw(Kw::Table);
				rec[2] = self.expr(false)?;
			}
			PARAM => {
				let p = self.expect(T::Param, "a parameter")?;
				rec[1] = self.leaf(Tag::Param, 0, p);
			}
			FUNCTION => rec[1] = self.custom_function_name()?,
			USER | ACCESS => {
				rec[1] = self.expr(true)?;
				self.expect_kw(Kw::On, "ON")?;
				rec[2] = self.base()?;
			}
			CONFIG => {
				let c = self.tok;
				match c.kw {
					Kw::Api if c.kind == T::Ident => {
						self.bump();
						rec[2] = 0;
						let cfg = self.api_config()?;
						self.vals.push(cfg);
					}
					Kw::Graphql if c.kind == T::Ident => {
						self.bump();
						rec[2] = 1;
						let first = self.ast.nodes.len();
						let mark = self.vals.len() as u32;
						self.graphql_config()?;
						let list = self.list_from(mark).0;
						self.push_clause(Cl::GraphqlConfig, list, 0, c.start, first)?;
					}
					Kw::Default if c.kind == T::Ident => {
						self.bump();
						rec[2] = 2;
						if !(self.at_kw(Kw::Namespace) || self.at_kw(Kw::Database)) {
							return Err(self.unexpected("a namespace or database name"));
						}
						loop {
							match self.tok.kw {
								Kw::Namespace => self.expr_clause(Cl::DefaultNs)?,
								Kw::Database => self.expr_clause(Cl::DefaultDb)?,
								_ => break,
							}
						}
					}
					_ => return Err(self.unexpected("a type of config")),
				}
			}
			MODULE => {
				let m = self.tok;
				match m.kw {
					Kw::Mod if m.kind == T::Ident => {
						self.bump();
						self.expect_adjacent(T::PathSep, "::")?;
						rec[1] = self.name()?;
					}
					Kw::Silo if m.kind == T::Ident => {
						self.bump();
						self.expect(T::PathSep, "::")?;
						let mark = self.vals.len() as u32;
						let org = self.name()?;
						self.vals.push(org);
						self.expect(T::PathSep, "::")?;
						let pkg = self.name()?;
						self.vals.push(pkg);
						self.expect(T::PathSep, "::")?;
						if !self.at(T::Lt) || self.tok.ws() {
							return Err(self.unexpected("<"));
						}
						self.model_version()?;
						rec[1] = self.list_from(mark).0;
						rec[2] = 1;
					}
					_ => return Err(self.unexpected("a module name")),
				}
			}
			_ => rec[1] = self.expr(true)?,
		}
		Ok(())
	}

	/// A `DROP x` clause, where `x` must be one of `allowed`; anything else
	/// is reported as not the `expected` clauses.
	fn alter_drop(&mut self, allowed: &[(Kw, Cl)], expected: &'static str) -> R<()> {
		let t = self.bump();
		let n = self.tok;
		let Some(&(_, cl)) = allowed.iter().find(|(k, _)| n.kind == T::Ident && n.kw == *k) else {
			return Err(self.unexpected(expected));
		};
		self.bump();
		let cl = if cl == Cl::InlineEdges {
			match self.tok.kw {
				Kw::Edges if self.tok.kind == T::Ident => {
					self.bump();
					Cl::InlineEdges
				}
				Kw::References if self.tok.kind == T::Ident => {
					self.bump();
					Cl::InlineRefs
				}
				_ => return Err(self.unexpected("`EDGES` or `REFERENCES`")),
			}
		} else {
			cl
		};
		self.push_clause(Cl::AlterDrop, cl as u32, 0, t.start, self.ast.nodes.len())
	}

	/// `DURATION FOR x NONE|duration, …`, with `fors` the accepted `x`s and
	/// `expected` naming them.
	fn alter_durations(&mut self, fors: &[(Kw, Cl)], expected: &'static str) -> R<()> {
		self.bump();
		loop {
			self.expect_kw(Kw::For, "FOR")?;
			let t = self.tok;
			let Some(&(_, cl)) = fors.iter().find(|(k, _)| t.kind == T::Ident && t.kw == *k) else {
				return Err(self.unexpected(expected));
			};
			self.bump();
			let first = self.ast.nodes.len();
			let d = if self.eat_kw(Kw::None_) {
				NONE
			} else {
				self.duration_lit()?
			};
			self.push_clause(cl, d, 0, t.start, first)?;
			if !self.eat(T::Comma) {
				return Ok(());
			}
		}
	}

	/// The clauses of an `ALTER` of `kind`. `field_is_id` is whether the target
	/// is the field `id`, whose `TYPE` may name the components of a tuple.
	fn alter_clauses(&mut self, kind: u8, field_is_id: bool) -> R<()> {
		use DefineKind as D;
		let k = |d: D| d as u8;
		loop {
			let t = self.tok;
			if kind == k(D::Function) && t.kind == T::LParen {
				self.alter_signature()?;
				continue;
			}
			if t.kind != T::Ident {
				return Ok(());
			}
			match t.kw {
				Kw::Comment => self.string_clause(Cl::Comment)?,
				Kw::Drop => {
					let (allowed, expected): (&[(Kw, Cl)], &str) = match kind {
						x if x == k(D::Table) => (
							&[
								(Kw::Comment, Cl::Comment),
								(Kw::ChangeFeed, Cl::Changefeed),
								(Kw::Inline, Cl::InlineEdges),
							],
							"`COMMENT`, `CHANGEFEED` or `INLINE`",
						),
						x if x == k(D::Event) => (
							&[
								(Kw::When, Cl::When),
								(Kw::Then, Cl::Then),
								(Kw::Comment, Cl::Comment),
								(Kw::Async, Cl::Async),
							],
							"`WHEN`, `THEN`, `COMMENT`, or `ASYNC`",
						),
						x if x == k(D::Field) => (
							&[
								(Kw::Type, Cl::FieldType),
								(Kw::Flexible, Cl::Flexible),
								(Kw::Readonly, Cl::Readonly),
								(Kw::Inline, Cl::Inline),
								(Kw::Value, Cl::Value),
								(Kw::Assert, Cl::Assert),
								(Kw::Default, Cl::Default),
								(Kw::Comment, Cl::Comment),
								(Kw::Reference, Cl::Reference),
							],
							"`FLEXIBLE`, `TYPE`, `READONLY`, `INLINE`, `VALUE`, `ASSERT`, `DEFAULT`, `COMMENT`, or `REFERENCE`",
						),
						x if x == k(D::Bucket) => (
							&[
								(Kw::Readonly, Cl::Readonly),
								(Kw::Backend, Cl::Backend),
								(Kw::Comment, Cl::Comment),
							],
							"`READONLY`, `BACKEND`, or `COMMENT`",
						),
						x if x == k(D::Analyzer) => (
							&[
								(Kw::Function, Cl::Function),
								(Kw::Tokenizers, Cl::Tokenizers),
								(Kw::Filters, Cl::Filters),
								(Kw::Comment, Cl::Comment),
							],
							"`FUNCTION`, `TOKENIZERS`, `FILTERS`, or `COMMENT`",
						),
						x if x == k(D::Access) => (
							&[
								(Kw::Authenticate, Cl::Authenticate),
								(Kw::Context, Cl::Context),
								(Kw::Comment, Cl::Comment),
							],
							"`AUTHENTICATE`, `CONTEXT` or `COMMENT`",
						),
						x if x == k(D::Sequence) => return Ok(()),
						x if x == k(D::Api) => (&[(Kw::Comment, Cl::Comment)], "COMMENT"),
						_ => (&[(Kw::Comment, Cl::Comment)], "`COMMENT`"),
					};
					self.alter_drop(allowed, expected)?;
				}
				_ => {
					if !self.alter_clause(kind, field_is_id, t)? {
						return Ok(());
					}
				}
			}
		}
	}

	/// One kind-specific `ALTER` clause at `t`; `false` when `t` starts none.
	fn alter_clause(&mut self, kind: u8, field_is_id: bool, t: Tok) -> R<bool> {
		use DefineKind as D;
		let is = |d: D| kind == d as u8;
		match t.kw {
			Kw::Compact if is(D::Table) => self.flag_clause(Cl::Compact)?,
			Kw::Inline if is(D::Table) => {
				self.bump();
				match self.tok.kw {
					Kw::Edges if self.tok.kind == T::Ident => self.uint_clause(Cl::InlineEdges)?,
					Kw::References if self.tok.kind == T::Ident => {
						self.uint_clause(Cl::InlineRefs)?
					}
					_ => return Err(self.unexpected("`EDGES` or `REFERENCES`")),
				}
			}
			Kw::Type if is(D::Table) => self.table_type_clause()?,
			Kw::Schemaless if is(D::Table) => self.flag_clause(Cl::Schemaless)?,
			Kw::Schemafull if is(D::Table) => self.flag_clause(Cl::Schemafull)?,
			Kw::Permissions if is(D::Table) || is(D::Field) => self.permissions(false)?,
			Kw::ChangeFeed if is(D::Table) => self.changefeed()?,
			Kw::When if is(D::Event) => self.expr_clause(Cl::When)?,
			Kw::Then if is(D::Event) => {
				self.bump();
				let first = self.ast.nodes.len();
				let list = self.expr_list(true)?;
				self.push_clause(Cl::Then, list, 0, t.start, first)?;
			}
			Kw::Async if is(D::Event) => self.flag_clause(Cl::Async)?,
			Kw::Retry if is(D::Event) => self.uint_clause(Cl::Retry)?,
			Kw::MaxDepth if is(D::Event) => self.uint_clause(Cl::MaxDepth)?,
			Kw::Prepare if is(D::Index) => {
				self.bump();
				self.eat_kw(Kw::Remove);
				self.push_clause(Cl::PrepareRemove, 0, 0, t.start, self.ast.nodes.len())?;
			}
			Kw::Type if is(D::Field) => {
				self.bump();
				let first = self.ast.nodes.len();
				let k = if field_is_id {
					self.id_kind()?
				} else {
					self.kind()?
				};
				let flexible = self.eat_kw(Kw::Flexible) as u32;
				self.push_clause(Cl::FieldType, k, flexible, t.start, first)?;
			}
			Kw::Flexible if is(D::Field) => self.flag_clause(Cl::Flexible)?,
			Kw::Readonly if is(D::Field) || is(D::Bucket) => self.flag_clause(Cl::Readonly)?,
			Kw::Inline if is(D::Field) => self.flag_clause(Cl::Inline)?,
			Kw::Value if is(D::Field) || is(D::Param) => self.expr_clause(Cl::Value)?,
			Kw::Assert if is(D::Field) => self.expr_clause(Cl::Assert)?,
			Kw::Default if is(D::Field) => {
				self.bump();
				let first = self.ast.nodes.len();
				let always = self.eat_kw(Kw::Always) as u32;
				let e = self.expr(true)?;
				self.push_clause(Cl::Default, e, always, t.start, first)?;
			}
			Kw::Reference if is(D::Field) => {
				self.bump();
				let first = self.ast.nodes.len();
				let (a, b) = self.reference_strategy()?;
				self.push_clause(Cl::Reference, a, b, t.start, first)?;
			}
			Kw::Permissions
				if is(D::Param) || is(D::Bucket) || is(D::Function) || is(D::Module) =>
			{
				self.perm_value_clause()?
			}
			Kw::Backend if is(D::Bucket) => self.string_clause(Cl::Backend)?,
			Kw::Function if is(D::Analyzer) => self.analyzer_function_clause()?,
			Kw::Tokenizers if is(D::Analyzer) => self.tokenizers_clause()?,
			Kw::Filters if is(D::Analyzer) => self.filters_clause()?,
			Kw::Password if is(D::User) => self.string_clause(Cl::Password)?,
			Kw::Passhash if is(D::User) => self.string_clause(Cl::Passhash)?,
			Kw::Passscram if is(D::User) => self.string_clause(Cl::Passscram)?,
			Kw::Roles if is(D::User) => {
				self.bump();
				let first = self.ast.nodes.len();
				let mark = self.vals.len() as u32;
				loop {
					let n = self.name()?;
					self.vals.push(n);
					if !self.eat(T::Comma) {
						break;
					}
				}
				let list = self.list_from(mark).0;
				self.push_clause(Cl::Roles, list, 0, t.start, first)?;
			}
			Kw::Duration if is(D::User) => self.alter_durations(
				&[(Kw::Token, Cl::DurToken), (Kw::Session, Cl::DurSession)],
				"`TOKEN` or `SESSION`",
			)?,
			Kw::Duration if is(D::Access) => self.alter_durations(
				&[
					(Kw::Grant, Cl::DurGrant),
					(Kw::Token, Cl::DurToken),
					(Kw::Session, Cl::DurSession),
				],
				"`GRANT`, `TOKEN`, or `SESSION`",
			)?,
			Kw::Authenticate if is(D::Access) => self.expr_clause(Cl::Authenticate)?,
			Kw::Context if is(D::Access) => self.expr_clause(Cl::Context)?,
			Kw::Timeout if is(D::Sequence) => {
				self.expr_clause(Cl::Timeout)?;
				return Ok(false);
			}
			Kw::For if is(D::Api) => self.alter_api_for()?,
			_ => return Ok(false),
		}
		Ok(true)
	}

	fn alter_signature(&mut self) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		if !self.eat(T::RParen) {
			loop {
				let p = self.expect(T::Param, "a parameter")?;
				let p = self.leaf(Tag::Param, 0, p);
				self.vals.push(p);
				self.expect(T::Colon, ":")?;
				let k = self.kind()?;
				self.vals.push(k);
				if !self.eat(T::Comma) {
					break;
				}
			}
			self.expect(T::RParen, ")")?;
		}
		let args = self.list_from(mark).0;
		let returns = if self.eat(T::Arrow) {
			self.kind()?
		} else {
			NONE
		};
		let block = self.block()?;
		let idx = self.record(&[args, returns, block]);
		self.push_clause(Cl::Signature, idx, 0, t.start, first)
	}

	fn alter_api_for(&mut self) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let m = self.tok;
		if m.is_kw(Kw::Any) {
			self.bump();
			let cfg = self.api_config()?;
			let fallback = if self.eat_kw(Kw::Then) {
				self.expr(true)?
			} else if self.eat_kw(Kw::Drop) {
				self.expect_kw(Kw::Then, "THEN")?;
				NONE - 1
			} else {
				NONE
			};
			return self.push_clause(Cl::ApiAny, cfg, fallback, t.start, first);
		}
		let mark = self.vals.len() as u32;
		loop {
			let n = self.tok;
			let ok = n.kind == T::Ident
				&& matches!(
					n.kw,
					Kw::Delete | Kw::Get | Kw::Patch | Kw::Post | Kw::Put | Kw::Trace
				);
			if !ok {
				return Err(self.unexpected_at(
					m,
					"`any`, `DELETE`, `GET`, `PATCH`, `POST`, `PUT` or `TRACE`",
				));
			}
			self.bump();
			let name = self.leaf(Tag::Name, 0, n);
			self.vals.push(name);
			if !self.eat(T::Comma) {
				break;
			}
		}
		let methods = self.list_from(mark).0;
		if self.eat_kw(Kw::Drop) {
			self.expect_kw(Kw::Then, "THEN")?;
			return self.push_clause(Cl::ApiDropAction, methods, 0, t.start, first);
		}
		let cfg = self.api_config()?;
		self.expect_kw(Kw::Then, "THEN")?;
		let action = self.expr(true)?;
		let idx = self.record(&[methods, cfg, action]);
		self.push_clause(Cl::ApiMethods, idx, 0, t.start, first)
	}
}
