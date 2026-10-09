//! `DEFINE` statements.
//!
//! Each definition is a [`Tag::Define`] node over a fixed record (name, `ON`
//! target, kind-specific slots) and a list of [`Tag::Clause`] nodes in source
//! order. Which clauses a definition accepts, and in what combination, follows
//! `syn` exactly; what a clause means, including the checks `syn` makes on
//! combinations of clauses, is left to lowering, which replays them in order.

use crate::ast::{Cl, DefineKind, define, jwt, vector};
use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	/// A clause node over everything appended since node `first`.
	pub(crate) fn clause(&mut self, cl: Cl, a: u32, b: u32, start: u32, first: usize) -> R<NodeId> {
		self.stmt_node(Tag::Clause, cl as u8, a, b, start, first)
	}

	/// Appends a clause to the value stack.
	pub(crate) fn push_clause(
		&mut self,
		cl: Cl,
		a: u32,
		b: u32,
		start: u32,
		first: usize,
	) -> R<()> {
		let c = self.clause(cl, a, b, start, first)?;
		self.vals.push(c);
		Ok(())
	}

	/// Parses `KEYWORD expr` clauses: consumes the keyword and the expression.
	pub(crate) fn expr_clause(&mut self, cl: Cl) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let e = self.expr(true)?;
		self.push_clause(cl, e, 0, t.start, first)
	}

	pub(crate) fn flag_clause(&mut self, cl: Cl) -> R<()> {
		let t = self.bump();
		self.push_clause(cl, 0, 0, t.start, self.ast.nodes.len())
	}

	pub(crate) fn string_clause(&mut self, cl: Cl) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let s = self.string_lit()?;
		self.push_clause(cl, s, 0, t.start, first)
	}

	pub(crate) fn uint_clause(&mut self, cl: Cl) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let n = self.uint_lit()?;
		self.push_clause(cl, n, 0, t.start, first)
	}

	/// `DEFINE …` after `DEFINE`.
	pub(crate) fn define(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let t = self.tok;
		let kind = match t.kw {
			_ if t.kind != T::Ident => {
				return Err(self.unexpected("expected a define statement keyword"));
			}
			Kw::Namespace => DefineKind::Namespace,
			Kw::Database => DefineKind::Database,
			Kw::Function => DefineKind::Function,
			Kw::User => DefineKind::User,
			Kw::Param => DefineKind::Param,
			Kw::Table | Kw::Tb => DefineKind::Table,
			Kw::Api => DefineKind::Api,
			Kw::Event => DefineKind::Event,
			Kw::Field => DefineKind::Field,
			Kw::Index => DefineKind::Index,
			Kw::Analyzer => DefineKind::Analyzer,
			Kw::Access => DefineKind::Access,
			Kw::Config => DefineKind::Config,
			Kw::Bucket => DefineKind::Bucket,
			Kw::Sequence => DefineKind::Sequence,
			Kw::Module => DefineKind::Module,
			_ => return Err(self.unexpected("expected a define statement keyword")),
		};
		self.bump();
		let mut rec = [NONE; define::LEN];
		rec[define::EXIST] = if self.eat_kw(Kw::If) {
			self.expect_kw(Kw::Not, "expected `NOT`")?;
			self.expect_kw(Kw::Exists, "expected `EXISTS`")?;
			1
		} else if self.eat_kw(Kw::Overwrite) {
			2
		} else {
			0
		};
		let mark = self.vals.len() as u32;
		match kind {
			DefineKind::Namespace => {
				rec[define::NAME] = self.expr(true)?;
				while self.at_kw(Kw::Comment) {
					self.expr_clause(Cl::Comment)?;
				}
			}
			DefineKind::Database => {
				rec[define::NAME] = self.expr(true)?;
				loop {
					match self.tok.kw {
						Kw::Comment => self.expr_clause(Cl::Comment)?,
						Kw::ChangeFeed => self.changefeed()?,
						Kw::Strict => self.flag_clause(Cl::Strict)?,
						_ => break,
					}
				}
			}
			DefineKind::Function => self.define_function(&mut rec)?,
			DefineKind::User => {
				rec[define::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "expected `ON`")?;
				rec[define::ON] = self.base()?;
				self.user_clauses()?;
			}
			DefineKind::Access => {
				rec[define::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "expected `ON`")?;
				rec[define::ON] = self.base()?;
				self.access_clauses()?;
			}
			DefineKind::Param => {
				let p = self.expect(T::Param, "expected a parameter")?;
				rec[define::NAME] = self.leaf(Tag::Param, 0, p);
				loop {
					match self.tok.kw {
						Kw::Value => self.expr_clause(Cl::Value)?,
						Kw::Comment => self.expr_clause(Cl::Comment)?,
						Kw::Permissions => self.perm_value_clause()?,
						_ => break,
					}
				}
			}
			DefineKind::Table => {
				rec[define::NAME] = self.expr(false)?;
				self.table_clauses()?;
			}
			DefineKind::Api => {
				rec[define::NAME] = self.expr(true)?;
				self.api_clauses()?;
			}
			DefineKind::Event => {
				rec[define::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "expected `ON`")?;
				self.eat_kw(Kw::Table);
				rec[define::ON] = self.expr(false)?;
				self.event_clauses()?;
			}
			DefineKind::Field => {
				rec[define::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "expected `ON`")?;
				self.eat_kw(Kw::Table);
				rec[define::ON] = self.expr(false)?;
				self.field_clauses()?;
			}
			DefineKind::Index => {
				rec[define::NAME] = self.expr(true)?;
				self.expect_kw(Kw::On, "expected `ON`")?;
				self.eat_kw(Kw::Table);
				rec[define::ON] = self.expr(false)?;
				self.index_clauses()?;
			}
			DefineKind::Analyzer => {
				rec[define::NAME] = self.expr(true)?;
				self.analyzer_clauses()?;
			}
			DefineKind::Config => {
				let c = self.tok;
				match c.kw {
					Kw::Api if c.kind == T::Ident => {
						self.bump();
						rec[define::ON] = 0;
						let cfg = self.api_config()?;
						self.vals.push(cfg);
					}
					Kw::Graphql if c.kind == T::Ident => {
						self.bump();
						rec[define::ON] = 1;
						self.graphql_config()?;
					}
					Kw::Default if c.kind == T::Ident => {
						self.bump();
						rec[define::ON] = 2;
						if !(self.at_kw(Kw::Namespace) || self.at_kw(Kw::Database)) {
							return Err(self.unexpected("expected a namespace or database name"));
						}
						loop {
							match self.tok.kw {
								Kw::Namespace => self.expr_clause(Cl::DefaultNs)?,
								Kw::Database => self.expr_clause(Cl::DefaultDb)?,
								_ => break,
							}
						}
					}
					_ => return Err(self.unexpected("expected a type of config")),
				}
			}
			DefineKind::Bucket => {
				rec[define::NAME] = self.expr(true)?;
				loop {
					match self.tok.kw {
						Kw::Backend => self.expr_clause(Cl::Backend)?,
						Kw::Permissions => self.perm_value_clause()?,
						Kw::Readonly => self.flag_clause(Cl::Readonly)?,
						Kw::Comment => self.expr_clause(Cl::Comment)?,
						_ => break,
					}
				}
			}
			DefineKind::Sequence => {
				rec[define::NAME] = self.expr(true)?;
				if self.at_kw(Kw::Batch) {
					self.expr_clause(Cl::Batch)?;
				}
				if self.at_kw(Kw::Start) {
					self.expr_clause(Cl::Start)?;
				}
				if self.at_kw(Kw::Timeout) {
					self.expr_clause(Cl::Timeout)?;
				}
			}
			DefineKind::Module => self.define_module(&mut rec)?,
		}
		rec[define::CLAUSES] = self.list_from(mark).0;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Define, kind as u8, idx, 0, start, first)
	}

	/// `CHANGEFEED duration [INCLUDE ORIGINAL]`.
	pub(crate) fn changefeed(&mut self) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let d = self.duration_lit()?;
		let original = if self.eat_kw(Kw::Include) {
			self.expect_kw(Kw::Original, "expected `ORIGINAL`")?;
			1
		} else {
			0
		};
		self.push_clause(Cl::Changefeed, d, original, t.start, first)
	}

	/// `NONE`, `FULL` or `WHERE expr`, as a [`Cl::Perm`] clause.
	pub(crate) fn perm_value(&mut self) -> R<NodeId> {
		let t = self.tok;
		let first = self.ast.nodes.len();
		let (a, b) = match t.kw {
			_ if t.kind != T::Ident => {
				return Err(self.unexpected("expected `NONE`, `FULL` or `WHERE`"));
			}
			Kw::None_ => {
				self.bump();
				(0, NONE)
			}
			Kw::Full => {
				self.bump();
				(1, NONE)
			}
			Kw::Where => {
				self.bump();
				(2, self.expr(true)?)
			}
			_ => return Err(self.unexpected("expected `NONE`, `FULL` or `WHERE`")),
		};
		self.clause(Cl::Perm, a, b, t.start, first)
	}

	/// `PERMISSIONS value`.
	pub(crate) fn perm_value_clause(&mut self) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let p = self.perm_value()?;
		self.push_clause(Cl::PermValue, p, 0, t.start, first)
	}

	/// Table (`field` false) or field `PERMISSIONS`.
	pub(crate) fn permissions(&mut self, field: bool) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		let n = self.tok;
		match n.kw {
			Kw::None_ | Kw::Full if n.kind == T::Ident => {
				let f = self.ast.nodes.len();
				let p = self.perm_value()?;
				let c = self.clause(Cl::PermFor, 15, p, n.start, f)?;
				self.vals.push(c);
			}
			Kw::For if n.kind == T::Ident => {
				self.bump();
				loop {
					let s = self.tok.start;
					let f = self.ast.nodes.len();
					let mut actions = 0;
					loop {
						let a = self.tok;
						let bit = match a.kw {
							_ if a.kind != T::Ident => 0,
							Kw::Select => 1,
							Kw::Create => 2,
							Kw::Update => 4,
							Kw::Delete => 8,
							_ => 0,
						};
						if bit == 8 && field {
							return Err(self.error(
								a.start,
								a.end,
								"can't define permission DELETE for fields",
							));
						}
						if bit == 0 {
							return Err(self
								.unexpected("expected `SELECT`, `CREATE`, `UPDATE` or `DELETE`"));
						}
						actions |= bit;
						self.bump();
						if !self.eat(T::Comma) {
							break;
						}
					}
					let p = self.perm_value()?;
					let c = self.clause(Cl::PermFor, actions, p, s, f)?;
					self.vals.push(c);
					self.eat(T::Comma);
					if !self.eat_kw(Kw::For) {
						break;
					}
				}
			}
			_ => return Err(self.unexpected("expected `NONE`, `FULL` or `FOR`")),
		}
		let list = self.list_from(mark).0;
		self.push_clause(Cl::Permissions, list, field as u32, t.start, first)
	}

	/// `fn::a::b` as a `Name` spanning the path after `fn::`.
	pub(crate) fn custom_function_name(&mut self) -> R<NodeId> {
		self.expect_kw(Kw::Fn, "expected `fn`")?;
		self.expect(T::PathSep, "expected `::`")?;
		let first = self.ident()?;
		let mut end = first.end;
		while self.eat(T::PathSep) {
			end = self.ident()?.end;
		}
		Ok(self.leaf_span(Tag::Name, 1, 0, 0, first.start, end))
	}

	pub(crate) fn define_function(&mut self, rec: &mut [u32]) -> R<()> {
		rec[define::NAME] = self.custom_function_name()?;
		self.expect(T::LParen, "expected `(`")?;
		let mark = self.vals.len() as u32;
		loop {
			if self.eat(T::RParen) {
				break;
			}
			let p = self.expect(T::Param, "expected a parameter")?;
			let p = self.leaf(Tag::Param, 0, p);
			self.vals.push(p);
			self.expect(T::Colon, "expected `:`")?;
			let k = self.kind()?;
			self.vals.push(k);
			if !self.eat(T::Comma) {
				self.expect(T::RParen, "expected `)`")?;
				break;
			}
		}
		rec[define::X1] = self.list_from(mark).0;
		if self.eat(T::Arrow) {
			rec[define::X2] = self.kind()?;
		}
		rec[define::X3] = self.block()?;
		loop {
			match self.tok.kw {
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Permissions => self.perm_value_clause()?,
				Kw::GraphqlAlias => self.string_clause(Cl::GraphqlAlias)?,
				Kw::GraphqlDeprecated => self.string_clause(Cl::GraphqlDeprecated)?,
				_ => break,
			}
		}
		Ok(())
	}

	pub(crate) fn user_clauses(&mut self) -> R<()> {
		loop {
			match self.tok.kw {
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Password => self.string_clause(Cl::Password)?,
				Kw::Passhash => self.string_clause(Cl::Passhash)?,
				Kw::Passscram => self.string_clause(Cl::Passscram)?,
				Kw::Roles => {
					let t = self.bump();
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
				Kw::Duration => {
					self.bump();
					self.expect_kw(Kw::For, "expected `FOR`")?;
					loop {
						match self.tok.kw {
							Kw::Token if self.tok.kind == T::Ident => {
								self.expr_clause(Cl::DurToken)?
							}
							Kw::Session if self.tok.kind == T::Ident => {
								self.expr_clause(Cl::DurSession)?
							}
							_ => return Err(self.unexpected("expected `TOKEN` or `SESSION`")),
						}
						self.eat(T::Comma);
						if !self.eat_kw(Kw::For) {
							break;
						}
					}
				}
				_ => return Ok(()),
			}
		}
	}

	pub(crate) fn access_clauses(&mut self) -> R<()> {
		loop {
			match self.tok.kw {
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Type => {
					let t = self.bump();
					let first = self.ast.nodes.len();
					match self.tok.kw {
						Kw::Jwt if self.tok.kind == T::Ident => {
							self.bump();
							let j = self.jwt()?;
							self.push_clause(Cl::AccessJwt, j, 0, t.start, first)?;
						}
						Kw::Record if self.tok.kind == T::Ident => {
							self.bump();
							let mark = self.vals.len() as u32;
							loop {
								match self.tok.kw {
									Kw::Signup => self.expr_clause(Cl::Signup)?,
									Kw::Signin => self.expr_clause(Cl::Signin)?,
									_ => break,
								}
							}
							while self.eat_kw(Kw::With) {
								let w = self.tok;
								match w.kw {
									Kw::Jwt if w.kind == T::Ident => {
										self.bump();
										let f = self.ast.nodes.len();
										let j = self.jwt()?;
										self.push_clause(Cl::WithJwt, j, 0, w.start, f)?;
									}
									Kw::Refresh if w.kind == T::Ident => {
										self.flag_clause(Cl::WithRefresh)?;
									}
									_ => return Err(self.unexpected("expected `JWT` or `REFRESH`")),
								}
								self.eat(T::Comma);
							}
							let list = self.list_from(mark).0;
							self.push_clause(Cl::AccessRecord, list, 0, t.start, first)?;
						}
						Kw::Bearer if self.tok.kind == T::Ident => {
							self.bump();
							self.expect_kw(Kw::For, "expected `FOR`")?;
							let subject = match self.tok.kw {
								Kw::User if self.tok.kind == T::Ident => 0,
								Kw::Record if self.tok.kind == T::Ident => 1,
								_ => return Err(self.unexpected("expected `USER` or `RECORD`")),
							};
							self.bump();
							let j = if self.eat_kw(Kw::With) {
								self.expect_kw(Kw::Jwt, "expected `JWT`")?;
								self.jwt()?
							} else {
								NONE
							};
							self.push_clause(Cl::AccessBearer, subject, j, t.start, first)?;
						}
						// `syn` ends the clause list here with `TYPE` consumed.
						_ => return Ok(()),
					}
				}
				Kw::Authenticate => self.expr_clause(Cl::Authenticate)?,
				Kw::Context => self.expr_clause(Cl::Context)?,
				Kw::Duration => {
					self.bump();
					while self.eat_kw(Kw::For) {
						match self.tok.kw {
							Kw::Grant if self.tok.kind == T::Ident => {
								self.expr_clause(Cl::DurGrant)?
							}
							Kw::Token if self.tok.kind == T::Ident => {
								self.expr_clause(Cl::DurToken)?
							}
							Kw::Session if self.tok.kind == T::Ident => {
								self.expr_clause(Cl::DurSession)?
							}
							_ => {
								return Err(
									self.unexpected("expected `GRANT`, `TOKEN` or `SESSION`")
								);
							}
						}
						self.eat(T::Comma);
					}
				}
				_ => return Ok(()),
			}
		}
	}

	/// An algorithm name: `HS256`, `EDDSA`, …
	pub(crate) fn algorithm(&mut self) -> R<NodeId> {
		let t = self.tok;
		if t.kind == T::Ident && is_algorithm(self.text_of(t)) {
			self.bump();
			return Ok(self.leaf(Tag::Name, 0, t));
		}
		Err(self.unexpected("expected a valid algorithm"))
	}

	/// A JWT definition, as a [`Cl::Jwt`] clause.
	pub(crate) fn jwt(&mut self) -> R<NodeId> {
		let start = self.tok.start;
		let first = self.ast.nodes.len();
		let mut rec = [NONE; jwt::LEN];
		match self.tok.kw {
			Kw::Algorithm if self.tok.kind == T::Ident => {
				self.bump();
				rec[jwt::ALG] = self.algorithm()?;
				self.expect_kw(Kw::Key, "expected a key")?;
				rec[jwt::KEY] = self.expr(true)?;
			}
			Kw::Url if self.tok.kind == T::Ident => {
				self.bump();
				rec[jwt::KEY] = self.expr(true)?;
			}
			_ => return Err(self.unexpected("expected `ALGORITHM` or `URL`")),
		}
		if self.eat_kw(Kw::Audience) {
			let mark = self.vals.len() as u32;
			loop {
				let e = self.expr(true)?;
				self.vals.push(e);
				if !self.eat(T::Comma) {
					break;
				}
			}
			rec[jwt::AUDIENCE] = self.list_from(mark).0;
		}
		if self.eat_kw(Kw::With) {
			self.expect_kw(Kw::Issuer, "expected `ISSUER`")?;
			rec[jwt::ISSUER] = 1;
			loop {
				match self.tok.kw {
					Kw::Algorithm if self.tok.kind == T::Ident => {
						self.bump();
						rec[jwt::ISSUER_ALG] = self.algorithm()?;
					}
					Kw::Key if self.tok.kind == T::Ident => {
						self.bump();
						rec[jwt::ISSUER_KEY] = self.expr(true)?;
					}
					_ => break,
				}
			}
		}
		let idx = self.record(&rec);
		self.clause(Cl::Jwt, idx, 0, start, first)
	}

	pub(crate) fn table_clauses(&mut self) -> R<()> {
		loop {
			match self.tok.kw {
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Drop => self.flag_clause(Cl::Drop)?,
				Kw::Type => self.table_type_clause()?,
				Kw::Schemaless => self.flag_clause(Cl::Schemaless)?,
				Kw::Schemafull => self.flag_clause(Cl::Schemafull)?,
				Kw::Permissions => self.permissions(false)?,
				Kw::ChangeFeed => self.changefeed()?,
				Kw::As => {
					let t = self.bump();
					let first = self.ast.nodes.len();
					let v = if self.eat(T::LParen) {
						let v = self.view()?;
						self.expect(T::RParen, "expected `)`")?;
						v
					} else if self.at_kw(Kw::Select) {
						self.view()?
					} else {
						return Err(self.unexpected("expected `SELECT`"));
					};
					self.push_clause(Cl::View, v, 0, t.start, first)?;
				}
				Kw::Inline => {
					self.bump();
					match self.tok.kw {
						Kw::Edges if self.tok.kind == T::Ident => {
							self.uint_clause(Cl::InlineEdges)?
						}
						Kw::References if self.tok.kind == T::Ident => {
							self.uint_clause(Cl::InlineRefs)?
						}
						_ => return Err(self.unexpected("expected `EDGES` or `REFERENCES`")),
					}
				}
				Kw::GraphqlAlias => self.string_clause(Cl::GraphqlAlias)?,
				Kw::GraphqlDeprecated => self.string_clause(Cl::GraphqlDeprecated)?,
				_ => return Ok(()),
			}
		}
	}

	/// `[ON DELETE REJECT | CASCADE | IGNORE | UNSET | THEN expr]` after
	/// `REFERENCE`, as a [`Cl::Reference`] payload.
	pub(crate) fn reference_strategy(&mut self) -> R<(u32, u32)> {
		if !self.eat_kw(Kw::On) {
			return Ok((5, NONE));
		}
		self.expect_kw(Kw::Delete, "expected `DELETE`")?;
		let n = self.tok;
		let a = match n.kw {
			_ if n.kind != T::Ident => 9,
			Kw::Reject => 0,
			Kw::Cascade => 1,
			Kw::Ignore => 2,
			Kw::Unset => 3,
			Kw::Then => {
				self.bump();
				return Ok((4, self.expr(true)?));
			}
			_ => 9,
		};
		if a == 9 {
			return Err(
				self.unexpected("expected `REJECT`, `CASCADE`, `IGNORE`, `UNSET` or `THEN`")
			);
		}
		self.bump();
		Ok((a, NONE))
	}

	/// `TYPE NORMAL | ANY | RELATION …`.
	pub(crate) fn table_type_clause(&mut self) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		match self.tok.kw {
			Kw::Normal if self.tok.kind == T::Ident => {
				self.bump();
				self.push_clause(Cl::TypeNormal, 0, 0, t.start, first)
			}
			Kw::Any if self.tok.kind == T::Ident => {
				self.bump();
				self.push_clause(Cl::TypeAny, 0, 0, t.start, first)
			}
			Kw::Relation if self.tok.kind == T::Ident => {
				self.bump();
				let mut rel = [NONE, NONE, 0];
				loop {
					match self.tok.kw {
						Kw::From | Kw::In => {
							self.bump();
							rel[0] = self.table_names()?;
						}
						Kw::To | Kw::Out => {
							self.bump();
							rel[1] = self.table_names()?;
						}
						Kw::Enforced => {
							self.bump();
							rel[2] |= 1;
						}
						Kw::Lightweight => {
							self.bump();
							rel[2] |= 2;
						}
						_ => break,
					}
				}
				let idx = self.record(&rel);
				self.push_clause(Cl::TypeRelation, idx, 0, t.start, first)
			}
			_ => Err(self.unexpected("expected `NORMAL`, `RELATION`, or `ANY`")),
		}
	}

	/// `a | b | c` table names, as a list of `Name`s.
	pub(crate) fn table_names(&mut self) -> R<u32> {
		let mark = self.vals.len() as u32;
		loop {
			let n = self.name()?;
			self.vals.push(n);
			if !self.eat(T::Pipe) {
				break;
			}
		}
		Ok(self.list_from(mark).0)
	}

	/// A table view: `SELECT fields FROM tables [WHERE …] [GROUP …]`, as a
	/// `Select` node whose `what` is a list of table `Name`s.
	pub(crate) fn view(&mut self) -> R<NodeId> {
		let t = self.expect_kw(Kw::Select, "expected `SELECT`")?;
		let first = self.ast.nodes.len();
		let mut rec = [NONE; crate::ast::select::LEN];
		if self.eat_kw(Kw::Value) {
			rec[crate::ast::select::VALUE] = self.value_selector()?;
		} else {
			rec[crate::ast::select::FIELDS] = self.fields()?;
		}
		self.expect_kw(Kw::From, "expected `FROM`")?;
		let mark = self.vals.len() as u32;
		loop {
			let n = self.name()?;
			self.vals.push(n);
			if !self.eat(T::Comma) {
				break;
			}
		}
		rec[crate::ast::select::WHAT] = self.list_from(mark).0;
		if self.eat_kw(Kw::Where) {
			rec[crate::ast::select::COND] = self.expr(true)?;
		}
		let mut flags = 0;
		self.group_clause(&mut rec, &mut flags)?;
		rec[crate::ast::select::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Select, 1, idx, 0, t.start, first)
	}

	pub(crate) fn api_clauses(&mut self) -> R<()> {
		while self.at_kw(Kw::For) {
			let t = self.bump();
			let first = self.ast.nodes.len();
			let m = self.tok;
			if m.is_kw(Kw::Any) {
				self.bump();
				let cfg = self.api_config()?;
				let fallback = if self.eat_kw(Kw::Then) {
					self.expr(true)?
				} else {
					NONE
				};
				self.push_clause(Cl::ApiAny, cfg, fallback, t.start, first)?;
				continue;
			}
			let mark = self.vals.len() as u32;
			loop {
				let t = self.tok;
				let ok = t.kind == T::Ident
					&& matches!(
						t.kw,
						Kw::Delete | Kw::Get | Kw::Patch | Kw::Post | Kw::Put | Kw::Trace
					);
				if !ok {
					return Err(self.error(
						m.start,
						m.end,
						"expected one of `DELETE`, `GET`, `PATCH`, `POST`, `PUT` or `TRACE`",
					));
				}
				self.bump();
				let n = self.leaf(Tag::Name, 0, t);
				self.vals.push(n);
				if !self.eat(T::Comma) {
					break;
				}
			}
			let methods = self.list_from(mark).0;
			let cfg = self.api_config()?;
			self.expect_kw(Kw::Then, "expected `THEN`")?;
			let action = self.expr(true)?;
			let idx = self.record(&[methods, cfg, action]);
			self.push_clause(Cl::ApiMethods, idx, 0, t.start, first)?;
		}
		if self.at_kw(Kw::Comment) {
			self.expr_clause(Cl::Comment)?;
		}
		Ok(())
	}

	/// `[PERMISSIONS …] [MIDDLEWARE f(…), …]`, repeated, as a
	/// [`Cl::ApiConfig`] clause.
	pub(crate) fn api_config(&mut self) -> R<NodeId> {
		let start = self.tok.start;
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		loop {
			match self.tok.kw {
				Kw::Permissions if self.tok.kind == T::Ident => self.perm_value_clause()?,
				Kw::Middleware if self.tok.kind == T::Ident => {
					let t = self.bump();
					let f = self.ast.nodes.len();
					let m = self.vals.len() as u32;
					loop {
						let c = self.middleware_call()?;
						self.vals.push(c);
						if !self.eat(T::Comma) {
							break;
						}
					}
					let list = self.list_from(m).0;
					self.push_clause(Cl::Middleware, list, 0, t.start, f)?;
				}
				_ => break,
			}
		}
		let list = self.list_from(mark).0;
		self.clause(Cl::ApiConfig, list, 0, start, first)
	}

	/// A middleware call: a function name and its arguments, with nothing
	/// after the closing `)`.
	pub(crate) fn middleware_call(&mut self) -> R<NodeId> {
		let t = self.tok;
		if !(t.kind == T::Ident || t.kind == T::EscIdent) || self.peek1().kind == T::Colon {
			return Err(self.unexpected("expected a function name"));
		}
		let call = self.atom(true)?;
		let n = self.ast.nodes[call as usize];
		if n.tag != Tag::Call || n.depth & COVERED != 0 {
			return Err(self.error(n.start, n.end, "expected a function call"));
		}
		Ok(call)
	}

	pub(crate) fn graphql_config(&mut self) -> R<()> {
		loop {
			let t = self.tok;
			if t.kind != T::Ident {
				return Ok(());
			}
			let first = self.ast.nodes.len();
			match t.kw {
				Kw::None_ | Kw::Auto => {
					self.bump();
					self.push_clause(Cl::GqlBoth, (t.kw == Kw::Auto) as u32, 0, t.start, first)?;
				}
				Kw::Tables | Kw::Functions => {
					self.bump();
					let cl = if t.kw == Kw::Tables {
						Cl::GqlTables
					} else {
						Cl::GqlFunctions
					};
					let n = self.tok;
					let (mode, list) = match n.kw {
						_ if n.kind != T::Ident => (9, NONE),
						Kw::None_ => {
							self.bump();
							(0, NONE)
						}
						Kw::Auto => {
							self.bump();
							(1, NONE)
						}
						Kw::Include | Kw::Exclude => {
							self.bump();
							let mark = self.vals.len() as u32;
							loop {
								let name = if cl == Cl::GqlTables {
									self.name()?
								} else {
									self.custom_function_name()?
								};
								self.vals.push(name);
								if !self.eat(T::Comma) {
									break;
								}
							}
							(2 + (n.kw == Kw::Exclude) as u32, self.list_from(mark).0)
						}
						_ => (9, NONE),
					};
					if mode == 9 {
						return Err(
							self.unexpected("expected `NONE`, `AUTO`, `INCLUDE` or `EXCLUDE`")
						);
					}
					self.push_clause(cl, mode, list, t.start, first)?;
				}
				Kw::None => {
					let text = self.text_of(t);
					if text.eq_ignore_ascii_case("DEPTH") {
						self.uint_clause(Cl::GqlDepth)?;
					} else if text.eq_ignore_ascii_case("COMPLEXITY") {
						self.uint_clause(Cl::GqlComplexity)?;
					} else if text.eq_ignore_ascii_case("INTROSPECTION") {
						self.bump();
						let mode = match self.tok.kw {
							Kw::Auto if self.tok.kind == T::Ident => 1,
							Kw::None_ if self.tok.kind == T::Ident => 0,
							_ => return Err(self.unexpected("expected `AUTO` or `NONE`")),
						};
						self.bump();
						self.push_clause(Cl::GqlIntrospection, mode, 0, t.start, first)?;
					} else {
						return Ok(());
					}
				}
				_ => return Ok(()),
			}
		}
	}

	pub(crate) fn event_clauses(&mut self) -> R<()> {
		let mut then = false;
		loop {
			match self.tok.kw {
				Kw::When => self.expr_clause(Cl::When)?,
				Kw::Then => {
					let t = self.bump();
					let first = self.ast.nodes.len();
					let list = self.expr_list(true)?;
					self.push_clause(Cl::Then, list, 0, t.start, first)?;
					then = true;
				}
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Async => self.flag_clause(Cl::Async)?,
				Kw::Retry => self.uint_clause(Cl::Retry)?,
				Kw::MaxDepth => self.uint_clause(Cl::MaxDepth)?,
				_ => break,
			}
		}
		if !then {
			return Err(self.unexpected("expected at least one `THEN` statement"));
		}
		Ok(())
	}

	pub(crate) fn field_clauses(&mut self) -> R<()> {
		loop {
			match self.tok.kw {
				Kw::Flexible => {
					let t = self.bump();
					return Err(self.error(
						t.start,
						t.end,
						"FLEXIBLE must be specified after TYPE",
					));
				}
				Kw::Type => {
					let t = self.bump();
					let first = self.ast.nodes.len();
					let k = self.kind()?;
					let flexible = self.eat_kw(Kw::Flexible) as u32;
					self.push_clause(Cl::FieldType, k, flexible, t.start, first)?;
				}
				Kw::Readonly => self.flag_clause(Cl::Readonly)?,
				Kw::Inline => self.flag_clause(Cl::Inline)?,
				Kw::Value => self.expr_clause(Cl::Value)?,
				Kw::Assert => self.expr_clause(Cl::Assert)?,
				Kw::Default => {
					let t = self.bump();
					let first = self.ast.nodes.len();
					let always = self.eat_kw(Kw::Always) as u32;
					let e = self.expr(true)?;
					self.push_clause(Cl::Default, e, always, t.start, first)?;
				}
				Kw::Permissions => self.permissions(true)?,
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Reference => {
					let t = self.bump();
					let first = self.ast.nodes.len();
					let (a, b) = self.reference_strategy()?;
					self.push_clause(Cl::Reference, a, b, t.start, first)?;
				}
				Kw::Computed => self.expr_clause(Cl::Computed)?,
				Kw::GraphqlAlias => self.string_clause(Cl::GraphqlAlias)?,
				Kw::GraphqlDeprecated => self.string_clause(Cl::GraphqlDeprecated)?,
				_ => return Ok(()),
			}
		}
	}

	pub(crate) fn index_clauses(&mut self) -> R<()> {
		loop {
			let t = self.tok;
			match t.kw {
				Kw::Fields => {
					self.bump();
					let first = self.ast.nodes.len();
					let list = self.expr_list(true)?;
					self.push_clause(Cl::Fields, list, 0, t.start, first)?;
				}
				Kw::Unique => self.flag_clause(Cl::Unique)?,
				Kw::Count => {
					self.bump();
					let first = self.ast.nodes.len();
					let cond = if self.eat_kw(Kw::Where) {
						self.expr(true)?
					} else {
						NONE
					};
					self.push_clause(Cl::Count, cond, 0, t.start, first)?;
				}
				Kw::Fulltext => {
					self.bump();
					let first = self.ast.nodes.len();
					let mut analyzer = NONE;
					let mut scoring = NONE;
					let mut highlights = false;
					loop {
						match self.tok.kw {
							Kw::Analyzer if self.tok.kind == T::Ident => {
								self.bump();
								analyzer = self.name()?;
							}
							Kw::Bm25 if self.tok.kind == T::Ident => {
								self.bump();
								scoring = if self.eat(T::LParen) {
									let k1 = self.float_lit()?;
									self.expect(T::Comma, "expected `,`")?;
									let b = self.float_lit()?;
									self.expect(T::RParen, "expected `)`")?;
									self.record(&[k1, b])
								} else {
									NONE - 1
								};
							}
							Kw::Highlights if self.tok.kind == T::Ident => {
								self.bump();
								highlights = true;
							}
							_ => break,
						}
					}
					self.push_clause(Cl::Fulltext, analyzer, scoring, t.start, first)?;
					if highlights {
						self.push_clause(Cl::Highlights, 0, 0, t.start, self.ast.nodes.len())?;
					}
				}
				Kw::Hnsw => self.vector_index(Cl::Hnsw)?,
				Kw::Concurrently => self.flag_clause(Cl::Concurrently)?,
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::None
					if t.kind == T::Ident && self.text_of(t).eq_ignore_ascii_case("DISKANN") =>
				{
					self.vector_index(Cl::Diskann)?
				}
				_ => return Ok(()),
			}
		}
	}

	/// `HNSW …` or `DISKANN …`.
	pub(crate) fn vector_index(&mut self, cl: Cl) -> R<()> {
		let t = self.bump();
		let first = self.ast.nodes.len();
		self.expect_kw(Kw::Dimension, "expected `DIMENSION`")?;
		let mut rec = [NONE; vector::LEN];
		rec[vector::DIMENSION] = self.uint_lit()?;
		let mut flags = 0;
		loop {
			let n = self.tok;
			if n.kind != T::Ident {
				break;
			}
			match n.kw {
				Kw::Distance => {
					self.bump();
					let d = self.tok;
					if d.kind != T::Ident || !is_distance(self.text_of(d)) {
						return Err(self.unexpected("expected a distance measure"));
					}
					self.bump();
					rec[vector::DISTANCE] = self.leaf(Tag::Name, 0, d);
					rec[vector::MINKOWSKI] = NONE;
					if self.text_of(d).eq_ignore_ascii_case("MINKOWSKI") {
						rec[vector::MINKOWSKI] = self.number_lit()?;
					}
				}
				Kw::Type => {
					self.bump();
					let v = self.tok;
					if v.kind != T::Ident || !is_vector_type(self.text_of(v)) {
						return Err(self.unexpected("expected a vector type"));
					}
					self.bump();
					rec[vector::TYPE] = self.leaf(Tag::Name, 0, v);
				}
				Kw::HashedVector => {
					self.bump();
					flags |= 4;
				}
				Kw::Lm if cl == Cl::Hnsw => {
					self.bump();
					rec[vector::LM] = self.number_lit()?;
				}
				Kw::M0 if cl == Cl::Hnsw => {
					self.bump();
					rec[vector::M0] = self.uint_lit()?;
				}
				Kw::M if cl == Cl::Hnsw => {
					self.bump();
					rec[vector::M] = self.uint_lit()?;
				}
				Kw::Efc if cl == Cl::Hnsw => {
					self.bump();
					rec[vector::EFC] = self.uint_lit()?;
				}
				Kw::ExtendCandidates if cl == Cl::Hnsw => {
					self.bump();
					flags |= 1;
				}
				Kw::KeepPrunedConnections if cl == Cl::Hnsw => {
					self.bump();
					flags |= 2;
				}
				Kw::None if cl == Cl::Diskann => {
					let text = self.text_of(n);
					if text.eq_ignore_ascii_case("DEGREE") {
						self.bump();
						rec[vector::M] = self.uint_lit()?;
					} else if text.eq_ignore_ascii_case("L_BUILD") {
						self.bump();
						rec[vector::M0] = self.uint_lit()?;
					} else if text.eq_ignore_ascii_case("ALPHA") {
						self.bump();
						rec[vector::LM] = self.number_lit()?;
					} else {
						break;
					}
				}
				_ => break,
			}
		}
		rec[vector::FLAGS] = flags;
		let idx = self.record(&rec);
		self.push_clause(cl, idx, 0, t.start, first)
	}

	pub(crate) fn analyzer_clauses(&mut self) -> R<()> {
		loop {
			match self.tok.kw {
				Kw::Filters => self.filters_clause()?,
				Kw::Tokenizers => self.tokenizers_clause()?,
				Kw::Function => self.analyzer_function_clause()?,
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				_ => return Ok(()),
			}
		}
	}

	/// `FILTERS filter, …`.
	pub(crate) fn filters_clause(&mut self) -> R<()> {
		let t = self.tok;
		self.bump();
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		loop {
			let f = self.tok;
			let ff = self.ast.nodes.len();
			let code = match f.kw {
				_ if f.kind != T::Ident => 9,
				Kw::Ascii => 0,
				Kw::Lowercase => 1,
				Kw::Uppercase => 2,
				Kw::Edgengram => 3,
				Kw::Ngram => 4,
				Kw::Snowball => 5,
				Kw::Mapper => 6,
				_ => 9,
			};
			if code == 9 {
				return Err(self.unexpected("expected a filter"));
			}
			self.bump();
			let b = match code {
				3 | 4 => {
					self.expect(T::LParen, "expected `(`")?;
					let x = self.uint_lit()?;
					self.expect(T::Comma, "expected `,`")?;
					let y = self.uint_lit()?;
					self.expect(T::RParen, "expected `)`")?;
					self.record(&[x, y])
				}
				5 => {
					self.expect(T::LParen, "expected `(`")?;
					let l = self.tok;
					if l.kind != T::Ident || !(is_language(self.text_of(l)) || l.kw == Kw::No) {
						return Err(self.unexpected("expected a language"));
					}
					self.bump();
					let n = self.leaf(Tag::Name, 0, l);
					self.expect(T::RParen, "expected `)`")?;
					n
				}
				6 => {
					self.expect(T::LParen, "expected `(`")?;
					let p = self.string_lit()?;
					self.expect(T::RParen, "expected `)`")?;
					p
				}
				_ => NONE,
			};
			let c = self.clause(Cl::Filter, code, b, f.start, ff)?;
			self.vals.push(c);
			if !self.eat(T::Comma) {
				break;
			}
		}
		let list = self.list_from(mark).0;
		self.push_clause(Cl::Filters, list, 0, t.start, first)?;
		Ok(())
	}

	/// `TOKENIZERS tokenizer, …`.
	pub(crate) fn tokenizers_clause(&mut self) -> R<()> {
		let t = self.tok;
		self.bump();
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		loop {
			let k = self.tok;
			let ok = k.kind == T::Ident
				&& matches!(k.kw, Kw::Blank | Kw::Camel | Kw::Class | Kw::Punct | Kw::Segment);
			if !ok {
				return Err(self.unexpected("expected a tokenizer"));
			}
			self.bump();
			let n = if k.kw == Kw::Segment {
				self.expect(T::LParen, "expected `(`")?;
				let l = self.tok;
				if !(l.is_kw(Kw::Chinese) || l.is_kw(Kw::Japanese) || l.is_kw(Kw::Korean)) {
					return Err(self.unexpected("expected `CHINESE`, `JAPANESE` or `KOREAN`"));
				}
				self.bump();
				let lang = self.leaf(Tag::Name, 0, l);
				self.expect(T::RParen, "expected `)`")?;
				self.leaf_span(Tag::Name, 1, lang, 0, k.start, self.prev_end)
			} else {
				self.leaf(Tag::Name, 0, k)
			};
			self.vals.push(n);
			if !self.eat(T::Comma) {
				break;
			}
		}
		let list = self.list_from(mark).0;
		self.push_clause(Cl::Tokenizers, list, 0, t.start, first)?;
		Ok(())
	}

	/// An analyzer's `FUNCTION fn::…` or `FUNCTION mod::…`.
	pub(crate) fn analyzer_function_clause(&mut self) -> R<()> {
		let t = self.tok;
		self.bump();
		let first = self.ast.nodes.len();
		let f = self.tok;
		let name = match f.kw {
			Kw::Fn if f.kind == T::Ident => self.custom_function_name()?,
			Kw::Mod if f.kind == T::Ident => {
				self.bump();
				self.expect(T::PathSep, "expected `::`")?;
				let s = self.ident()?;
				let mut end = s.end;
				while self.eat(T::PathSep) {
					end = self.ident()?.end;
				}
				self.leaf_span(Tag::Name, 2, 0, 0, f.start, end)
			}
			Kw::Ml | Kw::Silo if f.kind == T::Ident => {
				return Err(self.error(
					f.start,
					f.end,
					"analyzers cannot use model or silo functions",
				));
			}
			_ => return Err(self.unexpected("expected `fn::` or `mod::`")),
		};
		if self.at(T::LParen) {
			return Err(
				self.unexpected("analyzer FUNCTION expects a function name, not a function call")
			);
		}
		self.push_clause(Cl::Function, name, 0, t.start, first)?;
		Ok(())
	}

	pub(crate) fn define_module(&mut self, rec: &mut [u32]) -> R<()> {
		if self.at_kw(Kw::Mod) {
			self.bump();
			self.expect(T::PathSep, "expected `::`")?;
			rec[define::NAME] = self.name()?;
			self.expect_kw(Kw::From, "expected `FROM`")?;
		}
		let t = self.tok;
		rec[define::X1] = match t.kind {
			T::Ident if t.kw == Kw::Silo => {
				self.bump();
				self.expect(T::PathSep, "expected `::`")?;
				let mark = self.vals.len() as u32;
				let org = self.name()?;
				self.vals.push(org);
				self.expect(T::PathSep, "expected `::`")?;
				let pkg = self.name()?;
				self.vals.push(pkg);
				self.expect(T::PathSep, "expected `::`")?;
				self.model_version()?;
				self.list_from(mark).0
			}
			T::Str if t.aux() == b'f' => {
				self.bump();
				self.leaf(Tag::Str, b'f', t)
			}
			_ => return Err(self.unexpected("expected a module executable")),
		};
		let mut unsigned = false;
		loop {
			match self.tok.kw {
				Kw::Unsigned => {
					unsigned = true;
					self.flag_clause(Cl::Unsigned)?
				}
				Kw::Comment => self.expr_clause(Cl::Comment)?,
				Kw::Permissions => self.perm_value_clause()?,
				_ => break,
			}
		}
		if !unsigned {
			return Err(
				self.unexpected("expected the `UNSIGNED` keyword on this module definition")
			);
		}
		Ok(())
	}

	/// `<major.minor.patch>`, pushing the three numbers onto the value stack.
	pub(crate) fn model_version(&mut self) -> R<()> {
		self.expect(T::Lt, "expected `<`")?;
		for i in 0..3 {
			if i > 0 && (!self.at(T::Dot) || self.tok.ws()) {
				return Err(self.unexpected("expected `.`"));
			}
			if i > 0 {
				self.bump();
			}
			let d = self.expect(T::Digits, "expected a version number")?;
			let v = crate::lex::decode_uint(&self.src[d.start as usize..d.end as usize])
				.filter(|v| *v <= u32::MAX as u64)
				.ok_or_else(|| self.error(d.start, d.end, "invalid version number"))?;
			let n = self.leaf_span(Tag::Int, 0, v as u32, 0, d.start, d.end);
			self.vals.push(n);
		}
		self.expect(T::Gt, "expected `>`")?;
		Ok(())
	}
}

fn is_algorithm(text: &str) -> bool {
	const ALGS: &[&str] = &[
		"EDDSA", "ES256", "ES384", "ES512", "HS256", "HS384", "HS512", "PS256", "PS384", "PS512",
		"RS256", "RS384", "RS512",
	];
	ALGS.iter().any(|a| a.eq_ignore_ascii_case(text))
}

pub(crate) fn is_distance(text: &str) -> bool {
	const DIST: &[&str] = &[
		"CHEBYSHEV",
		"COSINE",
		"COSINE_NORMALIZED",
		"EUCLIDEAN",
		"HAMMING",
		"INNER_PRODUCT",
		"JACCARD",
		"MANHATTAN",
		"MINKOWSKI",
		"PEARSON",
	];
	DIST.iter().any(|a| a.eq_ignore_ascii_case(text))
}

fn is_vector_type(text: &str) -> bool {
	const VT: &[&str] = &["F16", "F32", "F64", "I16", "I32", "I64", "I8", "U8"];
	VT.iter().any(|a| a.eq_ignore_ascii_case(text))
}

/// The snowball languages and their abbreviations.
pub(crate) fn is_language(text: &str) -> bool {
	const LANG: &[&str] = &[
		"AR",
		"ARA",
		"ARABIC",
		"DA",
		"DAN",
		"DANISH",
		"DE",
		"DEU",
		"DUTCH",
		"EL",
		"ELL",
		"EN",
		"ENG",
		"ENGLISH",
		"ES",
		"FI",
		"FIN",
		"FINNISH",
		"FR",
		"FRA",
		"FRENCH",
		"GERMAN",
		"GREEK",
		"HU",
		"HUN",
		"HUNGARIAN",
		"IT",
		"ITA",
		"ITALIAN",
		"NL",
		"NLD",
		"NOR",
		"NORWEGIAN",
		"POR",
		"PORTUGUESE",
		"PT",
		"RO",
		"ROMANIAN",
		"RON",
		"RU",
		"RUS",
		"RUSSIAN",
		"SPA",
		"SPANISH",
		"SV",
		"SWE",
		"SWEDISH",
		"TA",
		"TAM",
		"TAMIL",
		"TR",
		"TUR",
		"TURKISH",
	];
	LANG.iter().any(|a| a.eq_ignore_ascii_case(text))
}
