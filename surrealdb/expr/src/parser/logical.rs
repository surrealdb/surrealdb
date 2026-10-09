//! Logical expressions decoded from the parser's flat arena.
//!
//! Leaf decoding and semantic rejection share the runtime parser's rules.
//! Operators and collections allocate only their logical representation.

use std::collections::VecDeque;

use surrealdb_parser_next::ast::{Binary, InfoTarget, NONE, NodeId, Prefix, Tag, info};
use surrealdb_strand::Strand;

use super::{Lower, R, Reject, binary};
use crate::expr::logical::lower::{PathMode, Step, fold_steps};
use crate::expr::logical::{
	LogicalBreak, LogicalCall, LogicalContinue, LogicalEval, LogicalExplain, LogicalExpr,
	LogicalForeach, LogicalIf, LogicalIfOp, LogicalInfo, LogicalLet, LogicalOp,
	LogicalPostfixOperator, LogicalReturn, LogicalSequence, LogicalThrow, lower, lower_expr,
};
use crate::expr::{ExplainFormat, Expr, Function, Param, PrefixOperator};

impl Lower<'_> {
	fn logical_native(&self, id: NodeId, stored: bool) -> R<Expr> {
		let mut expression = self.expr(id)?;
		if stored {
			crate::expr::credentials::prepare(&mut expression);
		}
		Ok(expression)
	}

	pub(crate) fn logical_expr(&self, id: NodeId, stored: bool) -> R<LogicalExpr> {
		match self.n(id).tag {
			Tag::Array | Tag::Set | Tag::Object | Tag::Prefix | Tag::Cast | Tag::Binary => {
				self.logical_nesting(id, stored)
			}
			_ => self.logical_other(id, stored),
		}
	}

	#[inline(never)]
	fn logical_nesting(&self, id: NodeId, stored: bool) -> R<LogicalExpr> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::Array => LogicalExpr::Array(self.logical_exprs(n.a, stored)?),
			Tag::Set => LogicalExpr::Set(self.logical_exprs(n.a, stored)?),
			Tag::Object => {
				let pairs = self.list(n.a);
				// GeoJSON normalization uses the same literal classifier as
				// static values; objects with arbitrary expressions stay objects.
				let geometry = pairs.len() == 4
					&& pairs
						.chunks_exact(2)
						.any(|kv| self.name(kv[0]).is_ok_and(|key| key == "type"));
				if geometry {
					return lower_expr(self.logical_native(id, stored)?)
						.map_err(|e| Reject::new(e.to_string()));
				}
				LogicalExpr::Object(
					pairs
						.chunks_exact(2)
						.map(|kv| Ok((self.name(kv[0])?, self.logical_expr(kv[1], stored)?)))
						.collect::<R<_>>()?,
				)
			}
			Tag::Prefix => LogicalExpr::Prefix {
				op: match n.op {
					x if x == Prefix::Not as u8 => PrefixOperator::Not,
					x if x == Prefix::Neg as u8 => PrefixOperator::Negate,
					x if x == Prefix::Pos as u8 => PrefixOperator::Positive,
					x if x == Prefix::Range as u8 => PrefixOperator::Range,
					_ => PrefixOperator::RangeInclusive,
				},
				expr: Box::new(self.logical_expr(n.a, stored)?),
			},
			Tag::Cast => LogicalExpr::Prefix {
				op: PrefixOperator::Cast(self.kind(n.a)?),
				expr: Box::new(self.logical_expr(n.b, stored)?),
			},
			Tag::Binary => LogicalExpr::Binary {
				left: Box::new(self.logical_expr(n.a, stored)?),
				op: binary(Binary::from_u8(n.op)),
				right: Box::new(self.logical_expr(n.b, stored)?),
			},
			_ => unreachable!("only nesting constructs are lowered here"),
		})
	}

	#[inline(never)]
	fn logical_other(&self, id: NodeId, stored: bool) -> R<LogicalExpr> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::Field => {
				if n.op == 1 {
					LogicalExpr::Current
				} else {
					LogicalExpr::Field(Strand::new(self.name(id)?))
				}
			}
			Tag::Idiom => {
				let (base, steps) = self.logical_path(id, stored)?;
				fold_steps(base, steps, PathMode::Continuation)
			}
			Tag::BinaryX => LogicalExpr::Binary {
				left: Box::new(self.logical_expr(n.a, stored)?),
				op: self.binary_extended(n)?,
				right: Box::new(self.logical_expr(self.ast.extra[n.b as usize], stored)?),
			},
			Tag::RecordId => {
				LogicalExpr::RecordId(Box::new(crate::expr::logical::LogicalRecordIdLit {
					table: surrealdb_strand::TableName::new(Strand::new(self.name(n.a)?)),
					key: self.logical_key(n.b, stored)?,
				}))
			}

			Tag::Param => LogicalExpr::Param(Param::from(Strand::new(self.param_name(id)?))),
			Tag::PostfixRange => LogicalExpr::Postfix {
				expr: Box::new(self.logical_expr(n.a, stored)?),
				op: if n.op == 0 {
					LogicalPostfixOperator::Range
				} else {
					LogicalPostfixOperator::RangeSkip
				},
			},
			Tag::CallExpr => LogicalExpr::Invoke {
				target: Box::new(self.logical_expr(n.a, stored)?),
				args: self.logical_exprs(n.b, stored)?,
			},
			Tag::Call => {
				let callee = self.n(n.a);
				let receiver = if callee.op == 0 {
					let path = self.path_name(n.a)?;
					if !super::CHECK_NAMES.with(|c| c.get()) {
						Function::Normal(path.to_lowercase())
					} else {
						match super::lit::builtin(&path, callee.start, callee.end)? {
							super::lit::Builtin::Function(f) => Function::Normal(f),
							super::lit::Builtin::Constant(c) => {
								return Ok(LogicalExpr::Invoke {
									target: Box::new(LogicalExpr::Constant(c)),
									args: self.logical_exprs(n.b, stored)?,
								});
							}
						}
					}
				} else {
					self.function_ref(n.a)?
				};
				LogicalExpr::Call(Box::new(LogicalCall {
					receiver,
					args: self.logical_exprs(n.b, stored)?,
				}))
			}
			Tag::Script => LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Script(crate::expr::Script(self.text(n.a).to_owned())),
				args: self.logical_exprs(n.b, stored)?,
			})),
			Tag::Closure => {
				use surrealdb_types::ToSql;
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				LogicalExpr::Closure(Box::new(crate::expr::logical::LogicalClosure {
					args: self
						.list(rec[0])
						.chunks_exact(2)
						.map(|pk| {
							Ok((
								Param::from(Strand::new(self.param_name(pk[0])?)),
								if pk[1] == NONE {
									crate::expr::Kind::Any
								} else {
									self.kind(pk[1])?
								},
							))
						})
						.collect::<R<_>>()?,
					returns: if rec[1] == NONE {
						None
					} else {
						Some(self.kind(rec[1])?)
					},
					body: self.logical_expr(rec[2], stored)?,
					body_sql: std::sync::Arc::from(self.expr(rec[2])?.to_sql()),
				}))
			}

			Tag::If => LogicalExpr::If(Box::new(LogicalIf {
				branches: self
					.list(n.a)
					.chunks_exact(2)
					.map(|cb| {
						Ok((self.logical_expr(cb[0], stored)?, self.logical_expr(cb[1], stored)?))
					})
					.collect::<R<_>>()?,
				otherwise: self.logical_opt(n.b, stored)?,
			})),
			Tag::Throw => LogicalExpr::Throw(Box::new(self.logical_expr(n.a, stored)?)),
			Tag::Break => LogicalExpr::Break,
			Tag::Continue => LogicalExpr::Continue,
			Tag::Block
			| Tag::Return
			| Tag::Let
			| Tag::For
			| Tag::Info
			| Tag::Explain
			| Tag::Select
			| Tag::Sleep => LogicalExpr::Subquery(Box::new(self.logical_statement(id, stored)?)),
			_ => lower_expr(self.logical_native(id, stored)?)
				.map_err(|e| Reject::new(e.to_string()))?,
		})
	}

	fn logical_key(&self, id: NodeId, stored: bool) -> R<crate::expr::logical::LogicalRecordIdKey> {
		use std::ops::Bound;

		use crate::expr::logical::{LogicalRecordIdKey as Key, LogicalRecordIdKeyRange};
		let n = self.n(id);
		Ok(match n.tag {
			Tag::Array => Key::Array(self.logical_exprs(n.a, stored)?),
			Tag::Object => Key::Object(
				self.list(n.a)
					.chunks_exact(2)
					.map(|kv| Ok((self.name(kv[0])?, self.logical_expr(kv[1], stored)?)))
					.collect::<R<_>>()?,
			),
			Tag::KeyRange => Key::Range(Box::new(LogicalRecordIdKeyRange {
				start: if n.a == NONE {
					Bound::Unbounded
				} else if n.op & 1 != 0 {
					Bound::Excluded(self.logical_key(n.a, stored)?)
				} else {
					Bound::Included(self.logical_key(n.a, stored)?)
				},
				end: if n.b == NONE {
					Bound::Unbounded
				} else if n.op & 2 != 0 {
					Bound::Included(self.logical_key(n.b, stored)?)
				} else {
					Bound::Excluded(self.logical_key(n.b, stored)?)
				},
			})),
			_ => match self.key(id)? {
				crate::expr::RecordIdKeyLit::Number(v) => Key::Number(v),
				crate::expr::RecordIdKeyLit::String(v) => Key::String(v),
				crate::expr::RecordIdKeyLit::Uuid(v) => Key::Uuid(v),
				crate::expr::RecordIdKeyLit::Generate(v) => Key::Generate(v),
				_ => unreachable!("compound keys are decoded directly"),
			},
		})
	}

	fn logical_destructure(
		&self,
		id: NodeId,
		stored: bool,
	) -> R<Vec<crate::expr::logical::LogicalDestructureField>> {
		use crate::expr::logical::LogicalDestructureField as Field;
		self.list(self.n(id).a)
			.iter()
			.map(|&id| {
				let n = self.n(id);
				let name = || self.name(n.a).map(Strand::new);
				Ok(match n.op {
					0 => Field::Field(name()?),
					1 => Field::Aliased(
						name()?,
						if matches!(self.n(n.b).tag, Tag::Field | Tag::Idiom) {
							self.logical_body(n.b, stored)?
						} else {
							self.logical_expr(n.b, stored)?
						},
					),
					2 => Field::All(name()?),
					3 => Field::Destructure(name()?, self.logical_destructure(n.b, stored)?),
					_ => Field::Rest,
				})
			})
			.collect()
	}

	fn logical_lookup(&self, id: NodeId, stored: bool) -> R<crate::expr::logical::LogicalLookup> {
		use super::ast::select as s;
		use crate::expr::logical::{LogicalLookup, LogicalLookupSubject, LogicalRecordIdKey};
		use crate::expr::lookup::LookupKind;
		let n = self.n(id);
		let rec = &self.ast.extra[n.a as usize..n.a as usize + s::LEN];
		let flags = rec[s::FLAGS];
		if flags & 128 != 0 {
			self.logical_validate_projection(rec, n.start)?;
		}
		let what = self
			.list(rec[s::WHAT])
			.iter()
			.map(|&id| {
				let subject = self.n(id);
				let table = surrealdb_strand::TableName::new(Strand::new(self.name(subject.a)?));
				let details = &self.ast.extra[subject.b as usize..subject.b as usize + 2];
				let referencing_field = if details[1] == NONE {
					None
				} else {
					Some(self.name(details[1])?)
				};
				Ok(if details[0] == NONE {
					LogicalLookupSubject::Table {
						table,
						referencing_field,
					}
				} else {
					let LogicalRecordIdKey::Range(range) = self.logical_key(details[0], stored)?
					else {
						return Err(Reject::new("not a range"));
					};
					LogicalLookupSubject::Range {
						table,
						range: *range,
						referencing_field,
					}
				})
			})
			.collect::<R<_>>()?;
		Ok(LogicalLookup {
			kind: match n.op {
				0 => LookupKind::Graph(crate::expr::Dir::Out),
				1 => LookupKind::Graph(crate::expr::Dir::In),
				2 => LookupKind::Graph(crate::expr::Dir::Both),
				_ => LookupKind::Reference,
			},
			expr: if flags & 128 == 0 {
				None
			} else {
				Some(self.logical_fields(rec[s::FIELDS], rec[s::VALUE], stored)?)
			},
			only: flags & 1 != 0,
			what,
			cond: self.logical_opt(rec[s::COND], stored)?,
			split: if flags & 128 == 0 || rec[s::SPLIT] == NONE {
				None
			} else {
				Some(self.logical_exprs(rec[s::SPLIT], stored)?)
			},
			group: if flags & 128 == 0 {
				None
			} else if flags & 2 != 0 {
				Some(Vec::new())
			} else if rec[s::GROUP] == NONE {
				None
			} else {
				Some(self.logical_exprs(rec[s::GROUP], stored)?)
			},
			order: self.logical_ordering(rec[s::ORDER], stored)?,
			limit: self.logical_opt(rec[s::LIMIT], stored)?,
			start: self.logical_opt(rec[s::START], stored)?,
			alias: if rec[s::FETCH] == NONE {
				None
			} else {
				Some(self.logical_expr(rec[s::FETCH], stored)?.output_path())
			},
		})
	}

	fn logical_validate_projection(&self, rec: &[u32], from: u32) -> R<()> {
		use super::ast::select as s;
		if rec[s::SPLIT] != NONE || rec[s::GROUP] != NONE || rec[s::FLAGS] & 2 != 0 {
			let fields = if rec[s::VALUE] != NONE {
				crate::expr::Fields::Value(Box::new(self.selector(self.n(rec[s::VALUE]))?))
			} else {
				self.fields(rec[s::FIELDS])?
			};
			self.split_group(&fields, rec, from, false)?;
		}
		Ok(())
	}

	fn logical_path(&self, id: NodeId, stored: bool) -> R<(LogicalExpr, VecDeque<Step>)> {
		let n = self.n(id);
		if n.tag == Tag::Field {
			return Ok((
				if n.op == 1 {
					LogicalExpr::Current
				} else {
					LogicalExpr::Field(Strand::new(self.name(id)?))
				},
				VecDeque::new(),
			));
		}
		let head = self.n(n.a);
		let (base, mut steps) = match head.tag {
			Tag::Field if head.op == 1 => (LogicalExpr::Current, VecDeque::new()),
			Tag::Doc => (LogicalExpr::Doc, VecDeque::new()),
			Tag::Field | Tag::Table if head.depth & super::ast::COVERED == 0 => {
				(LogicalExpr::Field(Strand::new(self.name(n.a)?)), VecDeque::new())
			}
			Tag::Idiom if head.depth & super::ast::COVERED == 0 => {
				self.logical_path(n.a, stored)?
			}
			_ => (self.logical_expr(n.a, stored)?, VecDeque::new()),
		};
		for &id in self.list(n.b) {
			let part = self.n(id);
			steps.push_back(match part.tag {
				Tag::PartField => Step::Access(Strand::new(self.name(id)?)),
				Tag::PartAll => Step::Each,
				Tag::PartLast => Step::Last,
				Tag::PartFlatten => Step::Flatten,
				Tag::PartOptional => Step::Optional,
				Tag::PartIndex => Step::Index(self.logical_expr(part.a, stored)?),
				Tag::PartWhere => Step::Filter(self.logical_expr(part.a, stored)?),
				Tag::PartMethod => Step::Method(
					Strand::new(self.name(part.a)?),
					self.logical_exprs(part.b, stored)?,
				),
				Tag::PartGraph => Step::Lookup(Box::new(self.logical_lookup(id, stored)?)),
				Tag::PartDestructure => Step::Destructure(self.logical_destructure(id, stored)?),
				Tag::PartRepeat => Step::RepeatRecurse,
				Tag::PartRecurse => {
					let r = &self.ast.extra[part.a as usize..part.a as usize + 7];
					let num = |id: u32| -> R<Option<u32>> {
						if id == NONE {
							return Ok(None);
						}
						u32::try_from(super::Ast::int(self.n(id)))
							.map(Some)
							.map_err(|_| Reject::new("u32"))
					};
					let recurse = if r[0] == 0 {
						crate::expr::part::Recurse::Fixed(
							num(r[1])?.expect("fixed recursion has a count"),
						)
					} else {
						crate::expr::part::Recurse::Range(num(r[1])?, num(r[2])?)
					};
					let inclusive = r[5] == 1;
					let instruction = match r[3] {
						0 => None,
						1 => Some(crate::expr::logical::LogicalRecurseInstruction::Path {
							inclusive,
						}),
						2 => Some(crate::expr::logical::LogicalRecurseInstruction::Collect {
							inclusive,
						}),
						_ => Some(crate::expr::logical::LogicalRecurseInstruction::Shortest {
							expects: self.logical_expr(r[4], stored)?,
							inclusive,
						}),
					};
					let body = if r[6] == NONE {
						None
					} else {
						Some(self.logical_body(r[6], stored)?)
					};
					Step::Recurse(recurse, body, instruction)
				}
				tag => panic!("not a path part: {tag:?}"),
			});
		}
		Ok((base, steps))
	}

	fn logical_body(&self, id: NodeId, stored: bool) -> R<LogicalExpr> {
		let (mut base, mut steps) = self.logical_path(id, stored)?;
		if let LogicalExpr::Field(name) = base {
			steps.push_front(Step::Access(name));
			base = LogicalExpr::Current;
		}
		Ok(fold_steps(base, steps, PathMode::Plain))
	}

	fn logical_fields(
		&self,
		fields: u32,
		value: u32,
		stored: bool,
	) -> R<crate::expr::logical::LogicalFields> {
		use crate::expr::logical::{LogicalField, LogicalFields, OutputPath};
		let selector = |id: u32, name_statement: bool| -> R<(LogicalExpr, Option<OutputPath>)> {
			let n = self.n(id);
			let expr = self.logical_expr(n.a, stored)?;
			let alias = if n.b != NONE {
				Some(self.logical_expr(n.b, stored)?.output_path())
			} else if name_statement && expr.output_name_renders_a_statement() {
				use surrealdb_types::ToSql;
				Some(OutputPath::field(self.expr(n.a)?.to_sql()))
			} else {
				None
			};
			Ok((expr, alias))
		};
		if value != NONE {
			let (expr, alias) = selector(value, false)?;
			return Ok(LogicalFields::Value {
				expr,
				alias,
			});
		}
		Ok(LogicalFields::Select(
			self.list(fields)
				.iter()
				.map(|&id| {
					if self.n(id).tag == Tag::FieldAll {
						return Ok(LogicalField::All);
					}
					let (expr, alias) = selector(id, true)?;
					Ok(LogicalField::user(expr, alias))
				})
				.collect::<R<_>>()?,
		))
	}

	fn logical_ordering(
		&self,
		list: u32,
		stored: bool,
	) -> R<Option<crate::expr::logical::LogicalOrdering>> {
		use crate::expr::logical::{LogicalOrder, LogicalOrdering};
		if list == NONE {
			return Ok(None);
		}
		let items = self.list(list);
		if items.len() == 1 && self.n(items[0]).tag == Tag::OrderRand {
			return Ok(Some(LogicalOrdering::Random));
		}
		Ok(Some(LogicalOrdering::Order(
			items
				.iter()
				.map(|&id| {
					let n = self.n(id);
					Ok(LogicalOrder {
						value: self.logical_expr(n.a, stored)?,
						collate: n.op & 2 != 0,
						numeric: n.op & 4 != 0,
						direction: n.op & 1 == 0,
					})
				})
				.collect::<R<_>>()?,
		)))
	}

	fn logical_select(&self, id: NodeId, stored: bool) -> R<LogicalOp> {
		use super::ast::select as s;
		use crate::expr::logical::lower::{SelectClauses, select_pipeline};
		use crate::expr::logical::{
			LogicalSource, LogicalSubquery, LogicalUnionAll, LogicalValues, SourceRef,
			is_value_literal,
		};
		let n = self.n(id);
		let rec = &self.ast.extra[n.a as usize..n.a as usize + s::LEN];
		let flags = rec[s::FLAGS];
		let for_update = flags & 64 != 0;
		let with = self.with(rec[s::WITH])?;
		let sources = self
			.list(rec[s::WHAT])
			.iter()
			.map(|&id| {
				if self.n(id).tag == Tag::Select && !for_update {
					return Ok(LogicalOp::Subquery(LogicalSubquery {
						input: Box::new(self.logical_select(id, stored)?),
					}));
				}
				let expr = self.logical_expr(id, stored)?;
				Ok(if is_value_literal(&expr) {
					LogicalOp::Values(LogicalValues {
						written: matches!(expr, LogicalExpr::Array(_)),
						expr,
						for_update,
					})
				} else {
					LogicalOp::Source(LogicalSource {
						source: SourceRef::classify(expr),
						schema: None,
						with: with.clone(),
						for_update,
					})
				})
			})
			.collect::<R<Vec<_>>>()?;
		let input = if sources.len() == 1 {
			sources.into_iter().next().expect("length checked")
		} else {
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs: sources,
			})
		};
		// Projection membership and the mutually exclusive SPLIT/GROUP clauses
		// share the parser's semantic checks and diagnostic spans.
		self.logical_validate_projection(rec, n.start)?;
		let fields = self.logical_fields(rec[s::FIELDS], rec[s::VALUE], stored)?;
		Ok(select_pipeline(SelectClauses {
			input,
			fields,
			omit: self.logical_exprs(rec[s::OMIT], stored)?,
			only: flags & 1 != 0,
			cond: self.logical_opt(rec[s::COND], stored)?,
			split: if rec[s::SPLIT] == NONE {
				None
			} else {
				Some(self.logical_exprs(rec[s::SPLIT], stored)?)
			},
			group: if flags & 2 != 0 {
				Some(Vec::new())
			} else if rec[s::GROUP] == NONE {
				None
			} else {
				Some(self.logical_exprs(rec[s::GROUP], stored)?)
			},
			order: self.logical_ordering(rec[s::ORDER], stored)?,
			limit: self.logical_opt(rec[s::LIMIT], stored)?,
			start: self.logical_opt(rec[s::START], stored)?,
			fetch: if rec[s::FETCH] == NONE {
				Vec::new()
			} else {
				self.logical_exprs(rec[s::FETCH], stored)?
			},
			version: self.logical_opt(rec[s::VERSION], stored)?.filter(|e| {
				!matches!(e, LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::None))
			}),
			timeout: self.logical_opt(rec[s::TIMEOUT], stored)?.filter(|e| {
				!matches!(e, LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::None))
			}),
			explain: super::explain(flags).map(|crate::expr::Explain(analyze)| analyze),
			tempfiles: flags & 8 != 0,
		}))
	}

	fn logical_exprs(&self, list: u32, stored: bool) -> R<Vec<LogicalExpr>> {
		self.list(list).iter().map(|&id| self.logical_expr(id, stored)).collect()
	}

	fn logical_opt(&self, id: NodeId, stored: bool) -> R<Option<LogicalExpr>> {
		if id == NONE {
			Ok(None)
		} else {
			self.logical_expr(id, stored).map(Some)
		}
	}

	fn logical_value_statement(&self, id: NodeId, stored: bool) -> R<LogicalOp> {
		if matches!(self.n(id).tag, Tag::If | Tag::Throw | Tag::Break | Tag::Continue) {
			Ok(LogicalOp::Eval(LogicalEval {
				expr: self.logical_expr(id, stored)?,
			}))
		} else {
			self.logical_statement(id, stored)
		}
	}

	fn if_arm_is_a_let(&self, id: NodeId) -> bool {
		let n = self.n(id);
		match n.tag {
			Tag::Let => true,
			Tag::If => {
				self.list(n.a).chunks_exact(2).any(|cb| self.if_arm_is_a_let(cb[1]))
					|| (n.b != NONE && self.if_arm_is_a_let(n.b))
			}
			_ => false,
		}
	}

	pub(crate) fn logical_statement(&self, id: NodeId, stored: bool) -> R<LogicalOp> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::Select => self.logical_select(id, stored)?,
			Tag::Block => LogicalOp::Sequence(LogicalSequence {
				stmts: self
					.list(n.a)
					.iter()
					.map(|&id| self.logical_statement(id, stored))
					.collect::<R<_>>()?,
			}),
			Tag::Let => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				LogicalOp::Let(LogicalLet {
					name: self.param_name(rec[0])?,
					kind: if rec[1] == NONE {
						None
					} else {
						Some(self.kind(rec[1])?)
					},
					value: Box::new(self.logical_statement(rec[2], stored)?),
				})
			}
			Tag::Return => LogicalOp::Return(LogicalReturn {
				what: Box::new(self.logical_value_statement(n.a, stored)?),
				fetch: if n.b == NONE {
					Vec::new()
				} else {
					self.logical_exprs(n.b, stored)?
				},
			}),
			Tag::For => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + 3];
				if self.n(rec[2]).tag != Tag::Block {
					return Err(Reject::new("not a block"));
				}
				LogicalOp::Foreach(LogicalForeach {
					param: Param::from(Strand::new(self.param_name(rec[0])?)),
					range: self.logical_expr(rec[1], stored)?,
					body: Box::new(self.logical_statement(rec[2], stored)?),
				})
			}
			Tag::If if self.if_arm_is_a_let(id) => LogicalOp::Eval(LogicalEval {
				expr: self.logical_expr(id, stored)?,
			}),
			Tag::If => LogicalOp::If(LogicalIfOp::new(
				self.list(n.a)
					.chunks_exact(2)
					.map(|cb| {
						Ok((
							self.logical_expr(cb[0], stored)?,
							self.logical_statement(cb[1], stored)?,
						))
					})
					.collect::<R<_>>()?,
				if n.b == NONE {
					None
				} else {
					Some(self.logical_statement(n.b, stored)?)
				},
			)),
			Tag::Throw => LogicalOp::Throw(LogicalThrow {
				value: self.logical_expr(n.a, stored)?,
			}),
			Tag::Break => LogicalOp::Break(LogicalBreak),
			Tag::Continue => LogicalOp::Continue(LogicalContinue),
			Tag::Explain => LogicalOp::Explain(LogicalExplain {
				input: Box::new(self.logical_value_statement(n.a, stored)?),
				analyze: n.op & 1 != 0,
				format: if n.op & 2 != 0 {
					ExplainFormat::Json
				} else {
					ExplainFormat::Text
				},
			}),
			Tag::Info => {
				let rec = &self.ast.extra[n.a as usize..n.a as usize + info::LEN];
				let structure = rec[info::FLAGS] & 1 != 0;
				let version = self.logical_opt(rec[info::VERSION], stored)?;
				let what = || self.logical_expr(rec[info::WHAT], stored);
				LogicalOp::Info(Box::new(match n.op {
					x if x == InfoTarget::Root as u8 => LogicalInfo::Root {
						structure,
						version,
					},
					x if x == InfoTarget::Ns as u8 => LogicalInfo::Ns {
						structure,
						version,
					},
					x if x == InfoTarget::Db as u8 => LogicalInfo::Db {
						structure,
						version,
					},
					x if x == InfoTarget::Table as u8 => LogicalInfo::Table {
						table: what()?,
						structure,
						version,
					},
					x if x == InfoTarget::User as u8 => {
						let base = rec[info::FLAGS] >> 1;
						LogicalInfo::User {
							user: what()?,
							base: (base != 0).then(|| Self::base(base)),
							structure,
						}
					}
					_ => LogicalInfo::Index {
						index: what()?,
						table: self.logical_expr(rec[info::TABLE], stored)?,
						structure,
					},
				}))
			}
			Tag::Sleep => {
				let (seconds, nanos) =
					surrealdb_parser_next::lex::decode_duration(self.text(n.a).as_bytes())
						.ok_or(Reject::new("duration overflow"))?;
				LogicalOp::Sleep(crate::expr::logical::LogicalSleep {
					duration: LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Duration(
						std::time::Duration::new(seconds, nanos).into(),
					)),
				})
			}

			Tag::Create
			| Tag::Update
			| Tag::Upsert
			| Tag::Delete
			| Tag::Insert
			| Tag::Relate
			| Tag::Define
			| Tag::Remove
			| Tag::Alter
			| Tag::Rebuild => {
				lower(self.logical_native(id, stored)?).map_err(|e| Reject::new(e.to_string()))?
			}
			_ => LogicalOp::Eval(LogicalEval {
				expr: self.logical_expr(id, stored)?,
			}),
		})
	}
}
