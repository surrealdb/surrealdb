//! Graph pattern parsing: path patterns, node and edge patterns, label
//! expressions and quantifiers.
//!
//! From 16.4-16.11 (GQL.g4:803-1146). A linear path is grammatically just a
//! sequence of element patterns (`pathTerm : pathFactor+`); node-edge-node
//! alternation is enforced here semantically, with targeted errors.

use parse_common::error::bail;

use crate::gql::ast::{
	EdgeDirection, EdgePattern, ElementPredicate, Ident, LabelExpr, NodePattern, PathMode,
	PathPattern, PathPatternPrefix, PathSearchKind, PathStep, Quantifier, QuantifierKind,
};
use crate::gql::parser::mac::{enter_object_recursion, expected, unexpected};
use crate::gql::parser::{ParseResult, Parser};
use crate::gql::token::{Keyword, NumberKind, TokenKind, t};

impl Parser<'_> {
	/// Parse a single path pattern: `pathVariableDeclaration?
	/// pathPatternPrefix? pathPatternExpression` (GQL.g4:834), where the
	/// expression must be a node pattern followed by zero or more edge-node
	/// steps.
	pub(super) fn parse_path_pattern(&mut self) -> ParseResult<PathPattern> {
		// A path variable declaration is an identifier directly followed by
		// `=`; an identifier in pattern position can be nothing else.
		let path_var = if Self::token_can_be_ident(self.peek_kind()) && self.peek1().kind == t!("=")
		{
			let ident = self.parse_ident()?;
			self.pop_peek();
			Some(ident)
		} else {
			None
		};

		// `pathPatternPrefix` (GQL.g4:896-962): an optional path-search selector
		// and/or path mode. WALK/TRAIL/SIMPLE/ACYCLIC are non-reserved words but
		// cannot start a path pattern expression, so a leading one is
		// unambiguously a prefix here (a path variable was handled above).
		let prefix = self.parse_path_pattern_prefix()?;

		let token = self.peek();
		if edge_pattern_starts(token.kind) {
			bail!(
				"Unexpected edge pattern, path patterns must start with a node pattern",
				@token.span
			);
		} else if simplified_path_starts(token.kind) {
			bail!(
				"Simplified path pattern expressions (`-/ … /->`) are not supported yet",
				@token.span
			);
		}

		let start = self.parse_node_pattern()?;
		let mut steps = Vec::new();
		loop {
			let token = self.peek();
			if matches!(token.kind, t!("*") | t!("+") | t!("?") | t!("{")) {
				bail!(
					"Quantifiers may only follow an edge pattern",
					@token.span
				);
			} else if token.kind == t!("(") {
				bail!(
					"Unexpected node pattern, expected an edge pattern between node patterns",
					@token.span
				);
			} else if simplified_path_starts(token.kind) {
				bail!(
					"Simplified path pattern expressions (`-/ … /->`) are not supported yet",
					@token.span
				);
			}
			let Some(edge) = self.parse_edge_pattern()? else {
				break;
			};
			let token = self.peek();
			if token.kind != t!("(") {
				unexpected!(self, token, "a node pattern after this edge pattern");
			}
			let node = self.parse_node_pattern()?;
			steps.push(PathStep {
				edge,
				node,
			});
		}

		Ok(PathPattern {
			path_var,
			prefix,
			start,
			steps,
		})
	}

	/// Parse an optional `pathPatternPrefix` (GQL.g4:896-962): a path-search
	/// selector (`ALL`/`ANY`/`SHORTEST` forms) and/or a path mode
	/// (`WALK`/`TRAIL`/`SIMPLE`/`ACYCLIC`). Returns `None` when the next token
	/// does not begin a prefix. The full ISO surface is accepted here; the
	/// lowering rejects the combinations it does not yet execute, so syntax
	/// errors stay limited to genuinely malformed prefixes (a bare `SHORTEST`
	/// with neither a count nor `GROUP`, or a zero count).
	fn parse_path_pattern_prefix(&mut self) -> ParseResult<Option<PathPatternPrefix>> {
		let first = self.peek();
		let start = first.span;
		let kind = if matches!(first.kind, t!("WALK") | t!("TRAIL") | t!("SIMPLE") | t!("ACYCLIC"))
		{
			let mode = self.parse_optional_path_mode()?;
			self.eat_path_or_paths();
			return Ok(Some(PathPatternPrefix {
				kind: PathSearchKind::All,
				mode,
				span: start.covers(self.last_span()),
			}));
		} else if first.kind == t!("ALL") {
			self.pop_peek();
			if self.eat(t!("SHORTEST")) {
				PathSearchKind::AllShortest
			} else {
				PathSearchKind::All
			}
		} else if first.kind == t!("ANY") {
			self.pop_peek();
			if self.eat(t!("SHORTEST")) {
				PathSearchKind::AnyShortest
			} else {
				PathSearchKind::Any {
					count: self.parse_optional_path_count()?,
				}
			}
		} else if first.kind == t!("SHORTEST") {
			self.pop_peek();
			let count = self.parse_optional_path_count()?;
			let mode = self.parse_optional_path_mode()?;
			self.eat_path_or_paths();
			let kind = if self.eat(t!("GROUP")) || self.eat(t!("GROUPS")) {
				PathSearchKind::ShortestGroups {
					count,
				}
			} else {
				match count {
					Some(count) => PathSearchKind::ShortestCounted {
						count,
					},
					None => bail!(
						"`SHORTEST` requires a path count (`SHORTEST k`) or `GROUP`",
						@start.covers(self.last_span())
					),
				}
			};
			return Ok(Some(PathPatternPrefix {
				kind,
				mode,
				span: start.covers(self.last_span()),
			}));
		} else {
			return Ok(None);
		};

		// Shared tail for `ALL` / `ANY` / `ALL SHORTEST` / `ANY SHORTEST`: an
		// optional path mode then the optional `PATH`/`PATHS` noise word.
		let mode = self.parse_optional_path_mode()?;
		self.eat_path_or_paths();
		Ok(Some(PathPatternPrefix {
			kind,
			mode,
			span: start.covers(self.last_span()),
		}))
	}

	/// Parse an optional path mode (`pathMode`, GQL.g4:907-912), consuming the
	/// keyword when present.
	fn parse_optional_path_mode(&mut self) -> ParseResult<Option<PathMode>> {
		let peek_kind = self.peek_kind();
		let mode = if peek_kind == t!("WALK") {
			PathMode::Walk
		} else if peek_kind == t!("TRAIL") {
			PathMode::Trail
		} else if peek_kind == t!("SIMPLE") {
			PathMode::Simple
		} else if peek_kind == t!("ACYCLIC") {
			PathMode::Acyclic
		} else {
			return Ok(None);
		};
		self.pop_peek();
		Ok(Some(mode))
	}

	/// Eat an optional `PATH` / `PATHS` keyword (`pathOrPaths`, GQL.g4:924-927).
	fn eat_path_or_paths(&mut self) -> bool {
		self.eat(t!("PATH")) || self.eat(t!("PATHS"))
	}

	/// Parse an optional positive path/group count (`nonNegativeIntegerSpecification`,
	/// GQL.g4:933-935/960-962) — an unsigned integer literal. Returns `None` when
	/// the next token is not an integer; rejects a zero count.
	fn parse_optional_path_count(&mut self) -> ParseResult<Option<u32>> {
		let token = self.peek();
		let TokenKind::Number {
			kind:
				kind @ (NumberKind::Integer | NumberKind::Hex | NumberKind::Octal | NumberKind::Binary),
			suffix: None,
		} = token.kind
		else {
			return Ok(None);
		};
		self.pop_peek();
		let count = self.parse_u32_token(token, kind)?;
		if count == 0 {
			bail!("A path search count must be a positive integer", @token.span);
		}
		Ok(Some(count))
	}

	/// Parse a node pattern: `LEFT_PAREN elementPatternFiller RIGHT_PAREN`
	/// (GQL.g4:993).
	fn parse_node_pattern(&mut self) -> ParseResult<NodePattern> {
		let token = self.peek();
		if token.kind != t!("(") {
			unexpected!(self, token, "a node pattern");
		}
		let open = self.pop_peek().span;

		// A `(` or an edge pattern directly inside the parentheses means
		// this is a `parenthesizedPathPatternExpression` (GQL.g4:1088), not
		// a node pattern.
		let next = self.peek();
		if next.kind == t!("(")
			|| edge_pattern_starts(next.kind)
			|| simplified_path_starts(next.kind)
		{
			bail!(
				"Parenthesized path pattern expressions are not supported yet",
				@next.span,
				@open => "expected this to start a node pattern"
			);
		}

		let (var, label, predicate) = self.parse_element_filler()?;

		// A `=` after the variable is a subpath variable declaration, which
		// also only occurs in parenthesized path pattern expressions.
		if self.peek_kind() == t!("=") {
			bail!(
				"Parenthesized path pattern expressions (subpath variables) are not supported yet",
				@self.peek().span
			);
		}

		self.expect_closing_delimiter(t!(")"), open)?;
		Ok(NodePattern {
			var,
			label,
			predicate,
			span: open.covers(self.last_span()),
		})
	}

	/// Parse an edge pattern, or return `None` when the next token does not
	/// start one. From `edgePattern` (GQL.g4:1035-1086): seven full forms
	/// wrapping an element pattern filler and seven abbreviated forms, with
	/// an optional postfix quantifier.
	fn parse_edge_pattern(&mut self) -> ParseResult<Option<EdgePattern>> {
		let token = self.peek();
		// Abbreviated forms first: `<-` `~` `->` `<~` `~>` `<->` `-`.
		let abbreviated = if token.kind == t!("<-") {
			Some(EdgeDirection::Left)
		} else if token.kind == t!("~") {
			Some(EdgeDirection::Undirected)
		} else if token.kind == t!("->") {
			Some(EdgeDirection::Right)
		} else if token.kind == t!("<~") {
			Some(EdgeDirection::LeftOrUndirected)
		} else if token.kind == t!("~>") {
			Some(EdgeDirection::UndirectedOrRight)
		} else if token.kind == t!("<->") {
			Some(EdgeDirection::LeftOrRight)
		} else if token.kind == t!("-") {
			Some(EdgeDirection::Any)
		} else {
			None
		};
		if let Some(direction) = abbreviated {
			self.pop_peek();
			let quantifier = self.parse_quantifier()?;
			return Ok(Some(EdgePattern {
				var: None,
				label: None,
				direction,
				predicate: None,
				quantifier,
				span: token.span.covers(self.last_span()),
			}));
		}

		// Full forms: the opening bracket token selects the possible
		// directions and the closing bracket token decides between them.
		let direction = if token.kind == t!("-[") {
			self.pop_peek();
			let filler = self.parse_element_filler()?;
			let close = self.next();
			let direction = if close.kind == t!("]->") {
				EdgeDirection::Right
			} else if close.kind == t!("]-") {
				EdgeDirection::Any
			} else {
				unexpected!(self, close, "`]->` or `]-`");
			};
			(filler, direction)
		} else if token.kind == t!("<-[") {
			self.pop_peek();
			let filler = self.parse_element_filler()?;
			let close = self.next();
			let direction = if close.kind == t!("]-") {
				EdgeDirection::Left
			} else if close.kind == t!("]->") {
				EdgeDirection::LeftOrRight
			} else {
				unexpected!(self, close, "`]-` or `]->`");
			};
			(filler, direction)
		} else if token.kind == t!("~[") {
			self.pop_peek();
			let filler = self.parse_element_filler()?;
			let close = self.next();
			let direction = if close.kind == t!("]~") {
				EdgeDirection::Undirected
			} else if close.kind == t!("]~>") {
				EdgeDirection::UndirectedOrRight
			} else {
				unexpected!(self, close, "`]~` or `]~>`");
			};
			(filler, direction)
		} else if token.kind == t!("<~[") {
			self.pop_peek();
			let filler = self.parse_element_filler()?;
			let close = self.next();
			let direction = if close.kind == t!("]~") {
				EdgeDirection::LeftOrUndirected
			} else {
				unexpected!(self, close, "`]~`");
			};
			(filler, direction)
		} else {
			return Ok(None);
		};
		let ((var, label, predicate), direction) = direction;
		let quantifier = self.parse_quantifier()?;
		Ok(Some(EdgePattern {
			var,
			label,
			direction,
			predicate,
			quantifier,
			span: token.span.covers(self.last_span()),
		}))
	}

	/// Parse an element pattern filler: `elementVariableDeclaration?
	/// isLabelExpression? elementPatternPredicate?` (GQL.g4:997), shared
	/// between node and edge patterns. All three parts are optional; the
	/// predicate is either an inline `WHERE` or a property map, never both.
	fn parse_element_filler(
		&mut self,
	) -> ParseResult<(Option<Ident>, Option<LabelExpr>, Option<ElementPredicate>)> {
		let token = self.peek();
		let var = if Self::token_can_be_ident(token.kind) {
			Some(self.parse_ident()?)
		} else if let TokenKind::Keyword(keyword) = token.kind {
			if !matches!(keyword, Keyword::Is | Keyword::Where) {
				bail!(
					"`{}` is a reserved word and cannot be used as a variable name",
					self.span_str(token.span),
					@token.span => "use a `\"…\"` or `` `…` `` delimited identifier instead"
				);
			}
			None
		} else {
			None
		};

		// Labels are introduced by `:` or the keyword `IS` (`isOrColon`,
		// GQL.g4:1024).
		let label = if self.eat(t!(":")) || self.eat(t!("IS")) {
			Some(self.parse_label_expr()?)
		} else {
			None
		};

		let peek_kind = self.peek_kind();
		let predicate = if peek_kind == t!("WHERE") {
			self.pop_peek();
			let expr = self.parse_expr()?;
			Some(ElementPredicate::Where(expr))
		} else if peek_kind == t!("{") {
			let open = self.pop_peek().span;
			let mut props = Vec::new();
			loop {
				let key = self.parse_ident()?;
				expected!(self, t!(":"));
				let value = self.parse_expr()?;
				props.push((key, value));
				if !self.eat(t!(",")) {
					break;
				}
			}
			self.expect_closing_delimiter(t!("}"), open)?;
			Some(ElementPredicate::Props(props))
		} else {
			None
		};

		// `elementPatternPredicate` (GQL.g4:1009) is a single alternative: a
		// filler has a WHERE clause or a property map, never both.
		let token = self.peek();
		if predicate.is_some() && matches!(token.kind, t!("WHERE") | t!("{")) {
			bail!(
				"An element pattern may have either a WHERE clause or a property map, not both",
				@token.span
			);
		}

		Ok((var, label, predicate))
	}

	/// Parse an optional postfix graph pattern quantifier: `*`, `+`, `?`,
	/// `{n}`, `{n,m}`, `{n,}`, `{,m}` or `{,}` (GQL.g4:1125-1146).
	fn parse_quantifier(&mut self) -> ParseResult<Option<Quantifier>> {
		let token = self.peek();
		let kind = if token.kind == t!("*") {
			self.pop_peek();
			QuantifierKind::Star
		} else if token.kind == t!("+") {
			self.pop_peek();
			QuantifierKind::Plus
		} else if token.kind == t!("?") {
			self.pop_peek();
			QuantifierKind::Question
		} else if token.kind == t!("{") {
			let open = self.pop_peek().span;
			let peek_kind = self.peek_kind();
			let lower = if peek_kind == t!(",") {
				None
			} else if matches!(peek_kind, TokenKind::Number { .. }) {
				Some(self.parse_quantifier_bound()?)
			} else {
				let token = self.peek();
				unexpected!(self, token, "an unsigned integer or `,`");
			};
			let kind = if self.eat(t!(",")) {
				let upper = if self.peek_kind() == t!("}") {
					None
				} else {
					Some(self.parse_quantifier_bound()?)
				};
				QuantifierKind::Range(lower, upper)
			} else {
				// Without a comma this is a `fixedQuantifier`, which
				// requires the bound.
				match lower {
					Some(n) => QuantifierKind::Fixed(n),
					None => {
						let token = self.peek();
						unexpected!(self, token, "an unsigned integer or `,`");
					}
				}
			};
			self.expect_closing_delimiter(t!("}"), open)?;
			kind
		} else {
			return Ok(None);
		};
		Ok(Some(Quantifier {
			kind,
			span: token.span.covers(self.last_span()),
		}))
	}

	/// Parse an unsigned integer quantifier bound.
	fn parse_quantifier_bound(&mut self) -> ParseResult<u32> {
		let token = self.next();
		if let TokenKind::Number {
			kind:
				kind @ (NumberKind::Integer | NumberKind::Hex | NumberKind::Octal | NumberKind::Binary),
			suffix: None,
		} = token.kind
		{
			self.parse_u32_token(token, kind)
		} else {
			unexpected!(self, token, "an unsigned integer");
		}
	}

	/// Parse a label expression (GQL.g4:1102-1109), with precedence
	/// `!` > `&` > `|`.
	fn parse_label_expr(&mut self) -> ParseResult<LabelExpr> {
		let mut left = self.parse_label_conjunction()?;
		while self.eat(t!("|")) {
			let right = self.parse_label_conjunction()?;
			let span = left.span().covers(right.span());
			left = LabelExpr::Disjunction(Box::new(left), Box::new(right), span);
		}
		Ok(left)
	}

	fn parse_label_conjunction(&mut self) -> ParseResult<LabelExpr> {
		let mut left = self.parse_label_negation()?;
		while self.eat(t!("&")) {
			let right = self.parse_label_negation()?;
			let span = left.span().covers(right.span());
			left = LabelExpr::Conjunction(Box::new(left), Box::new(right), span);
		}
		Ok(left)
	}

	/// Parses a label primary under any number of `!` prefixes.
	///
	/// A negation chain is not charged against a depth budget — labels never
	/// reach `LogicalExpr` — so it is parsed with a loop rather than by
	/// recursion, and nested from the inside out once the primary is known.
	fn parse_label_negation(&mut self) -> ParseResult<LabelExpr> {
		let mut bangs = Vec::new();
		while self.peek().kind == t!("!") {
			bangs.push(self.pop_peek().span);
		}
		let mut expr = self.parse_label_primary()?;
		while let Some(bang) = bangs.pop() {
			let span = bang.covers(expr.span());
			expr = LabelExpr::Negation(Box::new(expr), span);
		}
		Ok(expr)
	}

	fn parse_label_primary(&mut self) -> ParseResult<LabelExpr> {
		let token = self.peek();
		if token.kind == t!("%") {
			self.pop_peek();
			Ok(LabelExpr::Wildcard(token.span))
		} else if token.kind == t!("(") {
			enter_object_recursion!(this = self => {
				let open = this.pop_peek().span;
				let inner = this.parse_label_expr()?;
				this.expect_closing_delimiter(t!(")"), open)?;
				Ok(inner)
			})
		} else if Self::token_can_be_ident(token.kind) {
			Ok(LabelExpr::Name(self.parse_ident()?))
		} else if matches!(token.kind, TokenKind::Keyword(_)) {
			// Reserved keyword: produce the reserved word error.
			self.parse_ident().map(LabelExpr::Name)
		} else {
			unexpected!(self, token, "a label expression");
		}
	}
}

/// Returns whether the token kind starts (or is) an edge pattern: a compound
/// bracket token or an abbreviated edge form.
fn edge_pattern_starts(kind: TokenKind) -> bool {
	matches!(
		kind,
		t!("-[")
			| t!("<-[")
			| t!("~[")
			| t!("<~[")
			| t!("<-")
			| t!("~") | t!("->")
			| t!("<~")
			| t!("~>")
			| t!("<->")
			| t!("-")
	)
}

/// Returns whether the token kind starts a simplified path pattern
/// expression (16.12, the `-/ … /->` slash forms).
fn simplified_path_starts(kind: TokenKind) -> bool {
	matches!(kind, t!("-/") | t!("<-/") | t!("~/") | t!("<~/"))
}
