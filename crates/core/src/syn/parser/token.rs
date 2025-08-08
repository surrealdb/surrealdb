use super::Parser;
use crate::syn::token::{Glued, TokenKind, t};

impl Parser<'_> {
	/// Returns true if the next token can start a statement.
	pub(super) fn kind_starts_statement(kind: TokenKind) -> bool {
		matches!(
			kind,
			t!("ACCESS")
				| t!("ALTER")
				| t!("ANALYZE")
				| t!("BEGIN")
				| t!("BREAK")
				| t!("CANCEL")
				| t!("COMMIT")
				| t!("CONTINUE")
				| t!("CREATE")
				| t!("DEFINE")
				| t!("DELETE")
				| t!("FOR") | t!("IF")
				| t!("INFO") | t!("INSERT")
				| t!("KILL") | t!("LIVE")
				| t!("OPTION")
				| t!("REBUILD")
				| t!("RETURN")
				| t!("RELATE")
				| t!("REMOVE")
				| t!("SELECT")
				| t!("LET") | t!("SHOW")
				| t!("SLEEP")
				| t!("THROW")
				| t!("UPDATE")
				| t!("UPSERT")
				| t!("USE")
		)
	}

	/// Returns if a token kind can start an identifier.
	pub(super) fn kind_is_keyword_like(t: TokenKind) -> bool {
		matches!(
			t,
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
		)
	}

	/// Returns if a token kind can start an identifier.
	pub(super) fn kind_is_identifier(t: TokenKind) -> bool {
		matches!(
			t,
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::Identifier
		)
	}

	pub(super) fn kind_starts_record_id_key(kind: TokenKind) -> bool {
		Self::kind_is_identifier(kind)
			|| matches!(
				kind,
				TokenKind::Digits
					| t!("{") | t!("[")
					| t!("+") | t!("-")
					| t!("u'") | t!("u\"")
					| t!("'") | t!("\"")
					| TokenKind::Glued(Glued::Uuid | Glued::Strand)
			)
	}

	pub(super) fn kind_starts_subquery(kind: TokenKind) -> bool {
		matches!(
			kind,
			t!("RETURN")
				| t!("SELECT")
				| t!("CREATE")
				| t!("UPSERT")
				| t!("UPDATE")
				| t!("DELETE")
				| t!("RELATE")
				| t!("DEFINE")
				| t!("REMOVE")
				| t!("REBUILD")
				| t!("IF") | t!("INFO")
		)
	}

	pub(super) fn kind_starts_prime_value(kind: TokenKind) -> bool {
		matches!(
			kind,
			t!("+")
				| t!("-") | t!("u'")
				| t!("u\"") | t!("d'")
				| t!("d\"") | t!("r'")
				| t!("r\"") | t!("'")
				| t!("\"") | TokenKind::Digits
				| TokenKind::NaN
				| t!("true") | t!("false")
				| t!("fn") | t!("ml")
				| t!("(") | t!("{")
				| t!("[") | t!("/")
				| t!("|") | t!("||")
				| t!("<") | t!("$param")
				| t!("..") | TokenKind::Glued(_)
		) || Self::kind_starts_subquery(kind)
			|| Self::kind_is_identifier(kind)
	}

	pub(super) fn kind_starts_expression(kind: TokenKind) -> bool {
		matches!(kind, t!("..") | t!("<") | t!("->")) | Self::kind_starts_prime_value(kind)
	}

	pub(super) fn starts_disallowed_subquery_statement(kind: TokenKind) -> bool {
		matches!(
			kind,
			t!("ANALYZE")
				| t!("BEGIN")
				| t!("BREAK")
				| t!("CANCEL")
				| t!("COMMIT")
				| t!("CONTINUE")
				| t!("FOR") | t!("INFO")
				| t!("KILL") | t!("LIVE")
				| t!("OPTION")
				| t!("LET") | t!("SHOW")
				| t!("SLEEP")
				| t!("THROW")
				| t!("USE")
		)
	}
}
