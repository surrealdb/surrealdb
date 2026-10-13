/// A shorthand for GQL token kinds.
macro_rules! t {
	("invalid") => {
		$crate::gql::token::TokenKind::Invalid
	};
	("eof") => {
		$crate::gql::token::TokenKind::Eof
	};

	("(") => {
		$crate::gql::token::TokenKind::OpenParen
	};
	(")") => {
		$crate::gql::token::TokenKind::CloseParen
	};
	("[") => {
		$crate::gql::token::TokenKind::OpenBracket
	};
	("]") => {
		$crate::gql::token::TokenKind::CloseBracket
	};
	("{") => {
		$crate::gql::token::TokenKind::OpenBrace
	};
	("}") => {
		$crate::gql::token::TokenKind::CloseBrace
	};

	("'") => {
		$crate::gql::token::TokenKind::SingleQuoted {
			no_escape: false,
		}
	};
	("@'") => {
		$crate::gql::token::TokenKind::SingleQuoted {
			no_escape: true,
		}
	};
	("\"") => {
		$crate::gql::token::TokenKind::DoubleQuoted {
			no_escape: false,
		}
	};
	("@\"") => {
		$crate::gql::token::TokenKind::DoubleQuoted {
			no_escape: true,
		}
	};
	("`") => {
		$crate::gql::token::TokenKind::AccentQuoted {
			no_escape: false,
		}
	};
	("@`") => {
		$crate::gql::token::TokenKind::AccentQuoted {
			no_escape: true,
		}
	};

	("$param") => {
		$crate::gql::token::TokenKind::Parameter
	};
	("$$param") => {
		$crate::gql::token::TokenKind::SubstitutedParameter
	};

	("|+|") => {
		$crate::gql::token::TokenKind::MultisetAlternation
	};
	("]->") => {
		$crate::gql::token::TokenKind::BracketRightArrow
	};
	("]~>") => {
		$crate::gql::token::TokenKind::BracketTildeRightArrow
	};
	("||") => {
		$crate::gql::token::TokenKind::Concat
	};
	("::") => {
		$crate::gql::token::TokenKind::DoubleColon
	};
	("..") => {
		$crate::gql::token::TokenKind::DoublePeriod
	};
	(">=") => {
		$crate::gql::token::TokenKind::Gte
	};
	("<-") => {
		$crate::gql::token::TokenKind::LeftArrow
	};
	("<~") => {
		$crate::gql::token::TokenKind::LeftArrowTilde
	};
	("<-[") => {
		$crate::gql::token::TokenKind::LeftArrowBracket
	};
	("<~[") => {
		$crate::gql::token::TokenKind::LeftArrowTildeBracket
	};
	("<->") => {
		$crate::gql::token::TokenKind::LeftMinusRight
	};
	("<-/") => {
		$crate::gql::token::TokenKind::LeftMinusSlash
	};
	("<~/") => {
		$crate::gql::token::TokenKind::LeftTildeSlash
	};
	("<=") => {
		$crate::gql::token::TokenKind::Lte
	};
	("-[") => {
		$crate::gql::token::TokenKind::MinusLeftBracket
	};
	("-/") => {
		$crate::gql::token::TokenKind::MinusSlash
	};
	("<>") => {
		$crate::gql::token::TokenKind::Neq
	};
	("->") => {
		$crate::gql::token::TokenKind::RightArrow
	};
	("]-") => {
		$crate::gql::token::TokenKind::RightBracketMinus
	};
	("]~") => {
		$crate::gql::token::TokenKind::RightBracketTilde
	};
	("=>") => {
		$crate::gql::token::TokenKind::RightDoubleArrow
	};
	("/-") => {
		$crate::gql::token::TokenKind::SlashMinus
	};
	("/->") => {
		$crate::gql::token::TokenKind::SlashMinusRight
	};
	("/~") => {
		$crate::gql::token::TokenKind::SlashTilde
	};
	("/~>") => {
		$crate::gql::token::TokenKind::SlashTildeRight
	};
	("~[") => {
		$crate::gql::token::TokenKind::TildeLeftBracket
	};
	("~>") => {
		$crate::gql::token::TokenKind::TildeRightArrow
	};
	("~/") => {
		$crate::gql::token::TokenKind::TildeSlash
	};

	("&") => {
		$crate::gql::token::TokenKind::Ampersand
	};
	("@") => {
		$crate::gql::token::TokenKind::At
	};
	(":") => {
		$crate::gql::token::TokenKind::Colon
	};
	(",") => {
		$crate::gql::token::TokenKind::Comma
	};
	("=") => {
		$crate::gql::token::TokenKind::Eq
	};
	("!") => {
		$crate::gql::token::TokenKind::Exclamation
	};
	(">") => {
		$crate::gql::token::TokenKind::Gt
	};
	("<") => {
		$crate::gql::token::TokenKind::Lt
	};
	("-") => {
		$crate::gql::token::TokenKind::Minus
	};
	("%") => {
		$crate::gql::token::TokenKind::Percent
	};
	(".") => {
		$crate::gql::token::TokenKind::Period
	};
	("+") => {
		$crate::gql::token::TokenKind::Plus
	};
	("?") => {
		$crate::gql::token::TokenKind::Question
	};
	("/") => {
		$crate::gql::token::TokenKind::Slash
	};
	("*") => {
		$crate::gql::token::TokenKind::Star
	};
	("~") => {
		$crate::gql::token::TokenKind::Tilde
	};
	("|") => {
		$crate::gql::token::TokenKind::VerticalBar
	};

	($t:tt) => {
		$crate::gql::token::TokenKind::Keyword($crate::gql::token::keyword_t!($t))
	};
}

pub(crate) use t;
