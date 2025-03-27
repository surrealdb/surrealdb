/// A shorthand for token kinds.
macro_rules! t {
	(" ") => {
		$crate::syn::token::TokenKind::WhiteSpace
	};
	("invalid") => {
		$crate::syn::token::TokenKind::Invalid
	};
	("eof") => {
		$crate::syn::token::TokenKind::Eof
	};
	("[") => {
		$crate::syn::token::TokenKind::OpenDelim($crate::syn::token::Delim::Bracket)
	};
	("{") => {
		$crate::syn::token::TokenKind::OpenDelim($crate::syn::token::Delim::Brace)
	};
	("(") => {
		$crate::syn::token::TokenKind::OpenDelim($crate::syn::token::Delim::Paren)
	};
	("]") => {
		$crate::syn::token::TokenKind::CloseDelim($crate::syn::token::Delim::Bracket)
	};
	("}") => {
		$crate::syn::token::TokenKind::CloseDelim($crate::syn::token::Delim::Brace)
	};
	(")") => {
		$crate::syn::token::TokenKind::CloseDelim($crate::syn::token::Delim::Paren)
	};

	("r\"") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::RecordIdDouble)
	};
	("r'") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::RecordId)
	};
	("u\"") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::UuidDouble)
	};
	("u'") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::Uuid)
	};
	("d\"") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::DateTimeDouble)
	};
	("d'") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::DateTime)
	};
	("b\"") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::BytesDouble)
	};
	("b'") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::Bytes)
	};
	("f\"") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::FileDouble)
	};
	("f'") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::File)
	};
	("\"") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::PlainDouble)
	};
	("'") => {
		$crate::syn::token::TokenKind::Qoute($crate::syn::token::QouteKind::Plain)
	};
	("\"r") => {
		$crate::syn::token::TokenKind::CloseString {
			double: true,
		}
	};
	("'r") => {
		$crate::syn::token::TokenKind::CloseString {
			double: false,
		}
	};

	("f") => {
		$crate::syn::token::TokenKind::NumberSuffix($crate::syn::token::NumberSuffix::Float)
	};
	("dec") => {
		$crate::syn::token::TokenKind::NumberSuffix($crate::syn::token::NumberSuffix::Decimal)
	};

	("<") => {
		$crate::syn::token::TokenKind::LeftChefron
	};
	(">") => {
		$crate::syn::token::TokenKind::RightChefron
	};
	("<|") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::KnnOpen)
	};
	("|>") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::KnnClose)
	};

	(";") => {
		$crate::syn::token::TokenKind::SemiColon
	};
	(",") => {
		$crate::syn::token::TokenKind::Comma
	};
	("|") => {
		$crate::syn::token::TokenKind::Vert
	};
	("...") => {
		$crate::syn::token::TokenKind::DotDotDot
	};
	("..") => {
		$crate::syn::token::TokenKind::DotDot
	};
	(".") => {
		$crate::syn::token::TokenKind::Dot
	};
	("::") => {
		$crate::syn::token::TokenKind::PathSeperator
	};
	(":") => {
		$crate::syn::token::TokenKind::Colon
	};
	("->") => {
		$crate::syn::token::TokenKind::ArrowRight
	};

	("*") => {
		$crate::syn::token::TokenKind::Star
	};
	("$") => {
		$crate::syn::token::TokenKind::Dollar
	};

	("+") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Add)
	};
	("%") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Modulo)
	};
	("-") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Subtract)
	};
	("**") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Power)
	};
	("*=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AllEqual)
	};
	("*~") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AllLike)
	};
	("/") => {
		$crate::syn::token::TokenKind::ForwardSlash
	};
	("<=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::LessEqual)
	};
	(">=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::GreaterEqual)
	};
	("@") => {
		$crate::syn::token::TokenKind::At
	};
	("||") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Or)
	};
	("&&") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::And)
	};
	("×") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Mult)
	};
	("÷") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Divide)
	};

	("$param") => {
		$crate::syn::token::TokenKind::Parameter
	};

	("!") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Not)
	};
	("!~") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NotLike)
	};
	("!=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NotEqual)
	};

	("?") => {
		$crate::syn::token::TokenKind::Question
	};
	("?:") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Tco)
	};
	("??") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Nco)
	};
	("==") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Exact)
	};
	("!=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NotEqual)
	};
	("*=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AllEqual)
	};
	("?=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AnyEqual)
	};
	("=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Equal)
	};
	("!~") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NotLike)
	};
	("*~") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AllLike)
	};
	("?~") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AnyLike)
	};
	("~") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Like)
	};
	("+?=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Ext)
	};
	("+=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Inc)
	};
	("-=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Dec)
	};

	("∋") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Contains)
	};
	("∌") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NotContains)
	};
	("∈") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Inside)
	};
	("∉") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NotInside)
	};
	("⊇") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::ContainsAll)
	};
	("⊃") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::ContainsAny)
	};
	("⊅") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::ContainsNone)
	};
	("⊆") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AllInside)
	};
	("⊂") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::AnyInside)
	};
	("⊄") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::NoneInside)
	};

	// algorithms
	("EDDSA") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::EdDSA)
	};
	("ES256") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Es256)
	};
	("ES384") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Es384)
	};
	("ES512") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Es512)
	};
	("HS256") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Hs256)
	};
	("HS384") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Hs384)
	};
	("HS512") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Hs512)
	};
	("PS256") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Ps256)
	};
	("PS384") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Ps384)
	};
	("PS512") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Ps512)
	};
	("RS256") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Rs256)
	};
	("RS384") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Rs384)
	};
	("RS512") => {
		$crate::syn::token::TokenKind::Algorithm($crate::sql::Algorithm::Rs512)
	};

	// Distance
	("CHEBYSHEV") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Chebyshev)
	};
	("COSINE") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Cosine)
	};
	("EUCLIDEAN") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Euclidean)
	};
	("HAMMING") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Hamming)
	};
	("JACCARD") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Jaccard)
	};
	("MANHATTAN") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Manhattan)
	};
	("MAHALANOBIS") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Mahalanobis)
	};
	("MINKOWSKI") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Minkowski)
	};
	("PEARSON") => {
		$crate::syn::token::TokenKind::Distance($crate::syn::token::DistanceKind::Pearson)
	};

	// VectorType
	("F64") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::token::VectorTypeKind::F64)
	};
	("F32") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::token::VectorTypeKind::F32)
	};
	("I64") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::token::VectorTypeKind::I64)
	};
	("I32") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::token::VectorTypeKind::I32)
	};
	("I16") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::token::VectorTypeKind::I16)
	};

	($t:tt) => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::keyword_t!($t))
	};
}

pub(crate) use t;
