/// A shorthand for token kinds.
macro_rules! t {
	("invalid") => {
		$crate::syn::v2::token::TokenKind::Invalid
	};
	("eof") => {
		$crate::syn::v2::token::TokenKind::Eof
	};
	("[") => {
		$crate::syn::v2::token::TokenKind::OpenDelim($crate::syn::v2::token::Delim::Bracket)
	};
	("{") => {
		$crate::syn::v2::token::TokenKind::OpenDelim($crate::syn::v2::token::Delim::Brace)
	};
	("(") => {
		$crate::syn::v2::token::TokenKind::OpenDelim($crate::syn::v2::token::Delim::Paren)
	};
	("]") => {
		$crate::syn::v2::token::TokenKind::CloseDelim($crate::syn::v2::token::Delim::Bracket)
	};
	("}") => {
		$crate::syn::v2::token::TokenKind::CloseDelim($crate::syn::v2::token::Delim::Brace)
	};
	(")") => {
		$crate::syn::v2::token::TokenKind::CloseDelim($crate::syn::v2::token::Delim::Paren)
	};

	("r\"") => {
		$crate::syn::v2::token::TokenKind::OpenRecordString {
			double: true,
		}
	};
	("r'") => {
		$crate::syn::v2::token::TokenKind::OpenRecordString {
			double: false,
		}
	};

	("\"r") => {
		$crate::syn::v2::token::TokenKind::CloseRecordString {
			double: true,
		}
	};
	("'r") => {
		$crate::syn::v2::token::TokenKind::CloseRecordString {
			double: false,
		}
	};

	("<") => {
		$crate::syn::v2::token::TokenKind::LeftChefron
	};
	(">") => {
		$crate::syn::v2::token::TokenKind::RightChefron
	};
	("<|") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::KnnOpen)
	};
	("|>") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::KnnClose)
	};

	(";") => {
		$crate::syn::v2::token::TokenKind::SemiColon
	};
	(",") => {
		$crate::syn::v2::token::TokenKind::Comma
	};
	("|") => {
		$crate::syn::v2::token::TokenKind::Vert
	};
	("...") => {
		$crate::syn::v2::token::TokenKind::DotDotDot
	};
	("..") => {
		$crate::syn::v2::token::TokenKind::DotDot
	};
	(".") => {
		$crate::syn::v2::token::TokenKind::Dot
	};
	("::") => {
		$crate::syn::v2::token::TokenKind::PathSeperator
	};
	(":") => {
		$crate::syn::v2::token::TokenKind::Colon
	};
	("<-") => {
		$crate::syn::v2::token::TokenKind::ArrowLeft
	};
	("<->") => {
		$crate::syn::v2::token::TokenKind::BiArrow
	};
	("->") => {
		$crate::syn::v2::token::TokenKind::ArrowRight
	};

	("*") => {
		$crate::syn::v2::token::TokenKind::Star
	};
	("$") => {
		$crate::syn::v2::token::TokenKind::Dollar
	};

	("+") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Add)
	};
	("-") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Subtract)
	};
	("**") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Power)
	};
	("*=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AllEqual)
	};
	("*~") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AllLike)
	};
	("/") => {
		$crate::syn::v2::token::TokenKind::ForwardSlash
	};
	("<=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::LessEqual)
	};
	(">=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::GreaterEqual)
	};
	("@") => {
		$crate::syn::v2::token::TokenKind::At
	};
	("||") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Or)
	};
	("&&") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::And)
	};
	("×") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Mult)
	};
	("÷") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Divide)
	};

	("$param") => {
		$crate::syn::v2::token::TokenKind::Parameter
	};
	("123") => {
		$crate::syn::v2::token::TokenKind::Number(_)
	};

	("!") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Not)
	};
	("!~") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NotLike)
	};
	("!=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NotEqual)
	};

	("?") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Like)
	};
	("?:") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Tco)
	};
	("??") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Nco)
	};
	("==") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Exact)
	};
	("!=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NotEqual)
	};
	("*=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AllEqual)
	};
	("?=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AnyEqual)
	};
	("=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Equal)
	};
	("!~") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NotLike)
	};
	("*~") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AllLike)
	};
	("?~") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AnyLike)
	};
	("~") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Like)
	};
	("+?=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Ext)
	};
	("+=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Inc)
	};
	("-=") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Dec)
	};

	("∋") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Contains)
	};
	("∌") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NotContains)
	};
	("∈") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::Inside)
	};
	("∉") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NotInside)
	};
	("⊇") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::ContainsAll)
	};
	("⊃") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::ContainsAny)
	};
	("⊅") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::ContainsNone)
	};
	("⊆") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AllInside)
	};
	("⊂") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::AnyInside)
	};
	("⊄") => {
		$crate::syn::v2::token::TokenKind::Operator($crate::syn::v2::token::Operator::NoneInside)
	};

	// algorithms
	("EDDSA") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::EdDSA)
	};
	("ES256") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Es256)
	};
	("ES384") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Es384)
	};
	("ES512") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Es512)
	};
	("HS256") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Hs256)
	};
	("HS384") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Hs384)
	};
	("HS512") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Hs512)
	};
	("PS256") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Ps256)
	};
	("PS384") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Ps384)
	};
	("PS512") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Ps512)
	};
	("RS256") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Rs256)
	};
	("RS384") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Rs384)
	};
	("RS512") => {
		$crate::syn::v2::token::TokenKind::Algorithm($crate::sql::Algorithm::Rs512)
	};

	// Distance
	("CHEBYSHEV") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Chebyshev)
	};
	("COSINE") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Cosine)
	};
	("EUCLIDEAN") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Euclidean)
	};
	("HAMMING") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Hamming)
	};
	("JACCARD") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Jaccard)
	};
	("MANHATTAN") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Manhattan)
	};
	("MAHALANOBIS") => {
		$crate::syn::v2::token::TokenKind::Distance(
			$crate::syn::v2::token::DistanceKind::Mahalanobis,
		)
	};
	("MINKOWSKI") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Minkowski)
	};
	("PEARSON") => {
		$crate::syn::v2::token::TokenKind::Distance($crate::syn::v2::token::DistanceKind::Pearson)
	};

	// VectorType
	("F64") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::v2::token::VectorTypeKind::F64)
	};
	("F32") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::v2::token::VectorTypeKind::F32)
	};
	("I64") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::v2::token::VectorTypeKind::I64)
	};
	("I32") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::v2::token::VectorTypeKind::I32)
	};
	("I16") => {
		$crate::syn::token::TokenKind::VectorType($crate::syn::v2::token::VectorTypeKind::I16)
	};

	($t:tt) => {
		$crate::syn::v2::token::TokenKind::Keyword($crate::syn::v2::token::keyword_t!($t))
	};
}

pub(crate) use t;
