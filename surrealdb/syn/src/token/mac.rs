/// A shorthand for token kinds.
macro_rules! t {
	("invalid") => {
		$crate::token::TokenKind::Invalid
	};
	("eof") => {
		$crate::token::TokenKind::Eof
	};
	("[") => {
		$crate::token::TokenKind::OpenDelim($crate::token::Delim::Bracket)
	};
	("{") => {
		$crate::token::TokenKind::OpenDelim($crate::token::Delim::Brace)
	};
	("(") => {
		$crate::token::TokenKind::OpenDelim($crate::token::Delim::Paren)
	};
	("]") => {
		$crate::token::TokenKind::CloseDelim($crate::token::Delim::Bracket)
	};
	("}") => {
		$crate::token::TokenKind::CloseDelim($crate::token::Delim::Brace)
	};
	(")") => {
		$crate::token::TokenKind::CloseDelim($crate::token::Delim::Paren)
	};

	("r\"") => {
		$crate::token::TokenKind::String($crate::token::StringKind::RecordIdDouble)
	};
	("r'") => {
		$crate::token::TokenKind::String($crate::token::StringKind::RecordId)
	};
	("u\"") => {
		$crate::token::TokenKind::String($crate::token::StringKind::UuidDouble)
	};
	("u'") => {
		$crate::token::TokenKind::String($crate::token::StringKind::Uuid)
	};
	("d\"") => {
		$crate::token::TokenKind::String($crate::token::StringKind::DateTimeDouble)
	};
	("d'") => {
		$crate::token::TokenKind::String($crate::token::StringKind::DateTime)
	};
	("b\"") => {
		$crate::token::TokenKind::String($crate::token::StringKind::BytesDouble)
	};
	("b'") => {
		$crate::token::TokenKind::String($crate::token::StringKind::Bytes)
	};
	("f\"") => {
		$crate::token::TokenKind::String($crate::token::StringKind::FileDouble)
	};
	("f'") => {
		$crate::token::TokenKind::String($crate::token::StringKind::File)
	};
	("\"") => {
		$crate::token::TokenKind::String($crate::token::StringKind::PlainDouble)
	};
	("'") => {
		$crate::token::TokenKind::String($crate::token::StringKind::Plain)
	};
	("\"r") => {
		$crate::token::TokenKind::CloseString {
			double: true,
		}
	};
	("'r") => {
		$crate::token::TokenKind::CloseString {
			double: false,
		}
	};

	("f") => {
		$crate::token::TokenKind::NumberSuffix($crate::token::NumberSuffix::Float)
	};
	("dec") => {
		$crate::token::TokenKind::NumberSuffix($crate::token::NumberSuffix::Decimal)
	};

	("<") => {
		$crate::token::TokenKind::LeftChefron
	};
	(">") => {
		$crate::token::TokenKind::RightChefron
	};
	("<|") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::KnnOpen)
	};
	(";") => {
		$crate::token::TokenKind::SemiColon
	};
	(",") => {
		$crate::token::TokenKind::Comma
	};
	("|") => {
		$crate::token::TokenKind::Vert
	};
	("...") => {
		$crate::token::TokenKind::DotDotDot
	};
	("..") => {
		$crate::token::TokenKind::DotDot
	};
	(".") => {
		$crate::token::TokenKind::Dot
	};
	("::") => {
		$crate::token::TokenKind::PathSeperator
	};
	(":") => {
		$crate::token::TokenKind::Colon
	};
	("->") => {
		$crate::token::TokenKind::ArrowRight
	};

	("*") => {
		$crate::token::TokenKind::Star
	};
	("$") => {
		$crate::token::TokenKind::Dollar
	};

	("+") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Add)
	};
	("%") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Modulo)
	};
	("-") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Subtract)
	};
	("**") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Power)
	};
	("*=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AllEqual)
	};
	("*~") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AllLike)
	};
	("/") => {
		$crate::token::TokenKind::ForwardSlash
	};
	("<=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::LessEqual)
	};
	(">=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::GreaterEqual)
	};
	("@") => {
		$crate::token::TokenKind::At
	};
	("||") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Or)
	};
	("&&") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::And)
	};
	("×") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Mult)
	};
	("÷") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Divide)
	};

	("$param") => {
		$crate::token::TokenKind::Parameter
	};

	("!") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Not)
	};
	("!~") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NotLike)
	};
	("!=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NotEqual)
	};

	("?") => {
		$crate::token::TokenKind::Question
	};
	("?:") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Tco)
	};
	("==") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Exact)
	};
	("!=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NotEqual)
	};
	("*=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AllEqual)
	};
	("?=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AnyEqual)
	};
	("=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Equal)
	};
	("!~") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NotLike)
	};
	("*~") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AllLike)
	};
	("?~") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AnyLike)
	};
	("~") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Like)
	};
	("+?=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Ext)
	};
	("+=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Inc)
	};
	("-=") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Dec)
	};

	("∋") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Contains)
	};
	("∌") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NotContains)
	};
	("∈") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::Inside)
	};
	("∉") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NotInside)
	};
	("⊇") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::ContainsAll)
	};
	("⊃") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::ContainsAny)
	};
	("⊅") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::ContainsNone)
	};
	("⊆") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AllInside)
	};
	("⊂") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::AnyInside)
	};
	("⊄") => {
		$crate::token::TokenKind::Operator($crate::token::Operator::NoneInside)
	};

	// algorithms
	("EDDSA") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::EdDSA)
	};
	("ES256") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Es256)
	};
	("ES384") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Es384)
	};
	("ES512") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Es512)
	};
	("HS256") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Hs256)
	};
	("HS384") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Hs384)
	};
	("HS512") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Hs512)
	};
	("PS256") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Ps256)
	};
	("PS384") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Ps384)
	};
	("PS512") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Ps512)
	};
	("RS256") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Rs256)
	};
	("RS384") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Rs384)
	};
	("RS512") => {
		$crate::token::TokenKind::Algorithm(::surrealdb_sql::Algorithm::Rs512)
	};

	// Distance
	("CHEBYSHEV") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Chebyshev)
	};
	("COSINE") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Cosine)
	};
	("COSINE_NORMALIZED") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::CosineNormalized)
	};
	("EUCLIDEAN") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Euclidean)
	};
	("HAMMING") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Hamming)
	};
	("INNER_PRODUCT") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::InnerProduct)
	};
	("JACCARD") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Jaccard)
	};
	("MANHATTAN") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Manhattan)
	};
	("MAHALANOBIS") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Mahalanobis)
	};
	("MINKOWSKI") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Minkowski)
	};
	("PEARSON") => {
		$crate::token::TokenKind::Distance($crate::token::DistanceKind::Pearson)
	};

	// VectorType
	("F64") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::F64)
	};
	("F16") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::F16)
	};
	("F32") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::F32)
	};
	("I64") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::I64)
	};
	("I32") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::I32)
	};
	("I16") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::I16)
	};
	("I8") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::I8)
	};
	("U8") => {
		$crate::token::TokenKind::VectorType($crate::token::VectorTypeKind::U8)
	};

	($t:tt) => {
		$crate::token::TokenKind::Keyword($crate::token::keyword_t!($t))
	};
}

pub(crate) use t;

#[cfg(test)]
mod tests {
	use surrealdb_sql::Algorithm;

	use crate::token::TokenKind;

	#[test]
	fn algorithm_arms_expand() {
		assert_eq!(t!("EDDSA"), TokenKind::Algorithm(Algorithm::EdDSA));
		assert_eq!(t!("HS256"), TokenKind::Algorithm(Algorithm::Hs256));
		assert_eq!(t!("RS512"), TokenKind::Algorithm(Algorithm::Rs512));
	}
}
