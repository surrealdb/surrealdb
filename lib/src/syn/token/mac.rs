/// A shorthand for token kinds.
macro_rules! t {
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

	("<") => {
		$crate::syn::token::TokenKind::LeftChefron
	};
	(">") => {
		$crate::syn::token::TokenKind::RightChefron
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
	("<-") => {
		$crate::syn::token::TokenKind::ArrowLeft
	};
	("<->") => {
		$crate::syn::token::TokenKind::BiArrow
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
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Divide)
	};
	("<=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::LessEqual)
	};
	(">=") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::GreaterEqual)
	};
	("@") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::At)
	};
	("@@") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Matches)
	};
	("||") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Or)
	};
	("&&") => {
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::And)
	};

	("$param") => {
		$crate::syn::token::TokenKind::Parameter
	};
	("123") => {
		$crate::syn::token::TokenKind::Number
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
		$crate::syn::token::TokenKind::Operator($crate::syn::token::Operator::Like)
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
	("ALLINSIDE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::AllInside)
	};
	("AND") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::And)
	};
	("ANYINSIDE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::AnyInside)
	};
	("CONTAINS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Contain)
	};
	("CONTAINSALL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::ContainAll)
	};
	("CONTAINSANY") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::ContainAny)
	};
	("CONTAINSNONE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::ContainNone)
	};
	("CONTAINSNOT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::NotContain)
	};
	("INSIDE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Inside)
	};
	("INTERSECTS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Intersects)
	};
	("NONEINSIDE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::NoneInside)
	};
	("NOTINSIDE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::NotInside)
	};
	("OR") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Or)
	};
	("OUTSIDE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Outside)
	};

	("AFTER") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::After)
	};
	("ALL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::All)
	};
	("ANALYZE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Analyze)
	};
	("ARABIC") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Arabic)
	};
	("AS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::As)
	};
	("ASCII") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Ascii)
	};
	("ASSERT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Assert)
	};
	("BEFORE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Before)
	};
	("BEGIN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Begin)
	};
	("BLANK") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Blank)
	};
	("BM25") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Bm25)
	};
	("BREAK") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Break)
	};
	("BY") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::By)
	};
	("CAMEL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Camel)
	};
	("CANCEL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Cancel)
	};
	("CHANGEFEED") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::ChangeFeed)
	};
	("CHANGES") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Changes)
	};
	("CLASS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Class)
	};
	("COMMENT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Comment)
	};
	("COMMIT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Commit)
	};
	("CONTENT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Content)
	};
	("CONTINUE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Continue)
	};
	("COSINE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Cosine)
	};
	("CREATE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Create)
	};
	("DANISH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Danish)
	};
	("DATABASE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Database)
	};
	("DEFAULT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Default)
	};
	("DEFINE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Define)
	};
	("DELETE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Delete)
	};
	("DIFF") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Diff)
	};
	("DROP") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Drop)
	};
	("DUTCH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Dutch)
	};
	("EDDSA") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::EdDSA)
	};
	("EDGENGRAM") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Edgengram)
	};
	("ENGLISH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::English)
	};
	("ES256") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Es256)
	};
	("ES384") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Es384)
	};
	("ES512") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Es512)
	};
	("EUCLIDEAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Euclidean)
	};
	("EVENT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Event)
	};
	("EXPLAIN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Explain)
	};
	("FALSE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::False)
	};
	("FETCH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Fetch)
	};
	("FIELDS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Fields)
	};
	("FILTERS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Filters)
	};
	("FLEXIBILE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Flexibile)
	};
	("FOR") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::For)
	};
	("FRENCH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::French)
	};
	("FROM") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::From)
	};
	("FULL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Full)
	};
	("FUNCTION") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Function)
	};
	("GERMAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::German)
	};
	("GREEK") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Greek)
	};
	("GROUP") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Group)
	};
	("HAMMING") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Hamming)
	};
	("HS256") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Hs256)
	};
	("HS384") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Hs384)
	};
	("HS512") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Hs512)
	};
	("HUNGARIAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Hungarian)
	};
	("IGNORE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Ignore)
	};
	("INDEX") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Index)
	};
	("INFO") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Info)
	};
	("INSERT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Insert)
	};
	("INTO") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Into)
	};
	("ITALIAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Italian)
	};
	("IF") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::If)
	};
	("KILL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Kill)
	};
	("LET") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Let)
	};
	("LIMIT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Limit)
	};
	("LIVE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Live)
	};
	("LOWERCASE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Lowercase)
	};
	("MAHALANOBIS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Mahalanobis)
	};
	("MANHATTAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Manhattan)
	};
	("MERGE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Merge)
	};
	("MINKOWSKI") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Minkowski)
	};
	("MODEL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Model)
	};
	("NAMESPACE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Namespace)
	};
	("NGRAM") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Ngram)
	};
	("NOINDEX") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::NoIndex)
	};
	("NONE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::None)
	};
	("NORWEGIAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Norwegian)
	};
	("NULL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Null)
	};
	("OMIT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Omit)
	};
	("ON") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::On)
	};
	("ONLY") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Only)
	};
	("OPTION") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Option)
	};
	("ORDER") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Order)
	};
	("PARALLEL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Parallel)
	};
	("PARAM") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Param)
	};
	("PASSHASH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Passhash)
	};
	("PATCH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Patch)
	};
	("PERMISSIONS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Permissions)
	};
	("PORTUGUESE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Portuguese)
	};
	("PS256") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Ps256)
	};
	("PS384") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Ps384)
	};
	("PS512") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Ps512)
	};
	("PUNCT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Punct)
	};
	("RELATE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Relate)
	};
	("REMOVE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Remove)
	};
	("REPLACE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Replace)
	};
	("RETURN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Return)
	};
	("ROLES") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Roles)
	};
	("ROMANIAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Romanian)
	};
	("ROOT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Root)
	};
	("RS256") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Rs256)
	};
	("RS384") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Rs384)
	};
	("RS512") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Rs512)
	};
	("RUSSIAN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Russian)
	};
	("SCHEMAFULL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Schemafull)
	};
	("SCHEMALESS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Schemaless)
	};
	("SCOPE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Scope)
	};
	("SELECT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Select)
	};
	("SET") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Set)
	};
	("SESSION") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Session)
	};
	("SHOW") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Show)
	};
	("SIGNIM") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Signim)
	};
	("SIGNUP") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Signup)
	};
	("SINCE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Since)
	};
	("SLEEP") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Sleep)
	};
	("SNOWBALL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Snowball)
	};
	("SPANISH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Spanish)
	};
	("SPLIT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Split)
	};
	("START") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Start)
	};
	("SWEDISH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Swedish)
	};
	("TABLE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Table)
	};
	("TAMIL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Tamil)
	};
	("THEN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Then)
	};
	("THROW") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Throw)
	};
	("TIMEOUT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Timeout)
	};
	("TOKEIZERS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Tokeizers)
	};
	("TRANSACTION") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Transaction)
	};
	("TRUE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::True)
	};
	("TURKISH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Turkish)
	};
	("TYPE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Type)
	};
	("UNIQUE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Unique)
	};
	("UNSET") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Unset)
	};
	("UPDATE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Update)
	};
	("UPPERCASE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Uppercase)
	};
	("USE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Use)
	};
	("USER") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::User)
	};
	("VALUE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Value)
	};
	("VERSION") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Version)
	};
	("VS") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Vs)
	};
	("WHEN") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::When)
	};
	("WHERE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Where)
	};
	("WITH") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::With)
	};

	("ANY") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Any)
	};
	("FUTURE") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Future)
	};
	("BOOL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Bool)
	};
	("BYTES") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Bytes)
	};
	("DATETIME") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Datetime)
	};
	("DECIMAL") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Decimal)
	};
	("DURATION") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Duration)
	};
	("FLOAT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Float)
	};
	("INT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Int)
	};
	("NUMBER") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Number)
	};
	("OBJECT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Object)
	};
	("POINT") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Point)
	};
	("STRING") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::String)
	};
	("UUID") => {
		$crate::syn::token::TokenKind::Keyword($crate::syn::token::Keyword::Uuid)
	};
}

pub(crate) use t;
