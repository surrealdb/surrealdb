/// A shorthand for token kinds.
macro_rules! t {
	("[") => {
		$crate::sql::token::TokenKind::OpenDelim($crate::sql::token::Delim::Bracket)
	};
	("{") => {
		$crate::sql::token::TokenKind::OpenDelim($crate::sql::token::Delim::Brace)
	};
	("(") => {
		$crate::sql::token::TokenKind::OpenDelim($crate::sql::token::Delim::Paren)
	};
	("]") => {
		$crate::sql::token::TokenKind::CloseDelim($crate::sql::token::Delim::Bracket)
	};
	("}") => {
		$crate::sql::token::TokenKind::CloseDelim($crate::sql::token::Delim::Brace)
	};
	(")") => {
		$crate::sql::token::TokenKind::CloseDelim($crate::sql::token::Delim::Paren)
	};

	("<") => {
		$crate::sql::token::TokenKind::LeftChefron
	};
	(">") => {
		$crate::sql::token::TokenKind::RightChefron
	};

	(";") => {
		$crate::sql::token::TokenKind::SemiColon
	};
	(",") => {
		$crate::sql::token::TokenKind::Comma
	};
	("|") => {
		$crate::sql::token::TokenKind::Vert
	};
	("...") => {
		$crate::sql::token::TokenKind::DotDotDot
	};
	("..") => {
		$crate::sql::token::TokenKind::DotDot
	};
	(".") => {
		$crate::sql::token::TokenKind::Dot
	};
	("::") => {
		$crate::sql::token::TokenKind::PathSeperator
	};
	(":") => {
		$crate::sql::token::TokenKind::Colon
	};
	("<-") => {
		$crate::sql::token::TokenKind::ArrowLeft
	};
	("<->") => {
		$crate::sql::token::TokenKind::BiArrow
	};
	("->") => {
		$crate::sql::token::TokenKind::ArrowRight
	};

	("+") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Add)
	};
	("-") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Subtract)
	};
	("*") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Star)
	};
	("**") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Power)
	};
	("*=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AllEqual)
	};
	("*~") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AllLike)
	};
	("/") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Divide)
	};
	("<=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::LessEqual)
	};
	(">=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::GreaterEqual)
	};
	("@") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::At)
	};
	("@@") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Matches)
	};
	("||") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Or)
	};
	("&&") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::And)
	};

	("!") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Not)
	};
	("!~") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NotLike)
	};
	("!=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NotEqual)
	};

	("?") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Like)
	};
	("?:") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Tco)
	};
	("??") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Nco)
	};
	("==") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Exact)
	};
	("!=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NotEqual)
	};
	("*=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AllEqual)
	};
	("?=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AnyEqual)
	};
	("=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Equal)
	};
	("!~") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NotLike)
	};
	("*~") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AllLike)
	};
	("?~") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AnyLike)
	};
	("~") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Like)
	};
	("+?=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Ext)
	};
	("+=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Inc)
	};
	("-=") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Dec)
	};

	("∋") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Contains)
	};
	("∌") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NotContains)
	};
	("∈") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::Inside)
	};
	("∉") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NotInside)
	};
	("⊇") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::ContainsAll)
	};
	("⊃") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::ContainsAny)
	};
	("⊅") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::ContainsNone)
	};
	("⊆") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AllInside)
	};
	("⊂") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::AnyInside)
	};
	("⊄") => {
		$crate::sql::token::TokenKind::Operator($crate::sql::token::Operator::NoneInside)
	};
	("ALLINSIDE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::AllInside)
	};
	("AND") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::And)
	};
	("ANYINSIDE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::AnyInside)
	};
	("CONTAINS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Contain)
	};
	("CONTAINSALL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::ContainAll)
	};
	("CONTAINSANY") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::ContainAny)
	};
	("CONTAINSNONE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::ContainNone)
	};
	("CONTAINSNOT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::NotContain)
	};
	("INSIDE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Inside)
	};
	("INTERSECTS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Intersects)
	};
	("NONEINSIDE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::NoneInside)
	};
	("NOTINSIDE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::NotInside)
	};
	("OR") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Or)
	};
	("OUTSIDE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Outside)
	};

	("AFTER") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::After)
	};
	("ALL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::All)
	};
	("ANALYZE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Analyze)
	};
	("ARABIC") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Arabic)
	};
	("AS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::As)
	};
	("ASCII") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Ascii)
	};
	("ASSERT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Assert)
	};
	("BEFORE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Before)
	};
	("BEGIN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Begin)
	};
	("BLANK") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Blank)
	};
	("BM25") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Bm25)
	};
	("BREAK") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Break)
	};
	("BY") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::By)
	};
	("CAMEL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Camel)
	};
	("CANCEL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Cancel)
	};
	("CHANGEFEED") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::ChangeFeed)
	};
	("CHANGES") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Changes)
	};
	("CLASS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Class)
	};
	("COMMENT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Comment)
	};
	("COMMIT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Commit)
	};
	("CONTENT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Content)
	};
	("CONTINUE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Continue)
	};
	("COSINE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Cosine)
	};
	("CREATE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Create)
	};
	("DANISH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Danish)
	};
	("DATABASE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Database)
	};
	("DEFAULT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Default)
	};
	("DEFINE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Define)
	};
	("DELETE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Delete)
	};
	("DIFF") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Diff)
	};
	("DROP") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Drop)
	};
	("DUTCH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Dutch)
	};
	("EDDSA") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::EdDSA)
	};
	("EDGENGRAM") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Edgengram)
	};
	("ENGLISH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::English)
	};
	("ES256") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Es256)
	};
	("ES384") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Es384)
	};
	("ES512") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Es512)
	};
	("EUCLIDEAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Euclidean)
	};
	("EVENT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Event)
	};
	("EXPLAIN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Explain)
	};
	("FETCH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Fetch)
	};
	("FIELDS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Fields)
	};
	("FILTERS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Filters)
	};
	("FLEXIBILE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Flexibile)
	};
	("FOR") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::For)
	};
	("FRENCH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::French)
	};
	("FROM") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::From)
	};
	("FULL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Full)
	};
	("FUNCTION") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Function)
	};
	("GERMAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::German)
	};
	("GREEK") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Greek)
	};
	("GROUP") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Group)
	};
	("HAMMING") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Hamming)
	};
	("HS256") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Hs256)
	};
	("HS384") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Hs384)
	};
	("HS512") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Hs512)
	};
	("HUNGARIAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Hungarian)
	};
	("IGNORE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Ignore)
	};
	("INDEX") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Index)
	};
	("INFO") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Info)
	};
	("INSERT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Insert)
	};
	("INTO") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Into)
	};
	("ITALIAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Italian)
	};
	("IF") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::If)
	};
	("KILL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Kill)
	};
	("LET") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Let)
	};
	("LIMIT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Limit)
	};
	("LIVE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Live)
	};
	("LOWERCASE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Lowercase)
	};
	("MAHALANOBIS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Mahalanobis)
	};
	("MANHATTAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Manhattan)
	};
	("MERGE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Merge)
	};
	("MINKOWSKI") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Minkowski)
	};
	("MODEL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Model)
	};
	("NAMESPACE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Namespace)
	};
	("NGRAM") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Ngram)
	};
	("NOINDEX") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::NoIndex)
	};
	("NONE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::None)
	};
	("NORWEGIAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Norwegian)
	};
	("NULL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Null)
	};
	("OMIT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Omit)
	};
	("ON") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::On)
	};
	("ONLY") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Only)
	};
	("OPTION") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Option)
	};
	("ORDER") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Order)
	};
	("PARALLEL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Parallel)
	};
	("PARAM") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Param)
	};
	("PASSHASH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Passhash)
	};
	("PATCH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Patch)
	};
	("PERMISSIONS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Permissions)
	};
	("PORTUGUESE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Portuguese)
	};
	("PS256") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Ps256)
	};
	("PS384") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Ps384)
	};
	("PS512") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Ps512)
	};
	("PUNCT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Punct)
	};
	("RELATE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Relate)
	};
	("REMOVE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Remove)
	};
	("REPLACE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Replace)
	};
	("RETURN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Return)
	};
	("ROLES") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Roles)
	};
	("ROMANIAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Romanian)
	};
	("ROOT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Root)
	};
	("RS256") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Rs256)
	};
	("RS384") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Rs384)
	};
	("RS512") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Rs512)
	};
	("RUSSIAN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Russian)
	};
	("SCHEMAFULL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Schemafull)
	};
	("SCHEMALESS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Schemaless)
	};
	("SCOPE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Scope)
	};
	("SELECT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Select)
	};
	("SESSION") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Session)
	};
	("SHOW") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Show)
	};
	("SIGNIM") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Signim)
	};
	("SIGNUP") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Signup)
	};
	("SINCE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Since)
	};
	("SLEEP") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Sleep)
	};
	("SNOWBALL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Snowball)
	};
	("SPANISH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Spanish)
	};
	("SPLIT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Split)
	};
	("START") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Start)
	};
	("SWEDISH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Swedish)
	};
	("TABLE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Table)
	};
	("TAMIL") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Tamil)
	};
	("THEN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Then)
	};
	("THROW") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Throw)
	};
	("TIMEOUT") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Timeout)
	};
	("TOKEIZERS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Tokeizers)
	};
	("TRANSACTION") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Transaction)
	};
	("TURKISH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Turkish)
	};
	("TYPE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Type)
	};
	("UNIQUE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Unique)
	};
	("UPDATE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Update)
	};
	("UPPERCASE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Uppercase)
	};
	("USE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Use)
	};
	("USER") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::User)
	};
	("VALUE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Value)
	};
	("VERSION") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Version)
	};
	("VS") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Vs)
	};
	("WHEN") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::When)
	};
	("WHERE") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::Where)
	};
	("WITH") => {
		$crate::sql::token::TokenKind::Keyword($crate::sql::token::Keyword::With)
	};
}

pub(crate) use t;
