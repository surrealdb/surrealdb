use crate::{
	sql::{language::Language, Algorithm},
	syn::v2::token::{DistanceKind, Keyword, TokenKind},
};
use phf::phf_map;
use unicase::UniCase;

/// A map for mapping keyword strings to a tokenkind,
pub(crate) static KEYWORDS: phf::Map<UniCase<&'static str>, TokenKind> = phf_map! {
	// Keywords
	UniCase::ascii("AFTER") => TokenKind::Keyword(Keyword::After),
	UniCase::ascii("ALL") => TokenKind::Keyword(Keyword::All),
	UniCase::ascii("ANALYZE") => TokenKind::Keyword(Keyword::Analyze),
	UniCase::ascii("ANALYZER") => TokenKind::Keyword(Keyword::Analyzer),
	UniCase::ascii("AS") => TokenKind::Keyword(Keyword::As),
	UniCase::ascii("ASCENDING") => TokenKind::Keyword(Keyword::Ascending),
	UniCase::ascii("ASC") => TokenKind::Keyword(Keyword::Ascending),
	UniCase::ascii("ASCII") => TokenKind::Keyword(Keyword::Ascii),
	UniCase::ascii("ASSERT") => TokenKind::Keyword(Keyword::Assert),
	UniCase::ascii("AT") => TokenKind::Keyword(Keyword::At),
	UniCase::ascii("BEFORE") => TokenKind::Keyword(Keyword::Before),
	UniCase::ascii("BEGIN") => TokenKind::Keyword(Keyword::Begin),
	UniCase::ascii("BLANK") => TokenKind::Keyword(Keyword::Blank),
	UniCase::ascii("BM25") => TokenKind::Keyword(Keyword::Bm25),
	UniCase::ascii("BREAK") => TokenKind::Keyword(Keyword::Break),
	UniCase::ascii("BY") => TokenKind::Keyword(Keyword::By),
	UniCase::ascii("CAMEL") => TokenKind::Keyword(Keyword::Camel),
	UniCase::ascii("CANCEL") => TokenKind::Keyword(Keyword::Cancel),
	UniCase::ascii("CHANGEFEED") => TokenKind::Keyword(Keyword::ChangeFeed),
	UniCase::ascii("CHANGES") => TokenKind::Keyword(Keyword::Changes),
	UniCase::ascii("CAPACITY") => TokenKind::Keyword(Keyword::Capacity),
	UniCase::ascii("CLASS") => TokenKind::Keyword(Keyword::Class),
	UniCase::ascii("COMMENT") => TokenKind::Keyword(Keyword::Comment),
	UniCase::ascii("COMMIT") => TokenKind::Keyword(Keyword::Commit),
	UniCase::ascii("CONTENT") => TokenKind::Keyword(Keyword::Content),
	UniCase::ascii("CONTINUE") => TokenKind::Keyword(Keyword::Continue),
	UniCase::ascii("CREATE") => TokenKind::Keyword(Keyword::Create),
	UniCase::ascii("DATABASE") => TokenKind::Keyword(Keyword::Database),
	UniCase::ascii("DB") => TokenKind::Keyword(Keyword::Database),
	UniCase::ascii("DEFAULT") => TokenKind::Keyword(Keyword::Default),
	UniCase::ascii("DEFINE") => TokenKind::Keyword(Keyword::Define),
	UniCase::ascii("DELETE") => TokenKind::Keyword(Keyword::Delete),
	UniCase::ascii("DESCENDING") => TokenKind::Keyword(Keyword::Descending),
	UniCase::ascii("DESC") => TokenKind::Keyword(Keyword::Descending),
	UniCase::ascii("DIFF") => TokenKind::Keyword(Keyword::Diff),
	UniCase::ascii("DIMENSION") => TokenKind::Keyword(Keyword::Dimension),
	UniCase::ascii("DISTANCE") => TokenKind::Keyword(Keyword::Distance),
	UniCase::ascii("DIST") => TokenKind::Keyword(Keyword::Distance),
	UniCase::ascii("DOC_IDS_CACHE") => TokenKind::Keyword(Keyword::DocIdsCache),
	UniCase::ascii("DOC_IDS_ORDER") => TokenKind::Keyword(Keyword::DocIdsOrder),
	UniCase::ascii("DOC_LENGTHS_CACHE") => TokenKind::Keyword(Keyword::DocLengthsCache),
	UniCase::ascii("DOC_LENGTHS_ORDER") => TokenKind::Keyword(Keyword::DocLengthsOrder),
	UniCase::ascii("DROP") => TokenKind::Keyword(Keyword::Drop),
	UniCase::ascii("DUPLICATE") => TokenKind::Keyword(Keyword::Duplicate),
	UniCase::ascii("EDGENGRAM") => TokenKind::Keyword(Keyword::Edgengram),
	UniCase::ascii("EVENT") => TokenKind::Keyword(Keyword::Event),
	UniCase::ascii("ELSE") => TokenKind::Keyword(Keyword::Else),
	UniCase::ascii("END") => TokenKind::Keyword(Keyword::End),
	UniCase::ascii("EXISTS") => TokenKind::Keyword(Keyword::Exists),
	UniCase::ascii("EXPLAIN") => TokenKind::Keyword(Keyword::Explain),
	UniCase::ascii("false") => TokenKind::Keyword(Keyword::False),
	UniCase::ascii("FETCH") => TokenKind::Keyword(Keyword::Fetch),
	UniCase::ascii("FIELD") => TokenKind::Keyword(Keyword::Field),
	UniCase::ascii("FIELDS") => TokenKind::Keyword(Keyword::Fields),
	UniCase::ascii("COLUMNS") => TokenKind::Keyword(Keyword::Fields),
	UniCase::ascii("FILTERS") => TokenKind::Keyword(Keyword::Filters),
	UniCase::ascii("FLEXIBLE") => TokenKind::Keyword(Keyword::Flexible),
	UniCase::ascii("FLEXI") => TokenKind::Keyword(Keyword::Flexible),
	UniCase::ascii("FLEX") => TokenKind::Keyword(Keyword::Flexible),
	UniCase::ascii("FOR") => TokenKind::Keyword(Keyword::For),
	UniCase::ascii("FROM") => TokenKind::Keyword(Keyword::From),
	UniCase::ascii("FULL") => TokenKind::Keyword(Keyword::Full),
	UniCase::ascii("FUNCTION") => TokenKind::Keyword(Keyword::Function),
	UniCase::ascii("GROUP") => TokenKind::Keyword(Keyword::Group),
	UniCase::ascii("HIGHLIGHTS") => TokenKind::Keyword(Keyword::Highlights),
	UniCase::ascii("IGNORE") => TokenKind::Keyword(Keyword::Ignore),
	UniCase::ascii("INDEX") => TokenKind::Keyword(Keyword::Index),
	UniCase::ascii("INFO") => TokenKind::Keyword(Keyword::Info),
	UniCase::ascii("INSERT") => TokenKind::Keyword(Keyword::Insert),
	UniCase::ascii("INTO") => TokenKind::Keyword(Keyword::Into),
	UniCase::ascii("IF") => TokenKind::Keyword(Keyword::If),
	UniCase::ascii("IS") => TokenKind::Keyword(Keyword::Is),
	UniCase::ascii("KEY") => TokenKind::Keyword(Keyword::Key),
	UniCase::ascii("KILL") => TokenKind::Keyword(Keyword::Kill),
	UniCase::ascii("KNN") => TokenKind::Keyword(Keyword::Knn),
	UniCase::ascii("LET") => TokenKind::Keyword(Keyword::Let),
	UniCase::ascii("LIMIT") => TokenKind::Keyword(Keyword::Limit),
	UniCase::ascii("LIVE") => TokenKind::Keyword(Keyword::Live),
	UniCase::ascii("LOWERCASE") => TokenKind::Keyword(Keyword::Lowercase),
	UniCase::ascii("MERGE") => TokenKind::Keyword(Keyword::Merge),
	UniCase::ascii("MODEL") => TokenKind::Keyword(Keyword::Model),
	UniCase::ascii("MTREE") => TokenKind::Keyword(Keyword::MTree),
	UniCase::ascii("MTREE_CACHE") => TokenKind::Keyword(Keyword::MTreeCache),
	UniCase::ascii("NAMESPACE") => TokenKind::Keyword(Keyword::Namespace),
	UniCase::ascii("NS") => TokenKind::Keyword(Keyword::Namespace),
	UniCase::ascii("NGRAM") => TokenKind::Keyword(Keyword::Ngram),
	UniCase::ascii("NO") => TokenKind::Keyword(Keyword::No),
	UniCase::ascii("NOINDEX") => TokenKind::Keyword(Keyword::NoIndex),
	UniCase::ascii("NONE") => TokenKind::Keyword(Keyword::None),
	UniCase::ascii("NULL") => TokenKind::Keyword(Keyword::Null),
	UniCase::ascii("NUMERIC") => TokenKind::Keyword(Keyword::Numeric),
	UniCase::ascii("OMIT") => TokenKind::Keyword(Keyword::Omit),
	UniCase::ascii("ON") => TokenKind::Keyword(Keyword::On),
	UniCase::ascii("ONLY") => TokenKind::Keyword(Keyword::Only),
	UniCase::ascii("OPTION") => TokenKind::Keyword(Keyword::Option),
	UniCase::ascii("ORDER") => TokenKind::Keyword(Keyword::Order),
	UniCase::ascii("PARALLEL") => TokenKind::Keyword(Keyword::Parallel),
	UniCase::ascii("PARAM") => TokenKind::Keyword(Keyword::Param),
	UniCase::ascii("PASSHASH") => TokenKind::Keyword(Keyword::Passhash),
	UniCase::ascii("PASSWORD") => TokenKind::Keyword(Keyword::Password),
	UniCase::ascii("PATCH") => TokenKind::Keyword(Keyword::Patch),
	UniCase::ascii("PERMISSIONS") => TokenKind::Keyword(Keyword::Permissions),
	UniCase::ascii("POSTINGS_CACHE") => TokenKind::Keyword(Keyword::PostingsCache),
	UniCase::ascii("POSTINGS_ORDER") => TokenKind::Keyword(Keyword::PostingsOrder),
	UniCase::ascii("PUNCT") => TokenKind::Keyword(Keyword::Punct),
	UniCase::ascii("READONLY") => TokenKind::Keyword(Keyword::Readonly),
	UniCase::ascii("RELATE") => TokenKind::Keyword(Keyword::Relate),
	UniCase::ascii("RELATION") => TokenKind::Keyword(Keyword::Relation),
	UniCase::ascii("REMOVE") => TokenKind::Keyword(Keyword::Remove),
	UniCase::ascii("REPLACE") => TokenKind::Keyword(Keyword::Replace),
	UniCase::ascii("RETURN") => TokenKind::Keyword(Keyword::Return),
	UniCase::ascii("ROLES") => TokenKind::Keyword(Keyword::Roles),
	UniCase::ascii("ROOT") => TokenKind::Keyword(Keyword::Root),
	UniCase::ascii("KV") => TokenKind::Keyword(Keyword::Root),
	UniCase::ascii("SCHEMAFULL") => TokenKind::Keyword(Keyword::Schemafull),
	UniCase::ascii("SCHEMAFUL") => TokenKind::Keyword(Keyword::Schemafull),
	UniCase::ascii("SCHEMALESS") => TokenKind::Keyword(Keyword::Schemaless),
	UniCase::ascii("SCOPE") => TokenKind::Keyword(Keyword::Scope),
	UniCase::ascii("SC") => TokenKind::Keyword(Keyword::Scope),
	UniCase::ascii("SEARCH") => TokenKind::Keyword(Keyword::Search),
	UniCase::ascii("SELECT") => TokenKind::Keyword(Keyword::Select),
	UniCase::ascii("SESSION") => TokenKind::Keyword(Keyword::Session),
	UniCase::ascii("SET") => TokenKind::Keyword(Keyword::Set),
	UniCase::ascii("SHOW") => TokenKind::Keyword(Keyword::Show),
	UniCase::ascii("SIGNIN") => TokenKind::Keyword(Keyword::Signin),
	UniCase::ascii("SIGNUP") => TokenKind::Keyword(Keyword::Signup),
	UniCase::ascii("SINCE") => TokenKind::Keyword(Keyword::Since),
	UniCase::ascii("SLEEP") => TokenKind::Keyword(Keyword::Sleep),
	UniCase::ascii("SNOWBALL") => TokenKind::Keyword(Keyword::Snowball),
	UniCase::ascii("SPLIT") => TokenKind::Keyword(Keyword::Split),
	UniCase::ascii("START") => TokenKind::Keyword(Keyword::Start),
	UniCase::ascii("TABLE") => TokenKind::Keyword(Keyword::Table),
	UniCase::ascii("TB") => TokenKind::Keyword(Keyword::Table),
	UniCase::ascii("TERMS_CACHE") => TokenKind::Keyword(Keyword::TermsCache),
	UniCase::ascii("TERMS_ORDER") => TokenKind::Keyword(Keyword::TermsOrder),
	UniCase::ascii("THEN") => TokenKind::Keyword(Keyword::Then),
	UniCase::ascii("THROW") => TokenKind::Keyword(Keyword::Throw),
	UniCase::ascii("TIMEOUT") => TokenKind::Keyword(Keyword::Timeout),
	UniCase::ascii("TO") => TokenKind::Keyword(Keyword::To),
	UniCase::ascii("TOKENIZERS") => TokenKind::Keyword(Keyword::Tokenizers),
	UniCase::ascii("TOKEN") => TokenKind::Keyword(Keyword::Token),
	UniCase::ascii("TRANSACTION") => TokenKind::Keyword(Keyword::Transaction),
	UniCase::ascii("true") => TokenKind::Keyword(Keyword::True),
	UniCase::ascii("TYPE") => TokenKind::Keyword(Keyword::Type),
	UniCase::ascii("UNIQUE") => TokenKind::Keyword(Keyword::Unique),
	UniCase::ascii("UNSET") => TokenKind::Keyword(Keyword::Unset),
	UniCase::ascii("UPDATE") => TokenKind::Keyword(Keyword::Update),
	UniCase::ascii("UPPERCASE") => TokenKind::Keyword(Keyword::Uppercase),
	UniCase::ascii("USE") => TokenKind::Keyword(Keyword::Use),
	UniCase::ascii("USER") => TokenKind::Keyword(Keyword::User),
	UniCase::ascii("VALUE") => TokenKind::Keyword(Keyword::Value),
	UniCase::ascii("VALUES") => TokenKind::Keyword(Keyword::Values),
	UniCase::ascii("VERSION") => TokenKind::Keyword(Keyword::Version),
	UniCase::ascii("VS") => TokenKind::Keyword(Keyword::Vs),
	UniCase::ascii("WHEN") => TokenKind::Keyword(Keyword::When),
	UniCase::ascii("WHERE") => TokenKind::Keyword(Keyword::Where),
	UniCase::ascii("WITH") => TokenKind::Keyword(Keyword::With),
	UniCase::ascii("ALLINSIDE") => TokenKind::Keyword(Keyword::AllInside),
	UniCase::ascii("ANDKW") => TokenKind::Keyword(Keyword::AndKw),
	UniCase::ascii("ANYINSIDE") => TokenKind::Keyword(Keyword::AnyInside),
	UniCase::ascii("INSIDE") => TokenKind::Keyword(Keyword::Inside),
	UniCase::ascii("INTERSECTS") => TokenKind::Keyword(Keyword::Intersects),
	UniCase::ascii("NONEINSIDE") => TokenKind::Keyword(Keyword::NoneInside),
	UniCase::ascii("NOTINSIDE") => TokenKind::Keyword(Keyword::NotInside),
	UniCase::ascii("OR") => TokenKind::Keyword(Keyword::OrKw),
	UniCase::ascii("OUTSIDE") => TokenKind::Keyword(Keyword::Outside),
	UniCase::ascii("NOT") => TokenKind::Keyword(Keyword::Not),
	UniCase::ascii("AND") => TokenKind::Keyword(Keyword::And),
	UniCase::ascii("COLLATE") => TokenKind::Keyword(Keyword::Collate),
	UniCase::ascii("CONTAINSALL") => TokenKind::Keyword(Keyword::ContainsAll),
	UniCase::ascii("CONTAINSANY") => TokenKind::Keyword(Keyword::ContainsAny),
	UniCase::ascii("CONTAINSNONE") => TokenKind::Keyword(Keyword::ContainsNone),
	UniCase::ascii("CONTAINSNOT") => TokenKind::Keyword(Keyword::ContainsNot),
	UniCase::ascii("CONTAINS") => TokenKind::Keyword(Keyword::Contains),
	UniCase::ascii("IN") => TokenKind::Keyword(Keyword::In),
	UniCase::ascii("OUT") => TokenKind::Keyword(Keyword::Out),
	UniCase::ascii("NORMAL") => TokenKind::Keyword(Keyword::Normal),

	UniCase::ascii("ANY") => TokenKind::Keyword(Keyword::Any),
	UniCase::ascii("ARRAY") => TokenKind::Keyword(Keyword::Array),
	UniCase::ascii("GEOMETRY") => TokenKind::Keyword(Keyword::Geometry),
	UniCase::ascii("RECORD") => TokenKind::Keyword(Keyword::Record),
	UniCase::ascii("FUTURE") => TokenKind::Keyword(Keyword::Future),
	UniCase::ascii("BOOL") => TokenKind::Keyword(Keyword::Bool),
	UniCase::ascii("BYTES") => TokenKind::Keyword(Keyword::Bytes),
	UniCase::ascii("DATETIME") => TokenKind::Keyword(Keyword::Datetime),
	UniCase::ascii("DECIMAL") => TokenKind::Keyword(Keyword::Decimal),
	UniCase::ascii("DURATION") => TokenKind::Keyword(Keyword::Duration),
	UniCase::ascii("FLOAT") => TokenKind::Keyword(Keyword::Float),
	UniCase::ascii("fn") => TokenKind::Keyword(Keyword::Fn),
	UniCase::ascii("ml") => TokenKind::Keyword(Keyword::ML),
	UniCase::ascii("INT") => TokenKind::Keyword(Keyword::Int),
	UniCase::ascii("NUMBER") => TokenKind::Keyword(Keyword::Number),
	UniCase::ascii("OBJECT") => TokenKind::Keyword(Keyword::Object),
	UniCase::ascii("STRING") => TokenKind::Keyword(Keyword::String),
	UniCase::ascii("UUID") => TokenKind::Keyword(Keyword::Uuid),
	UniCase::ascii("ULID") => TokenKind::Keyword(Keyword::Ulid),
	UniCase::ascii("RAND") => TokenKind::Keyword(Keyword::Rand),
	UniCase::ascii("FEATURE") => TokenKind::Keyword(Keyword::Feature),
	UniCase::ascii("LINE") => TokenKind::Keyword(Keyword::Line),
	UniCase::ascii("POINT") => TokenKind::Keyword(Keyword::Point),
	UniCase::ascii("POLYGON") => TokenKind::Keyword(Keyword::Polygon),
	UniCase::ascii("MULTIPOINT") => TokenKind::Keyword(Keyword::MultiPoint),
	UniCase::ascii("MULTILINE") => TokenKind::Keyword(Keyword::MultiLine),
	UniCase::ascii("MULTIPOLYGON") => TokenKind::Keyword(Keyword::MultiPolygon),
	UniCase::ascii("COLLECTION") => TokenKind::Keyword(Keyword::Collection),

	// Languages
	UniCase::ascii("ARABIC") => TokenKind::Language(Language::Arabic),
	UniCase::ascii("ARA") => TokenKind::Language(Language::Arabic),
	UniCase::ascii("AR") => TokenKind::Language(Language::Arabic),
	UniCase::ascii("DANISH") => TokenKind::Language(Language::Danish),
	UniCase::ascii("DAN") => TokenKind::Language(Language::Danish),
	UniCase::ascii("DA") => TokenKind::Language(Language::Danish),
	UniCase::ascii("DUTCH") => TokenKind::Language(Language::Dutch),
	UniCase::ascii("NLD") => TokenKind::Language(Language::Dutch),
	UniCase::ascii("NL") => TokenKind::Language(Language::Dutch),
	UniCase::ascii("ENGLISH") => TokenKind::Language(Language::English),
	UniCase::ascii("ENG") => TokenKind::Language(Language::English),
	UniCase::ascii("EN") => TokenKind::Language(Language::English),
	UniCase::ascii("FRENCH") => TokenKind::Language(Language::French),
	UniCase::ascii("FRA") => TokenKind::Language(Language::French),
	UniCase::ascii("FR") => TokenKind::Language(Language::French),
	UniCase::ascii("GERMAN") => TokenKind::Language(Language::German),
	UniCase::ascii("DEU") => TokenKind::Language(Language::German),
	UniCase::ascii("DE") => TokenKind::Language(Language::German),
	UniCase::ascii("GREEK") => TokenKind::Language(Language::Greek),
	UniCase::ascii("ELL") => TokenKind::Language(Language::Greek),
	UniCase::ascii("EL") => TokenKind::Language(Language::Greek),
	UniCase::ascii("HUNGARIAN") => TokenKind::Language(Language::Hungarian),
	UniCase::ascii("HUN") => TokenKind::Language(Language::Hungarian),
	UniCase::ascii("HU") => TokenKind::Language(Language::Hungarian),
	UniCase::ascii("ITALIAN") => TokenKind::Language(Language::Italian),
	UniCase::ascii("ITA") => TokenKind::Language(Language::Italian),
	UniCase::ascii("IT") => TokenKind::Language(Language::Italian),
	UniCase::ascii("NORWEGIAN") => TokenKind::Language(Language::Norwegian),
	UniCase::ascii("NOR") => TokenKind::Language(Language::Norwegian),
	UniCase::ascii("PORTUGUESE") => TokenKind::Language(Language::Portuguese),
	UniCase::ascii("POR") => TokenKind::Language(Language::Portuguese),
	UniCase::ascii("PT") => TokenKind::Language(Language::Portuguese),
	UniCase::ascii("ROMANIAN") => TokenKind::Language(Language::Romanian),
	UniCase::ascii("RON") => TokenKind::Language(Language::Romanian),
	UniCase::ascii("RO") => TokenKind::Language(Language::Romanian),
	UniCase::ascii("RUSSIAN") => TokenKind::Language(Language::Russian),
	UniCase::ascii("RUS") => TokenKind::Language(Language::Russian),
	UniCase::ascii("RU") => TokenKind::Language(Language::Russian),
	UniCase::ascii("SPANISH") => TokenKind::Language(Language::Spanish),
	UniCase::ascii("SPA") => TokenKind::Language(Language::Spanish),
	UniCase::ascii("ES") => TokenKind::Language(Language::Spanish),
	UniCase::ascii("SWEDISH") => TokenKind::Language(Language::Swedish),
	UniCase::ascii("SWE") => TokenKind::Language(Language::Swedish),
	UniCase::ascii("SV") => TokenKind::Language(Language::Swedish),
	UniCase::ascii("TAMIL") => TokenKind::Language(Language::Tamil),
	UniCase::ascii("TAM") => TokenKind::Language(Language::Tamil),
	UniCase::ascii("TA") => TokenKind::Language(Language::Tamil),
	UniCase::ascii("TURKISH") => TokenKind::Language(Language::Turkish),
	UniCase::ascii("TUR") => TokenKind::Language(Language::Turkish),
	UniCase::ascii("TR") => TokenKind::Language(Language::Turkish),

	// Algorithms
	UniCase::ascii("EDDSA") => TokenKind::Algorithm(Algorithm::EdDSA),
	UniCase::ascii("ES256") => TokenKind::Algorithm(Algorithm::Es256),
	UniCase::ascii("ES384") => TokenKind::Algorithm(Algorithm::Es384),
	UniCase::ascii("ES512") => TokenKind::Algorithm(Algorithm::Es512),
	UniCase::ascii("HS256") => TokenKind::Algorithm(Algorithm::Hs256),
	UniCase::ascii("HS384") => TokenKind::Algorithm(Algorithm::Hs384),
	UniCase::ascii("HS512") => TokenKind::Algorithm(Algorithm::Hs512),
	UniCase::ascii("PS256") => TokenKind::Algorithm(Algorithm::Ps256),
	UniCase::ascii("PS384") => TokenKind::Algorithm(Algorithm::Ps384),
	UniCase::ascii("PS512") => TokenKind::Algorithm(Algorithm::Ps512),
	UniCase::ascii("RS256") => TokenKind::Algorithm(Algorithm::Rs256),
	UniCase::ascii("RS384") => TokenKind::Algorithm(Algorithm::Rs384),
	UniCase::ascii("RS512") => TokenKind::Algorithm(Algorithm::Rs512),
	UniCase::ascii("JWKS") => jwks_token_kind(), // Necessary because `phf_map!` doesn't support `cfg` attributes

	// Distance
	UniCase::ascii("EUCLIDEAN") => TokenKind::Distance(DistanceKind::Euclidean),
	UniCase::ascii("MANHATTAN") => TokenKind::Distance(DistanceKind::Manhattan),
	UniCase::ascii("HAMMING") => TokenKind::Distance(DistanceKind::Hamming),
	UniCase::ascii("MINKOWSKI") => TokenKind::Distance(DistanceKind::Minkowski),
};

const fn jwks_token_kind() -> TokenKind {
	#[cfg(feature = "jwks")]
	let token = TokenKind::Algorithm(Algorithm::Jwks);
	#[cfg(not(feature = "jwks"))]
	let token = TokenKind::Identifier;
	token
}
