use common::span::Span;
use logos::{Lexer, Logos};
use std::fmt;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum LexError {
	UnexpectedEof(Span),
	InvalidUtf8(Span),
	InvalidToken(Span),
}

impl Default for LexError {
	fn default() -> Self {
		LexError::InvalidToken(Span::empty())
	}
}

impl LexError {
	pub fn span(&self) -> Span {
		let (LexError::UnexpectedEof(x) | LexError::InvalidUtf8(x) | LexError::InvalidToken(x)) =
			self;
		*x
	}

	fn from_lexer<'a>(l: &mut Lexer<'a, BaseTokenKind>) -> Self {
		let span = l.span();
		let span = Span::from_range((span.start as u32)..(span.end as u32));

		if std::str::from_utf8(l.slice()).is_err() {
			LexError::InvalidUtf8(span)
		} else if dbg!(l.remainder()).is_empty() {
			LexError::UnexpectedEof(span)
		} else {
			LexError::InvalidToken(span)
		}
	}
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Joined {
	Seperated,
	#[default]
	Joined,
}

fn whitespace_callback(lexer: &mut Lexer<BaseTokenKind>) {
	lexer.extras = Joined::Seperated;
}

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(source = [u8])]
#[logos(utf8 = false)]
#[logos(extras = Joined)]
#[logos(error(LexError, LexError::from_lexer))]
#[logos(subpattern backtick_ident = r"`([^`\\]|\\[`\\])*`")]
#[logos(subpattern bracket_ident = r"⟨([^⟩\\]|\\[⟩\\])*⟩")]
#[logos(subpattern whitespace = r"[\u{0009}\u{000b}\u{0000c}\u{FEFF}\p{Space_Separator}\n\r\u{2028}\u{2029}]+")]
#[logos(subpattern multi_line_comment = r"/\*([^*]|\*[^/])*\*/")]
#[logos(subpattern line_comment = r"(//|#|--)[^\n\r\u{2028}\u{2029}]*")]
#[logos(skip(r"((?&whitespace)|(?&line_comment)|(?&multi_line_comment))+", whitespace_callback))]
pub enum BaseTokenKind {
	#[token("{")]
	OpenBrace,
	#[token("}")]
	CloseBrace,
	#[token("[")]
	OpenBracket,
	#[token("]")]
	CloseBracket,
	#[token("(")]
	OpenParen,
	#[token(")")]
	CloseParen,

	#[token(";")]
	SemiColon,
	#[token(",")]
	Comma,
	#[token("@")]
	At,
	#[token("/")]
	Slash,
	#[token("%")]
	Percent,

	#[token("||")]
	HLineHLine,
	#[token("|>")]
	HLineRightShevron,

	#[token("&&")]
	AndAnd,

	#[token(".")]
	Dot,
	#[token("..")]
	DotDot,
	#[token("...")]
	DotDotDot,

	#[token("!")]
	Exclaim,
	#[token("!=")]
	ExclaimEq,

	#[token("?")]
	Question,
	#[token("?=")]
	QuestionEqual,
	#[token("?:")]
	QuestionColon,

	#[token("<")]
	LeftShevron,
	#[token("<=")]
	LeftShevronEqual,
	#[token("<|")]
	LeftShevronHLine,

	#[token(">")]
	RightShevron,
	#[token(">=")]
	RightShevronEqual,

	#[token("-")]
	Dash,
	#[token("-=")]
	DashEqual,
	#[token("->")]
	DashRightShevron,

	#[token("+")]
	Plus,
	#[token("+=")]
	PlusEqual,
	#[token("+?=")]
	PlusQuestionEqual,

	#[token("*")]
	Star,
	#[token("*=")]
	StarEqual,
	#[token("**")]
	StarStar,

	#[token("=")]
	Equal,
	#[token("==")]
	EqualEqual,

	#[token(":")]
	Colon,
	#[token("::")]
	ColonColon,

	#[regex(r"(?i)ACCESS")]
	KwAccess,
	#[regex(r"(?i)AFTER")]
	KwAfter,
	#[regex(r"(?i)ALGORITHM")]
	KwAlgorithm,
	#[regex(r"(?i)ALL")]
	KwAll,
	#[regex(r"(?i)ALTER")]
	KwAlter,
	#[regex(r"(?i)ALWAYS")]
	KwAlways,
	#[regex(r"(?i)ANALYZE")]
	KwAnalyze,
	#[regex(r"(?i)ANALYZER")]
	KwAnalyzer,
	#[regex(r"(?i)API")]
	KwApi,
	#[regex(r"(?i)AS")]
	KwAs,
	#[regex(r"(?i)ASCENDING")]
	#[regex(r"(?i)ASC")]
	KwAscending,
	#[regex(r"(?i)ASCII")]
	KwAscii,
	#[regex(r"(?i)ASSERT")]
	KwAssert,
	#[regex(r"(?i)AT")]
	KwAt,
	#[regex(r"(?i)AUTHENTICATE")]
	KwAuthenticate,
	#[regex(r"(?i)AUTO")]
	KwAuto,
	#[regex(r"(?i)BACKEND")]
	KwBackend,
	#[regex(r"(?i)BATCH")]
	KwBatch,
	#[regex(r"(?i)BEARER")]
	KwBearer,
	#[regex(r"(?i)BEFORE")]
	KwBefore,
	#[regex(r"(?i)BEGIN")]
	KwBegin,
	#[regex(r"(?i)BLANK")]
	KwBlank,
	#[regex(r"(?i)BM25")]
	KwBm25,
	#[regex(r"(?i)BREAK")]
	KwBreak,
	#[regex(r"(?i)BUCKET")]
	KwBucket,
	#[regex(r"(?i)BY")]
	KwBy,
	#[regex(r"(?i)CAMEL")]
	KwCamel,
	#[regex(r"(?i)CANCEL")]
	KwCancel,
	#[regex(r"(?i)CASCADE")]
	KwCascade,
	#[regex(r"(?i)CHANGEFEED")]
	KwChangeFeed,
	#[regex(r"(?i)CHANGES")]
	KwChanges,
	#[regex(r"(?i)CAPACITY")]
	KwCapacity,
	#[regex(r"(?i)CLASS")]
	KwClass,
	#[regex(r"(?i)COMMENT")]
	KwComment,
	#[regex(r"(?i)COMMIT")]
	KwCommit,
	#[regex(r"(?i)CONCURRENTLY")]
	KwConcurrently,
	#[regex(r"(?i)CONFIG")]
	KwConfig,
	#[regex(r"(?i)CONTENT")]
	KwContent,
	#[regex(r"(?i)CONTINUE")]
	KwContinue,
	#[regex(r"(?i)COMPUTED")]
	KwComputed,
	#[regex(r"(?i)CREATE")]
	KwCreate,
	#[regex(r"(?i)DATABASE")]
	#[regex(r"(?i)DB")]
	KwDatabase,
	#[regex(r"(?i)DEFAULT")]
	KwDefault,
	#[regex(r"(?i)DEFINE")]
	KwDefine,
	#[regex(r"(?i)DELETE")]
	KwDelete,
	#[regex(r"(?i)DESCENDING")]
	#[regex(r"(?i)DESC")]
	KwDescending,
	#[regex(r"(?i)DIFF")]
	KwDiff,
	#[regex(r"(?i)DIMENSION")]
	KwDimension,
	#[regex(r"(?i)DISTANCE")]
	#[regex(r"(?i)DIST")]
	KwDistance,
	#[regex(r"(?i)DOC_IDS_CACHE")]
	KwDocIdsCache,
	#[regex(r"(?i)DOC_IDS_ORDER")]
	KwDocIdsOrder,
	#[regex(r"(?i)DOC_LENGTHS_CACHE")]
	KwDocLengthsCache,
	#[regex(r"(?i)DOC_LENGTHS_ORDER")]
	KwDocLengthsOrder,
	#[regex(r"(?i)DROP")]
	KwDrop,
	#[regex(r"(?i)DUPLICATE")]
	KwDuplicate,
	#[regex(r"(?i)EDGENGRAM")]
	KwEdgengram,
	#[regex(r"(?i)EFC")]
	KwEfc,
	#[regex(r"(?i)EVENT")]
	KwEvent,
	#[regex(r"(?i)ELSE")]
	KwElse,
	#[regex(r"(?i)END")]
	KwEnd,
	#[regex(r"(?i)ENFORCED")]
	KwEnforced,
	#[regex(r"(?i)EXCLUDE")]
	KwExclude,
	#[regex(r"(?i)EXISTS")]
	KwExists,
	#[regex(r"(?i)EXPIRED")]
	KwExpired,
	#[regex(r"(?i)EXPLAIN")]
	KwExplain,
	#[regex(r"(?i)EXPUNGE")]
	KwExpunge,
	#[regex(r"(?i)EXTEND_CANDIDATES")]
	KwExtendCandidates,
	#[regex(r"(?i)false")]
	KwFalse,
	#[regex(r"(?i)FETCH")]
	KwFetch,
	#[regex(r"(?i)FIELD")]
	KwField,
	#[regex(r"(?i)FIELDS")]
	#[regex(r"(?i)COLUMNS")]
	KwFields,
	#[regex(r"(?i)FILTERS")]
	KwFilters,
	#[regex(r"(?i)FLEXIBLE")]
	#[regex(r"(?i)FLEXI")]
	#[regex(r"(?i)FLEX")]
	KwFlexible,
	#[regex(r"(?i)FOR")]
	KwFor,
	#[regex(r"(?i)FROM")]
	KwFrom,
	#[regex(r"(?i)FULL")]
	KwFull,
	#[regex(r"(?i)FULLTEXT")]
	KwFulltext,
	#[regex(r"(?i)FUNCTION")]
	KwFunction,
	#[regex(r"(?i)FUNCTIONS")]
	KwFunctions,
	#[regex(r"(?i)GRANT")]
	KwGrant,
	#[regex(r"(?i)GRAPHQL")]
	KwGraphql,
	#[regex(r"(?i)GROUP")]
	KwGroup,
	#[regex(r"(?i)HEADERS")]
	KwHeaders,
	#[regex(r"(?i)HIGHLIGHTS")]
	KwHighlights,
	#[regex(r"(?i)HNSW")]
	KwHnsw,
	#[regex(r"(?i)IGNORE")]
	KwIgnore,
	#[regex(r"(?i)INCLUDE")]
	KwInclude,
	#[regex(r"(?i)INDEX")]
	KwIndex,
	#[regex(r"(?i)INFO")]
	KwInfo,
	#[regex(r"(?i)INSERT")]
	KwInsert,
	#[regex(r"(?i)INTO")]
	KwInto,
	#[regex(r"(?i)IF")]
	KwIf,
	#[regex(r"(?i)IS")]
	KwIs,
	#[regex(r"(?i)ISSUER")]
	KwIssuer,
	#[regex(r"(?i)JWT")]
	KwJwt,
	#[regex(r"(?i)JWKS")]
	KwJwks,
	#[regex(r"(?i)KEY")]
	KwKey,
	#[regex(r"(?i)KEEP_PRUNED_CONNECTIONS")]
	KwKeepPrunedConnections,
	#[regex(r"(?i)KILL")]
	KwKill,
	#[regex(r"(?i)LET")]
	KwLet,
	#[regex(r"(?i)LIMIT")]
	KwLimit,
	#[regex(r"(?i)LIVE")]
	KwLive,
	#[regex(r"(?i)LOWERCASE")]
	KwLowercase,
	#[regex(r"(?i)LM")]
	KwLm,
	#[regex(r"(?i)M", priority = 3)]
	KwM,
	#[regex(r"(?i)M0")]
	KwM0,
	#[regex(r"(?i)MAPPER")]
	KwMapper,
	#[regex(r"(?i)MIDDLEWARE")]
	KwMiddleware,
	#[regex(r"(?i)ML")]
	KwML,
	#[regex(r"(?i)MERGE")]
	KwMerge,
	#[regex(r"(?i)MODEL")]
	KwModel,
	#[regex(r"(?i)MTREE")]
	KwMTree,
	#[regex(r"(?i)MTREE_CACHE")]
	KwMTreeCache,
	#[regex(r"(?i)NAMESPACE")]
	#[regex(r"(?i)NS")]
	KwNamespace,
	#[regex(r"(?i)NGRAM")]
	KwNgram,
	#[regex(r"(?i)NO")]
	KwNo,
	#[regex(r"(?i)NOINDEX")]
	KwNoIndex,
	#[regex(r"(?i)NONE")]
	KwNone,
	#[regex(r"(?i)NULL")]
	KwNull,
	#[regex(r"(?i)NUMERIC")]
	KwNumeric,
	#[regex(r"(?i)OMIT")]
	KwOmit,
	#[regex(r"(?i)ON")]
	KwOn,
	#[regex(r"(?i)ONLY")]
	KwOnly,
	#[regex(r"(?i)OPTION")]
	KwOption,
	#[regex(r"(?i)ORDER")]
	KwOrder,
	#[regex(r"(?i)ORIGINAL")]
	KwOriginal,
	#[regex(r"(?i)OVERWRITE")]
	KwOverwrite,
	#[regex(r"(?i)PARALLEL")]
	KwParallel,
	#[regex(r"(?i)PARAM")]
	KwKwParam,
	#[regex(r"(?i)PASSHASH")]
	KwPasshash,
	#[regex(r"(?i)PASSWORD")]
	KwPassword,
	#[regex(r"(?i)PATCH")]
	KwPatch,
	#[regex(r"(?i)PERMISSIONS")]
	KwPermissions,
	#[regex(r"(?i)POSTINGS_CACHE")]
	KwPostingsCache,
	#[regex(r"(?i)POSTINGS_ORDER")]
	KwPostingsOrder,
	#[regex(r"(?i)PUNCT")]
	KwPunct,
	#[regex(r"(?i)PURGE")]
	KwPurge,
	#[regex(r"(?i)RANGE")]
	KwRange,
	#[regex(r"(?i)READONLY")]
	KwReadonly,
	#[regex(r"(?i)REJECT")]
	KwReject,
	#[regex(r"(?i)RELATE")]
	KwRelate,
	#[regex(r"(?i)RELATION")]
	KwRelation,
	#[regex(r"(?i)REBUILD")]
	KwRebuild,
	#[regex(r"(?i)REFERENCE")]
	KwReference,
	#[regex(r"(?i)REFRESH")]
	KwRefresh,
	#[regex(r"(?i)REMOVE")]
	KwRemove,
	#[regex(r"(?i)REPLACE")]
	KwReplace,
	#[regex(r"(?i)RETURN")]
	KwReturn,
	#[regex(r"(?i)REVOKE")]
	KwRevoke,
	#[regex(r"(?i)REVOKED")]
	KwRevoked,
	#[regex(r"(?i)ROLES")]
	KwRoles,
	#[regex(r"(?i)ROOT")]
	#[regex(r"(?i)KV")]
	KwRoot,
	#[regex(r"(?i)SCHEMAFULL")]
	#[regex(r"(?i)SCHEMAFUL")]
	KwSchemafull,
	#[regex(r"(?i)SCHEMALESS")]
	KwSchemaless,
	#[regex(r"(?i)SCOPE")]
	#[regex(r"(?i)SC")]
	KwScope,
	#[regex(r"(?i)SEARCH")]
	KwSearch,
	#[regex(r"(?i)SELECT")]
	KwSelect,
	#[regex(r"(?i)SEQUENCE")]
	KwSequence,
	#[regex(r"(?i)SESSION")]
	KwSession,
	#[regex(r"(?i)SET")]
	KwSet,
	#[regex(r"(?i)SHOW")]
	KwShow,
	#[regex(r"(?i)SIGNIN")]
	KwSignin,
	#[regex(r"(?i)SIGNUP")]
	KwSignup,
	#[regex(r"(?i)SINCE")]
	KwSince,
	#[regex(r"(?i)SLEEP")]
	KwSleep,
	#[regex(r"(?i)SNOWBALL")]
	KwSnowball,
	#[regex(r"(?i)SPLIT")]
	KwSplit,
	#[regex(r"(?i)START")]
	KwStart,
	#[regex(r"(?i)STRUCTURE")]
	KwStructure,
	#[regex(r"(?i)TABLE")]
	#[regex(r"(?i)TB")]
	KwTable,
	#[regex(r"(?i)TABLES")]
	KwTables,
	#[regex(r"(?i)TEMPFILES")]
	KwTempFiles,
	#[regex(r"(?i)TERMS_CACHE")]
	KwTermsCache,
	#[regex(r"(?i)TERMS_ORDER")]
	KwTermsOrder,
	#[regex(r"(?i)THEN")]
	KwThen,
	#[regex(r"(?i)THROW")]
	KwThrow,
	#[regex(r"(?i)TIMEOUT")]
	KwTimeout,
	#[regex(r"(?i)TO")]
	KwTo,
	#[regex(r"(?i)TOKENIZERS")]
	KwTokenizers,
	#[regex(r"(?i)TOKEN")]
	KwToken,
	#[regex(r"(?i)TRANSACTION")]
	KwTransaction,
	#[regex(r"(?i)true")]
	KwTrue,
	#[regex(r"(?i)TYPE")]
	KwType,
	#[regex(r"(?i)UNIQUE")]
	KwUnique,
	#[regex(r"(?i)UNSET")]
	KwUnset,
	#[regex(r"(?i)UPDATE")]
	KwUpdate,
	#[regex(r"(?i)UPSERT")]
	KwUpsert,
	#[regex(r"(?i)UPPERCASE")]
	KwUppercase,
	#[regex(r"(?i)URL")]
	KwUrl,
	#[regex(r"(?i)USE")]
	KwUse,
	#[regex(r"(?i)USER")]
	KwUser,
	#[regex(r"(?i)VALUE")]
	KwValue,
	#[regex(r"(?i)VALUES")]
	KwValues,
	#[regex(r"(?i)VERSION")]
	KwVersion,
	#[regex(r"(?i)VS")]
	KwVs,
	#[regex(r"(?i)WHEN")]
	KwWhen,
	#[regex(r"(?i)WHERE")]
	KwWhere,
	#[regex(r"(?i)WITH")]
	KwWith,
	#[regex(r"(?i)ALLINSIDE")]
	KwAllInside,
	#[regex(r"(?i)ANDKW")]
	KwAndKw,
	#[regex(r"(?i)ANYINSIDE")]
	KwAnyInside,
	#[regex(r"(?i)INSIDE")]
	KwInside,
	#[regex(r"(?i)INTERSECTS")]
	KwIntersects,
	#[regex(r"(?i)NONEINSIDE")]
	KwNoneInside,
	#[regex(r"(?i)NOTINSIDE")]
	KwNotInside,
	#[regex(r"(?i)OR")]
	KwOrKw,
	#[regex(r"(?i)OUTSIDE")]
	KwOutside,
	#[regex(r"(?i)NOT")]
	KwNot,
	#[regex(r"(?i)AND")]
	KwAnd,
	#[regex(r"(?i)COLLATE")]
	KwCollate,
	#[regex(r"(?i)CONTAINSALL")]
	KwContainsAll,
	#[regex(r"(?i)CONTAINSANY")]
	KwContainsAny,
	#[regex(r"(?i)CONTAINSNONE")]
	KwContainsNone,
	#[regex(r"(?i)CONTAINSNOT")]
	KwContainsNot,
	#[regex(r"(?i)CONTAINS")]
	KwContains,
	#[regex(r"(?i)IN")]
	KwIn,
	#[regex(r"(?i)OUT")]
	KwOut,
	#[regex(r"(?i)NORMAL")]
	KwNormal,

	// Types
	#[regex(r"(?i)ANY")]
	KwAny,
	#[regex(r"(?i)ARRAY")]
	KwArray,
	#[regex(r"(?i)GEOMETRY")]
	KwGeometry,
	#[regex(r"(?i)RECORD")]
	KwRecord,
	#[regex(r"(?i)BOOL")]
	KwBool,
	#[regex(r"(?i)BYTES")]
	KwBytes,
	#[regex(r"(?i)DATETIME")]
	KwDatetime,
	#[regex(r"(?i)DECIMAL")]
	KwDecimal,
	#[regex(r"(?i)DURATION")]
	KwDuration,
	#[regex(r"(?i)FLOAT")]
	KwFloat,
	#[regex(r"(?i)fn")]
	KwFn,
	#[regex(r"(?i)INT")]
	KwInt,
	#[regex(r"(?i)NUMBER")]
	KwNumber,
	#[regex(r"(?i)OBJECT")]
	KwObject,
	#[regex(r"(?i)REGEX")]
	KwRegex,
	#[regex(r"(?i)STRING")]
	KwString,
	#[regex(r"(?i)UUID")]
	KwUuid,
	#[regex(r"(?i)ULID")]
	KwUlid,
	#[regex(r"(?i)RAND")]
	KwRand,
	#[regex(r"(?i)REFERENCES")]
	KwReferences,
	#[regex(r"(?i)FEATURE")]
	KwFeature,
	#[regex(r"(?i)LINE")]
	KwLine,
	#[regex(r"(?i)POINT")]
	KwPoint,
	#[regex(r"(?i)POLYGON")]
	KwPolygon,
	#[regex(r"(?i)MULTIPOINT")]
	KwMultiPoint,
	#[regex(r"(?i)MULTILINE")]
	KwMultiLine,
	#[regex(r"(?i)MULTIPOLYGON")]
	KwMultiPolygon,
	#[regex(r"(?i)COLLECTION")]
	KwCollection,
	#[regex(r"(?i)FILE")]
	KwFile,

	// Languages
	#[regex(r"(?i)ARABIC")]
	#[regex(r"(?i)ARA")]
	#[regex(r"(?i)AR")]
	KwArabic,
	#[regex(r"(?i)DANISH")]
	#[regex(r"(?i)DAN")]
	#[regex(r"(?i)DA")]
	KwDanish,
	#[regex(r"(?i)DUTCH")]
	#[regex(r"(?i)NLD")]
	#[regex(r"(?i)NL")]
	KwDutch,
	#[regex(r"(?i)ENGLISH")]
	#[regex(r"(?i)ENG")]
	#[regex(r"(?i)EN")]
	KwEnglish,
	#[regex(r"(?i)FINNISH")]
	#[regex(r"(?i)FIN")]
	#[regex(r"(?i)FI")]
	KwFinnish,
	#[regex(r"(?i)FRENCH")]
	#[regex(r"(?i)FRA")]
	#[regex(r"(?i)FR")]
	KwFrench,
	#[regex(r"(?i)GERMAN")]
	#[regex(r"(?i)DEU")]
	#[regex(r"(?i)DE")]
	KwGerman,
	#[regex(r"(?i)GREEK")]
	#[regex(r"(?i)ELL")]
	#[regex(r"(?i)EL")]
	KwGreek,
	#[regex(r"(?i)HUNGARIAN")]
	#[regex(r"(?i)HUN")]
	#[regex(r"(?i)HU")]
	KwHungarian,
	#[regex(r"(?i)ITALIAN")]
	#[regex(r"(?i)ITA")]
	#[regex(r"(?i)IT")]
	KwItalian,
	#[regex(r"(?i)NORWEGIAN")]
	#[regex(r"(?i)NOR")]
	KwNorwegian,
	#[regex(r"(?i)PORTUGUESE")]
	#[regex(r"(?i)POR")]
	#[regex(r"(?i)PT")]
	KwPortuguese,
	#[regex(r"(?i)ROMANIAN")]
	#[regex(r"(?i)RON")]
	#[regex(r"(?i)RO")]
	KwRomanian,
	#[regex(r"(?i)RUSSIAN")]
	#[regex(r"(?i)RUS")]
	#[regex(r"(?i)RU")]
	KwRussian,
	#[regex(r"(?i)SPANISH")]
	#[regex(r"(?i)SPA")]
	#[regex(r"(?i)ES")]
	KwSpanish,
	#[regex(r"(?i)SWEDISH")]
	#[regex(r"(?i)SWE")]
	#[regex(r"(?i)SV")]
	KwSwedish,
	#[regex(r"(?i)TAMIL")]
	#[regex(r"(?i)TAM")]
	#[regex(r"(?i)TA")]
	KwTamil,
	#[regex(r"(?i)TURKISH")]
	#[regex(r"(?i)TUR")]
	#[regex(r"(?i)TR")]
	KwTurkish,

	// Algorithms
	#[regex(r"(?i)EDDSA")]
	KwEdDSA,
	#[regex(r"(?i)ES256")]
	KwEs256,
	#[regex(r"(?i)ES384")]
	KwEs384,
	#[regex(r"(?i)ES512")]
	KwEs512,
	#[regex(r"(?i)HS256")]
	KwHs256,
	#[regex(r"(?i)HS384")]
	KwHs384,
	#[regex(r"(?i)HS512")]
	KwHs512,
	#[regex(r"(?i)PS256")]
	KwPs256,
	#[regex(r"(?i)PS384")]
	KwPs384,
	#[regex(r"(?i)PS512")]
	KwPs512,
	#[regex(r"(?i)RS256")]
	KwRs256,
	#[regex(r"(?i)RS384")]
	KwRs384,
	#[regex(r"(?i)RS512")]
	KwRs512,

	// Distance
	#[regex(r"(?i)CHEBYSHEV")]
	KwChebyshev,
	#[regex(r"(?i)COSINE")]
	KwCosine,
	#[regex(r"(?i)EUCLIDEAN")]
	KwEuclidean,
	#[regex(r"(?i)JACCARD")]
	KwJaccard,
	#[regex(r"(?i)HAMMING")]
	KwHamming,
	#[regex(r"(?i)MANHATTAN")]
	KwManhattan,
	#[regex(r"(?i)MINKOWSKI")]
	KwMinkowski,
	#[regex(r"(?i)PEARSON")]
	KwPearson,

	// VectorTypes
	#[regex(r"(?i)F64")]
	KwF64,
	#[regex(r"(?i)F32")]
	KwF32,
	#[regex(r"(?i)I64")]
	KwI64,
	#[regex(r"(?i)I32")]
	KwI32,
	#[regex(r"(?i)I16")]
	KwI16,

	// HTTP methods
	#[regex(r"(?i)GET")]
	KwGet,
	#[regex(r"(?i)POST")]
	KwPost,
	#[regex(r"(?i)PUT")]
	KwPut,
	#[regex(r"(?i)TRACE")]
	KwTrace,

	#[regex(r#""([^"\\]|\\.)*""#)]
	#[regex(r#"'([^'\\]|\\.)*'"#)]
	String,
	#[regex(r#"r"([^"\\]|\\")*""#)]
	#[regex(r#"r'([^'\\]|\\')*'"#)]
	RecordIdString,
	#[regex(r#"u"([^"\\]|\\")*""#)]
	#[regex(r#"u'([^'\\]|\\')*'"#)]
	UuidString,
	#[regex(r#"d"([^"\\]|\\")*""#)]
	#[regex(r#"d'([^'\\]|\\')*'"#)]
	DateTimeString,

	#[regex(r"\$(?&backtick_ident)")]
	#[regex(r"\$(?&bracket_ident)")]
	#[regex(r"\$\p{XID_Continue}*")]
	Param,
	#[regex(r"(?&backtick_ident)")]
	#[regex(r"(?&bracket_ident)")]
	#[regex(r"\p{XID_Start}\p{XID_Continue}*")]
	Ident,

	#[regex(r"[0-9]+f")]
	#[regex(r"[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?(f)?")]
	#[regex(r"NaN")]
	#[regex(r"[+-]?Infinity")]
	Float,
	#[regex(r"[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?dec")]
	Decimal,
	#[regex(r"[0-9]+", priority = 3)]
	Int,
}

impl BaseTokenKind {
	pub fn as_str(&self) -> &'static str {
		todo!()
	}
}

impl fmt::Display for BaseTokenKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

#[macro_export]
macro_rules! t {
	(;) => {
		crate::lex::BaseTokenKind::SemiColon
	};
	(,) => {
		crate::lex::BaseTokenKind::Comma
	};
	(@) => {
		crate::lex::BaseTokenKind::At
	};
	(/) => {
		crate::lex::BaseTokenKind::Slash
	};
	(%) => {
		crate::lex::BaseTokenKind::Percent
	};

	(||) => {
		crate::lex::BaseTokenKind::HLineHLine
	};
	(|>) => {
		crate::lex::BaseTokenKind::HLineRightShevron
	};

	(&&) => {
		crate::lex::BaseTokenKind::AndAnd
	};

	(.) => {
		crate::lex::BaseTokenKind::Dot
	};
	(..) => {
		crate::lex::BaseTokenKind::DotDot
	};
	(...) => {
		crate::lex::BaseTokenKind::DotDotDot
	};

	(!) => {
		crate::lex::BaseTokenKind::Exclaim
	};
	(!=) => {
		crate::lex::BaseTokenKind::ExclaimEq
	};

	(?) => {
		crate::lex::BaseTokenKind::Question
	};
	(?=) => {
		crate::lex::BaseTokenKind::QuestionEqual
	};
	(?:) => {
		crate::lex::BaseTokenKind::QuestionColon
	};

	(<) => {
		crate::lex::BaseTokenKind::LeftShevron
	};
	(<=) => {
		crate::lex::BaseTokenKind::LeftShevronEqual
	};
	(<|) => {
		crate::lex::BaseTokenKind::LeftShevronHLine
	};

	(>) => {
		crate::lex::BaseTokenKind::RightShevron
	};
	(>=) => {
		crate::lex::BaseTokenKind::RightShevronEqual
	};

	(-) => {
		crate::lex::BaseTokenKind::Dash
	};
	(-=) => {
		crate::lex::BaseTokenKind::DashEqual
	};
	(->) => {
		crate::lex::BaseTokenKind::DashRightShevron
	};

	(+) => {
		crate::lex::BaseTokenKind::Plus
	};
	(+=) => {
		crate::lex::BaseTokenKind::PlusEqual
	};
	(+?=) => {
		crate::lex::BaseTokenKind::PlusQuestionEqual
	};

	(*) => {
		crate::lex::BaseTokenKind::Star
	};
	(*=) => {
		crate::lex::BaseTokenKind::StarEqual
	};
	(**) => {
		crate::lex::BaseTokenKind::StarStar
	};

	(=) => {
		crate::lex::BaseTokenKind::Equal
	};
	(==) => {
		crate::lex::BaseTokenKind::EqualEqual
	};

	(:) => {
		crate::lex::BaseTokenKind::Colon
	};
	(::) => {
		crate::lex::BaseTokenKind::ColonColon
	};

	(ACCESS) => {
		crate::lex::BaseTokenKind::KwAccess
	};
	(AFTER) => {
		crate::lex::BaseTokenKind::KwAfter
	};
	(ALGORITHM) => {
		crate::lex::BaseTokenKind::KwAlgorithm
	};
	(ALL) => {
		crate::lex::BaseTokenKind::KwAll
	};
	(ALTER) => {
		crate::lex::BaseTokenKind::KwAlter
	};
	(ALWAYS) => {
		crate::lex::BaseTokenKind::KwAlways
	};
	(ANALYZE) => {
		crate::lex::BaseTokenKind::KwAnalyze
	};
	(ANALYZER) => {
		crate::lex::BaseTokenKind::KwAnalyzer
	};
	(API) => {
		crate::lex::BaseTokenKind::KwApi
	};
	(AS) => {
		crate::lex::BaseTokenKind::KwAs
	};
	(ASCENDING) => {
		crate::lex::BaseTokenKind::KwAscending
	};
	(ASCII) => {
		crate::lex::BaseTokenKind::KwAscii
	};
	(ASSERT) => {
		crate::lex::BaseTokenKind::KwAssert
	};
	(AT) => {
		crate::lex::BaseTokenKind::KwAt
	};
	(AUTHENTICATE) => {
		crate::lex::BaseTokenKind::KwAuthenticate
	};
	(AUTO) => {
		crate::lex::BaseTokenKind::KwAuto
	};
	(BACKEND) => {
		crate::lex::BaseTokenKind::KwBackend
	};
	(BATCH) => {
		crate::lex::BaseTokenKind::KwBatch
	};
	(BEARER) => {
		crate::lex::BaseTokenKind::KwBearer
	};
	(BEFORE) => {
		crate::lex::BaseTokenKind::KwBefore
	};
	(BEGIN) => {
		crate::lex::BaseTokenKind::KwBegin
	};
	(BLANK) => {
		crate::lex::BaseTokenKind::KwBlank
	};
	(BM25) => {
		crate::lex::BaseTokenKind::KwBm25
	};
	(BREAK) => {
		crate::lex::BaseTokenKind::KwBreak
	};
	(BUCKET) => {
		crate::lex::BaseTokenKind::KwBucket
	};
	(BY) => {
		crate::lex::BaseTokenKind::KwBy
	};
	(CAMEL) => {
		crate::lex::BaseTokenKind::KwCamel
	};
	(CANCEL) => {
		crate::lex::BaseTokenKind::KwCancel
	};
	(CASCADE) => {
		crate::lex::BaseTokenKind::KwCascade
	};
	(CHANGEFEED) => {
		crate::lex::BaseTokenKind::KwChangeFeed
	};
	(CHANGES) => {
		crate::lex::BaseTokenKind::KwChanges
	};
	(CAPACITY) => {
		crate::lex::BaseTokenKind::KwCapacity
	};
	(CLASS) => {
		crate::lex::BaseTokenKind::KwClass
	};
	(COMMENT) => {
		crate::lex::BaseTokenKind::KwComment
	};
	(COMMIT) => {
		crate::lex::BaseTokenKind::KwCommit
	};
	(CONCURRENTLY) => {
		crate::lex::BaseTokenKind::KwConcurrently
	};
	(CONFIG) => {
		crate::lex::BaseTokenKind::KwConfig
	};
	(CONTENT) => {
		crate::lex::BaseTokenKind::KwContent
	};
	(CONTINUE) => {
		crate::lex::BaseTokenKind::KwContinue
	};
	(COMPUTED) => {
		crate::lex::BaseTokenKind::KwComputed
	};
	(CREATE) => {
		crate::lex::BaseTokenKind::KwCreate
	};
	(DATABASE) => {
		crate::lex::BaseTokenKind::KwDatabase
	};
	(DEFAULT) => {
		crate::lex::BaseTokenKind::KwDefault
	};
	(DEFINE) => {
		crate::lex::BaseTokenKind::KwDefine
	};
	(DELETE) => {
		crate::lex::BaseTokenKind::KwDelete
	};
	(DESCENDING) => {
		crate::lex::BaseTokenKind::KwDescending
	};
	(DIFF) => {
		crate::lex::BaseTokenKind::KwDiff
	};
	(DIMENSION) => {
		crate::lex::BaseTokenKind::KwDimension
	};
	(DISTANCE) => {
		crate::lex::BaseTokenKind::KwDistance
	};
	(DOC_IDS_CACHE) => {
		crate::lex::BaseTokenKind::KwDocIdsCache
	};
	(DOC_IDS_ORDER) => {
		crate::lex::BaseTokenKind::KwDocIdsOrder
	};
	(DOC_LENGTHS_CACHE) => {
		crate::lex::BaseTokenKind::KwDocLengthsCache
	};
	(DOC_LENGTHS_ORDER) => {
		crate::lex::BaseTokenKind::KwDocLengthsOrder
	};
	(DROP) => {
		crate::lex::BaseTokenKind::KwDrop
	};
	(DUPLICATE) => {
		crate::lex::BaseTokenKind::KwDuplicate
	};
	(EDGENGRAM) => {
		crate::lex::BaseTokenKind::KwEdgengram
	};
	(EFC) => {
		crate::lex::BaseTokenKind::KwEfc
	};
	(EVENT) => {
		crate::lex::BaseTokenKind::KwEvent
	};
	(ELSE) => {
		crate::lex::BaseTokenKind::KwElse
	};
	(END) => {
		crate::lex::BaseTokenKind::KwEnd
	};
	(ENFORCED) => {
		crate::lex::BaseTokenKind::KwEnforced
	};
	(EXCLUDE) => {
		crate::lex::BaseTokenKind::KwExclude
	};
	(EXISTS) => {
		crate::lex::BaseTokenKind::KwExists
	};
	(EXPIRED) => {
		crate::lex::BaseTokenKind::KwExpired
	};
	(EXPLAIN) => {
		crate::lex::BaseTokenKind::KwExplain
	};
	(EXPUNGE) => {
		crate::lex::BaseTokenKind::KwExpunge
	};
	(EXTEND_CANDIDATES) => {
		crate::lex::BaseTokenKind::KwExtendCandidates
	};
	(false) => {
		crate::lex::BaseTokenKind::KwFalse
	};
	(FETCH) => {
		crate::lex::BaseTokenKind::KwFetch
	};
	(FIELD) => {
		crate::lex::BaseTokenKind::KwField
	};
	(FIELDS) => {
		crate::lex::BaseTokenKind::KwFields
	};
	(FILTERS) => {
		crate::lex::BaseTokenKind::KwFilters
	};
	(FLEXIBLE) => {
		crate::lex::BaseTokenKind::KwFlexible
	};
	(FOR) => {
		crate::lex::BaseTokenKind::KwFor
	};
	(FROM) => {
		crate::lex::BaseTokenKind::KwFrom
	};
	(FULL) => {
		crate::lex::BaseTokenKind::KwFull
	};
	(FULLTEXT) => {
		crate::lex::BaseTokenKind::KwFulltext
	};
	(FUNCTION) => {
		crate::lex::BaseTokenKind::KwFunction
	};
	(FUNCTIONS) => {
		crate::lex::BaseTokenKind::KwFunctions
	};
	(GRANT) => {
		crate::lex::BaseTokenKind::KwGrant
	};
	(GRAPHQL) => {
		crate::lex::BaseTokenKind::KwGraphql
	};
	(GROUP) => {
		crate::lex::BaseTokenKind::KwGroup
	};
	(HEADERS) => {
		crate::lex::BaseTokenKind::KwHeaders
	};
	(HIGHLIGHTS) => {
		crate::lex::BaseTokenKind::KwHighlights
	};
	(HNSW) => {
		crate::lex::BaseTokenKind::KwHnsw
	};
	(IGNORE) => {
		crate::lex::BaseTokenKind::KwIgnore
	};
	(INCLUDE) => {
		crate::lex::BaseTokenKind::KwInclude
	};
	(INDEX) => {
		crate::lex::BaseTokenKind::KwIndex
	};
	(INFO) => {
		crate::lex::BaseTokenKind::KwInfo
	};
	(INSERT) => {
		crate::lex::BaseTokenKind::KwInsert
	};
	(INTO) => {
		crate::lex::BaseTokenKind::KwInto
	};
	(IF) => {
		crate::lex::BaseTokenKind::KwIf
	};
	(IS) => {
		crate::lex::BaseTokenKind::KwIs
	};
	(ISSUER) => {
		crate::lex::BaseTokenKind::KwIssuer
	};
	(JWT) => {
		crate::lex::BaseTokenKind::KwJwt
	};
	(JWKS) => {
		crate::lex::BaseTokenKind::KwJwks
	};
	(KEY) => {
		crate::lex::BaseTokenKind::KwKey
	};
	(KEEP_PRUNED_CONNECTIONS) => {
		crate::lex::BaseTokenKind::KwKeepPrunedConnections
	};
	(KILL) => {
		crate::lex::BaseTokenKind::KwKill
	};
	(LET) => {
		crate::lex::BaseTokenKind::KwLet
	};
	(LIMIT) => {
		crate::lex::BaseTokenKind::KwLimit
	};
	(LIVE) => {
		crate::lex::BaseTokenKind::KwLive
	};
	(LOWERCASE) => {
		crate::lex::BaseTokenKind::KwLowercase
	};
	(LM) => {
		crate::lex::BaseTokenKind::KwLm
	};
	(M) => {
		crate::lex::BaseTokenKind::KwM
	};
	(M0) => {
		crate::lex::BaseTokenKind::KwM0
	};
	(MAPPER) => {
		crate::lex::BaseTokenKind::KwMapper
	};
	(MIDDLEWARE) => {
		crate::lex::BaseTokenKind::KwMiddleware
	};
	(ML) => {
		crate::lex::BaseTokenKind::KwML
	};
	(MERGE) => {
		crate::lex::BaseTokenKind::KwMerge
	};
	(MODEL) => {
		crate::lex::BaseTokenKind::KwModel
	};
	(MTREE) => {
		crate::lex::BaseTokenKind::KwMTree
	};
	(MTREE_CACHE) => {
		crate::lex::BaseTokenKind::KwMTreeCache
	};
	(NAMESPACE) => {
		crate::lex::BaseTokenKind::KwNamespace
	};
	(NGRAM) => {
		crate::lex::BaseTokenKind::KwNgram
	};
	(NO) => {
		crate::lex::BaseTokenKind::KwNo
	};
	(NOINDEX) => {
		crate::lex::BaseTokenKind::KwNoIndex
	};
	(NONE) => {
		crate::lex::BaseTokenKind::KwNone
	};
	(NULL) => {
		crate::lex::BaseTokenKind::KwNull
	};
	(NUMERIC) => {
		crate::lex::BaseTokenKind::KwNumeric
	};
	(OMIT) => {
		crate::lex::BaseTokenKind::KwOmit
	};
	(ON) => {
		crate::lex::BaseTokenKind::KwOn
	};
	(ONLY) => {
		crate::lex::BaseTokenKind::KwOnly
	};
	(OPTION) => {
		crate::lex::BaseTokenKind::KwOption
	};
	(ORDER) => {
		crate::lex::BaseTokenKind::KwOrder
	};
	(ORIGINAL) => {
		crate::lex::BaseTokenKind::KwOriginal
	};
	(OVERWRITE) => {
		crate::lex::BaseTokenKind::KwOverwrite
	};
	(PARALLEL) => {
		crate::lex::BaseTokenKind::KwParallel
	};
	(PARAM) => {
		crate::lex::BaseTokenKind::KwKwParam
	};
	(PASSHASH) => {
		crate::lex::BaseTokenKind::KwPasshash
	};
	(PASSWORD) => {
		crate::lex::BaseTokenKind::KwPassword
	};
	(PATCH) => {
		crate::lex::BaseTokenKind::KwPatch
	};
	(PERMISSIONS) => {
		crate::lex::BaseTokenKind::KwPermissions
	};
	(POSTINGS_CACHE) => {
		crate::lex::BaseTokenKind::KwPostingsCache
	};
	(POSTINGS_ORDER) => {
		crate::lex::BaseTokenKind::KwPostingsOrder
	};
	(PUNCT) => {
		crate::lex::BaseTokenKind::KwPunct
	};
	(PURGE) => {
		crate::lex::BaseTokenKind::KwPurge
	};
	(RANGE) => {
		crate::lex::BaseTokenKind::KwRange
	};
	(READONLY) => {
		crate::lex::BaseTokenKind::KwReadonly
	};
	(REJECT) => {
		crate::lex::BaseTokenKind::KwReject
	};
	(RELATE) => {
		crate::lex::BaseTokenKind::KwRelate
	};
	(RELATION) => {
		crate::lex::BaseTokenKind::KwRelation
	};
	(REBUILD) => {
		crate::lex::BaseTokenKind::KwRebuild
	};
	(REFERENCE) => {
		crate::lex::BaseTokenKind::KwReference
	};
	(REFRESH) => {
		crate::lex::BaseTokenKind::KwRefresh
	};
	(REMOVE) => {
		crate::lex::BaseTokenKind::KwRemove
	};
	(REPLACE) => {
		crate::lex::BaseTokenKind::KwReplace
	};
	(RETURN) => {
		crate::lex::BaseTokenKind::KwReturn
	};
	(REVOKE) => {
		crate::lex::BaseTokenKind::KwRevoke
	};
	(REVOKED) => {
		crate::lex::BaseTokenKind::KwRevoked
	};
	(ROLES) => {
		crate::lex::BaseTokenKind::KwRoles
	};
	(ROOT) => {
		crate::lex::BaseTokenKind::KwRoot
	};
	(SCHEMAFULL) => {
		crate::lex::BaseTokenKind::KwSchemafull
	};
	(SCHEMALESS) => {
		crate::lex::BaseTokenKind::KwSchemaless
	};
	(SCOPE) => {
		crate::lex::BaseTokenKind::KwScope
	};
	(SEARCH) => {
		crate::lex::BaseTokenKind::KwSearch
	};
	(SELECT) => {
		crate::lex::BaseTokenKind::KwSelect
	};
	(SEQUENCE) => {
		crate::lex::BaseTokenKind::KwSequence
	};
	(SESSION) => {
		crate::lex::BaseTokenKind::KwSession
	};
	(SET) => {
		crate::lex::BaseTokenKind::KwSet
	};
	(SHOW) => {
		crate::lex::BaseTokenKind::KwShow
	};
	(SIGNIN) => {
		crate::lex::BaseTokenKind::KwSignin
	};
	(SIGNUP) => {
		crate::lex::BaseTokenKind::KwSignup
	};
	(SINCE) => {
		crate::lex::BaseTokenKind::KwSince
	};
	(SLEEP) => {
		crate::lex::BaseTokenKind::KwSleep
	};
	(SNOWBALL) => {
		crate::lex::BaseTokenKind::KwSnowball
	};
	(SPLIT) => {
		crate::lex::BaseTokenKind::KwSplit
	};
	(START) => {
		crate::lex::BaseTokenKind::KwStart
	};
	(STRUCTURE) => {
		crate::lex::BaseTokenKind::KwStructure
	};
	(TABLE) => {
		crate::lex::BaseTokenKind::KwTable
	};
	(TABLES) => {
		crate::lex::BaseTokenKind::KwTables
	};
	(TEMPFILES) => {
		crate::lex::BaseTokenKind::KwTempFiles
	};
	(TERMS_CACHE) => {
		crate::lex::BaseTokenKind::KwTermsCache
	};
	(TERMS_ORDER) => {
		crate::lex::BaseTokenKind::KwTermsOrder
	};
	(THEN) => {
		crate::lex::BaseTokenKind::KwThen
	};
	(THROW) => {
		crate::lex::BaseTokenKind::KwThrow
	};
	(TIMEOUT) => {
		crate::lex::BaseTokenKind::KwTimeout
	};
	(TO) => {
		crate::lex::BaseTokenKind::KwTo
	};
	(TOKENIZERS) => {
		crate::lex::BaseTokenKind::KwTokenizers
	};
	(TOKEN) => {
		crate::lex::BaseTokenKind::KwToken
	};
	(TRANSACTION) => {
		crate::lex::BaseTokenKind::KwTransaction
	};
	(true) => {
		crate::lex::BaseTokenKind::KwTrue
	};
	(TYPE) => {
		crate::lex::BaseTokenKind::KwType
	};
	(UNIQUE) => {
		crate::lex::BaseTokenKind::KwUnique
	};
	(UNSET) => {
		crate::lex::BaseTokenKind::KwUnset
	};
	(UPDATE) => {
		crate::lex::BaseTokenKind::KwUpdate
	};
	(UPSERT) => {
		crate::lex::BaseTokenKind::KwUpsert
	};
	(UPPERCASE) => {
		crate::lex::BaseTokenKind::KwUppercase
	};
	(URL) => {
		crate::lex::BaseTokenKind::KwUrl
	};
	(USE) => {
		crate::lex::BaseTokenKind::KwUse
	};
	(USER) => {
		crate::lex::BaseTokenKind::KwUser
	};
	(VALUE) => {
		crate::lex::BaseTokenKind::KwValue
	};
	(VALUES) => {
		crate::lex::BaseTokenKind::KwValues
	};
	(VERSION) => {
		crate::lex::BaseTokenKind::KwVersion
	};
	(VS) => {
		crate::lex::BaseTokenKind::KwVs
	};
	(WHEN) => {
		crate::lex::BaseTokenKind::KwWhen
	};
	(WHERE) => {
		crate::lex::BaseTokenKind::KwWhere
	};
	(WITH) => {
		crate::lex::BaseTokenKind::KwWith
	};
	(ALLINSIDE) => {
		crate::lex::BaseTokenKind::KwAllInside
	};
	(ANDKW) => {
		crate::lex::BaseTokenKind::KwAndKw
	};
	(ANYINSIDE) => {
		crate::lex::BaseTokenKind::KwAnyInside
	};
	(INSIDE) => {
		crate::lex::BaseTokenKind::KwInside
	};
	(INTERSECTS) => {
		crate::lex::BaseTokenKind::KwIntersects
	};
	(NONEINSIDE) => {
		crate::lex::BaseTokenKind::KwNoneInside
	};
	(NOTINSIDE) => {
		crate::lex::BaseTokenKind::KwNotInside
	};
	(OR) => {
		crate::lex::BaseTokenKind::KwOrKw
	};
	(OUTSIDE) => {
		crate::lex::BaseTokenKind::KwOutside
	};
	(NOT) => {
		crate::lex::BaseTokenKind::KwNot
	};
	(AND) => {
		crate::lex::BaseTokenKind::KwAnd
	};
	(COLLATE) => {
		crate::lex::BaseTokenKind::KwCollate
	};
	(CONTAINSALL) => {
		crate::lex::BaseTokenKind::KwContainsAll
	};
	(CONTAINSANY) => {
		crate::lex::BaseTokenKind::KwContainsAny
	};
	(CONTAINSNONE) => {
		crate::lex::BaseTokenKind::KwContainsNone
	};
	(CONTAINSNOT) => {
		crate::lex::BaseTokenKind::KwContainsNot
	};
	(CONTAINS) => {
		crate::lex::BaseTokenKind::KwContains
	};
	(IN) => {
		crate::lex::BaseTokenKind::KwIn
	};
	(OUT) => {
		crate::lex::BaseTokenKind::KwOut
	};
	(NORMAL) => {
		crate::lex::BaseTokenKind::KwNormal
	};

	// Types
	(ANY) => {
		crate::lex::BaseTokenKind::KwAny
	};
	(ARRAY) => {
		crate::lex::BaseTokenKind::KwArray
	};
	(GEOMETRY) => {
		crate::lex::BaseTokenKind::KwGeometry
	};
	(RECORD) => {
		crate::lex::BaseTokenKind::KwRecord
	};
	(BOOL) => {
		crate::lex::BaseTokenKind::KwBool
	};
	(BYTES) => {
		crate::lex::BaseTokenKind::KwBytes
	};
	(DATETIME) => {
		crate::lex::BaseTokenKind::KwDatetime
	};
	(DECIMAL) => {
		crate::lex::BaseTokenKind::KwDecimal
	};
	(DURATION) => {
		crate::lex::BaseTokenKind::KwDuration
	};
	(FLOAT) => {
		crate::lex::BaseTokenKind::KwFloat
	};
	(fn) => {
		crate::lex::BaseTokenKind::KwFn
	};
	(INT) => {
		crate::lex::BaseTokenKind::KwInt
	};
	(NUMBER) => {
		crate::lex::BaseTokenKind::KwNumber
	};
	(OBJECT) => {
		crate::lex::BaseTokenKind::KwObject
	};
	(REGEX) => {
		crate::lex::BaseTokenKind::KwRegex
	};
	(STRING) => {
		crate::lex::BaseTokenKind::KwString
	};
	(UUID) => {
		crate::lex::BaseTokenKind::KwUuid
	};
	(ULID) => {
		crate::lex::BaseTokenKind::KwUlid
	};
	(RAND) => {
		crate::lex::BaseTokenKind::KwRand
	};
	(REFERENCES) => {
		crate::lex::BaseTokenKind::KwReferences
	};
	(FEATURE) => {
		crate::lex::BaseTokenKind::KwFeature
	};
	(LINE) => {
		crate::lex::BaseTokenKind::KwLine
	};
	(POINT) => {
		crate::lex::BaseTokenKind::KwPoint
	};
	(POLYGON) => {
		crate::lex::BaseTokenKind::KwPolygon
	};
	(MULTIPOINT) => {
		crate::lex::BaseTokenKind::KwMultiPoint
	};
	(MULTILINE) => {
		crate::lex::BaseTokenKind::KwMultiLine
	};
	(MULTIPOLYGON) => {
		crate::lex::BaseTokenKind::KwMultiPolygon
	};
	(COLLECTION) => {
		crate::lex::BaseTokenKind::KwCollection
	};
	(FILE) => {
		crate::lex::BaseTokenKind::KwFile
	};

	// Languages
	(ARABIC) => {
		crate::lex::BaseTokenKind::KwArabic
	};
	(DANISH) => {
		crate::lex::BaseTokenKind::KwDanish
	};
	(DUTCH) => {
		crate::lex::BaseTokenKind::KwDutch
	};
	(ENGLISH) => {
		crate::lex::BaseTokenKind::KwEnglish
	};
	(FINISH) => {
		crate::lex::BaseTokenKind::KwFinnish
	};
	(FRANCH) => {
		crate::lex::BaseTokenKind::KwFrench
	};
	(GERMAN) => {
		crate::lex::BaseTokenKind::KwGerman
	};
	(GREEK) => {
		crate::lex::BaseTokenKind::KwGreek
	};
	(HUNGRARIAN) => {
		crate::lex::BaseTokenKind::KwHungarian
	};
	(ITALIAN) => {
		crate::lex::BaseTokenKind::KwItalian
	};
	(NORWEGIAN) => {
		crate::lex::BaseTokenKind::KwNorwegian
	};
	(PORTUGUESE) => {
		crate::lex::BaseTokenKind::KwPortuguese
	};
	(ROMANIAN) => {
		crate::lex::BaseTokenKind::KwRomanian
	};
	(RUSSIAN) => {
		crate::lex::BaseTokenKind::KwRussian
	};
	(SPANISH) => {
		crate::lex::BaseTokenKind::KwSpanish
	};
	(SWEDISH) => {
		crate::lex::BaseTokenKind::KwSwedish
	};
	(TAMIL) => {
		crate::lex::BaseTokenKind::KwTamil
	};
	(TURKISH) => {
		crate::lex::BaseTokenKind::KwTurkish
	};

	// Algorithms
	(EDDSA) => {
		crate::lex::BaseTokenKind::KwEdDSA
	};
	(ES256) => {
		crate::lex::BaseTokenKind::KwEs256
	};
	(ES384) => {
		crate::lex::BaseTokenKind::KwEs384
	};
	(ES512) => {
		crate::lex::BaseTokenKind::KwEs512
	};
	(HS256) => {
		crate::lex::BaseTokenKind::KwHs256
	};
	(HS384) => {
		crate::lex::BaseTokenKind::KwHs384
	};
	(HS512) => {
		crate::lex::BaseTokenKind::KwHs512
	};
	(PS256) => {
		crate::lex::BaseTokenKind::KwPs256
	};
	(PS384) => {
		crate::lex::BaseTokenKind::KwPs384
	};
	(PS512) => {
		crate::lex::BaseTokenKind::KwPs512
	};
	(RS256) => {
		crate::lex::BaseTokenKind::KwRs256
	};
	(RS384) => {
		crate::lex::BaseTokenKind::KwRs384
	};
	(RS512) => {
		crate::lex::BaseTokenKind::KwRs512
	};

	// Distance
	(CHEBYSHEV) => {
		crate::lex::BaseTokenKind::KwChebyshev
	};
	(COSINE) => {
		crate::lex::BaseTokenKind::KwCosine
	};
	(EUCLIDEAN) => {
		crate::lex::BaseTokenKind::KwEuclidean
	};
	(JACCARD) => {
		crate::lex::BaseTokenKind::KwJaccard
	};
	(HAMMING) => {
		crate::lex::BaseTokenKind::KwHamming
	};
	(MANHATTAN) => {
		crate::lex::BaseTokenKind::KwManhattan
	};
	(MINKOWSKI) => {
		crate::lex::BaseTokenKind::KwMinkowski
	};
	(PEARSON) => {
		crate::lex::BaseTokenKind::KwPearson
	};

	// VectorTypes
	(F64) => {
		crate::lex::BaseTokenKind::KwF64
	};
	(F32) => {
		crate::lex::BaseTokenKind::KwF32
	};
	(I64) => {
		crate::lex::BaseTokenKind::KwI64
	};
	(I32) => {
		crate::lex::BaseTokenKind::KwI32
	};
	(I16) => {
		crate::lex::BaseTokenKind::KwI16
	};

	// HTTP methods
	(GET) => {
		crate::lex::BaseTokenKind::KwGet
	};
	(POST) => {
		crate::lex::BaseTokenKind::KwPost
	};
	(PUT) => {
		crate::lex::BaseTokenKind::KwPut
	};
	(TRACE) => {
		crate::lex::BaseTokenKind::KwTrace
	};
}
