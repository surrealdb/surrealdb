use std::fmt;

use logos::{Lexer, Logos};

use crate::{Joined, LexError};

fn whitespace_callback(lexer: &mut Lexer<BaseTokenKind>) {
	lexer.extras = Joined::Seperated;
}

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
#[logos(error(LexError, LexError::from_lexer))]
#[logos(subpattern duration_part = r"[0-9]+(y|w|d|h|m|s|ms|us|µs|ns)")]
#[logos(subpattern backtick_ident = r"`([^`\\]|\\.)*`")]
#[logos(subpattern bracket_ident = r"⟨([^⟩\\]|\\.)*⟩")]
#[logos(subpattern whitespace = r"[\u{0009}\u{000b}\u{0000c}\u{FEFF}\p{Space_Separator}\n\r\u{2028}\u{2029}]+")]
#[logos(subpattern multi_line_comment = r"/\*([^*]|\*[^/])*\*/")]
#[logos(subpattern line_comment = r"(//|#|--)[^\n\r\u{2028}\u{2029}]*")]
#[logos(skip(r"((?&whitespace)|(?&line_comment)|(?&multi_line_comment))+", whitespace_callback))]
pub enum BaseTokenKind {
	#[token("{")]
	/// `{`
	OpenBrace,
	#[token("}")]
	/// `}`
	CloseBrace,
	#[token("[")]
	/// `[`
	OpenBracket,
	#[token("]")]
	/// `]`
	CloseBracket,
	#[token("(")]
	/// `(`
	OpenParen,
	#[token(")")]
	/// `)`
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

	#[token("|")]
	HLine,
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

	#[token("$")]
	Dollar,

	#[token("×")]
	Times,
	#[token("÷")]
	Divide,
	#[token("∋")]
	Contains,
	#[token("∌")]
	NotContains,
	#[token("∈")]
	Inside,
	#[token("∉")]
	NotInside,
	#[token("⊇")]
	ContainsAll,
	#[token("⊃")]
	ContainsAny,
	#[token("⊅")]
	ContainsNone,
	#[token("⊆")]
	AllInside,
	#[token("⊂")]
	AnyInside,
	#[token("⊄")]
	NoneInside,

	// Algorithms
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
	#[regex(r"(?i)ASYNC")]
	KwAsync,
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
	#[regex(r"(?i)COMPACT")]
	KwCompact,
	#[regex(r"(?i)COMPLEXITY")]
	KwComplexity,
	#[regex(r"(?i)COMPUTED")]
	KwComputed,
	#[regex(r"(?i)CONCURRENTLY")]
	KwConcurrently,
	#[regex(r"(?i)CONFIG")]
	KwConfig,
	#[regex(r"(?i)CONTENT")]
	KwContent,
	#[regex(r"(?i)CONTINUE")]
	KwContinue,
	#[regex(r"(?i)COUNT")]
	KwCount,
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
	#[regex(r"(?i)DEPTH")]
	KwDepth,
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
	#[regex(r"(?i)FORMAT")]
	KwFormat,
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
	#[regex(r"(?i)HASHED_VECTOR")]
	KwHashedVector,
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
	#[regex(r"(?i)INTROSPECTION")]
	KwIntrospection,
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
	#[regex(r"(?i)MAXDEPTH")]
	KwMaxdepth,
	#[regex(r"(?i)MIDDLEWARE")]
	KwMiddleware,
	#[regex(r"(?i)ML")]
	KwML,
	#[regex(r"(?i)MERGE")]
	KwMerge,
	#[regex(r"(?i)MODEL")]
	KwModel,
	#[regex(r"(?i)MODULE")]
	KwModule,
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
	#[regex(r"(?i)PREPARE")]
	KwPrepare,
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
	#[regex(r"(?i)RETRY")]
	KwRetry,
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
	#[regex(r"(?i)STRICT")]
	KwStrict,
	#[regex(r"(?i)STRUCTURE")]
	KwStructure,
	#[regex(r"(?i)SYSTEM")]
	KwSystem,
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
	#[regex(r"(?i)TEXT")]
	KwText,
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

	#[regex(r"(?i)QUERY_TIMEOUT")]
	KwQueryTimeout,

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
	#[regex(r"(?i)JSON")]
	KwJson,
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
	#[regex(r#"r"([^"\\]|\\.)*""#)]
	#[regex(r#"r'([^'\\]|\\.)*'"#)]
	RecordIdString,
	#[regex(r#"u"([^"\\]|\\.)*""#)]
	#[regex(r#"u'([^'\\]|\\.)*'"#)]
	UuidString,
	#[regex(r#"d"([^"\\]|\\.)*""#)]
	#[regex(r#"d'([^'\\]|\\.)*'"#)]
	DateTimeString,
	#[regex(r#"f"([^"\\]|\\.)*""#)]
	#[regex(r#"f'([^'\\]|\\.)*'"#)]
	FileString,

	#[regex(r"\$(?&backtick_ident)")]
	#[regex(r"\$(?&bracket_ident)")]
	#[regex(r"\$\p{XID_Continue}+", priority = 3)]
	Param,
	#[regex(r"(?&backtick_ident)")]
	#[regex(r"(?&bracket_ident)")]
	#[regex(r"\p{XID_Start}\p{XID_Continue}*")]
	Ident,

	#[token("NaN")]
	NaN,
	#[token(r"Infinity")]
	#[token(r"+Infinity")]
	PosInfinity,
	#[token(r"-Infinity")]
	NegInfinity,
	#[regex(r"[0-9]+f")]
	#[regex(r"[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?(f)?")]
	Float,
	#[regex(r"[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?dec")]
	Decimal,
	#[regex(r"[0-9]+", priority = 3)]
	Int,
	#[regex(r"(?&duration_part)+")]
	Duration,
}

impl BaseTokenKind {
	/// Returns a description of the token, used for generating an error when expecting a specific
	/// token.
	pub fn description(&self) -> &'static str {
		match self {
			BaseTokenKind::OpenParen => "`(`",
			BaseTokenKind::CloseParen => "`)`",
			BaseTokenKind::OpenBrace => "`{`",
			BaseTokenKind::CloseBrace => "`}`",
			BaseTokenKind::OpenBracket => "`[`",
			BaseTokenKind::CloseBracket => "`]`",
			BaseTokenKind::SemiColon => "`;`",
			BaseTokenKind::Comma => "`,`",
			BaseTokenKind::At => "`@`",
			BaseTokenKind::Slash => "`/`",
			BaseTokenKind::Percent => "`%`",
			BaseTokenKind::HLine => "`|`",
			BaseTokenKind::HLineHLine => "`||`",
			BaseTokenKind::HLineRightShevron => "`|>`",
			BaseTokenKind::AndAnd => "`&&`",
			BaseTokenKind::Dot => "`.`",
			BaseTokenKind::DotDot => "`..`",
			BaseTokenKind::DotDotDot => "`...`",
			BaseTokenKind::Exclaim => "`!`",
			BaseTokenKind::ExclaimEq => "`!=`",
			BaseTokenKind::Question => "`?`",
			BaseTokenKind::QuestionEqual => "`?=`",
			BaseTokenKind::QuestionColon => "`?:`",
			BaseTokenKind::LeftShevron => "`<`",
			BaseTokenKind::LeftShevronEqual => "`<=`",
			BaseTokenKind::LeftShevronHLine => "`<|`",
			BaseTokenKind::RightShevron => "`>`",
			BaseTokenKind::RightShevronEqual => "`>=`",
			BaseTokenKind::Dash => "`-`",
			BaseTokenKind::DashEqual => "`-=`",
			BaseTokenKind::DashRightShevron => "`->`",
			BaseTokenKind::Plus => "`+`",
			BaseTokenKind::PlusEqual => "`+=`",
			BaseTokenKind::PlusQuestionEqual => "`+?=`",
			BaseTokenKind::Star => "`*`",
			BaseTokenKind::StarEqual => "`*=`",
			BaseTokenKind::StarStar => "`**`",
			BaseTokenKind::Equal => "`=`",
			BaseTokenKind::EqualEqual => "`==`",
			BaseTokenKind::Colon => "`:`",
			BaseTokenKind::ColonColon => "`::`",
			BaseTokenKind::Dollar => "`$`",
			BaseTokenKind::Times => "`×`",
			BaseTokenKind::Divide => "`÷`",
			BaseTokenKind::Contains => "`∋`",
			BaseTokenKind::NotContains => "`∌`",
			BaseTokenKind::Inside => "`∈`",
			BaseTokenKind::NotInside => "`∉`",
			BaseTokenKind::ContainsAll => "`⊇`",
			BaseTokenKind::ContainsAny => "`⊃`",
			BaseTokenKind::ContainsNone => "`⊅`",
			BaseTokenKind::AllInside => "`⊆`",
			BaseTokenKind::AnyInside => "`⊂`",
			BaseTokenKind::NoneInside => "`⊄`",
			BaseTokenKind::KwAccess => "keyword `ACCESS`",
			BaseTokenKind::KwAfter => "keyword `AFTER`",
			BaseTokenKind::KwAlgorithm => "keyword `ALGORITHM`",
			BaseTokenKind::KwAll => "keyword `ALL`",
			BaseTokenKind::KwAlter => "keyword `ALTER`",
			BaseTokenKind::KwAlways => "keyword `ALWAYS`",
			BaseTokenKind::KwAnalyze => "keyword `ANALYZE`",
			BaseTokenKind::KwAnalyzer => "keyword `ANALYZER`",
			BaseTokenKind::KwApi => "keyword `API`",
			BaseTokenKind::KwAs => "keyword `AS`",
			BaseTokenKind::KwAscending => "keyword `ASCENDING`",
			BaseTokenKind::KwAscii => "keyword `ASCII`",
			BaseTokenKind::KwAssert => "keyword `ASSERT`",
			BaseTokenKind::KwAt => "keyword `AT`",
			BaseTokenKind::KwAuthenticate => "keyword `AUTHENTICATE`",
			BaseTokenKind::KwAuto => "keyword `AUTO`",
			BaseTokenKind::KwAsync => "keyword `ASYNC`",
			BaseTokenKind::KwBackend => "keyword `BACKEND`",
			BaseTokenKind::KwBatch => "keyword `BATCH`",
			BaseTokenKind::KwBearer => "keyword `BEARER`",
			BaseTokenKind::KwBefore => "keyword `BEFORE`",
			BaseTokenKind::KwBegin => "keyword `BEGIN`",
			BaseTokenKind::KwBlank => "keyword `BLANK`",
			BaseTokenKind::KwBm25 => "keyword `BM25`",
			BaseTokenKind::KwBreak => "keyword `BREAK`",
			BaseTokenKind::KwBucket => "keyword `BUCKET`",
			BaseTokenKind::KwBy => "keyword `BY`",
			BaseTokenKind::KwCamel => "keyword `CAMEL`",
			BaseTokenKind::KwCancel => "keyword `CANCEL`",
			BaseTokenKind::KwCascade => "keyword `CASCADE`",
			BaseTokenKind::KwChangeFeed => "keyword `CHANGEFEED`",
			BaseTokenKind::KwChanges => "keyword `CHANGES`",
			BaseTokenKind::KwCapacity => "keyword `CAPACITY`",
			BaseTokenKind::KwClass => "keyword `CLASS`",
			BaseTokenKind::KwComment => "keyword `COMMENT`",
			BaseTokenKind::KwCommit => "keyword `COMMIT`",
			BaseTokenKind::KwCompact => "keyword `COMPACT`",
			BaseTokenKind::KwComplexity => "keyword `COMPLEXITY`",
			BaseTokenKind::KwComputed => "keyword `COMPUTED`",
			BaseTokenKind::KwConcurrently => "keyword `CONCURRENTLY`",
			BaseTokenKind::KwConfig => "keyword `CONFIG`",
			BaseTokenKind::KwContent => "keyword `CONTENT`",
			BaseTokenKind::KwContinue => "keyword `CONTINUE`",
			BaseTokenKind::KwCount => "keyword `COUNT`",
			BaseTokenKind::KwCreate => "keyword `CREATE`",
			BaseTokenKind::KwDatabase => "keyword `DATABASE`",
			BaseTokenKind::KwDefault => "keyword `DEFAULT`",
			BaseTokenKind::KwDefine => "keyword `DEFINE`",
			BaseTokenKind::KwDelete => "keyword `DELETE`",
			BaseTokenKind::KwDepth => "keyword `DEPTH`",
			BaseTokenKind::KwDescending => "keyword `DESCENDING`",
			BaseTokenKind::KwDiff => "keyword `DIFF`",
			BaseTokenKind::KwDimension => "keyword `DIMENSION`",
			BaseTokenKind::KwDistance => "keyword `DISTANCE`",
			BaseTokenKind::KwDocIdsCache => "keyword `DOCIDSCACHE`",
			BaseTokenKind::KwDocIdsOrder => "keyword `DOCIDSORDER`",
			BaseTokenKind::KwDocLengthsCache => "keyword `DOCLENGTHSCACHE`",
			BaseTokenKind::KwDocLengthsOrder => "keyword `DOCLENGTHSORDER`",
			BaseTokenKind::KwDrop => "keyword `DROP`",
			BaseTokenKind::KwDuplicate => "keyword `DUPLICATE`",
			BaseTokenKind::KwEdgengram => "keyword `EDGENGRAM`",
			BaseTokenKind::KwEfc => "keyword `EFC`",
			BaseTokenKind::KwEvent => "keyword `EVENT`",
			BaseTokenKind::KwElse => "keyword `ELSE`",
			BaseTokenKind::KwEnd => "keyword `END`",
			BaseTokenKind::KwEnforced => "keyword `ENFORCED`",
			BaseTokenKind::KwExclude => "keyword `EXCLUDE`",
			BaseTokenKind::KwExists => "keyword `EXISTS`",
			BaseTokenKind::KwExpired => "keyword `EXPIRED`",
			BaseTokenKind::KwExplain => "keyword `EXPLAIN`",
			BaseTokenKind::KwExpunge => "keyword `EXPUNGE`",
			BaseTokenKind::KwExtendCandidates => "keyword `EXTENDCANDIDATES`",
			BaseTokenKind::KwFalse => "keyword `FALSE`",
			BaseTokenKind::KwFetch => "keyword `FETCH`",
			BaseTokenKind::KwField => "keyword `FIELD`",
			BaseTokenKind::KwFields => "keyword `FIELDS`",
			BaseTokenKind::KwFilters => "keyword `FILTERS`",
			BaseTokenKind::KwFlexible => "keyword `FLEXIBLE`",
			BaseTokenKind::KwFor => "keyword `FOR`",
			BaseTokenKind::KwFormat => "keyword `FORMAT`",
			BaseTokenKind::KwFrom => "keyword `FROM`",
			BaseTokenKind::KwFull => "keyword `FULL`",
			BaseTokenKind::KwFulltext => "keyword `FULLTEXT`",
			BaseTokenKind::KwFunction => "keyword `FUNCTION`",
			BaseTokenKind::KwFunctions => "keyword `FUNCTIONS`",
			BaseTokenKind::KwGrant => "keyword `GRANT`",
			BaseTokenKind::KwGraphql => "keyword `GRAPHQL`",
			BaseTokenKind::KwGroup => "keyword `GROUP`",
			BaseTokenKind::KwHashedVector => "keyword `HASHED_VECTOR`",
			BaseTokenKind::KwHeaders => "keyword `HEADERS`",
			BaseTokenKind::KwHighlights => "keyword `HIGHLIGHTS`",
			BaseTokenKind::KwHnsw => "keyword `HNSW`",
			BaseTokenKind::KwIgnore => "keyword `IGNORE`",
			BaseTokenKind::KwInclude => "keyword `INCLUDE`",
			BaseTokenKind::KwIndex => "keyword `INDEX`",
			BaseTokenKind::KwInfo => "keyword `INFO`",
			BaseTokenKind::KwInsert => "keyword `INSERT`",
			BaseTokenKind::KwInto => "keyword `INTO`",
			BaseTokenKind::KwIntrospection => "keyword `INTROSPECTION`",
			BaseTokenKind::KwIf => "keyword `IF`",
			BaseTokenKind::KwIs => "keyword `IS`",
			BaseTokenKind::KwIssuer => "keyword `ISSUER`",
			BaseTokenKind::KwJwt => "keyword `JWT`",
			BaseTokenKind::KwJwks => "keyword `JWKS`",
			BaseTokenKind::KwKey => "keyword `KEY`",
			BaseTokenKind::KwKeepPrunedConnections => "keyword `KEEPPRUNEDCONNECTIONS`",
			BaseTokenKind::KwKill => "keyword `KILL`",
			BaseTokenKind::KwLet => "keyword `LET`",
			BaseTokenKind::KwLimit => "keyword `LIMIT`",
			BaseTokenKind::KwLive => "keyword `LIVE`",
			BaseTokenKind::KwLowercase => "keyword `LOWERCASE`",
			BaseTokenKind::KwLm => "keyword `LM`",
			BaseTokenKind::KwM => "keyword `M`",
			BaseTokenKind::KwM0 => "keyword `M0`",
			BaseTokenKind::KwMapper => "keyword `MAPPER`",
			BaseTokenKind::KwMaxdepth => "keyword `MAXDEPTH`",
			BaseTokenKind::KwMiddleware => "keyword `MIDDLEWARE`",
			BaseTokenKind::KwML => "keyword `ML`",
			BaseTokenKind::KwMerge => "keyword `MERGE`",
			BaseTokenKind::KwModel => "keyword `MODEL`",
			BaseTokenKind::KwModule => "keyword `MODULE`",
			BaseTokenKind::KwMTree => "keyword `MTREE`",
			BaseTokenKind::KwMTreeCache => "keyword `MTREECACHE`",
			BaseTokenKind::KwNamespace => "keyword `NAMESPACE`",
			BaseTokenKind::KwNgram => "keyword `NGRAM`",
			BaseTokenKind::KwNo => "keyword `NO`",
			BaseTokenKind::KwNoIndex => "keyword `NOINDEX`",
			BaseTokenKind::KwNone => "keyword `NONE`",
			BaseTokenKind::KwNull => "keyword `NULL`",
			BaseTokenKind::KwNumeric => "keyword `NUMERIC`",
			BaseTokenKind::KwOmit => "keyword `OMIT`",
			BaseTokenKind::KwOn => "keyword `ON`",
			BaseTokenKind::KwOnly => "keyword `ONLY`",
			BaseTokenKind::KwOption => "keyword `OPTION`",
			BaseTokenKind::KwOrder => "keyword `ORDER`",
			BaseTokenKind::KwOriginal => "keyword `ORIGINAL`",
			BaseTokenKind::KwOverwrite => "keyword `OVERWRITE`",
			BaseTokenKind::KwParallel => "keyword `PARALLEL`",
			BaseTokenKind::KwKwParam => "keyword `KWPARAM`",
			BaseTokenKind::KwPasshash => "keyword `PASSHASH`",
			BaseTokenKind::KwPassword => "keyword `PASSWORD`",
			BaseTokenKind::KwPatch => "keyword `PATCH`",
			BaseTokenKind::KwPermissions => "keyword `PERMISSIONS`",
			BaseTokenKind::KwPostingsCache => "keyword `POSTINGSCACHE`",
			BaseTokenKind::KwPostingsOrder => "keyword `POSTINGSORDER`",
			BaseTokenKind::KwPrepare => "keyword `PREPARE`",
			BaseTokenKind::KwPunct => "keyword `PUNCT`",
			BaseTokenKind::KwPurge => "keyword `PURGE`",
			BaseTokenKind::KwRange => "keyword `RANGE`",
			BaseTokenKind::KwReadonly => "keyword `READONLY`",
			BaseTokenKind::KwReject => "keyword `REJECT`",
			BaseTokenKind::KwRelate => "keyword `RELATE`",
			BaseTokenKind::KwRelation => "keyword `RELATION`",
			BaseTokenKind::KwRebuild => "keyword `REBUILD`",
			BaseTokenKind::KwReference => "keyword `REFERENCE`",
			BaseTokenKind::KwRefresh => "keyword `REFRESH`",
			BaseTokenKind::KwRemove => "keyword `REMOVE`",
			BaseTokenKind::KwReplace => "keyword `REPLACE`",
			BaseTokenKind::KwRetry => "keyword `RETRY`",
			BaseTokenKind::KwReturn => "keyword `RETURN`",
			BaseTokenKind::KwRevoke => "keyword `REVOKE`",
			BaseTokenKind::KwRevoked => "keyword `REVOKED`",
			BaseTokenKind::KwRoles => "keyword `ROLES`",
			BaseTokenKind::KwRoot => "keyword `ROOT`",
			BaseTokenKind::KwSchemafull => "keyword `SCHEMAFULL`",
			BaseTokenKind::KwSchemaless => "keyword `SCHEMALESS`",
			BaseTokenKind::KwScope => "keyword `SCOPE`",
			BaseTokenKind::KwSearch => "keyword `SEARCH`",
			BaseTokenKind::KwSelect => "keyword `SELECT`",
			BaseTokenKind::KwSequence => "keyword `SEQUENCE`",
			BaseTokenKind::KwSession => "keyword `SESSION`",
			BaseTokenKind::KwSet => "keyword `SET`",
			BaseTokenKind::KwShow => "keyword `SHOW`",
			BaseTokenKind::KwSignin => "keyword `SIGNIN`",
			BaseTokenKind::KwSignup => "keyword `SIGNUP`",
			BaseTokenKind::KwSince => "keyword `SINCE`",
			BaseTokenKind::KwSleep => "keyword `SLEEP`",
			BaseTokenKind::KwSnowball => "keyword `SNOWBALL`",
			BaseTokenKind::KwSplit => "keyword `SPLIT`",
			BaseTokenKind::KwStart => "keyword `START`",
			BaseTokenKind::KwStrict => "keyword `STRICT`",
			BaseTokenKind::KwStructure => "keyword `STRUCTURE`",
			BaseTokenKind::KwSystem => "keyword `SYSTEM`",
			BaseTokenKind::KwTable => "keyword `TABLE`",
			BaseTokenKind::KwTables => "keyword `TABLES`",
			BaseTokenKind::KwTempFiles => "keyword `TEMPFILES`",
			BaseTokenKind::KwTermsCache => "keyword `TERMSCACHE`",
			BaseTokenKind::KwTermsOrder => "keyword `TERMSORDER`",
			BaseTokenKind::KwText => "keyword `TEXT`",
			BaseTokenKind::KwThen => "keyword `THEN`",
			BaseTokenKind::KwThrow => "keyword `THROW`",
			BaseTokenKind::KwTimeout => "keyword `TIMEOUT`",
			BaseTokenKind::KwTo => "keyword `TO`",
			BaseTokenKind::KwTokenizers => "keyword `TOKENIZERS`",
			BaseTokenKind::KwToken => "keyword `TOKEN`",
			BaseTokenKind::KwTransaction => "keyword `TRANSACTION`",
			BaseTokenKind::KwQueryTimeout => "keyword `QUERY_TIMEOUT`",
			BaseTokenKind::KwTrue => "keyword `TRUE`",
			BaseTokenKind::KwType => "keyword `TYPE`",
			BaseTokenKind::KwUnique => "keyword `UNIQUE`",
			BaseTokenKind::KwUnset => "keyword `UNSET`",
			BaseTokenKind::KwUpdate => "keyword `UPDATE`",
			BaseTokenKind::KwUpsert => "keyword `UPSERT`",
			BaseTokenKind::KwUppercase => "keyword `UPPERCASE`",
			BaseTokenKind::KwUrl => "keyword `URL`",
			BaseTokenKind::KwUse => "keyword `USE`",
			BaseTokenKind::KwUser => "keyword `USER`",
			BaseTokenKind::KwValue => "keyword `VALUE`",
			BaseTokenKind::KwValues => "keyword `VALUES`",
			BaseTokenKind::KwVersion => "keyword `VERSION`",
			BaseTokenKind::KwVs => "keyword `VS`",
			BaseTokenKind::KwWhen => "keyword `WHEN`",
			BaseTokenKind::KwWhere => "keyword `WHERE`",
			BaseTokenKind::KwWith => "keyword `WITH`",
			BaseTokenKind::KwAllInside => "keyword `ALLINSIDE`",
			BaseTokenKind::KwAndKw => "keyword `ANDKW`",
			BaseTokenKind::KwAnyInside => "keyword `ANYINSIDE`",
			BaseTokenKind::KwInside => "keyword `INSIDE`",
			BaseTokenKind::KwIntersects => "keyword `INTERSECTS`",
			BaseTokenKind::KwJson => "keyword `JSON`",
			BaseTokenKind::KwNoneInside => "keyword `NONEINSIDE`",
			BaseTokenKind::KwNotInside => "keyword `NOTINSIDE`",
			BaseTokenKind::KwOrKw => "keyword `ORKW`",
			BaseTokenKind::KwOutside => "keyword `OUTSIDE`",
			BaseTokenKind::KwNot => "keyword `NOT`",
			BaseTokenKind::KwAnd => "keyword `AND`",
			BaseTokenKind::KwCollate => "keyword `COLLATE`",
			BaseTokenKind::KwContainsAll => "keyword `CONTAINSALL`",
			BaseTokenKind::KwContainsAny => "keyword `CONTAINSANY`",
			BaseTokenKind::KwContainsNone => "keyword `CONTAINSNONE`",
			BaseTokenKind::KwContainsNot => "keyword `CONTAINSNOT`",
			BaseTokenKind::KwContains => "keyword `CONTAINS`",
			BaseTokenKind::KwIn => "keyword `IN`",
			BaseTokenKind::KwOut => "keyword `OUT`",
			BaseTokenKind::KwNormal => "keyword `NORMAL`",
			BaseTokenKind::KwAny => "keyword `ANY`",
			BaseTokenKind::KwArray => "keyword `ARRAY`",
			BaseTokenKind::KwGeometry => "keyword `GEOMETRY`",
			BaseTokenKind::KwRecord => "keyword `RECORD`",
			BaseTokenKind::KwBool => "keyword `BOOL`",
			BaseTokenKind::KwBytes => "keyword `BYTES`",
			BaseTokenKind::KwDatetime => "keyword `DATETIME`",
			BaseTokenKind::KwDecimal => "keyword `DECIMAL`",
			BaseTokenKind::KwDuration => "keyword `DURATION`",
			BaseTokenKind::KwFloat => "keyword `FLOAT`",
			BaseTokenKind::KwInt => "keyword `INT`",
			BaseTokenKind::KwNumber => "keyword `NUMBER`",
			BaseTokenKind::KwObject => "keyword `OBJECT`",
			BaseTokenKind::KwRegex => "keyword `REGEX`",
			BaseTokenKind::KwString => "keyword `STRING`",
			BaseTokenKind::KwUuid => "keyword `UUID`",
			BaseTokenKind::KwUlid => "keyword `ULID`",
			BaseTokenKind::KwRand => "keyword `RAND`",
			BaseTokenKind::KwReferences => "keyword `REFERENCES`",
			BaseTokenKind::KwFeature => "keyword `FEATURE`",
			BaseTokenKind::KwLine => "keyword `LINE`",
			BaseTokenKind::KwPoint => "keyword `POINT`",
			BaseTokenKind::KwPolygon => "keyword `POLYGON`",
			BaseTokenKind::KwMultiPoint => "keyword `MULTIPOINT`",
			BaseTokenKind::KwMultiLine => "keyword `MULTILINE`",
			BaseTokenKind::KwMultiPolygon => "keyword `MULTIPOLYGON`",
			BaseTokenKind::KwCollection => "keyword `COLLECTION`",
			BaseTokenKind::KwFile => "keyword `FILE`",
			BaseTokenKind::KwEdDSA => "keyword `EDDSA`",
			BaseTokenKind::KwEs256 => "keyword `ES256`",
			BaseTokenKind::KwEs384 => "keyword `ES384`",
			BaseTokenKind::KwEs512 => "keyword `ES512`",
			BaseTokenKind::KwHs256 => "keyword `HS256`",
			BaseTokenKind::KwHs384 => "keyword `HS384`",
			BaseTokenKind::KwHs512 => "keyword `HS512`",
			BaseTokenKind::KwPs256 => "keyword `PS256`",
			BaseTokenKind::KwPs384 => "keyword `PS384`",
			BaseTokenKind::KwPs512 => "keyword `PS512`",
			BaseTokenKind::KwRs256 => "keyword `RS256`",
			BaseTokenKind::KwRs384 => "keyword `RS384`",
			BaseTokenKind::KwRs512 => "keyword `RS512`",
			BaseTokenKind::KwChebyshev => "keyword `CHEBYSHEV`",
			BaseTokenKind::KwCosine => "keyword `COSINE`",
			BaseTokenKind::KwEuclidean => "keyword `EUCLIDEAN`",
			BaseTokenKind::KwJaccard => "keyword `JACCARD`",
			BaseTokenKind::KwHamming => "keyword `HAMMING`",
			BaseTokenKind::KwManhattan => "keyword `MANHATTAN`",
			BaseTokenKind::KwMinkowski => "keyword `MINKOWSKI`",
			BaseTokenKind::KwPearson => "keyword `PEARSON`",
			BaseTokenKind::KwF64 => "keyword `F64`",
			BaseTokenKind::KwF32 => "keyword `F32`",
			BaseTokenKind::KwI64 => "keyword `I64`",
			BaseTokenKind::KwI32 => "keyword `I32`",
			BaseTokenKind::KwI16 => "keyword `I16`",
			BaseTokenKind::KwGet => "keyword `GET`",
			BaseTokenKind::KwPost => "keyword `POST`",
			BaseTokenKind::KwPut => "keyword `PUT`",
			BaseTokenKind::KwTrace => "keyword `TRACE`",
			BaseTokenKind::String => "a string",
			BaseTokenKind::RecordIdString => "a record-id string",
			BaseTokenKind::UuidString => "a uuid",
			BaseTokenKind::DateTimeString => "a datetime",
			BaseTokenKind::FileString => "a file path",
			BaseTokenKind::Param => "a parameter",
			BaseTokenKind::Ident => "an identifier",
			BaseTokenKind::NaN => "`NaN`",
			BaseTokenKind::PosInfinity => "infinity",
			BaseTokenKind::NegInfinity => "negative infinity",
			BaseTokenKind::Float => "a float",
			BaseTokenKind::Decimal => "a decimal",
			BaseTokenKind::Int => "an integer",
			BaseTokenKind::Duration => "a duration",
		}
	}

	/// Returns if the token kind can be an identifier.
	pub fn is_identifier(&self) -> bool {
		matches!(
			self,
			Self::Ident
				| BaseTokenKind::KwAfter
				| BaseTokenKind::KwAlgorithm
				| BaseTokenKind::KwAll
				| BaseTokenKind::KwAlter
				| BaseTokenKind::KwAlways
				| BaseTokenKind::KwAnalyze
				| BaseTokenKind::KwAnalyzer
				| BaseTokenKind::KwApi
				| BaseTokenKind::KwAs
				| BaseTokenKind::KwAscending
				| BaseTokenKind::KwAscii
				| BaseTokenKind::KwAssert
				| BaseTokenKind::KwAt
				| BaseTokenKind::KwAuthenticate
				| BaseTokenKind::KwAuto
				| BaseTokenKind::KwAsync
				| BaseTokenKind::KwBackend
				| BaseTokenKind::KwBatch
				| BaseTokenKind::KwBearer
				| BaseTokenKind::KwBefore
				| BaseTokenKind::KwBegin
				| BaseTokenKind::KwBlank
				| BaseTokenKind::KwBm25
				| BaseTokenKind::KwBreak
				| BaseTokenKind::KwBucket
				| BaseTokenKind::KwBy
				| BaseTokenKind::KwCamel
				| BaseTokenKind::KwCancel
				| BaseTokenKind::KwCascade
				| BaseTokenKind::KwChangeFeed
				| BaseTokenKind::KwChanges
				| BaseTokenKind::KwCapacity
				| BaseTokenKind::KwClass
				| BaseTokenKind::KwComment
				| BaseTokenKind::KwCommit
				| BaseTokenKind::KwCompact
				| BaseTokenKind::KwComplexity
				| BaseTokenKind::KwComputed
				| BaseTokenKind::KwConcurrently
				| BaseTokenKind::KwConfig
				| BaseTokenKind::KwContent
				| BaseTokenKind::KwContinue
				| BaseTokenKind::KwCount
				| BaseTokenKind::KwCreate
				| BaseTokenKind::KwDatabase
				| BaseTokenKind::KwDefault
				| BaseTokenKind::KwDefine
				| BaseTokenKind::KwDelete
				| BaseTokenKind::KwDepth
				| BaseTokenKind::KwDescending
				| BaseTokenKind::KwDiff
				| BaseTokenKind::KwDimension
				| BaseTokenKind::KwDistance
				| BaseTokenKind::KwDocIdsCache
				| BaseTokenKind::KwDocIdsOrder
				| BaseTokenKind::KwDocLengthsCache
				| BaseTokenKind::KwDocLengthsOrder
				| BaseTokenKind::KwDrop
				| BaseTokenKind::KwDuplicate
				| BaseTokenKind::KwEdgengram
				| BaseTokenKind::KwEfc
				| BaseTokenKind::KwEvent
				| BaseTokenKind::KwElse
				| BaseTokenKind::KwEnd
				| BaseTokenKind::KwEnforced
				| BaseTokenKind::KwExclude
				| BaseTokenKind::KwExists
				| BaseTokenKind::KwExpired
				| BaseTokenKind::KwExplain
				| BaseTokenKind::KwExpunge
				| BaseTokenKind::KwExtendCandidates
				| BaseTokenKind::KwFalse
				| BaseTokenKind::KwFetch
				| BaseTokenKind::KwField
				| BaseTokenKind::KwFields
				| BaseTokenKind::KwFilters
				| BaseTokenKind::KwFlexible
				| BaseTokenKind::KwFor
				| BaseTokenKind::KwFormat
				| BaseTokenKind::KwFrom
				| BaseTokenKind::KwFull
				| BaseTokenKind::KwFulltext
				| BaseTokenKind::KwFunction
				| BaseTokenKind::KwFunctions
				| BaseTokenKind::KwGrant
				| BaseTokenKind::KwGraphql
				| BaseTokenKind::KwGroup
				| BaseTokenKind::KwHashedVector
				| BaseTokenKind::KwHeaders
				| BaseTokenKind::KwHighlights
				| BaseTokenKind::KwHnsw
				| BaseTokenKind::KwIgnore
				| BaseTokenKind::KwInclude
				| BaseTokenKind::KwIndex
				| BaseTokenKind::KwInfo
				| BaseTokenKind::KwInsert
				| BaseTokenKind::KwInto
				| BaseTokenKind::KwIntrospection
				| BaseTokenKind::KwIf
				| BaseTokenKind::KwIs
				| BaseTokenKind::KwIssuer
				| BaseTokenKind::KwJwt
				| BaseTokenKind::KwJwks
				| BaseTokenKind::KwKey
				| BaseTokenKind::KwKeepPrunedConnections
				| BaseTokenKind::KwKill
				| BaseTokenKind::KwLet
				| BaseTokenKind::KwLimit
				| BaseTokenKind::KwLive
				| BaseTokenKind::KwLowercase
				| BaseTokenKind::KwLm
				| BaseTokenKind::KwM
				| BaseTokenKind::KwM0
				| BaseTokenKind::KwMapper
				| BaseTokenKind::KwMaxdepth
				| BaseTokenKind::KwMiddleware
				| BaseTokenKind::KwML
				| BaseTokenKind::KwMerge
				| BaseTokenKind::KwModel
				| BaseTokenKind::KwModule
				| BaseTokenKind::KwMTree
				| BaseTokenKind::KwMTreeCache
				| BaseTokenKind::KwNamespace
				| BaseTokenKind::KwNgram
				| BaseTokenKind::KwNo
				| BaseTokenKind::KwNoIndex
				| BaseTokenKind::KwNone
				| BaseTokenKind::KwNull
				| BaseTokenKind::KwNumeric
				| BaseTokenKind::KwOmit
				| BaseTokenKind::KwOn
				| BaseTokenKind::KwOnly
				| BaseTokenKind::KwOption
				| BaseTokenKind::KwOrder
				| BaseTokenKind::KwOriginal
				| BaseTokenKind::KwOverwrite
				| BaseTokenKind::KwParallel
				| BaseTokenKind::KwKwParam
				| BaseTokenKind::KwPasshash
				| BaseTokenKind::KwPassword
				| BaseTokenKind::KwPatch
				| BaseTokenKind::KwPermissions
				| BaseTokenKind::KwPostingsCache
				| BaseTokenKind::KwPostingsOrder
				| BaseTokenKind::KwPrepare
				| BaseTokenKind::KwPunct
				| BaseTokenKind::KwPurge
				| BaseTokenKind::KwRange
				| BaseTokenKind::KwReadonly
				| BaseTokenKind::KwReject
				| BaseTokenKind::KwRelate
				| BaseTokenKind::KwRelation
				| BaseTokenKind::KwRebuild
				| BaseTokenKind::KwReference
				| BaseTokenKind::KwRefresh
				| BaseTokenKind::KwRemove
				| BaseTokenKind::KwReplace
				| BaseTokenKind::KwRetry
				| BaseTokenKind::KwReturn
				| BaseTokenKind::KwRevoke
				| BaseTokenKind::KwRevoked
				| BaseTokenKind::KwRoles
				| BaseTokenKind::KwRoot
				| BaseTokenKind::KwSchemafull
				| BaseTokenKind::KwSchemaless
				| BaseTokenKind::KwScope
				| BaseTokenKind::KwSearch
				| BaseTokenKind::KwSelect
				| BaseTokenKind::KwSequence
				| BaseTokenKind::KwSession
				| BaseTokenKind::KwSet
				| BaseTokenKind::KwShow
				| BaseTokenKind::KwSignin
				| BaseTokenKind::KwSignup
				| BaseTokenKind::KwSince
				| BaseTokenKind::KwSleep
				| BaseTokenKind::KwSnowball
				| BaseTokenKind::KwSplit
				| BaseTokenKind::KwStart
				| BaseTokenKind::KwStrict
				| BaseTokenKind::KwStructure
				| BaseTokenKind::KwSystem
				| BaseTokenKind::KwTable
				| BaseTokenKind::KwTables
				| BaseTokenKind::KwTempFiles
				| BaseTokenKind::KwTermsCache
				| BaseTokenKind::KwTermsOrder
				| BaseTokenKind::KwText
				| BaseTokenKind::KwThen
				| BaseTokenKind::KwThrow
				| BaseTokenKind::KwTimeout
				| BaseTokenKind::KwTo
				| BaseTokenKind::KwTokenizers
				| BaseTokenKind::KwToken
				| BaseTokenKind::KwTransaction
				| BaseTokenKind::KwQueryTimeout
				| BaseTokenKind::KwTrue
				| BaseTokenKind::KwType
				| BaseTokenKind::KwUnique
				| BaseTokenKind::KwUnset
				| BaseTokenKind::KwUpdate
				| BaseTokenKind::KwUpsert
				| BaseTokenKind::KwUppercase
				| BaseTokenKind::KwUrl
				| BaseTokenKind::KwUse
				| BaseTokenKind::KwUser
				| BaseTokenKind::KwValue
				| BaseTokenKind::KwValues
				| BaseTokenKind::KwVersion
				| BaseTokenKind::KwVs
				| BaseTokenKind::KwWhen
				| BaseTokenKind::KwWhere
				| BaseTokenKind::KwWith
				| BaseTokenKind::KwAllInside
				| BaseTokenKind::KwAndKw
				| BaseTokenKind::KwAnyInside
				| BaseTokenKind::KwInside
				| BaseTokenKind::KwIntersects
				| BaseTokenKind::KwJson
				| BaseTokenKind::KwNoneInside
				| BaseTokenKind::KwNotInside
				| BaseTokenKind::KwOrKw
				| BaseTokenKind::KwOutside
				| BaseTokenKind::KwNot
				| BaseTokenKind::KwAnd
				| BaseTokenKind::KwCollate
				| BaseTokenKind::KwContainsAll
				| BaseTokenKind::KwContainsAny
				| BaseTokenKind::KwContainsNone
				| BaseTokenKind::KwContainsNot
				| BaseTokenKind::KwContains
				| BaseTokenKind::KwIn
				| BaseTokenKind::KwOut
				| BaseTokenKind::KwNormal
				| BaseTokenKind::KwAny
				| BaseTokenKind::KwArray
				| BaseTokenKind::KwGeometry
				| BaseTokenKind::KwRecord
				| BaseTokenKind::KwBool
				| BaseTokenKind::KwBytes
				| BaseTokenKind::KwDatetime
				| BaseTokenKind::KwDecimal
				| BaseTokenKind::KwDuration
				| BaseTokenKind::KwFloat
				| BaseTokenKind::KwInt
				| BaseTokenKind::KwNumber
				| BaseTokenKind::KwObject
				| BaseTokenKind::KwRegex
				| BaseTokenKind::KwString
				| BaseTokenKind::KwUuid
				| BaseTokenKind::KwUlid
				| BaseTokenKind::KwRand
				| BaseTokenKind::KwReferences
				| BaseTokenKind::KwFeature
				| BaseTokenKind::KwLine
				| BaseTokenKind::KwPoint
				| BaseTokenKind::KwPolygon
				| BaseTokenKind::KwMultiPoint
				| BaseTokenKind::KwMultiLine
				| BaseTokenKind::KwMultiPolygon
				| BaseTokenKind::KwCollection
				| BaseTokenKind::KwFile
				| BaseTokenKind::KwEdDSA
				| BaseTokenKind::KwEs256
				| BaseTokenKind::KwEs384
				| BaseTokenKind::KwEs512
				| BaseTokenKind::KwHs256
				| BaseTokenKind::KwHs384
				| BaseTokenKind::KwHs512
				| BaseTokenKind::KwPs256
				| BaseTokenKind::KwPs384
				| BaseTokenKind::KwPs512
				| BaseTokenKind::KwRs256
				| BaseTokenKind::KwRs384
				| BaseTokenKind::KwRs512
				| BaseTokenKind::KwChebyshev
				| BaseTokenKind::KwCosine
				| BaseTokenKind::KwEuclidean
				| BaseTokenKind::KwJaccard
				| BaseTokenKind::KwHamming
				| BaseTokenKind::KwManhattan
				| BaseTokenKind::KwMinkowski
				| BaseTokenKind::KwPearson
				| BaseTokenKind::KwF64
				| BaseTokenKind::KwF32
				| BaseTokenKind::KwI64
				| BaseTokenKind::KwI32
				| BaseTokenKind::KwI16
				| BaseTokenKind::KwGet
				| BaseTokenKind::KwPost
				| BaseTokenKind::KwPut
				| BaseTokenKind::KwTrace
		)
	}
}

impl fmt::Display for BaseTokenKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.description())
	}
}
