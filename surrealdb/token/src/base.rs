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
			BaseTokenKind::Times => todo!(),
			BaseTokenKind::Divide => todo!(),
			BaseTokenKind::Contains => todo!(),
			BaseTokenKind::NotContains => todo!(),
			BaseTokenKind::Inside => todo!(),
			BaseTokenKind::NotInside => todo!(),
			BaseTokenKind::ContainsAll => todo!(),
			BaseTokenKind::ContainsAny => todo!(),
			BaseTokenKind::ContainsNone => todo!(),
			BaseTokenKind::AllInside => todo!(),
			BaseTokenKind::AnyInside => todo!(),
			BaseTokenKind::NoneInside => todo!(),
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
			BaseTokenKind::KwConcurrently => "keyword `CONCURRENTLY`",
			BaseTokenKind::KwConfig => "keyword `CONFIG`",
			BaseTokenKind::KwContent => "keyword `CONTENT`",
			BaseTokenKind::KwContinue => "keyword `CONTINUE`",
			BaseTokenKind::KwComputed => "keyword `COMPUTED`",
			BaseTokenKind::KwCreate => "keyword `CREATE`",
			BaseTokenKind::KwDatabase => "keyword `DATABASE`",
			BaseTokenKind::KwDefault => "keyword `DEFAULT`",
			BaseTokenKind::KwDefine => "keyword `DEFINE`",
			BaseTokenKind::KwDelete => "keyword `DELETE`",
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
			BaseTokenKind::KwFrom => "keyword `FROM`",
			BaseTokenKind::KwFull => "keyword `FULL`",
			BaseTokenKind::KwFulltext => "keyword `FULLTEXT`",
			BaseTokenKind::KwFunction => "keyword `FUNCTION`",
			BaseTokenKind::KwFunctions => "keyword `FUNCTIONS`",
			BaseTokenKind::KwGrant => "keyword `GRANT`",
			BaseTokenKind::KwGraphql => "keyword `GRAPHQL`",
			BaseTokenKind::KwGroup => "keyword `GROUP`",
			BaseTokenKind::KwHeaders => "keyword `HEADERS`",
			BaseTokenKind::KwHighlights => "keyword `HIGHLIGHTS`",
			BaseTokenKind::KwHnsw => "keyword `HNSW`",
			BaseTokenKind::KwIgnore => "keyword `IGNORE`",
			BaseTokenKind::KwInclude => "keyword `INCLUDE`",
			BaseTokenKind::KwIndex => "keyword `INDEX`",
			BaseTokenKind::KwInfo => "keyword `INFO`",
			BaseTokenKind::KwInsert => "keyword `INSERT`",
			BaseTokenKind::KwInto => "keyword `INTO`",
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
			BaseTokenKind::KwMiddleware => "keyword `MIDDLEWARE`",
			BaseTokenKind::KwML => "keyword `ML`",
			BaseTokenKind::KwMerge => "keyword `MERGE`",
			BaseTokenKind::KwModel => "keyword `MODEL`",
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
			BaseTokenKind::KwStructure => "keyword `STRUCTURE`",
			BaseTokenKind::KwTable => "keyword `TABLE`",
			BaseTokenKind::KwTables => "keyword `TABLES`",
			BaseTokenKind::KwTempFiles => "keyword `TEMPFILES`",
			BaseTokenKind::KwTermsCache => "keyword `TERMSCACHE`",
			BaseTokenKind::KwTermsOrder => "keyword `TERMSORDER`",
			BaseTokenKind::KwThen => "keyword `THEN`",
			BaseTokenKind::KwThrow => "keyword `THROW`",
			BaseTokenKind::KwTimeout => "keyword `TIMEOUT`",
			BaseTokenKind::KwTo => "keyword `TO`",
			BaseTokenKind::KwTokenizers => "keyword `TOKENIZERS`",
			BaseTokenKind::KwToken => "keyword `TOKEN`",
			BaseTokenKind::KwTransaction => "keyword `TRANSACTION`",
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
			BaseTokenKind::KwArabic => "keyword `ARABIC`",
			BaseTokenKind::KwDanish => "keyword `DANISH`",
			BaseTokenKind::KwDutch => "keyword `DUTCH`",
			BaseTokenKind::KwEnglish => "keyword `ENGLISH`",
			BaseTokenKind::KwFinnish => "keyword `FINNISH`",
			BaseTokenKind::KwFrench => "keyword `FRENCH`",
			BaseTokenKind::KwGerman => "keyword `GERMAN`",
			BaseTokenKind::KwGreek => "keyword `GREEK`",
			BaseTokenKind::KwHungarian => "keyword `HUNGARIAN`",
			BaseTokenKind::KwItalian => "keyword `ITALIAN`",
			BaseTokenKind::KwNorwegian => "keyword `NORWEGIAN`",
			BaseTokenKind::KwPortuguese => "keyword `PORTUGUESE`",
			BaseTokenKind::KwRomanian => "keyword `ROMANIAN`",
			BaseTokenKind::KwRussian => "keyword `RUSSIAN`",
			BaseTokenKind::KwSpanish => "keyword `SPANISH`",
			BaseTokenKind::KwSwedish => "keyword `SWEDISH`",
			BaseTokenKind::KwTamil => "keyword `TAMIL`",
			BaseTokenKind::KwTurkish => "keyword `TURKISH`",
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
				| Self::KwAccess
				| Self::KwAfter
				| Self::KwAlgorithm
				| Self::KwAll
				| Self::KwAlter
				| Self::KwAlways
				| Self::KwAnalyze
				| Self::KwAnalyzer
				| Self::KwApi
				| Self::KwAs | Self::KwAscending
				| Self::KwAscii
				| Self::KwAssert
				| Self::KwAt | Self::KwAuthenticate
				| Self::KwAuto
				| Self::KwBackend
				| Self::KwBatch
				| Self::KwBearer
				| Self::KwBefore
				| Self::KwBegin
				| Self::KwBlank
				| Self::KwBm25
				| Self::KwBreak
				| Self::KwBucket
				| Self::KwBy | Self::KwCamel
				| Self::KwCancel
				| Self::KwCascade
				| Self::KwChangeFeed
				| Self::KwChanges
				| Self::KwCapacity
				| Self::KwClass
				| Self::KwComment
				| Self::KwCommit
				| Self::KwConcurrently
				| Self::KwConfig
				| Self::KwContent
				| Self::KwContinue
				| Self::KwComputed
				| Self::KwCreate
				| Self::KwDatabase
				| Self::KwDefault
				| Self::KwDefine
				| Self::KwDelete
				| Self::KwDescending
				| Self::KwDiff
				| Self::KwDimension
				| Self::KwDistance
				| Self::KwDocIdsCache
				| Self::KwDocIdsOrder
				| Self::KwDocLengthsCache
				| Self::KwDocLengthsOrder
				| Self::KwDrop
				| Self::KwDuplicate
				| Self::KwEdgengram
				| Self::KwEfc
				| Self::KwEvent
				| Self::KwElse
				| Self::KwEnd
				| Self::KwEnforced
				| Self::KwExclude
				| Self::KwExists
				| Self::KwExpired
				| Self::KwExplain
				| Self::KwExpunge
				| Self::KwExtendCandidates
				| Self::KwFalse
				| Self::KwFetch
				| Self::KwField
				| Self::KwFields
				| Self::KwFilters
				| Self::KwFlexible
				| Self::KwFor
				| Self::KwFrom
				| Self::KwFull
				| Self::KwFulltext
				| Self::KwFunction
				| Self::KwFunctions
				| Self::KwGrant
				| Self::KwGraphql
				| Self::KwGroup
				| Self::KwHeaders
				| Self::KwHighlights
				| Self::KwHnsw
				| Self::KwIgnore
				| Self::KwInclude
				| Self::KwIndex
				| Self::KwInfo
				| Self::KwInsert
				| Self::KwInto
				| Self::KwIf | Self::KwIs
				| Self::KwIssuer
				| Self::KwJwt
				| Self::KwJwks
				| Self::KwKey
				| Self::KwKeepPrunedConnections
				| Self::KwKill
				| Self::KwLet
				| Self::KwLimit
				| Self::KwLive
				| Self::KwLowercase
				| Self::KwLm | Self::KwM
				| Self::KwM0 | Self::KwMapper
				| Self::KwMiddleware
				| Self::KwML | Self::KwMerge
				| Self::KwModel
				| Self::KwMTree
				| Self::KwMTreeCache
				| Self::KwNamespace
				| Self::KwNgram
				| Self::KwNo | Self::KwNoIndex
				| Self::KwNone
				| Self::KwNull
				| Self::KwNumeric
				| Self::KwOmit
				| Self::KwOn | Self::KwOnly
				| Self::KwOption
				| Self::KwOrder
				| Self::KwOriginal
				| Self::KwOverwrite
				| Self::KwParallel
				| Self::KwKwParam
				| Self::KwPasshash
				| Self::KwPassword
				| Self::KwPatch
				| Self::KwPermissions
				| Self::KwPostingsCache
				| Self::KwPostingsOrder
				| Self::KwPunct
				| Self::KwPurge
				| Self::KwRange
				| Self::KwReadonly
				| Self::KwReject
				| Self::KwRelate
				| Self::KwRelation
				| Self::KwRebuild
				| Self::KwReference
				| Self::KwRefresh
				| Self::KwRemove
				| Self::KwReplace
				| Self::KwReturn
				| Self::KwRevoke
				| Self::KwRevoked
				| Self::KwRoles
				| Self::KwRoot
				| Self::KwSchemafull
				| Self::KwSchemaless
				| Self::KwScope
				| Self::KwSearch
				| Self::KwSelect
				| Self::KwSequence
				| Self::KwSession
				| Self::KwSet
				| Self::KwShow
				| Self::KwSignin
				| Self::KwSignup
				| Self::KwSince
				| Self::KwSleep
				| Self::KwSnowball
				| Self::KwSplit
				| Self::KwStart
				| Self::KwStructure
				| Self::KwTable
				| Self::KwTables
				| Self::KwTempFiles
				| Self::KwTermsCache
				| Self::KwTermsOrder
				| Self::KwThen
				| Self::KwThrow
				| Self::KwTimeout
				| Self::KwTo | Self::KwTokenizers
				| Self::KwToken
				| Self::KwTransaction
				| Self::KwTrue
				| Self::KwType
				| Self::KwUnique
				| Self::KwUnset
				| Self::KwUpdate
				| Self::KwUpsert
				| Self::KwUppercase
				| Self::KwUrl
				| Self::KwUse
				| Self::KwUser
				| Self::KwValue
				| Self::KwValues
				| Self::KwVersion
				| Self::KwVs | Self::KwWhen
				| Self::KwWhere
				| Self::KwWith
				| Self::KwAllInside
				| Self::KwAndKw
				| Self::KwAnyInside
				| Self::KwInside
				| Self::KwIntersects
				| Self::KwNoneInside
				| Self::KwNotInside
				| Self::KwOrKw
				| Self::KwOutside
				| Self::KwNot
				| Self::KwAnd
				| Self::KwCollate
				| Self::KwContainsAll
				| Self::KwContainsAny
				| Self::KwContainsNone
				| Self::KwContainsNot
				| Self::KwContains
				| Self::KwIn | Self::KwOut
				| Self::KwNormal
				| Self::KwAny
				| Self::KwArray
				| Self::KwGeometry
				| Self::KwRecord
				| Self::KwBool
				| Self::KwBytes
				| Self::KwDatetime
				| Self::KwDecimal
				| Self::KwDuration
				| Self::KwFloat
				| Self::KwInt
				| Self::KwNumber
				| Self::KwObject
				| Self::KwRegex
				| Self::KwString
				| Self::KwUuid
				| Self::KwUlid
				| Self::KwRand
				| Self::KwReferences
				| Self::KwFeature
				| Self::KwLine
				| Self::KwPoint
				| Self::KwPolygon
				| Self::KwMultiPoint
				| Self::KwMultiLine
				| Self::KwMultiPolygon
				| Self::KwCollection
				| Self::KwFile
				| Self::KwArabic
				| Self::KwDanish
				| Self::KwDutch
				| Self::KwEnglish
				| Self::KwFinnish
				| Self::KwFrench
				| Self::KwGerman
				| Self::KwGreek
				| Self::KwHungarian
				| Self::KwItalian
				| Self::KwNorwegian
				| Self::KwPortuguese
				| Self::KwRomanian
				| Self::KwRussian
				| Self::KwSpanish
				| Self::KwSwedish
				| Self::KwTamil
				| Self::KwTurkish
				| Self::KwEdDSA
				| Self::KwEs256
				| Self::KwEs384
				| Self::KwEs512
				| Self::KwHs256
				| Self::KwHs384
				| Self::KwHs512
				| Self::KwPs256
				| Self::KwPs384
				| Self::KwPs512
				| Self::KwRs256
				| Self::KwRs384
				| Self::KwRs512
				| Self::KwChebyshev
				| Self::KwCosine
				| Self::KwEuclidean
				| Self::KwJaccard
				| Self::KwHamming
				| Self::KwManhattan
				| Self::KwMinkowski
				| Self::KwPearson
				| Self::KwF64
				| Self::KwF32
				| Self::KwI64
				| Self::KwI32
				| Self::KwI16
				| Self::KwGet
				| Self::KwPost
				| Self::KwPut
				| Self::KwTrace
		)
	}
}

impl fmt::Display for BaseTokenKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.description())
	}
}
