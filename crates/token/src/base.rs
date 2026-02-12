use std::fmt;

use logos::{Lexer, Logos};

use crate::{Joined, LexError};

fn whitespace_callback(lexer: &mut Lexer<BaseTokenKind>) {
	lexer.extras = Joined::Seperated;
}

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
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
	#[regex(r"\$\p{XID_Continue}+", priority = 3)]
	Param,
	#[regex(r"(?&backtick_ident)")]
	#[regex(r"(?&bracket_ident)")]
	#[regex(r"\p{XID_Start}\p{XID_Continue}*")]
	Ident,

	#[regex(r"NaN")]
	NaN,
	#[regex(r"(?:\+)?Infinity")]
	PosInfinity,
	#[regex(r"-Infinity")]
	NegInfinity,
	#[regex(r"[0-9]+f")]
	#[regex(r"[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?(f)?")]
	Float,
	#[regex(r"[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?dec")]
	Decimal,
	#[regex(r"[0-9]+", priority = 3)]
	Int,
}

impl BaseTokenKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			BaseTokenKind::OpenParen => "(",
			BaseTokenKind::CloseParen => ")",
			_ => todo!(),
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
				| Self::KwFn | Self::KwInt
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
		f.write_str(self.as_str())
	}
}
