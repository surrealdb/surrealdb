#[macro_export]
macro_rules! T {
	(;) => {
		$crate::BaseTokenKind::SemiColon
	};
	(,) => {
		$crate::BaseTokenKind::Comma
	};
	(@) => {
		$crate::BaseTokenKind::At
	};
	(/) => {
		$crate::BaseTokenKind::Slash
	};
	(%) => {
		$crate::BaseTokenKind::Percent
	};
	(|) => {
		$crate::BaseTokenKind::HLine
	};
	(||) => {
		$crate::BaseTokenKind::HLineHLine
	};
	(|>) => {
		$crate::BaseTokenKind::HLineRightShevron
	};

	(&&) => {
		$crate::BaseTokenKind::AndAnd
	};

	(.) => {
		$crate::BaseTokenKind::Dot
	};
	(..) => {
		$crate::BaseTokenKind::DotDot
	};
	(...) => {
		$crate::BaseTokenKind::DotDotDot
	};

	(!) => {
		$crate::BaseTokenKind::Exclaim
	};
	(!=) => {
		$crate::BaseTokenKind::ExclaimEq
	};

	(?) => {
		$crate::BaseTokenKind::Question
	};
	(?=) => {
		$crate::BaseTokenKind::QuestionEqual
	};
	(?:) => {
		$crate::BaseTokenKind::QuestionColon
	};

	(<) => {
		$crate::BaseTokenKind::LeftShevron
	};
	(<=) => {
		$crate::BaseTokenKind::LeftShevronEqual
	};
	(<|) => {
		$crate::BaseTokenKind::LeftShevronHLine
	};

	(>) => {
		$crate::BaseTokenKind::RightShevron
	};
	(>=) => {
		$crate::BaseTokenKind::RightShevronEqual
	};

	(-) => {
		$crate::BaseTokenKind::Dash
	};
	(-=) => {
		$crate::BaseTokenKind::DashEqual
	};
	(->) => {
		$crate::BaseTokenKind::DashRightShevron
	};

	(+) => {
		$crate::BaseTokenKind::Plus
	};
	(+=) => {
		$crate::BaseTokenKind::PlusEqual
	};
	(+?=) => {
		$crate::BaseTokenKind::PlusQuestionEqual
	};

	(*) => {
		$crate::BaseTokenKind::Star
	};
	(*=) => {
		$crate::BaseTokenKind::StarEqual
	};
	(**) => {
		$crate::BaseTokenKind::StarStar
	};

	(=) => {
		$crate::BaseTokenKind::Equal
	};
	(==) => {
		$crate::BaseTokenKind::EqualEqual
	};

	(:) => {
		$crate::BaseTokenKind::Colon
	};
	(::) => {
		$crate::BaseTokenKind::ColonColon
	};

	($) => {
		$crate::BaseTokenKind::Dollar
	};

	(ACCESS) => {
		$crate::BaseTokenKind::KwAccess
	};
	(AFTER) => {
		$crate::BaseTokenKind::KwAfter
	};
	(ALGORITHM) => {
		$crate::BaseTokenKind::KwAlgorithm
	};
	(ALL) => {
		$crate::BaseTokenKind::KwAll
	};
	(ALTER) => {
		$crate::BaseTokenKind::KwAlter
	};
	(ALWAYS) => {
		$crate::BaseTokenKind::KwAlways
	};
	(ANALYZE) => {
		$crate::BaseTokenKind::KwAnalyze
	};
	(ANALYZER) => {
		$crate::BaseTokenKind::KwAnalyzer
	};
	(API) => {
		$crate::BaseTokenKind::KwApi
	};
	(AS) => {
		$crate::BaseTokenKind::KwAs
	};
	(ASCENDING) => {
		$crate::BaseTokenKind::KwAscending
	};
	(ASCII) => {
		$crate::BaseTokenKind::KwAscii
	};
	(ASSERT) => {
		$crate::BaseTokenKind::KwAssert
	};
	(AT) => {
		$crate::BaseTokenKind::KwAt
	};
	(AUTHENTICATE) => {
		$crate::BaseTokenKind::KwAuthenticate
	};
	(AUTO) => {
		$crate::BaseTokenKind::KwAuto
	};
	(BACKEND) => {
		$crate::BaseTokenKind::KwBackend
	};
	(BATCH) => {
		$crate::BaseTokenKind::KwBatch
	};
	(BEARER) => {
		$crate::BaseTokenKind::KwBearer
	};
	(BEFORE) => {
		$crate::BaseTokenKind::KwBefore
	};
	(BEGIN) => {
		$crate::BaseTokenKind::KwBegin
	};
	(BLANK) => {
		$crate::BaseTokenKind::KwBlank
	};
	(BM25) => {
		$crate::BaseTokenKind::KwBm25
	};
	(BREAK) => {
		$crate::BaseTokenKind::KwBreak
	};
	(BUCKET) => {
		$crate::BaseTokenKind::KwBucket
	};
	(BY) => {
		$crate::BaseTokenKind::KwBy
	};
	(CAMEL) => {
		$crate::BaseTokenKind::KwCamel
	};
	(CANCEL) => {
		$crate::BaseTokenKind::KwCancel
	};
	(CASCADE) => {
		$crate::BaseTokenKind::KwCascade
	};
	(CHANGEFEED) => {
		$crate::BaseTokenKind::KwChangeFeed
	};
	(CHANGES) => {
		$crate::BaseTokenKind::KwChanges
	};
	(CAPACITY) => {
		$crate::BaseTokenKind::KwCapacity
	};
	(CLASS) => {
		$crate::BaseTokenKind::KwClass
	};
	(COMMENT) => {
		$crate::BaseTokenKind::KwComment
	};
	(COMMIT) => {
		$crate::BaseTokenKind::KwCommit
	};
	(CONCURRENTLY) => {
		$crate::BaseTokenKind::KwConcurrently
	};
	(CONFIG) => {
		$crate::BaseTokenKind::KwConfig
	};
	(CONTENT) => {
		$crate::BaseTokenKind::KwContent
	};
	(CONTINUE) => {
		$crate::BaseTokenKind::KwContinue
	};
	(COMPUTED) => {
		$crate::BaseTokenKind::KwComputed
	};
	(CREATE) => {
		$crate::BaseTokenKind::KwCreate
	};
	(DATABASE) => {
		$crate::BaseTokenKind::KwDatabase
	};
	(DEFAULT) => {
		$crate::BaseTokenKind::KwDefault
	};
	(DEFINE) => {
		$crate::BaseTokenKind::KwDefine
	};
	(DELETE) => {
		$crate::BaseTokenKind::KwDelete
	};
	(DESCENDING) => {
		$crate::BaseTokenKind::KwDescending
	};
	(DIFF) => {
		$crate::BaseTokenKind::KwDiff
	};
	(DIMENSION) => {
		$crate::BaseTokenKind::KwDimension
	};
	(DISTANCE) => {
		$crate::BaseTokenKind::KwDistance
	};
	(DOC_IDS_CACHE) => {
		$crate::BaseTokenKind::KwDocIdsCache
	};
	(DOC_IDS_ORDER) => {
		$crate::BaseTokenKind::KwDocIdsOrder
	};
	(DOC_LENGTHS_CACHE) => {
		$crate::BaseTokenKind::KwDocLengthsCache
	};
	(DOC_LENGTHS_ORDER) => {
		$crate::BaseTokenKind::KwDocLengthsOrder
	};
	(DROP) => {
		$crate::BaseTokenKind::KwDrop
	};
	(DUPLICATE) => {
		$crate::BaseTokenKind::KwDuplicate
	};
	(EDGENGRAM) => {
		$crate::BaseTokenKind::KwEdgengram
	};
	(EFC) => {
		$crate::BaseTokenKind::KwEfc
	};
	(EVENT) => {
		$crate::BaseTokenKind::KwEvent
	};
	(ELSE) => {
		$crate::BaseTokenKind::KwElse
	};
	(END) => {
		$crate::BaseTokenKind::KwEnd
	};
	(ENFORCED) => {
		$crate::BaseTokenKind::KwEnforced
	};
	(EXCLUDE) => {
		$crate::BaseTokenKind::KwExclude
	};
	(EXISTS) => {
		$crate::BaseTokenKind::KwExists
	};
	(EXPIRED) => {
		$crate::BaseTokenKind::KwExpired
	};
	(EXPLAIN) => {
		$crate::BaseTokenKind::KwExplain
	};
	(EXPUNGE) => {
		$crate::BaseTokenKind::KwExpunge
	};
	(EXTEND_CANDIDATES) => {
		$crate::BaseTokenKind::KwExtendCandidates
	};
	(false) => {
		$crate::BaseTokenKind::KwFalse
	};
	(FETCH) => {
		$crate::BaseTokenKind::KwFetch
	};
	(FIELD) => {
		$crate::BaseTokenKind::KwField
	};
	(FIELDS) => {
		$crate::BaseTokenKind::KwFields
	};
	(FILTERS) => {
		$crate::BaseTokenKind::KwFilters
	};
	(FLEXIBLE) => {
		$crate::BaseTokenKind::KwFlexible
	};
	(FOR) => {
		$crate::BaseTokenKind::KwFor
	};
	(FROM) => {
		$crate::BaseTokenKind::KwFrom
	};
	(FULL) => {
		$crate::BaseTokenKind::KwFull
	};
	(FULLTEXT) => {
		$crate::BaseTokenKind::KwFulltext
	};
	(FUNCTION) => {
		$crate::BaseTokenKind::KwFunction
	};
	(FUNCTIONS) => {
		$crate::BaseTokenKind::KwFunctions
	};
	(GRANT) => {
		$crate::BaseTokenKind::KwGrant
	};
	(GRAPHQL) => {
		$crate::BaseTokenKind::KwGraphql
	};
	(GROUP) => {
		$crate::BaseTokenKind::KwGroup
	};
	(HEADERS) => {
		$crate::BaseTokenKind::KwHeaders
	};
	(HIGHLIGHTS) => {
		$crate::BaseTokenKind::KwHighlights
	};
	(HNSW) => {
		$crate::BaseTokenKind::KwHnsw
	};
	(IGNORE) => {
		$crate::BaseTokenKind::KwIgnore
	};
	(INCLUDE) => {
		$crate::BaseTokenKind::KwInclude
	};
	(INDEX) => {
		$crate::BaseTokenKind::KwIndex
	};
	(INFO) => {
		$crate::BaseTokenKind::KwInfo
	};
	(INSERT) => {
		$crate::BaseTokenKind::KwInsert
	};
	(INTO) => {
		$crate::BaseTokenKind::KwInto
	};
	(IF) => {
		$crate::BaseTokenKind::KwIf
	};
	(IS) => {
		$crate::BaseTokenKind::KwIs
	};
	(ISSUER) => {
		$crate::BaseTokenKind::KwIssuer
	};
	(JWT) => {
		$crate::BaseTokenKind::KwJwt
	};
	(JWKS) => {
		$crate::BaseTokenKind::KwJwks
	};
	(KEY) => {
		$crate::BaseTokenKind::KwKey
	};
	(KEEP_PRUNED_CONNECTIONS) => {
		$crate::BaseTokenKind::KwKeepPrunedConnections
	};
	(KILL) => {
		$crate::BaseTokenKind::KwKill
	};
	(LET) => {
		$crate::BaseTokenKind::KwLet
	};
	(LIMIT) => {
		$crate::BaseTokenKind::KwLimit
	};
	(LIVE) => {
		$crate::BaseTokenKind::KwLive
	};
	(LOWERCASE) => {
		$crate::BaseTokenKind::KwLowercase
	};
	(LM) => {
		$crate::BaseTokenKind::KwLm
	};
	(M) => {
		$crate::BaseTokenKind::KwM
	};
	(M0) => {
		$crate::BaseTokenKind::KwM0
	};
	(MAPPER) => {
		$crate::BaseTokenKind::KwMapper
	};
	(MIDDLEWARE) => {
		$crate::BaseTokenKind::KwMiddleware
	};
	(ML) => {
		$crate::BaseTokenKind::KwML
	};
	(MERGE) => {
		$crate::BaseTokenKind::KwMerge
	};
	(MODEL) => {
		$crate::BaseTokenKind::KwModel
	};
	(MTREE) => {
		$crate::BaseTokenKind::KwMTree
	};
	(MTREE_CACHE) => {
		$crate::BaseTokenKind::KwMTreeCache
	};
	(NAMESPACE) => {
		$crate::BaseTokenKind::KwNamespace
	};
	(NGRAM) => {
		$crate::BaseTokenKind::KwNgram
	};
	(NO) => {
		$crate::BaseTokenKind::KwNo
	};
	(NOINDEX) => {
		$crate::BaseTokenKind::KwNoIndex
	};
	(NONE) => {
		$crate::BaseTokenKind::KwNone
	};
	(NULL) => {
		$crate::BaseTokenKind::KwNull
	};
	(NUMERIC) => {
		$crate::BaseTokenKind::KwNumeric
	};
	(OMIT) => {
		$crate::BaseTokenKind::KwOmit
	};
	(ON) => {
		$crate::BaseTokenKind::KwOn
	};
	(ONLY) => {
		$crate::BaseTokenKind::KwOnly
	};
	(OPTION) => {
		$crate::BaseTokenKind::KwOption
	};
	(ORDER) => {
		$crate::BaseTokenKind::KwOrder
	};
	(ORIGINAL) => {
		$crate::BaseTokenKind::KwOriginal
	};
	(OVERWRITE) => {
		$crate::BaseTokenKind::KwOverwrite
	};
	(PARALLEL) => {
		$crate::BaseTokenKind::KwParallel
	};
	(PARAM) => {
		$crate::BaseTokenKind::KwKwParam
	};
	(PASSHASH) => {
		$crate::BaseTokenKind::KwPasshash
	};
	(PASSWORD) => {
		$crate::BaseTokenKind::KwPassword
	};
	(PATCH) => {
		$crate::BaseTokenKind::KwPatch
	};
	(PERMISSIONS) => {
		$crate::BaseTokenKind::KwPermissions
	};
	(POSTINGS_CACHE) => {
		$crate::BaseTokenKind::KwPostingsCache
	};
	(POSTINGS_ORDER) => {
		$crate::BaseTokenKind::KwPostingsOrder
	};
	(PUNCT) => {
		$crate::BaseTokenKind::KwPunct
	};
	(PURGE) => {
		$crate::BaseTokenKind::KwPurge
	};
	(RANGE) => {
		$crate::BaseTokenKind::KwRange
	};
	(READONLY) => {
		$crate::BaseTokenKind::KwReadonly
	};
	(REJECT) => {
		$crate::BaseTokenKind::KwReject
	};
	(RELATE) => {
		$crate::BaseTokenKind::KwRelate
	};
	(RELATION) => {
		$crate::BaseTokenKind::KwRelation
	};
	(REBUILD) => {
		$crate::BaseTokenKind::KwRebuild
	};
	(REFERENCE) => {
		$crate::BaseTokenKind::KwReference
	};
	(REFRESH) => {
		$crate::BaseTokenKind::KwRefresh
	};
	(REMOVE) => {
		$crate::BaseTokenKind::KwRemove
	};
	(REPLACE) => {
		$crate::BaseTokenKind::KwReplace
	};
	(RETURN) => {
		$crate::BaseTokenKind::KwReturn
	};
	(REVOKE) => {
		$crate::BaseTokenKind::KwRevoke
	};
	(REVOKED) => {
		$crate::BaseTokenKind::KwRevoked
	};
	(ROLES) => {
		$crate::BaseTokenKind::KwRoles
	};
	(ROOT) => {
		$crate::BaseTokenKind::KwRoot
	};
	(SCHEMAFULL) => {
		$crate::BaseTokenKind::KwSchemafull
	};
	(SCHEMALESS) => {
		$crate::BaseTokenKind::KwSchemaless
	};
	(SCOPE) => {
		$crate::BaseTokenKind::KwScope
	};
	(SEARCH) => {
		$crate::BaseTokenKind::KwSearch
	};
	(SELECT) => {
		$crate::BaseTokenKind::KwSelect
	};
	(SEQUENCE) => {
		$crate::BaseTokenKind::KwSequence
	};
	(SESSION) => {
		$crate::BaseTokenKind::KwSession
	};
	(SET) => {
		$crate::BaseTokenKind::KwSet
	};
	(SHOW) => {
		$crate::BaseTokenKind::KwShow
	};
	(SIGNIN) => {
		$crate::BaseTokenKind::KwSignin
	};
	(SIGNUP) => {
		$crate::BaseTokenKind::KwSignup
	};
	(SINCE) => {
		$crate::BaseTokenKind::KwSince
	};
	(SLEEP) => {
		$crate::BaseTokenKind::KwSleep
	};
	(SNOWBALL) => {
		$crate::BaseTokenKind::KwSnowball
	};
	(SPLIT) => {
		$crate::BaseTokenKind::KwSplit
	};
	(START) => {
		$crate::BaseTokenKind::KwStart
	};
	(STRUCTURE) => {
		$crate::BaseTokenKind::KwStructure
	};
	(TABLE) => {
		$crate::BaseTokenKind::KwTable
	};
	(TABLES) => {
		$crate::BaseTokenKind::KwTables
	};
	(TEMPFILES) => {
		$crate::BaseTokenKind::KwTempFiles
	};
	(TERMS_CACHE) => {
		$crate::BaseTokenKind::KwTermsCache
	};
	(TERMS_ORDER) => {
		$crate::BaseTokenKind::KwTermsOrder
	};
	(THEN) => {
		$crate::BaseTokenKind::KwThen
	};
	(THROW) => {
		$crate::BaseTokenKind::KwThrow
	};
	(TIMEOUT) => {
		$crate::BaseTokenKind::KwTimeout
	};
	(TO) => {
		$crate::BaseTokenKind::KwTo
	};
	(TOKENIZERS) => {
		$crate::BaseTokenKind::KwTokenizers
	};
	(TOKEN) => {
		$crate::BaseTokenKind::KwToken
	};
	(TRANSACTION) => {
		$crate::BaseTokenKind::KwTransaction
	};
	(true) => {
		$crate::BaseTokenKind::KwTrue
	};
	(TYPE) => {
		$crate::BaseTokenKind::KwType
	};
	(UNIQUE) => {
		$crate::BaseTokenKind::KwUnique
	};
	(UNSET) => {
		$crate::BaseTokenKind::KwUnset
	};
	(UPDATE) => {
		$crate::BaseTokenKind::KwUpdate
	};
	(UPSERT) => {
		$crate::BaseTokenKind::KwUpsert
	};
	(UPPERCASE) => {
		$crate::BaseTokenKind::KwUppercase
	};
	(URL) => {
		$crate::BaseTokenKind::KwUrl
	};
	(USE) => {
		$crate::BaseTokenKind::KwUse
	};
	(USER) => {
		$crate::BaseTokenKind::KwUser
	};
	(VALUE) => {
		$crate::BaseTokenKind::KwValue
	};
	(VALUES) => {
		$crate::BaseTokenKind::KwValues
	};
	(VERSION) => {
		$crate::BaseTokenKind::KwVersion
	};
	(VS) => {
		$crate::BaseTokenKind::KwVs
	};
	(WHEN) => {
		$crate::BaseTokenKind::KwWhen
	};
	(WHERE) => {
		$crate::BaseTokenKind::KwWhere
	};
	(WITH) => {
		$crate::BaseTokenKind::KwWith
	};
	(ALLINSIDE) => {
		$crate::BaseTokenKind::KwAllInside
	};
	(ANDKW) => {
		$crate::BaseTokenKind::KwAndKw
	};
	(ANYINSIDE) => {
		$crate::BaseTokenKind::KwAnyInside
	};
	(INSIDE) => {
		$crate::BaseTokenKind::KwInside
	};
	(INTERSECTS) => {
		$crate::BaseTokenKind::KwIntersects
	};
	(NONEINSIDE) => {
		$crate::BaseTokenKind::KwNoneInside
	};
	(NOTINSIDE) => {
		$crate::BaseTokenKind::KwNotInside
	};
	(OR) => {
		$crate::BaseTokenKind::KwOrKw
	};
	(OUTSIDE) => {
		$crate::BaseTokenKind::KwOutside
	};
	(NOT) => {
		$crate::BaseTokenKind::KwNot
	};
	(AND) => {
		$crate::BaseTokenKind::KwAnd
	};
	(COLLATE) => {
		$crate::BaseTokenKind::KwCollate
	};
	(CONTAINSALL) => {
		$crate::BaseTokenKind::KwContainsAll
	};
	(CONTAINSANY) => {
		$crate::BaseTokenKind::KwContainsAny
	};
	(CONTAINSNONE) => {
		$crate::BaseTokenKind::KwContainsNone
	};
	(CONTAINSNOT) => {
		$crate::BaseTokenKind::KwContainsNot
	};
	(CONTAINS) => {
		$crate::BaseTokenKind::KwContains
	};
	(IN) => {
		$crate::BaseTokenKind::KwIn
	};
	(OUT) => {
		$crate::BaseTokenKind::KwOut
	};
	(NORMAL) => {
		$crate::BaseTokenKind::KwNormal
	};

	// Types
	(ANY) => {
		$crate::BaseTokenKind::KwAny
	};
	(ARRAY) => {
		$crate::BaseTokenKind::KwArray
	};
	(GEOMETRY) => {
		$crate::BaseTokenKind::KwGeometry
	};
	(RECORD) => {
		$crate::BaseTokenKind::KwRecord
	};
	(BOOL) => {
		$crate::BaseTokenKind::KwBool
	};
	(BYTES) => {
		$crate::BaseTokenKind::KwBytes
	};
	(DATETIME) => {
		$crate::BaseTokenKind::KwDatetime
	};
	(DECIMAL) => {
		$crate::BaseTokenKind::KwDecimal
	};
	(DURATION) => {
		$crate::BaseTokenKind::KwDuration
	};
	(FLOAT) => {
		$crate::BaseTokenKind::KwFloat
	};
	(INT) => {
		$crate::BaseTokenKind::KwInt
	};
	(NUMBER) => {
		$crate::BaseTokenKind::KwNumber
	};
	(OBJECT) => {
		$crate::BaseTokenKind::KwObject
	};
	(REGEX) => {
		$crate::BaseTokenKind::KwRegex
	};
	(STRING) => {
		$crate::BaseTokenKind::KwString
	};
	(UUID) => {
		$crate::BaseTokenKind::KwUuid
	};
	(ULID) => {
		$crate::BaseTokenKind::KwUlid
	};
	(RAND) => {
		$crate::BaseTokenKind::KwRand
	};
	(REFERENCES) => {
		$crate::BaseTokenKind::KwReferences
	};
	(FEATURE) => {
		$crate::BaseTokenKind::KwFeature
	};
	(LINE) => {
		$crate::BaseTokenKind::KwLine
	};
	(POINT) => {
		$crate::BaseTokenKind::KwPoint
	};
	(POLYGON) => {
		$crate::BaseTokenKind::KwPolygon
	};
	(MULTIPOINT) => {
		$crate::BaseTokenKind::KwMultiPoint
	};
	(MULTILINE) => {
		$crate::BaseTokenKind::KwMultiLine
	};
	(MULTIPOLYGON) => {
		$crate::BaseTokenKind::KwMultiPolygon
	};
	(COLLECTION) => {
		$crate::BaseTokenKind::KwCollection
	};
	(FILE) => {
		$crate::BaseTokenKind::KwFile
	};

	// Languages
	(ARABIC) => {
		$crate::BaseTokenKind::KwArabic
	};
	(DANISH) => {
		$crate::BaseTokenKind::KwDanish
	};
	(DUTCH) => {
		$crate::BaseTokenKind::KwDutch
	};
	(ENGLISH) => {
		$crate::BaseTokenKind::KwEnglish
	};
	(FINISH) => {
		$crate::BaseTokenKind::KwFinnish
	};
	(FRANCH) => {
		$crate::BaseTokenKind::KwFrench
	};
	(GERMAN) => {
		$crate::BaseTokenKind::KwGerman
	};
	(GREEK) => {
		$crate::BaseTokenKind::KwGreek
	};
	(HUNGRARIAN) => {
		$crate::BaseTokenKind::KwHungarian
	};
	(ITALIAN) => {
		$crate::BaseTokenKind::KwItalian
	};
	(NORWEGIAN) => {
		$crate::BaseTokenKind::KwNorwegian
	};
	(PORTUGUESE) => {
		$crate::BaseTokenKind::KwPortuguese
	};
	(ROMANIAN) => {
		$crate::BaseTokenKind::KwRomanian
	};
	(RUSSIAN) => {
		$crate::BaseTokenKind::KwRussian
	};
	(SPANISH) => {
		$crate::BaseTokenKind::KwSpanish
	};
	(SWEDISH) => {
		$crate::BaseTokenKind::KwSwedish
	};
	(TAMIL) => {
		$crate::BaseTokenKind::KwTamil
	};
	(TURKISH) => {
		$crate::BaseTokenKind::KwTurkish
	};

	// Algorithms
	(EDDSA) => {
		$crate::BaseTokenKind::KwEdDSA
	};
	(ES256) => {
		$crate::BaseTokenKind::KwEs256
	};
	(ES384) => {
		$crate::BaseTokenKind::KwEs384
	};
	(ES512) => {
		$crate::BaseTokenKind::KwEs512
	};
	(HS256) => {
		$crate::BaseTokenKind::KwHs256
	};
	(HS384) => {
		$crate::BaseTokenKind::KwHs384
	};
	(HS512) => {
		$crate::BaseTokenKind::KwHs512
	};
	(PS256) => {
		$crate::BaseTokenKind::KwPs256
	};
	(PS384) => {
		$crate::BaseTokenKind::KwPs384
	};
	(PS512) => {
		$crate::BaseTokenKind::KwPs512
	};
	(RS256) => {
		$crate::BaseTokenKind::KwRs256
	};
	(RS384) => {
		$crate::BaseTokenKind::KwRs384
	};
	(RS512) => {
		$crate::BaseTokenKind::KwRs512
	};

	// Distance
	(CHEBYSHEV) => {
		$crate::BaseTokenKind::KwChebyshev
	};
	(COSINE) => {
		$crate::BaseTokenKind::KwCosine
	};
	(EUCLIDEAN) => {
		$crate::BaseTokenKind::KwEuclidean
	};
	(JACCARD) => {
		$crate::BaseTokenKind::KwJaccard
	};
	(HAMMING) => {
		$crate::BaseTokenKind::KwHamming
	};
	(MANHATTAN) => {
		$crate::BaseTokenKind::KwManhattan
	};
	(MINKOWSKI) => {
		$crate::BaseTokenKind::KwMinkowski
	};
	(PEARSON) => {
		$crate::BaseTokenKind::KwPearson
	};

	// VectorTypes
	(F64) => {
		$crate::BaseTokenKind::KwF64
	};
	(F32) => {
		$crate::BaseTokenKind::KwF32
	};
	(I64) => {
		$crate::BaseTokenKind::KwI64
	};
	(I32) => {
		$crate::BaseTokenKind::KwI32
	};
	(I16) => {
		$crate::BaseTokenKind::KwI16
	};

	// HTTP methods
	(GET) => {
		$crate::BaseTokenKind::KwGet
	};
	(POST) => {
		$crate::BaseTokenKind::KwPost
	};
	(PUT) => {
		$crate::BaseTokenKind::KwPut
	};
	(TRACE) => {
		$crate::BaseTokenKind::KwTrace
	};
}
