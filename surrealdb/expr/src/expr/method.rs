//! The static mirror of the builtin method dispatch tables.
//!
//! Method-call syntax (`value.name(args)`) dispatches to a builtin function
//! when `name` is a registered method, and otherwise falls through to a
//! closure stored under that name on the receiver. Read-only classification
//! has to make that distinction before any engine resolves the call: a
//! builtin method writes nothing the classifier cannot already see in its
//! arguments, while an unregistered name invokes a runtime value whose body
//! is invisible, so it must be over-approximated as writable.
//!
//! [`BUILTIN_METHOD_NAMES`] mirrors the names registered by the executor's
//! method registry (`build_method_registry` in the core crate), which cannot
//! be consulted from here without inverting the crate dependency. A test in
//! that module asserts the two stay identical.
//!
//! It lives in this layer rather than the expression layer because both the
//! surface AST and the expression tree classify a method call the same way.

/// Every method name that dispatches to a builtin function, sorted so
/// membership is a binary search.
pub const BUILTIN_METHOD_NAMES: &[&str] = &[
	"abs",
	"acos",
	"acot",
	"add",
	"age",
	"all",
	"any",
	"append",
	"area",
	"asin",
	"at",
	"atan",
	"bearing",
	"boolean_and",
	"boolean_not",
	"boolean_or",
	"boolean_xor",
	"bucket",
	"capitalize",
	"ceil",
	"centroid",
	"chain",
	"clump",
	"combine",
	"complement",
	"concat",
	"contains",
	"copy",
	"copy_if_not_exists",
	"cos",
	"cot",
	"day",
	"days",
	"deg2rad",
	"delete",
	"diff",
	"difference",
	"distance",
	"distance_damerau_levenshtein",
	"distance_hamming",
	"distance_levenshtein",
	"distance_normalized_damerau_levenshtein",
	"distance_normalized_levenshtein",
	"distinct",
	"ends_with",
	"entries",
	"every",
	"exists",
	"expect",
	"extend",
	"extract",
	"fill",
	"filter",
	"filter_index",
	"find",
	"find_index",
	"first",
	"flatten",
	"floor",
	"fold",
	"format",
	"get",
	"group",
	"hash_decode",
	"hash_encode",
	"head",
	"hour",
	"hours",
	"html_encode",
	"html_sanitize",
	"id",
	"includes",
	"index_of",
	"insert",
	"intersect",
	"is_alpha",
	"is_alphanum",
	"is_array",
	"is_ascii",
	"is_bool",
	"is_bytes",
	"is_collection",
	"is_date",
	"is_datetime",
	"is_decimal",
	"is_domain",
	"is_duration",
	"is_edge",
	"is_email",
	"is_empty",
	"is_float",
	"is_geometry",
	"is_hexadecimal",
	"is_int",
	"is_interval",
	"is_ip",
	"is_ipv4",
	"is_ipv6",
	"is_latitude",
	"is_leap_year",
	"is_line",
	"is_longitude",
	"is_multiline",
	"is_multipoint",
	"is_multipolygon",
	"is_none",
	"is_null",
	"is_number",
	"is_numeric",
	"is_object",
	"is_point",
	"is_polygon",
	"is_range",
	"is_record",
	"is_semver",
	"is_set",
	"is_string",
	"is_time",
	"is_timestamp",
	"is_ulid",
	"is_url",
	"is_uuid",
	"is_valid",
	"join",
	"justify_days",
	"justify_hours",
	"justify_interval",
	"key",
	"keys",
	"last",
	"len",
	"list",
	"ln",
	"log",
	"log10",
	"log2",
	"logical_and",
	"logical_or",
	"logical_xor",
	"lowercase",
	"map",
	"matches",
	"max",
	"micros",
	"millis",
	"min",
	"mins",
	"minute",
	"month",
	"months",
	"nano",
	"nanos",
	"patch",
	"pop",
	"prepend",
	"presign_get",
	"presign_put",
	"push",
	"put",
	"put_if_not_exists",
	"rad2deg",
	"reduce",
	"remove",
	"rename",
	"rename_if_not_exists",
	"repeat",
	"replace",
	"reverse",
	"round",
	"second",
	"secs",
	"semver_compare",
	"semver_inc_major",
	"semver_inc_minor",
	"semver_inc_patch",
	"semver_major",
	"semver_minor",
	"semver_patch",
	"semver_set_major",
	"semver_set_minor",
	"semver_set_patch",
	"set_day",
	"set_hour",
	"set_minute",
	"set_month",
	"set_nanosecond",
	"set_second",
	"set_year",
	"shuffle",
	"sign",
	"similarity_fuzzy",
	"similarity_jaro",
	"similarity_jaro_winkler",
	"similarity_smithwaterman",
	"similarity_sorensen_dice",
	"sin",
	"slice",
	"slug",
	"some",
	"sort",
	"sort_asc",
	"sort_desc",
	"sort_lexical",
	"sort_natural",
	"sort_natural_lexical",
	"split",
	"starts_with",
	"swap",
	"table",
	"tan",
	"tb",
	"to_array",
	"to_bool",
	"to_bytes",
	"to_date",
	"to_datetime",
	"to_decimal",
	"to_duration",
	"to_float",
	"to_geometry",
	"to_int",
	"to_interval",
	"to_number",
	"to_point",
	"to_range",
	"to_record",
	"to_set",
	"to_string",
	"to_string_lossy",
	"to_time",
	"to_timestamp",
	"to_uuid",
	"transpose",
	"trim",
	"trunc",
	"type_of",
	"union",
	"unix",
	"uppercase",
	"values",
	"vector_add",
	"vector_angle",
	"vector_cross",
	"vector_distance_chebyshev",
	"vector_distance_cosine",
	"vector_distance_euclidean",
	"vector_distance_hamming",
	"vector_distance_mahalanobis",
	"vector_distance_manhattan",
	"vector_distance_minkowski",
	"vector_divide",
	"vector_dot",
	"vector_magnitude",
	"vector_multiply",
	"vector_normalize",
	"vector_project",
	"vector_scale",
	"vector_similarity_cosine",
	"vector_similarity_jaccard",
	"vector_similarity_pearson",
	"vector_similarity_spearman",
	"vector_subtract",
	"vector_sum",
	"wday",
	"week",
	"weeks",
	"windows",
	"words",
	"yday",
	"year",
	"years",
];

/// Whether `name` in `value.name(args)` dispatches to a builtin function
/// rather than a closure stored on the receiver.
pub fn is_builtin_method(name: &str) -> bool {
	BUILTIN_METHOD_NAMES.binary_search(&name).is_ok()
}

/// Whether the named builtin can evaluate arbitrary nested statements, and so
/// needs a write transaction whatever its arguments look like.
///
/// This is **not** the question of whether a builtin changes anything. Several
/// do without evaluating a statement — advancing a sequence, writing to a
/// bucket, a `POST` — and they are not listed here, because what they change is
/// not staged through the caller's transaction and needs no write transaction
/// to reach. A caller asking "does this mutate state" wants a different
/// predicate.
///
/// Both `read_only` predicates and the planner's access-mode choice ask this
/// one, so a builtin added to any of them is added to all three.
pub fn is_writer_builtin(name: &str) -> bool {
	matches!(name, "api::invoke" | "eval::gql" | "eval::surql")
}

/// The builtins that leave something behind once the statement is done: a write
/// staged through the transaction, a write outside it, or a counter advanced.
///
/// Read by the runtime write refusal (`Options::no_write`), which guards a
/// context that may not modify state — a `SELECT` PERMISSIONS predicate, a
/// `COMPUTED` field body, a Surrealism function that declared a read-only
/// transaction. The statement forms are classified by
/// `Expr::is_mutation_statement`; this is the other half, because a call
/// reaches the datastore without any statement appearing in the expression at
/// all: `RETURN sequence::nextval('n')` advances a global counter, and
/// `file::put` writes to a bucket.
///
/// Kept sorted, and pinned against `EffectSet::mutates_state` by
/// `every_effect_declaration_is_pinned` in the exec function registry — so a
/// builtin that declares a mutating effect and is missing here is a build
/// failure, not a silent hole.
pub fn mutates_state_builtin(name: &str) -> bool {
	MUTATING.binary_search(&name).is_ok()
}

/// The table [`mutates_state_builtin`] searches.
///
/// Module-level rather than function-local so the sortedness guard can assert
/// about *this* array. A copy of it in the test would let an entry added in the
/// wrong position pass the guard while `binary_search` answered "does not
/// mutate" — the permissive direction.
const MUTATING: &[&str] = &[
	"api::invoke",
	"eval::gql",
	"eval::surql",
	"file::copy",
	"file::copy_if_not_exists",
	"file::delete",
	"file::put",
	"file::put_if_not_exists",
	"file::rename",
	"file::rename_if_not_exists",
	"http::delete",
	"http::patch",
	"http::post",
	"http::put",
	"sequence::nextval",
];

#[cfg(test)]
mod tests {
	use super::{BUILTIN_METHOD_NAMES, MUTATING};

	/// Membership is a binary search, which is only correct on a sorted,
	/// duplicate-free table.
	#[test]
	fn the_name_table_is_sorted_and_unique() {
		for pair in BUILTIN_METHOD_NAMES.windows(2) {
			assert!(pair[0] < pair[1], "{:?} must sort strictly before {:?}", pair[0], pair[1]);
		}
	}

	/// `mutates_state_builtin` is a binary search too, and an unsorted entry
	/// would read as "does not mutate" — the permissive direction.
	///
	/// Asserted about `MUTATING` itself, not a copy of its contents: a copy
	/// would stay sorted while the array it stands for did not.
	#[test]
	fn the_mutating_table_is_sorted_and_unique() {
		for pair in MUTATING.windows(2) {
			assert!(pair[0] < pair[1], "{:?} must sort strictly before {:?}", pair[0], pair[1]);
		}
		for name in MUTATING {
			assert!(super::mutates_state_builtin(name), "{name}");
		}
		assert!(!super::mutates_state_builtin("math::abs"));
		assert!(!super::mutates_state_builtin("http::get"));
	}
}
