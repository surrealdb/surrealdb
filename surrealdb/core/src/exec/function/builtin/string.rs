//! String functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

// Single string argument functions
define_pure_function!(StringCapitalize, "string::capitalize", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> String, crate::fnc::string::capitalize);
define_pure_function!(StringLen, "string::len", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Int, crate::fnc::string::len);
define_pure_function!(StringLowercase, "string::lowercase", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> String, crate::fnc::string::lowercase);
define_pure_function!(StringReverse, "string::reverse", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> String, crate::fnc::string::reverse);
define_pure_function!(StringSlug, "string::slug", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> String, crate::fnc::string::slug);
define_pure_function!(StringTrim, "string::trim", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> String, crate::fnc::string::trim);
define_pure_function!(StringUppercase, "string::uppercase", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> String, crate::fnc::string::uppercase);
define_pure_function!(StringWords, "string::words", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Any, crate::fnc::string::words);

// Two string argument functions
define_pure_function!(StringContains, "string::contains", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String, search: String) -> Bool, crate::fnc::string::contains);
define_pure_function!(StringEndsWith, "string::ends_with", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String, suffix: String) -> Bool, crate::fnc::string::ends_with);
define_pure_function!(StringMatches, "string::matches", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, pattern: String) -> Bool, crate::fnc::string::matches);
define_pure_function!(StringRepeat, "string::repeat", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::string::repeat_walk, (value: String, count: Int) -> String, crate::fnc::string::repeat);
define_pure_function!(StringSplit, "string::split", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String, delimiter: String) -> Any, crate::fnc::string::split);
define_pure_function!(StringStartsWith, "string::starts_with", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String, prefix: String) -> Bool, crate::fnc::string::starts_with);

// Three argument string functions
define_pure_function!(StringReplace, "string::replace", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, search: Any, replacement: String) -> String, crate::fnc::string::replace);
define_pure_function!(StringSlice, "string::slice", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, ?start: Any, ?end: Int) -> String, crate::fnc::string::slice);

// Variadic string functions
define_pure_function!(StringConcat, "string::concat", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::string::concat_walk, (...values: Any) -> String, crate::fnc::string::concat);
define_pure_function!(StringJoin, "string::join", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::string::join_walk, (separator: String, ...values: Any) -> String, crate::fnc::string::join);

// String distance functions
define_pure_function!(StringDistanceDamerauLevenshtein, "string::distance::damerau_levenshtein", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::distance::damerau_levenshtein);
define_pure_function!(StringDistanceHamming, "string::distance::hamming", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::distance::hamming);
define_pure_function!(StringDistanceLevenshtein, "string::distance::levenshtein", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::distance::levenshtein);
define_pure_function!(StringDistanceNormalizedDamerauLevenshtein, "string::distance::normalized_damerau_levenshtein", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Float, crate::fnc::string::distance::normalized_damerau_levenshtein);
define_pure_function!(StringDistanceNormalizedLevenshtein, "string::distance::normalized_levenshtein", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Float, crate::fnc::string::distance::normalized_levenshtein);
define_pure_function!(StringDistanceOsa, "string::distance::osa", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::distance::osa_distance);

// String HTML functions
define_pure_function!(StringHtmlEncode, "string::html::encode", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> String, crate::fnc::string::html::encode);
define_pure_function!(StringHtmlSanitize, "string::html::sanitize", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> String, crate::fnc::string::html::sanitize);

// String is:: functions
define_pure_function!(StringIsAlpha, "string::is_alpha", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::alpha);
define_pure_function!(StringIsAlphanum, "string::is_alphanum", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::alphanum);
define_pure_function!(StringIsAscii, "string::is_ascii", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::ascii);
define_pure_function!(StringIsDatetime, "string::is_datetime", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String, ?format: String) -> Bool, crate::fnc::string::is::datetime);
define_pure_function!(StringIsDomain, "string::is_domain", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::domain);
define_pure_function!(StringIsEmail, "string::is_email", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::email);
define_pure_function!(StringIsHexadecimal, "string::is_hexadecimal", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::hexadecimal);
define_pure_function!(StringIsIp, "string::is_ip", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::ip);
define_pure_function!(StringIsIpv4, "string::is_ipv4", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::ipv4);
define_pure_function!(StringIsIpv6, "string::is_ipv6", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::ipv6);
define_pure_function!(StringIsLatitude, "string::is_latitude", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::latitude);
define_pure_function!(StringIsLongitude, "string::is_longitude", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::longitude);
define_pure_function!(StringIsNumeric, "string::is_numeric", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::numeric);
define_pure_function!(StringIsRecord, "string::is_record", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, ?table: Any) -> Bool, crate::fnc::string::is::record);
define_pure_function!(StringIsSemver, "string::is_semver", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::semver);
define_pure_function!(StringIsUlid, "string::is_ulid", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::ulid);
#[cfg(feature = "url")]
define_pure_function!(StringIsUrl, "string::is_url", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::url);
define_pure_function!(StringIsUuid, "string::is_uuid", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: String) -> Bool, crate::fnc::string::is::uuid);

// String similarity functions
define_pure_function!(StringSimilarityFuzzy, "string::similarity::fuzzy", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::similarity::fuzzy);
define_pure_function!(StringSimilarityJaro, "string::similarity::jaro", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Float, crate::fnc::string::similarity::jaro);
define_pure_function!(StringSimilarityJaroWinkler, "string::similarity::jaro_winkler", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Float, crate::fnc::string::similarity::jaro_winkler);
define_pure_function!(StringSimilaritySmithwaterman, "string::similarity::smithwaterman", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::similarity::smithwaterman);
define_pure_function!(StringSimilaritySorensenDice, "string::similarity::sorensen_dice", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Float, crate::fnc::string::similarity::sorensen_dice);

// String semver functions
define_pure_function!(StringSemverCompare, "string::semver::compare", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: String, b: String) -> Int, crate::fnc::string::semver::compare);
define_pure_function!(StringSemverMajor, "string::semver::major", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> Int, crate::fnc::string::semver::major);
define_pure_function!(StringSemverMinor, "string::semver::minor", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> Int, crate::fnc::string::semver::minor);
define_pure_function!(StringSemverPatch, "string::semver::patch", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> Int, crate::fnc::string::semver::patch);
define_pure_function!(StringSemverIncMajor, "string::semver::inc::major", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> String, crate::fnc::string::semver::inc::major);
define_pure_function!(StringSemverIncMinor, "string::semver::inc::minor", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> String, crate::fnc::string::semver::inc::minor);
define_pure_function!(StringSemverIncPatch, "string::semver::inc::patch", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> String, crate::fnc::string::semver::inc::patch);
define_pure_function!(StringSemverSetMajor, "string::semver::set::major", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, major: Int) -> String, crate::fnc::string::semver::set::major);
define_pure_function!(StringSemverSetMinor, "string::semver::set::minor", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, minor: Int) -> String, crate::fnc::string::semver::set::minor);
define_pure_function!(StringSemverSetPatch, "string::semver::set::patch", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String, patch: Int) -> String, crate::fnc::string::semver::set::patch);

/// The `string::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&StringCapitalize,
	&StringConcat,
	&StringContains,
	&StringDistanceDamerauLevenshtein,
	&StringDistanceHamming,
	&StringDistanceLevenshtein,
	&StringDistanceNormalizedDamerauLevenshtein,
	&StringDistanceNormalizedLevenshtein,
	&StringDistanceOsa,
	&StringEndsWith,
	&StringHtmlEncode,
	&StringHtmlSanitize,
	&StringIsAlpha,
	&StringIsAlphanum,
	&StringIsAscii,
	&StringIsDatetime,
	&StringIsDomain,
	&StringIsEmail,
	&StringIsHexadecimal,
	&StringIsIp,
	&StringIsIpv4,
	&StringIsIpv6,
	&StringIsLatitude,
	&StringIsLongitude,
	&StringIsNumeric,
	&StringIsRecord,
	&StringIsSemver,
	&StringIsUlid,
	&StringIsUuid,
	&StringJoin,
	&StringLen,
	&StringLowercase,
	&StringMatches,
	&StringRepeat,
	&StringReplace,
	&StringReverse,
	&StringSemverCompare,
	&StringSemverIncMajor,
	&StringSemverIncMinor,
	&StringSemverIncPatch,
	&StringSemverMajor,
	&StringSemverMinor,
	&StringSemverPatch,
	&StringSemverSetMajor,
	&StringSemverSetMinor,
	&StringSemverSetPatch,
	&StringSimilarityFuzzy,
	&StringSimilarityJaro,
	&StringSimilarityJaroWinkler,
	&StringSimilaritySmithwaterman,
	&StringSimilaritySorensenDice,
	&StringSlice,
	&StringSlug,
	&StringSplit,
	&StringStartsWith,
	&StringTrim,
	&StringUppercase,
	&StringWords,
	#[cfg(feature = "url")]
	&StringIsUrl,
	#[cfg(not(feature = "url"))]
	&super::disabled::Disabled {
		name: "string::is_url",
		feature: "url",
	},
];
