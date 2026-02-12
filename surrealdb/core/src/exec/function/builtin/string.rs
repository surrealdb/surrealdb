//! String functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// Single string argument functions
define_pure_function!(StringCapitalize, "string::capitalize", (value: String) -> String, crate::fnc::string::capitalize);
define_pure_function!(StringLen, "string::len", (value: String) -> Int, crate::fnc::string::len);
define_pure_function!(StringLowercase, "string::lowercase", (value: String) -> String, crate::fnc::string::lowercase);
define_pure_function!(StringReverse, "string::reverse", (value: String) -> String, crate::fnc::string::reverse);
define_pure_function!(StringSlug, "string::slug", (value: String) -> String, crate::fnc::string::slug);
define_pure_function!(StringTrim, "string::trim", (value: String) -> String, crate::fnc::string::trim);
define_pure_function!(StringUppercase, "string::uppercase", (value: String) -> String, crate::fnc::string::uppercase);
define_pure_function!(StringWords, "string::words", (value: String) -> Any, crate::fnc::string::words);

// Two string argument functions
define_pure_function!(StringContains, "string::contains", (value: String, search: String) -> Bool, crate::fnc::string::contains);
define_pure_function!(StringEndsWith, "string::ends_with", (value: String, suffix: String) -> Bool, crate::fnc::string::ends_with);
define_pure_function!(StringMatches, "string::matches", (value: String, pattern: String) -> Bool, crate::fnc::string::matches);
define_pure_function!(StringRepeat, "string::repeat", (value: String, count: Int) -> String, crate::fnc::string::repeat);
define_pure_function!(StringSplit, "string::split", (value: String, delimiter: String) -> Any, crate::fnc::string::split);
define_pure_function!(StringStartsWith, "string::starts_with", (value: String, prefix: String) -> Bool, crate::fnc::string::starts_with);

// Three argument string functions
define_pure_function!(StringReplace, "string::replace", (value: String, search: String, replacement: String) -> String, crate::fnc::string::replace);
define_pure_function!(StringSlice, "string::slice", (value: String, start: Int, ?length: Int) -> String, crate::fnc::string::slice);

// Variadic string functions
define_pure_function!(StringConcat, "string::concat", (...values: Any) -> String, crate::fnc::string::concat);
define_pure_function!(StringJoin, "string::join", (separator: String, ...values: Any) -> String, crate::fnc::string::join);

// String distance functions
define_pure_function!(StringDistanceDamerauLevenshtein, "string::distance::damerau_levenshtein", (a: String, b: String) -> Int, crate::fnc::string::distance::damerau_levenshtein);
define_pure_function!(StringDistanceHamming, "string::distance::hamming", (a: String, b: String) -> Int, crate::fnc::string::distance::hamming);
define_pure_function!(StringDistanceLevenshtein, "string::distance::levenshtein", (a: String, b: String) -> Int, crate::fnc::string::distance::levenshtein);
define_pure_function!(StringDistanceNormalizedDamerauLevenshtein, "string::distance::normalized_damerau_levenshtein", (a: String, b: String) -> Float, crate::fnc::string::distance::normalized_damerau_levenshtein);
define_pure_function!(StringDistanceNormalizedLevenshtein, "string::distance::normalized_levenshtein", (a: String, b: String) -> Float, crate::fnc::string::distance::normalized_levenshtein);
define_pure_function!(StringDistanceOsa, "string::distance::osa", (a: String, b: String) -> Int, crate::fnc::string::distance::osa_distance);

// String HTML functions
define_pure_function!(StringHtmlEncode, "string::html::encode", (value: String) -> String, crate::fnc::string::html::encode);
define_pure_function!(StringHtmlSanitize, "string::html::sanitize", (value: String) -> String, crate::fnc::string::html::sanitize);

// String is:: functions
define_pure_function!(StringIsAlpha, "string::is_alpha", (value: String) -> Bool, crate::fnc::string::is::alpha);
define_pure_function!(StringIsAlphanum, "string::is_alphanum", (value: String) -> Bool, crate::fnc::string::is::alphanum);
define_pure_function!(StringIsAscii, "string::is_ascii", (value: String) -> Bool, crate::fnc::string::is::ascii);
define_pure_function!(StringIsDatetime, "string::is_datetime", (value: String, format: String) -> Bool, crate::fnc::string::is::datetime);
define_pure_function!(StringIsDomain, "string::is_domain", (value: String) -> Bool, crate::fnc::string::is::domain);
define_pure_function!(StringIsEmail, "string::is_email", (value: String) -> Bool, crate::fnc::string::is::email);
define_pure_function!(StringIsHexadecimal, "string::is_hexadecimal", (value: String) -> Bool, crate::fnc::string::is::hexadecimal);
define_pure_function!(StringIsIp, "string::is_ip", (value: String) -> Bool, crate::fnc::string::is::ip);
define_pure_function!(StringIsIpv4, "string::is_ipv4", (value: String) -> Bool, crate::fnc::string::is::ipv4);
define_pure_function!(StringIsIpv6, "string::is_ipv6", (value: String) -> Bool, crate::fnc::string::is::ipv6);
define_pure_function!(StringIsLatitude, "string::is_latitude", (value: String) -> Bool, crate::fnc::string::is::latitude);
define_pure_function!(StringIsLongitude, "string::is_longitude", (value: String) -> Bool, crate::fnc::string::is::longitude);
define_pure_function!(StringIsNumeric, "string::is_numeric", (value: String) -> Bool, crate::fnc::string::is::numeric);
define_pure_function!(StringIsRecord, "string::is_record", (value: String) -> Bool, crate::fnc::string::is::record);
define_pure_function!(StringIsSemver, "string::is_semver", (value: String) -> Bool, crate::fnc::string::is::semver);
define_pure_function!(StringIsUlid, "string::is_ulid", (value: String) -> Bool, crate::fnc::string::is::ulid);
define_pure_function!(StringIsUrl, "string::is_url", (value: String) -> Bool, crate::fnc::string::is::url);
define_pure_function!(StringIsUuid, "string::is_uuid", (value: String) -> Bool, crate::fnc::string::is::uuid);

// String similarity functions
define_pure_function!(StringSimilarityFuzzy, "string::similarity::fuzzy", (a: String, b: String) -> Int, crate::fnc::string::similarity::fuzzy);
define_pure_function!(StringSimilarityJaro, "string::similarity::jaro", (a: String, b: String) -> Float, crate::fnc::string::similarity::jaro);
define_pure_function!(StringSimilarityJaroWinkler, "string::similarity::jaro_winkler", (a: String, b: String) -> Float, crate::fnc::string::similarity::jaro_winkler);
define_pure_function!(StringSimilaritySmithwaterman, "string::similarity::smithwaterman", (a: String, b: String) -> Float, crate::fnc::string::similarity::smithwaterman);
define_pure_function!(StringSimilaritySorensenDice, "string::similarity::sorensen_dice", (a: String, b: String) -> Float, crate::fnc::string::similarity::sorensen_dice);

// String semver functions
define_pure_function!(StringSemverCompare, "string::semver::compare", (a: String, b: String) -> Int, crate::fnc::string::semver::compare);
define_pure_function!(StringSemverMajor, "string::semver::major", (value: String) -> Int, crate::fnc::string::semver::major);
define_pure_function!(StringSemverMinor, "string::semver::minor", (value: String) -> Int, crate::fnc::string::semver::minor);
define_pure_function!(StringSemverPatch, "string::semver::patch", (value: String) -> Int, crate::fnc::string::semver::patch);
define_pure_function!(StringSemverIncMajor, "string::semver::inc::major", (value: String) -> String, crate::fnc::string::semver::inc::major);
define_pure_function!(StringSemverIncMinor, "string::semver::inc::minor", (value: String) -> String, crate::fnc::string::semver::inc::minor);
define_pure_function!(StringSemverIncPatch, "string::semver::inc::patch", (value: String) -> String, crate::fnc::string::semver::inc::patch);
define_pure_function!(StringSemverSetMajor, "string::semver::set::major", (value: String, major: Int) -> String, crate::fnc::string::semver::set::major);
define_pure_function!(StringSemverSetMinor, "string::semver::set::minor", (value: String, minor: Int) -> String, crate::fnc::string::semver::set::minor);
define_pure_function!(StringSemverSetPatch, "string::semver::set::patch", (value: String, patch: Int) -> String, crate::fnc::string::semver::set::patch);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		StringCapitalize,
		StringConcat,
		StringContains,
		StringDistanceDamerauLevenshtein,
		StringDistanceHamming,
		StringDistanceLevenshtein,
		StringDistanceNormalizedDamerauLevenshtein,
		StringDistanceNormalizedLevenshtein,
		StringDistanceOsa,
		StringEndsWith,
		StringHtmlEncode,
		StringHtmlSanitize,
		StringIsAlpha,
		StringIsAlphanum,
		StringIsAscii,
		StringIsDatetime,
		StringIsDomain,
		StringIsEmail,
		StringIsHexadecimal,
		StringIsIp,
		StringIsIpv4,
		StringIsIpv6,
		StringIsLatitude,
		StringIsLongitude,
		StringIsNumeric,
		StringIsRecord,
		StringIsSemver,
		StringIsUlid,
		StringIsUrl,
		StringIsUuid,
		StringJoin,
		StringLen,
		StringLowercase,
		StringMatches,
		StringRepeat,
		StringReplace,
		StringReverse,
		StringSemverCompare,
		StringSemverIncMajor,
		StringSemverIncMinor,
		StringSemverIncPatch,
		StringSemverMajor,
		StringSemverMinor,
		StringSemverPatch,
		StringSemverSetMajor,
		StringSemverSetMinor,
		StringSemverSetPatch,
		StringSimilarityFuzzy,
		StringSimilarityJaro,
		StringSimilarityJaroWinkler,
		StringSimilaritySmithwaterman,
		StringSimilaritySorensenDice,
		StringSlice,
		StringSlug,
		StringSplit,
		StringStartsWith,
		StringTrim,
		StringUppercase,
		StringWords,
	);
}
