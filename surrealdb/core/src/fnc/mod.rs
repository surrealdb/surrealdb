//! Executes functions from SQL. If there is an SQL function it will be defined
//! in this module.

use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::closure::FrameClosureEvaluator;
use crate::val::Value;
pub mod api;
pub(crate) mod closure;
pub mod encoding;
pub mod enumeration;
pub mod eval;
#[cfg(not(all(
	feature = "collate",
	feature = "geo",
	feature = "crypto-digest",
	feature = "crypto-password",
	feature = "html-sanitize"
)))]
mod feature;
#[cfg(test)]
mod feature_tests;
pub mod file;
pub mod http;
pub(crate) mod module;
pub(crate) mod mutability;
pub mod operate;
pub mod record;
pub mod schema;
pub mod script;
pub mod search;
pub mod sequence;
pub mod session;
pub mod sleep;
pub mod r#type;
pub mod util;
// The pure families live one crate down; these keep every `fnc::math::…`
// path in core, in exec's builtins and in the script bridge resolving.
#[cfg(feature = "collate")]
pub use surrealdb_runtime::array;
#[cfg(feature = "geo")]
pub use surrealdb_runtime::geo;
pub use surrealdb_runtime::{
	args, bytes, count, duration, interval, math, not, object, parse, rand, set, time, time_kinds,
};
#[cfg(not(feature = "collate"))]
pub mod array;
pub mod crypto;
#[cfg(not(feature = "geo"))]
pub mod geo;
pub mod sql;
pub mod string;
pub mod value;
pub mod vector;

/// Each function is specified by its name (a string literal) followed by its
/// path. The path may be followed by one parenthesized argument, e.g. ctx,
/// which is passed to the function before the remainder of the arguments. The
/// path may be followed by `.await` to signify that it is `async`. Finally, the
/// path may be prefixed by a parenthesized wrapper function e.g.
/// `cpu_intensive`.
macro_rules! dispatch {
    ($ctx: ident, $name: ident, $args: expr_2021, $message: expr_2021,
        $($function_name: literal =>
            $(($wrapper: tt))* $($function_path: ident)::+ $(($ctx_arg: expr_2021))* $(.$await:tt)*,)+
    ) => {
        {
            match $name {
                $(
                    $function_name => {
                        let args = args::FromArgs::from_args($name, $args)?;
                        #[allow(clippy::redundant_closure_call)]
                        $($wrapper)*(|| $($function_path)::+($($ctx_arg,)* args))()$(.$await)*
                    },
                )+
                _ => {
                    return Err(::anyhow::Error::new($crate::exec::Error::InvalidFunction{
                        name: String::from($name),
                        message: $message.to_string()
                    }))
                }
            }
        }
    };
}

/// Attempts to run any synchronous function.
///
/// The by-name entry the script bridge calls, and the reference the registry
/// parity tests compare the registry's declarations against. The engine itself
/// resolves functions through the function registry; a build without
/// `scripting` reaches this only from tests, so the families it dispatches to
/// are kept alive from here.
#[cfg_attr(not(any(feature = "scripting", test)), allow(dead_code))]
pub fn synchronous(
	ctx: &FrozenContext,
	doc: Option<&CursorDoc>,
	name: &str,
	args: Vec<Value>,
) -> Result<Value> {
	dispatch!(
		ctx,
		name,
		args,
		"no such builtin function found",
		//
		"array::add" => array::add,
		"array::append" => array::append,
		"array::at" => array::at,
		"array::boolean_and" => array::boolean_and,
		"array::boolean_not" => array::boolean_not,
		"array::boolean_or" => array::boolean_or,
		"array::boolean_xor" => array::boolean_xor,
		"array::clump" => array::clump,
		"array::combine" => array::combine,
		"array::complement" => array::complement,
		"array::concat" => array::concat,
		"array::difference" => array::difference,
		"array::distinct" => array::distinct,
		"array::fill" => array::fill,
		"array::first" => array::first,
		"array::flatten" => array::flatten,
		"array::group" => array::group,
		"array::insert" => array::insert,
		"array::intersect" => array::intersect,
		"array::is_empty" => array::is_empty,
		"array::join" => array::join,
		"array::last" => array::last,
		"array::len" => array::len,
		"array::logical_and" => array::logical_and,
		"array::logical_or" => array::logical_or,
		"array::logical_xor" => array::logical_xor,
		"array::matches" => array::matches,
		"array::max" => array::max,
		"array::min" => array::min,
		"array::pop" => array::pop,
		"array::prepend" => array::prepend,
		"array::push" => array::push,
		"array::range" => array::range,
		"array::remove" => array::remove,
		"array::repeat" => array::repeat,
		"array::reverse" => array::reverse,
		"array::sequence" => array::sequence,
		"array::shuffle" => array::shuffle,
		"array::slice" => array::slice,
		"array::sort" => array::sort,
		"array::sort_natural" => array::sort_natural,
		"array::sort_lexical" => array::sort_lexical,
		"array::sort_natural_lexical" => array::sort_natural_lexical,
		"array::swap" => array::swap,
		"array::transpose" => array::transpose,
		"array::union" => array::union,
		"array::sort::asc" => array::sort::asc,
		"array::sort::desc" => array::sort::desc,
		"array::windows" => array::windows,
		//
		"bytes::len" => bytes::len,
		//
		"count" => count::count,
		//
		"crypto::blake3" => crypto::blake3,
		"crypto::joaat" => crypto::joaat,
		"crypto::md5" => crypto::md5,
		"crypto::sha1" => crypto::sha1,
		"crypto::sha256" => crypto::sha256,
		"crypto::sha512" => crypto::sha512,
		//
		"interval::days" => interval::days,
		"interval::hours" => interval::hours,
		"interval::justify_days" => interval::justify_days,
		"interval::justify_hours" => interval::justify_hours,
		"interval::justify_interval" => interval::justify_interval,
		"interval::make" => interval::make,
		"interval::micros" => interval::micros,
		"interval::mins" => interval::mins,
		"interval::months" => interval::months,
		"interval::secs" => interval::secs,
		"interval::years" => interval::years,
		"duration::days" => duration::days,
		"duration::hours" => duration::hours,
		"duration::micros" => duration::micros,
		"duration::millis" => duration::millis,
		"duration::mins" => duration::mins,
		"duration::nanos" => duration::nanos,
		"duration::secs" => duration::secs,
		"duration::weeks" => duration::weeks,
		"duration::years" => duration::years,
		"duration::from_days" => duration::from::days,
		"duration::from_hours" => duration::from::hours,
		"duration::from_micros" => duration::from::micros,
		"duration::from_millis" => duration::from::millis,
		"duration::from_mins" => duration::from::mins,
		"duration::from_nanos" => duration::from::nanos,
		"duration::from_secs" => duration::from::secs,
		"duration::from_weeks" => duration::from::weeks,
		//
		"file::bucket" => file::bucket,
		"file::key" => file::key,
		//
		"encoding::base64::decode" => encoding::base64::decode,
		"encoding::base64::encode" => encoding::base64::encode,
		"encoding::cbor::decode" => encoding::cbor::decode,
		"encoding::cbor::encode" => encoding::cbor::encode,
		"encoding::json::decode" => encoding::json::decode,
		"encoding::json::encode" => encoding::json::encode,
		//
		"geo::area" => geo::area,
		"geo::bearing" => geo::bearing,
		"geo::centroid" => geo::centroid,
		"geo::distance" => geo::distance,
		"geo::hash::decode" => geo::hash::decode,
		"geo::hash::encode" => geo::hash::encode,
		"geo::is_valid" => geo::is::valid,
		//
		"math::abs" => math::abs,
		"math::acos" => math::acos,
		"math::acot" => math::acot,
		"math::asin" => math::asin,
		"math::atan" => math::atan,
		"math::bottom" => math::bottom,
		"math::ceil" => math::ceil,
		"math::clamp" => math::clamp,
		"math::cos" => math::cos,
		"math::cot" => math::cot,
		"math::deg2rad" => math::deg2rad,
		"math::fixed" => math::fixed,
		"math::floor" => math::floor,
		"math::interquartile" => math::interquartile,
		"math::lerp" => math::lerp,
		"math::lerpangle" => math::lerpangle,
		"math::ln" => math::ln,
		"math::log" => math::log,
		"math::log10" => math::log10,
		"math::log2" => math::log2,
		"math::max" => math::max,
		"math::mean" => math::mean,
		"math::median" => math::median,
		"math::midhinge" => math::midhinge,
		"math::min" => math::min,
		"math::mode" => math::mode,
		"math::nearestrank" => math::nearestrank,
		"math::percentile" => math::percentile,
		"math::pow" => math::pow,
		"math::product" => math::product,
		"math::rad2deg" => math::rad2deg,
		"math::round" => math::round,
		"math::sign" => math::sign,
		"math::sin" => math::sin,
		"math::spread" => math::spread,
		"math::sqrt" => math::sqrt,
		"math::stddev" => math::stddev,
		"math::sum" => math::sum,
		"math::tan" => math::tan,
		"math::top" => math::top,
		"math::trimean" => math::trimean,
		"math::variance" => math::variance,
		//
		"meta::id" => record::id,
		"meta::tb" => record::tb,
		//
		"not" => not::not,
		//
		"object::entries" => object::entries,
		"object::extend" => object::extend,
		"object::remove" => object::remove,
		"object::from_entries" => object::from_entries,
		"object::is_empty" => object::is_empty,
		"object::keys" => object::keys,
		"object::len" => object::len,
		"object::values" => object::values,
		//
		"parse::email::host" => parse::email::host,
		"parse::email::user" => parse::email::user,
		"parse::url::domain" => parse::url::domain,
		"parse::url::fragment" => parse::url::fragment,
		"parse::url::host" => parse::url::host,
		"parse::url::path" => parse::url::path,
		"parse::url::port" => parse::url::port,
		"parse::url::query" => parse::url::query,
		"parse::url::scheme" => parse::url::scheme,
		//
		"rand" => rand::rand,
		"rand::bool" => rand::bool,
		"rand::duration" => rand::duration,
		"rand::enum" => rand::r#enum,
		"rand::float" => rand::float,
		"rand::id" => rand::id,
		"rand::int" => rand::int,
		"rand::string" => rand::string,
		"rand::time" => rand::time,
		"rand::ulid" => rand::ulid,
		"rand::uuid::v4" => rand::uuid::v4,
		"rand::uuid::v7" => rand::uuid::v7,
		"rand::uuid" => rand::uuid,
		//
		"record::id" => record::id,
		"record::table" => record::tb,
		"record::tb" => record::tb,
		//
		"session::ac" => session::ac(ctx),
		"session::db" => session::db(ctx),
		"session::id" => session::id(ctx),
		"session::ip" => session::ip(ctx),
		"session::ns" => session::ns(ctx),
		"session::origin" => session::origin(ctx),
		"session::rd" => session::rd(ctx),
		"session::token" => session::token(ctx),
		//
		"set::add" => set::add,
		"set::at" => set::at,
		"set::complement" => set::complement,
		"set::contains" => set::contains,
		"set::difference" => set::difference,
		"set::first" => set::first,
		"set::flatten" => set::flatten,
		"set::intersect" => set::intersect,
		"set::is_empty" => set::is_empty,
		"set::join" => set::join,
		"set::last" => set::last,
		"set::len" => set::len,
		"set::max" => set::max,
		"set::min" => set::min,
		"set::remove" => set::remove,
		"set::slice" => set::slice,
		"set::union" => set::union,
		//
		"sql::fit" => sql::fit,
		"sql::network_key" => sql::network_key,
		"sql::network_text" => sql::network_text,
		"sql::range_contains" => sql::range_contains,
		"sql::range_lower" => sql::range_lower,
		"sql::range_overlaps" => sql::range_overlaps,
		"sql::range_upper" => sql::range_upper,
		"sql::text" => sql::text,
		//
		"string::capitalize" => string::capitalize,
		"string::concat" => string::concat,
		"string::contains" => string::contains,
		"string::ends_with" => string::ends_with,
		"string::join" => string::join,
		"string::len" => string::len,
		"string::lowercase" => string::lowercase,
		"string::matches" => string::matches,
		"string::repeat" => string::repeat,
		"string::replace" => string::replace,
		"string::reverse" => string::reverse,
		"string::slice" => string::slice,
		"string::slug" => string::slug,
		"string::split" => string::split,
		"string::starts_with" => string::starts_with,
		"string::trim" => string::trim,
		"string::uppercase" => string::uppercase,
		"string::words" => string::words,
		//
		"string::distance::damerau_levenshtein" => string::distance::damerau_levenshtein,
		"string::distance::hamming" => string::distance::hamming,
		"string::distance::levenshtein" => string::distance::levenshtein,
		"string::distance::normalized_damerau_levenshtein" => string::distance::normalized_damerau_levenshtein,
		"string::distance::normalized_levenshtein" => string::distance::normalized_levenshtein,
		"string::distance::osa" => string::distance::osa_distance,
		//
		"string::html::encode" => string::html::encode,
		"string::html::sanitize" => string::html::sanitize,
		"string::is_alphanum" => string::is::alphanum,
		"string::is_alpha" => string::is::alpha,
		"string::is_ascii" => string::is::ascii,
		"string::is_datetime" => string::is::datetime,
		"string::is_domain" => string::is::domain,
		"string::is_email" => string::is::email,
		"string::is_hexadecimal" => string::is::hexadecimal,
		"string::is_ip" => string::is::ip,
		"string::is_ipv4" => string::is::ipv4,
		"string::is_ipv6" => string::is::ipv6,
		"string::is_latitude" => string::is::latitude,
		"string::is_longitude" => string::is::longitude,
		"string::is_numeric" => string::is::numeric,
		"string::is_semver" => string::is::semver,
		"string::is_url" => string::is::url,
		"string::is_ulid" => string::is::ulid,
		"string::is_uuid" => string::is::uuid,
		"string::is_record" => string::is::record,
		//
		"string::similarity::fuzzy" => string::similarity::fuzzy,
		"string::similarity::jaro" => string::similarity::jaro,
		"string::similarity::jaro_winkler" => string::similarity::jaro_winkler,
		"string::similarity::smithwaterman" => string::similarity::smithwaterman,
		"string::similarity::sorensen_dice" => string::similarity::sorensen_dice,
		//
		"string::semver::compare" => string::semver::compare,
		"string::semver::major" => string::semver::major,
		"string::semver::minor" => string::semver::minor,
		"string::semver::patch" => string::semver::patch,
		"string::semver::inc::major" => string::semver::inc::major,
		"string::semver::inc::minor" => string::semver::inc::minor,
		"string::semver::inc::patch" => string::semver::inc::patch,
		"string::semver::set::major" => string::semver::set::major,
		"string::semver::set::minor" => string::semver::set::minor,
		"string::semver::set::patch" => string::semver::set::patch,
		//
		"time::ceil" => time_kinds::ceil,
		"time::day" => time_kinds::day,
		"time::floor" => time_kinds::floor,
		"time::format" => time_kinds::format,
		"time::group" => time_kinds::group,
		"time::hour" => time_kinds::hour,
		"time::max" => time_kinds::max,
		"time::min" => time_kinds::min,
		"time::minute" => time_kinds::minute,
		"time::month" => time_kinds::month,
		"time::nano" => time_kinds::nano,
		"time::micros" => time_kinds::micros,
		"time::millis" => time_kinds::millis,
		"time::now" => time::now,
		"time::make_date" => time_kinds::make_date,
		"time::make_time" => time_kinds::make_time,
		"time::make_timestamp" => time_kinds::make_timestamp,
		"time::age" => time_kinds::age,
		"time::extract" => time_kinds::extract,
		"time::trunc" => time_kinds::trunc,
		"time::round" => time_kinds::round,
		"time::second" => time_kinds::second,
		"time::timezone" => time::timezone,
		"time::unix" => time::unix,
		"time::wday" => time_kinds::wday,
		"time::week" => time_kinds::week,
		"time::yday" => time_kinds::yday,
		"time::year" => time_kinds::year,
		"time::from_nanos" => time::from::nanos,
		"time::from_micros" => time::from::micros,
		"time::from_millis" => time::from::millis,
		"time::from_secs" => time::from::secs,
		"time::from_ulid" => time::from::ulid,
		"time::from_unix" => time::from::unix,
		"time::from_uuid" => time::from::uuid,
		"time::is_leap_year" => time_kinds::is_leap_year,
		"time::set_year" => time_kinds::set_year,
		"time::set_month" => time_kinds::set_month,
		"time::set_day" => time_kinds::set_day,
		"time::set_hour" => time_kinds::set_hour,
		"time::set_minute" => time_kinds::set_minute,
		"time::set_second" => time_kinds::set_second,
		"time::set_nanosecond" => time_kinds::set_nanosecond,
		//
		"type::array" => r#type::array,
		"type::bool" => r#type::bool,
		"type::bytes" => r#type::bytes,
		"type::date" => r#type::date,
		"type::time" => r#type::time,
		"type::timestamp" => r#type::timestamp,
		"type::interval" => r#type::interval,
		"type::datetime" => r#type::datetime,
		"type::decimal" => r#type::decimal,
		"type::duration" => r#type::duration,
		"type::f16" => r#type::f16,
		"type::f32" => r#type::f32,
		"type::file" => r#type::file,
		"type::float" => r#type::float,
		"type::geometry" => r#type::geometry,
		"type::i8" => r#type::i8,
		"type::i16" => r#type::i16,
		"type::i32" => r#type::i32,
		"type::int" => r#type::int,
		"type::number" => r#type::number,
		"type::of" => r#type::type_of,
		"type::point" => r#type::point,
		"type::range" => r#type::range,
		"type::record" => r#type::record,
		"type::set" => r#type::set,
		"type::string" => r#type::string,
		"type::string_lossy" => r#type::string_lossy,
		"type::table" => r#type::table,
		"type::u8" => r#type::u8,
		"type::u16" => r#type::u16,
		"type::u32" => r#type::u32,
		"type::u64" => r#type::u64,
		"type::uuid" => r#type::uuid,
		"type::is_array" => r#type::is::array,
		"type::is_bool" => r#type::is::bool,
		"type::is_bytes" => r#type::is::bytes,
		"type::is_collection" => r#type::is::collection,
		"type::is_date" => r#type::is::date,
		"type::is_time" => r#type::is::time,
		"type::is_timestamp" => r#type::is::timestamp,
		"type::is_interval" => r#type::is::interval,
		"type::is_datetime" => r#type::is::datetime,
		"type::is_decimal" => r#type::is::decimal,
		"type::is_duration" => r#type::is::duration,
		"type::is_f16" => r#type::is::f16,
		"type::is_f32" => r#type::is::f32,
		"type::is_float" => r#type::is::float,
		"type::is_geometry" => r#type::is::geometry,
		"type::is_i8" => r#type::is::i8,
		"type::is_i16" => r#type::is::i16,
		"type::is_i32" => r#type::is::i32,
		"type::is_int" => r#type::is::int,
		"type::is_line" => r#type::is::line,
		"type::is_none" => r#type::is::none,
		"type::is_null" => r#type::is::null,
		"type::is_multiline" => r#type::is::multiline,
		"type::is_multipoint" => r#type::is::multipoint,
		"type::is_multipolygon" => r#type::is::multipolygon,
		"type::is_number" => r#type::is::number,
		"type::is_object" => r#type::is::object,
		"type::is_point" => r#type::is::point,
		"type::is_polygon" => r#type::is::polygon,
		"type::is_range" => r#type::is::range,
		"type::is_record" => r#type::is::record,
		"type::is_set" => r#type::is::set,
		"type::is_string" => r#type::is::string,
		"type::is_u8" => r#type::is::u8,
		"type::is_u16" => r#type::is::u16,
		"type::is_u32" => r#type::is::u32,
		"type::is_u64" => r#type::is::u64,
		"type::is_uuid" => r#type::is::uuid,
		//
		"vector::add" => vector::add,
		"vector::angle" => vector::angle,
		"vector::cross" => vector::cross,
		"vector::dot" => vector::dot,
		"vector::divide" => vector::divide,
		"vector::magnitude" => vector::magnitude,
		"vector::multiply" => vector::multiply,
		"vector::normalize" => vector::normalize,
		"vector::project" => vector::project,
		"vector::scale" => vector::scale,
		"vector::subtract" => vector::subtract,
		"vector::sum" => vector::sum,
		"vector::distance::chebyshev" => vector::distance::chebyshev,
		"vector::distance::cosine" => vector::distance::cosine,
		"vector::distance::euclidean" => vector::distance::euclidean,
		"vector::distance::hamming" => vector::distance::hamming,
		"vector::distance::knn" => vector::distance::knn((ctx, doc)),
		"vector::distance::mahalanobis" => vector::distance::mahalanobis,
		"vector::distance::manhattan" => vector::distance::manhattan,
		"vector::distance::minkowski" => vector::distance::minkowski,
		"vector::similarity::cosine" => vector::similarity::cosine,
		"vector::similarity::jaccard" => vector::similarity::jaccard,
		"vector::similarity::pearson" => vector::similarity::pearson,
		"vector::similarity::spearman" => vector::similarity::spearman,
	)
}

/// Attempts to run any asynchronous function. See [`synchronous`] for who
/// calls this.
#[cfg_attr(not(any(feature = "scripting", test)), allow(dead_code))]
pub async fn asynchronous(
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	name: &str,
	args: Vec<Value>,
) -> Result<Value> {
	// Wrappers return a function as opposed to a value so that the dispatch! method
	// can always perform a function call.
	#[cfg(not(target_family = "wasm"))]
	fn cpu_intensive<R: Send + 'static>(
		function: impl FnOnce() -> R + Send + 'static,
	) -> impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = R> + Send>> {
		|| Box::pin(crate::exe::spawn(function))
	}

	#[cfg(target_family = "wasm")]
	fn cpu_intensive<R: Send + 'static>(
		function: impl FnOnce() -> R + Send + 'static,
	) -> impl FnOnce() -> std::future::Ready<R> {
		|| std::future::ready(function())
	}

	// The environment through which the closure-taking builtins invoke user
	// closures. `dispatch!` evaluates each arm inside a closure, so the
	// evaluator has to outlive that closure rather than be built inside an arm.
	let ev = FrameClosureEvaluator::new(ctx, opt, doc);

	dispatch!(
		ctx,
		name,
		args,
		"no such builtin function found",
		//
		"api::invoke" => api::invoke((ctx, opt)).await,
		"api::req::body" => api::req::body((ctx, opt, doc)).await,
		"api::req::max_body" => api::req::max_body((ctx, opt, doc)).await,
		"api::res::body" => api::res::body((ctx, opt, doc)).await,
		"api::timeout" => api::timeout((ctx, opt, doc)).await,
		"api::res::status" => api::res::status((ctx, opt, doc)).await,
		"api::res::header" => api::res::header((ctx, opt, doc)).await,
		"api::res::headers" => api::res::headers((ctx, opt, doc)).await,
		//
		"eval::surql" => eval::surql((ctx, opt, doc)).await,
		"eval::gql" => eval::gql((ctx, opt, doc)).await,
		//
		"array::all" => array::all(Some(&ev)).await,
		"array::any" => array::any(Some(&ev)).await,
		"array::every" => array::all(Some(&ev)).await,
		"array::filter" => array::filter(Some(&ev)).await,
		"array::filter_index" => array::filter_index(Some(&ev)).await,
		"array::find" => array::find(Some(&ev)).await,
		"array::find_index" => array::find_index(Some(&ev)).await,
		"array::fold" => array::fold(Some(&ev)).await,
		"array::includes" => array::any(Some(&ev)).await,
		"array::index_of" => array::find_index(Some(&ev)).await,
		"array::map" => array::map(Some(&ev)).await,
		"array::reduce" => array::reduce(Some(&ev)).await,
		"array::some" => array::any(Some(&ev)).await,
		//
		"crypto::argon2::compare" => (cpu_intensive) crypto::argon2::cmp.await,
		"crypto::argon2::generate" => (cpu_intensive) crypto::argon2::r#gen.await,
		"crypto::bcrypt::compare" => (cpu_intensive) crypto::bcrypt::cmp.await,
		"crypto::bcrypt::generate" => (cpu_intensive) crypto::bcrypt::r#gen.await,
		"crypto::pbkdf2::compare" => (cpu_intensive) crypto::pbkdf2::cmp.await,
		"crypto::pbkdf2::generate" => (cpu_intensive) crypto::pbkdf2::r#gen.await,
		"crypto::scrypt::compare" => (cpu_intensive) crypto::scrypt::cmp.await,
		"crypto::scrypt::generate" => (cpu_intensive) crypto::scrypt::r#gen.await,
		//
		"file::put" => file::put((ctx, opt, doc)).await,
		"file::put_if_not_exists" => file::put_if_not_exists((ctx, opt, doc)).await,
		"file::get" => file::get((ctx, opt, doc)).await,
		"file::presign_get" => file::presign_get((ctx, opt, doc)).await,
		"file::presign_put" => file::presign_put((ctx, opt, doc)).await,
		"file::head" => file::head((ctx, opt, doc)).await,
		"file::delete" => file::delete((ctx, opt, doc)).await,
		"file::copy" => file::copy((ctx, opt, doc)).await,
		"file::copy_if_not_exists" => file::copy_if_not_exists((ctx, opt, doc)).await,
		"file::rename" => file::rename((ctx, opt, doc)).await,
		"file::rename_if_not_exists" => file::rename_if_not_exists((ctx, opt, doc)).await,
		"file::exists" => file::exists((ctx, opt, doc)).await,
		"file::list" => file::list((ctx, opt, doc)).await,
		//
		"http::head" => http::head(ctx).await,
		"http::get" => http::get(ctx).await,
		"http::put" => http::put(ctx).await,
		"http::post" =>  http::post(ctx).await,
		"http::patch" => http::patch(ctx).await,
		"http::delete" => http::delete(ctx).await,
		//
		"record::exists" => record::exists((ctx, Some(opt), doc)).await,
		"record::is_edge" => record::is::edge((ctx, Some(opt), doc)).await,
		//
		"search::analyze" => search::analyze((ctx, Some(opt))).await,
		"search::linear" => search::linear(ctx).await,
		"search::rrf" => search::rrf(ctx).await,
		"search::score" => search::score((ctx, doc)).await,
		"search::highlight" => search::highlight((ctx, doc)).await,
		"search::offsets" => search::offsets((ctx, doc)).await,
		//
		"set::all" => set::all(Some(&ev)).await,
		"set::any" => set::any(Some(&ev)).await,
		"set::filter" => set::filter(Some(&ev)).await,
		"set::find" => set::find(Some(&ev)).await,
		"set::fold" => set::fold(Some(&ev)).await,
		"set::map" => set::map(Some(&ev)).await,
		"set::reduce" => set::reduce(Some(&ev)).await,
		//
		"sleep" => sleep::sleep(ctx).await,
		//
		"sequence::nextval" => sequence::nextval((ctx, opt)).await,
		//
		"type::field" => r#type::field((ctx, Some(opt), doc)).await,
		"type::fields" => r#type::fields((ctx, Some(opt), doc)).await,
		//
		"value::chain" => value::chain(Some(&ev)).await,
		"value::diff" => value::diff.await,
		"value::expect" => value::expect(Some(&ev)).await,
		"value::patch" => value::patch.await,
		"schema::table::exists" => schema::table::exists((ctx, Some(opt))).await,
		"enum::cast" => enumeration::cast((ctx, Some(opt))).await,
		"enum::max" => enumeration::max((ctx, Some(opt))).await,
		"enum::min" => enumeration::min((ctx, Some(opt))).await,
		"enum::rank" => enumeration::rank((ctx, Some(opt))).await,
		"enum::values" => enumeration::values((ctx, Some(opt))).await,
	)
}

#[cfg(test)]
mod tests {
	use regex::Regex;

	use crate::dbs::Capabilities;
	use crate::expr::{Expr, Function};
	use crate::syn::ParserConfig;

	#[tokio::test]
	async fn implementations_are_present() {
		// Accumulate and display all problems at once to avoid a test -> fix -> test ->
		// fix cycle.
		let mut problems = Vec::new();

		// Read the source code of this file
		let fnc_mod = include_str!("mod.rs");

		let exp_regex = Regex::new(r"exp\(.*\) ").unwrap();

		for line in fnc_mod.lines() {
			let line = line.trim();
			let line = if line.starts_with("exp") {
				&exp_regex.replace(line, "")
			} else {
				line
			};

			if !(line.contains("=>") && (line.starts_with('"') || line.ends_with(','))) {
				// This line does not define a function name.
				continue;
			}

			let (quote, _) = line.split_once("=>").unwrap();
			let name = quote.trim().trim_matches('"');
			// The `sql::*` functions are reached only from plans the SQL front ends build; they
			// have no SurrealQL syntax and no JavaScript binding.
			if name.starts_with("sql::") {
				continue;
			}

			let res = crate::syn::expr_with_capabilities(
				&format!("{}()", name),
				&Capabilities::all(),
				&ParserConfig::default(),
			);

			if let Ok(Expr::FunctionCall(call)) = res {
				if let Function::Normal(parsed_name) = call.receiver {
					if parsed_name != name {
						problems.push(format!("function `{name}` parsed as `{parsed_name}`"));
					}
				} else {
					problems.push(format!("couldn't parse {name} function"));
				}
			} else {
				problems.push(format!("couldn't parse {name} function"));
			}

			#[cfg(all(feature = "scripting", feature = "kv-mem"))]
			{
				let name = name.replace("::", ".");
				let sql =
					format!("RETURN function() {{ return typeof surrealdb.functions.{name}; }}");
				let dbs = crate::kvs::Datastore::builder()
					.with_capabilities(Capabilities::all())
					.build_with_path("memory")
					.await
					.unwrap();
				let ses = crate::dbs::Session::owner().with_ns("test").with_db("test");
				let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
				let tmp = res.remove(0).result.unwrap();
				if tmp == crate::types::PublicValue::String("object".to_owned()) {
					// Assume this function is superseded by a module of the
					// same name.
				} else if tmp != crate::types::PublicValue::String("function".to_owned()) {
					problems.push(format!("function {name} not exported to JavaScript: {tmp:?}"));
				}
			}
		}

		if !problems.is_empty() {
			eprintln!("Functions not fully implemented:");
			for problem in problems {
				eprintln!(" - {problem}");
			}
			panic!(
				"ensure functions can be parsed in core/src/sql/function.rs and are exported to JS in core/src/fnc/script/modules/surrealdb"
			);
		}
	}
}
