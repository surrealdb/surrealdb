use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

pub mod args;
pub mod array;
pub mod cast;
pub mod count;
pub mod crypto;
pub mod duration;
pub mod future;
pub mod geo;
pub mod http;
pub mod is;
pub mod math;
pub mod meta;
pub mod operate;
pub mod parse;
pub mod rand;
pub mod script;
pub mod session;
pub mod string;
pub mod time;
pub mod r#type;
pub mod util;

/// Attempts to run any function.
pub async fn run(ctx: &Context<'_>, name: &str, args: Vec<Value>) -> Result<Value, Error> {
	if name.starts_with("http")
		|| (name.starts_with("crypto") && (name.ends_with("compare") || name.ends_with("generate")))
	{
		asynchronous(ctx, name, args).await
	} else {
		synchronous(ctx, name, args)
	}
}

// Each function is specified by its name (a string literal) followed by its path. The path
// may be followed by one parenthesized argument, e.g. ctx, which is passed to the function
// before the remainder of the arguments. The path may be followed by `.await` to signify that
// it is `async`. Finally, the path may be prefixed by a parenthesized wrapper function e.g.
// `cpu_intensive`.
macro_rules! dispatch {
	($name: ident, $args: ident, $($function_name: literal => $(($wrapper: tt))* $($function_path: ident)::+ $(($ctx_arg: expr))* $(.$await:tt)*,)+) => {
		{
			match $name {
				$($function_name => {
					let args = args::FromArgs::from_args($name, $args)?;
					#[allow(clippy::redundant_closure_call)]
					$($wrapper)*(|| $($function_path)::+($($ctx_arg,)* args))()$(.$await)*
				},)+
				_ => unreachable!()
			}
		}
	};
}

/// Attempts to run any synchronous function.
pub fn synchronous(ctx: &Context<'_>, name: &str, args: Vec<Value>) -> Result<Value, Error> {
	dispatch!(
		name,
		args,
		"array::combine" => array::combine,
		"array::concat" => array::concat,
		"array::difference" => array::difference,
		"array::distinct" => array::distinct,
		"array::intersect" => array::intersect,
		"array::len" => array::len,
		"array::sort" => array::sort,
		"array::union" => array::union,
		"array::sort::asc" => array::sort::asc,
		"array::sort::desc" => array::sort::desc,
		//
		"count" => count::count,
		//
		"crypto::md5" => crypto::md5,
		"crypto::sha1" => crypto::sha1,
		"crypto::sha256" => crypto::sha256,
		"crypto::sha512" => crypto::sha512,
		//
		"duration::days" => duration::days,
		"duration::hours" => duration::hours,
		"duration::mins" => duration::mins,
		"duration::secs" => duration::secs,
		"duration::weeks" => duration::weeks,
		"duration::years" => duration::years,
		//
		"geo::area" => geo::area,
		"geo::bearing" => geo::bearing,
		"geo::centroid" => geo::centroid,
		"geo::distance" => geo::distance,
		"geo::hash::decode" => geo::hash::decode,
		"geo::hash::encode" => geo::hash::encode,
		//
		"is::alphanum" => is::alphanum,
		"is::alpha" => is::alpha,
		"is::ascii" => is::ascii,
		"is::domain" => is::domain,
		"is::email" => is::email,
		"is::hexadecimal" => is::hexadecimal,
		"is::latitude" => is::latitude,
		"is::longitude" => is::longitude,
		"is::numeric" => is::numeric,
		"is::semver" => is::semver,
		"is::uuid" => is::uuid,
		//
		"math::abs" => math::abs,
		"math::bottom" => math::bottom,
		"math::ceil" => math::ceil,
		"math::fixed" => math::fixed,
		"math::floor" => math::floor,
		"math::interquartile" => math::interquartile,
		"math::max" => math::max,
		"math::mean" => math::mean,
		"math::median" => math::median,
		"math::midhinge" => math::midhinge,
		"math::min" => math::min,
		"math::mode" => math::mode,
		"math::nearestrank" => math::nearestrank,
		"math::percentile" => math::percentile,
		"math::product" => math::product,
		"math::round" => math::round,
		"math::spread" => math::spread,
		"math::sqrt" => math::sqrt,
		"math::stddev" => math::stddev,
		"math::sum" => math::sum,
		"math::top" => math::top,
		"math::trimean" => math::trimean,
		"math::variance" => math::variance,
		//
		"meta::id" => meta::id,
		"meta::table" => meta::tb,
		"meta::tb" => meta::tb,
		//
		"parse::email::host" => parse::email::host,
		"parse::email::user" => parse::email::user,
		"parse::url::domain" => parse::url::domain,
		"parse::url::fragment" => parse::url::fragment,
		"parse::url::host" => parse::url::host,
		"parse::url::path" => parse::url::path,
		"parse::url::port" => parse::url::port,
		"parse::url::query" => parse::url::query,
		//
		"rand::bool" => rand::bool,
		"rand::enum" => rand::r#enum,
		"rand::float" => rand::float,
		"rand::guid" => rand::guid,
		"rand::int" => rand::int,
		"rand::string" => rand::string,
		"rand::time" => rand::time,
		"rand::uuid" => rand::uuid,
		"rand" => rand::rand,
		//
		"session::db" => session::db(ctx),
		"session::id" => session::id(ctx),
		"session::ip" => session::ip(ctx),
		"session::ns" => session::ns(ctx),
		"session::origin" => session::origin(ctx),
		"session::sc" => session::sc(ctx),
		"session::sd" => session::sd(ctx),
		"session::token" => session::token(ctx),
		//
		"string::concat" => string::concat,
		"string::endsWith" => string::ends_with,
		"string::join" => string::join,
		"string::length" => string::length,
		"string::lowercase" => string::lowercase,
		"string::repeat" => string::repeat,
		"string::replace" => string::replace,
		"string::reverse" => string::reverse,
		"string::slice" => string::slice,
		"string::slug" => string::slug,
		"string::split" => string::split,
		"string::startsWith" => string::starts_with,
		"string::trim" => string::trim,
		"string::uppercase" => string::uppercase,
		"string::words" => string::words,
		//
		"time::day" => time::day,
		"time::floor" => time::floor,
		"time::group" => time::group,
		"time::hour" => time::hour,
		"time::mins" => time::mins,
		"time::month" => time::month,
		"time::nano" => time::nano,
		"time::now" => time::now,
		"time::round" => time::round,
		"time::secs" => time::secs,
		"time::unix" => time::unix,
		"time::wday" => time::wday,
		"time::week" => time::week,
		"time::yday" => time::yday,
		"time::year" => time::year,
		//
		"type::bool" => r#type::bool,
		"type::datetime" => r#type::datetime,
		"type::decimal" => r#type::decimal,
		"type::duration" => r#type::duration,
		"type::float" => r#type::float,
		"type::int" => r#type::int,
		"type::number" => r#type::number,
		"type::point" => r#type::point,
		"type::regex" => r#type::regex,
		"type::string" => r#type::string,
		"type::table" => r#type::table,
		"type::thing" => r#type::thing,
	)
}

/// Attempts to run any asynchronous function.
pub async fn asynchronous(
	_ctx: &Context<'_>,
	name: &str,
	args: Vec<Value>,
) -> Result<Value, Error> {
	// Wrappers return a function as opposed to a value so that the dispatch! method can always
	// perform a function call.
	#[cfg(feature = "parallel")]
	fn cpu_intensive<R: Send + 'static>(
		function: impl FnOnce() -> R + Send + 'static,
	) -> impl FnOnce() -> executor::Task<R> {
		|| crate::exe::spawn(async move { function() })
	}

	#[cfg(not(feature = "parallel"))]
	fn cpu_intensive<R: Send + 'static>(
		function: impl FnOnce() -> R + Send + 'static,
	) -> impl FnOnce() -> std::future::Ready<R> {
		|| std::future::ready(function())
	}

	dispatch!(
		name,
		args,
		"crypto::argon2::compare" => (cpu_intensive) crypto::argon2::cmp.await,
		"crypto::argon2::generate" => (cpu_intensive) crypto::argon2::gen.await,
		"crypto::bcrypt::compare" => (cpu_intensive) crypto::bcrypt::cmp.await,
		"crypto::bcrypt::generate" => (cpu_intensive) crypto::bcrypt::gen.await,
		"crypto::pbkdf2::compare" => (cpu_intensive) crypto::pbkdf2::cmp.await,
		"crypto::pbkdf2::generate" => (cpu_intensive) crypto::pbkdf2::gen.await,
		"crypto::scrypt::compare" => (cpu_intensive) crypto::scrypt::cmp.await,
		"crypto::scrypt::generate" => (cpu_intensive) crypto::scrypt::gen.await,
		//
		"http::head" => http::head.await,
		"http::get" => http::get.await,
		"http::put" => http::put.await,
		"http::post" =>  http::post.await,
		"http::patch" => http::patch.await,
		"http::delete" => http::delete.await,
	)
}
