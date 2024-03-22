//! Executes functions from SQL. If there is an SQL function it will be defined in this module.
use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::value::Value;

pub mod args;
pub mod array;
pub mod bytes;
pub mod count;
pub mod crypto;
pub mod duration;
pub mod encoding;
pub mod geo;
pub mod http;
pub mod math;
pub mod meta;
pub mod not;
pub mod object;
pub mod operate;
pub mod parse;
pub mod rand;
pub mod script;
pub mod search;
pub mod session;
pub mod sleep;
pub mod string;
pub mod time;
pub mod r#type;
pub mod util;
pub mod vector;

/// Attempts to run any function
pub async fn run(
	ctx: &Context<'_>,
	opt: &Options,
	txn: &Transaction,
	doc: Option<&CursorDoc<'_>>,
	name: &str,
	args: Vec<Value>,
) -> Result<Value, Error> {
	if name.eq("sleep")
		|| name.starts_with("search")
		|| name.starts_with("http")
		|| name.starts_with("type::field")
		|| name.starts_with("type::fields")
		|| name.starts_with("crypto::argon2")
		|| name.starts_with("crypto::bcrypt")
		|| name.starts_with("crypto::pbkdf2")
		|| name.starts_with("crypto::scrypt")
	{
		asynchronous(ctx, Some(opt), Some(txn), doc, name, args).await
	} else {
		synchronous(ctx, name, args)
	}
}

/// Each function is specified by its name (a string literal) followed by its path. The path
/// may be followed by one parenthesized argument, e.g. ctx, which is passed to the function
/// before the remainder of the arguments. The path may be followed by `.await` to signify that
/// it is `async`. Finally, the path may be prefixed by a parenthesized wrapper function e.g.
/// `cpu_intensive`.
macro_rules! dispatch {
	($name: ident, $args: ident, $($function_name: literal => $(($wrapper: tt))* $($function_path: ident)::+ $(($ctx_arg: expr))* $(.$await:tt)*,)+) => {
		{
			match $name {
				$($function_name => {
					let args = args::FromArgs::from_args($name, $args)?;
					#[allow(clippy::redundant_closure_call)]
					$($wrapper)*(|| $($function_path)::+($($ctx_arg,)* args))()$(.$await)*
				},)+
				_ => {
					return Err($crate::err::Error::InvalidFunction{
						name: String::from($name),
						message: "no such builtin function".to_string()
					})
				}
			}
		}
	};
}

/// Attempts to run any synchronous function.
pub fn synchronous(ctx: &Context<'_>, name: &str, args: Vec<Value>) -> Result<Value, Error> {
	dispatch!(
		name,
		args,
		"array::add" => array::add,
		"array::all" => array::all,
		"array::any" => array::any,
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
		"array::filter_index" => array::filter_index,
		"array::find_index" => array::find_index,
		"array::first" => array::first,
		"array::flatten" => array::flatten,
		"array::group" => array::group,
		"array::insert" => array::insert,
		"array::intersect" => array::intersect,
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
		"array::remove" => array::remove,
		"array::reverse" => array::reverse,
		"array::slice" => array::slice,
		"array::sort" => array::sort,
		"array::transpose" => array::transpose,
		"array::union" => array::union,
		"array::sort::asc" => array::sort::asc,
		"array::sort::desc" => array::sort::desc,
		//
		"bytes::len" => bytes::len,
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
		"duration::micros" => duration::micros,
		"duration::millis" => duration::millis,
		"duration::mins" => duration::mins,
		"duration::nanos" => duration::nanos,
		"duration::secs" => duration::secs,
		"duration::weeks" => duration::weeks,
		"duration::years" => duration::years,
		"duration::from::days" => duration::from::days,
		"duration::from::hours" => duration::from::hours,
		"duration::from::micros" => duration::from::micros,
		"duration::from::millis" => duration::from::millis,
		"duration::from::mins" => duration::from::mins,
		"duration::from::nanos" => duration::from::nanos,
		"duration::from::secs" => duration::from::secs,
		"duration::from::weeks" => duration::from::weeks,
		//
		"encoding::base64::decode" => encoding::base64::decode,
		"encoding::base64::encode" => encoding::base64::encode,
		//
		"geo::area" => geo::area,
		"geo::bearing" => geo::bearing,
		"geo::centroid" => geo::centroid,
		"geo::distance" => geo::distance,
		"geo::hash::decode" => geo::hash::decode,
		"geo::hash::encode" => geo::hash::encode,
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
		"math::pow" => math::pow,
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
		"not" => not::not,
		//
		"object::entries" => object::entries,
		"object::from_entries" => object::from_entries,
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
		"rand::enum" => rand::r#enum,
		"rand::float" => rand::float,
		"rand::guid" => rand::guid,
		"rand::int" => rand::int,
		"rand::string" => rand::string,
		"rand::time" => rand::time,
		"rand::ulid" => rand::ulid,
		"rand::uuid::v4" => rand::uuid::v4,
		"rand::uuid::v7" => rand::uuid::v7,
		"rand::uuid" => rand::uuid,
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
		"string::contains" => string::contains,
		"string::endsWith" => string::ends_with,
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
		"string::startsWith" => string::starts_with,
		"string::trim" => string::trim,
		"string::uppercase" => string::uppercase,
		"string::words" => string::words,
		"string::distance::hamming" => string::distance::hamming,
		"string::distance::levenshtein" => string::distance::levenshtein,
		"string::is::alphanum" => string::is::alphanum,
		"string::is::alpha" => string::is::alpha,
		"string::is::ascii" => string::is::ascii,
		"string::is::datetime" => string::is::datetime,
		"string::is::domain" => string::is::domain,
		"string::is::email" => string::is::email,
		"string::is::hexadecimal" => string::is::hexadecimal,
		"string::is::latitude" => string::is::latitude,
		"string::is::longitude" => string::is::longitude,
		"string::is::numeric" => string::is::numeric,
		"string::is::semver" => string::is::semver,
		"string::is::url" => string::is::url,
		"string::is::uuid" => string::is::uuid,
		"string::similarity::fuzzy" => string::similarity::fuzzy,
		"string::similarity::jaro" => string::similarity::jaro,
		"string::similarity::smithwaterman" => string::similarity::smithwaterman,
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
		"time::ceil" => time::ceil,
		"time::day" => time::day,
		"time::floor" => time::floor,
		"time::format" => time::format,
		"time::group" => time::group,
		"time::hour" => time::hour,
		"time::max" => time::max,
		"time::min" => time::min,
		"time::minute" => time::minute,
		"time::month" => time::month,
		"time::nano" => time::nano,
		"time::micros" => time::micros,
		"time::millis" => time::millis,
		"time::now" => time::now,
		"time::round" => time::round,
		"time::second" => time::second,
		"time::timezone" => time::timezone,
		"time::unix" => time::unix,
		"time::wday" => time::wday,
		"time::week" => time::week,
		"time::yday" => time::yday,
		"time::year" => time::year,
		"time::from::nanos" => time::from::nanos,
		"time::from::micros" => time::from::micros,
		"time::from::millis" => time::from::millis,
		"time::from::secs" => time::from::secs,
		"time::from::unix" => time::from::unix,
		//
		"type::bool" => r#type::bool,
		"type::datetime" => r#type::datetime,
		"type::decimal" => r#type::decimal,
		"type::duration" => r#type::duration,
		"type::float" => r#type::float,
		"type::int" => r#type::int,
		"type::number" => r#type::number,
		"type::point" => r#type::point,
		"type::string" => r#type::string,
		"type::table" => r#type::table,
		"type::thing" => r#type::thing,
		"type::range" => r#type::range,
		"type::is::array" => r#type::is::array,
		"type::is::bool" => r#type::is::bool,
		"type::is::bytes" => r#type::is::bytes,
		"type::is::collection" => r#type::is::collection,
		"type::is::datetime" => r#type::is::datetime,
		"type::is::decimal" => r#type::is::decimal,
		"type::is::duration" => r#type::is::duration,
		"type::is::float" => r#type::is::float,
		"type::is::geometry" => r#type::is::geometry,
		"type::is::int" => r#type::is::int,
		"type::is::line" => r#type::is::line,
		"type::is::none" => r#type::is::none,
		"type::is::null" => r#type::is::null,
		"type::is::multiline" => r#type::is::multiline,
		"type::is::multipoint" => r#type::is::multipoint,
		"type::is::multipolygon" => r#type::is::multipolygon,
		"type::is::number" => r#type::is::number,
		"type::is::object" => r#type::is::object,
		"type::is::point" => r#type::is::point,
		"type::is::polygon" => r#type::is::polygon,
		"type::is::record" => r#type::is::record,
		"type::is::string" => r#type::is::string,
		"type::is::uuid" => r#type::is::uuid,
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
		"vector::subtract" => vector::subtract,
		"vector::distance::chebyshev" => vector::distance::chebyshev,
		"vector::distance::euclidean" => vector::distance::euclidean,
		"vector::distance::hamming" => vector::distance::hamming,
		"vector::distance::mahalanobis" => vector::distance::mahalanobis,
		"vector::distance::manhattan" => vector::distance::manhattan,
		"vector::distance::minkowski" => vector::distance::minkowski,
		"vector::similarity::cosine" => vector::similarity::cosine,
		"vector::similarity::jaccard" => vector::similarity::jaccard,
		"vector::similarity::pearson" => vector::similarity::pearson,
		"vector::similarity::spearman" => vector::similarity::spearman,
	)
}

/// Attempts to run any asynchronous function.
pub async fn asynchronous(
	ctx: &Context<'_>,
	opt: Option<&Options>,
	txn: Option<&Transaction>,
	doc: Option<&CursorDoc<'_>>,
	name: &str,
	args: Vec<Value>,
) -> Result<Value, Error> {
	// Wrappers return a function as opposed to a value so that the dispatch! method can always
	// perform a function call.
	#[cfg(not(target_arch = "wasm32"))]
	fn cpu_intensive<R: Send + 'static>(
		function: impl FnOnce() -> R + Send + 'static,
	) -> impl FnOnce() -> executor::Task<R> {
		|| crate::exe::spawn(async move { function() })
	}

	#[cfg(target_arch = "wasm32")]
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
		"http::head" => http::head(ctx).await,
		"http::get" => http::get(ctx).await,
		"http::put" => http::put(ctx).await,
		"http::post" =>  http::post(ctx).await,
		"http::patch" => http::patch(ctx).await,
		"http::delete" => http::delete(ctx).await,
		//
		"search::analyze" => search::analyze((ctx, txn, opt)).await,
		"search::score" => search::score((ctx, txn, doc)).await,
		"search::highlight" => search::highlight((ctx,txn, doc)).await,
		"search::offsets" => search::offsets((ctx, txn, doc)).await,
		//
		"sleep" => sleep::sleep(ctx).await,
		//
		"type::field" => r#type::field((ctx, opt, txn, doc)).await,
		"type::fields" => r#type::fields((ctx, opt, txn, doc)).await,
	)
}

#[cfg(test)]
mod tests {
	#[cfg(all(feature = "scripting", feature = "kv-mem"))]
	use crate::dbs::Capabilities;
	use crate::sql::{statements::OutputStatement, Function, Query, Statement, Value};

	#[tokio::test]
	async fn implementations_are_present() {
		// Accumulate and display all problems at once to avoid a test -> fix -> test -> fix cycle.
		let mut problems = Vec::new();

		// Read the source code of this file
		let fnc_mod = include_str!("mod.rs");
		for line in fnc_mod.lines() {
			if !(line.contains("=>")
				&& (line.trim().starts_with('"') || line.trim().ends_with(',')))
			{
				// This line does not define a function name.
				continue;
			}

			let (quote, _) = line.split_once("=>").unwrap();
			let name = quote.trim().trim_matches('"');

			let res = crate::syn::parse(&format!("RETURN {}()", name));
			if let Ok(Query(mut x)) = res {
				match x.0.pop() {
					Some(Statement::Output(OutputStatement {
						what: Value::Function(x),
						..
					})) => match *x {
						Function::Normal(parsed_name, _) => {
							if parsed_name != name {
								problems
									.push(format!("function `{name}` parsed as `{parsed_name}`"));
							}
						}
						_ => {
							problems.push(format!("couldn't parse {name} function"));
						}
					},
					_ => {
						problems.push(format!("couldn't parse {name} function"));
					}
				}
			} else {
				problems.push(format!("couldn't parse {name} function"));
			}

			#[cfg(all(feature = "scripting", feature = "kv-mem"))]
			{
				use crate::sql::Value;

				let name = name.replace("::", ".");
				let sql =
					format!("RETURN function() {{ return typeof surrealdb.functions.{name}; }}");
				let dbs = crate::kvs::Datastore::new("memory")
					.await
					.unwrap()
					.with_capabilities(Capabilities::all());
				let ses = crate::dbs::Session::owner().with_ns("test").with_db("test");
				let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
				let tmp = res.remove(0).result.unwrap();
				if tmp == Value::from("object") {
					// Assume this function is superseded by a module of the same name.
				} else if tmp != Value::from("function") {
					problems.push(format!("function {name} not exported to JavaScript: {tmp:?}"));
				}
			}
		}

		if !problems.is_empty() {
			eprintln!("Functions not fully implemented:");
			for problem in problems {
				eprintln!(" - {problem}");
			}
			panic!("ensure functions can be parsed in lib/src/sql/function.rs and are exported to JS in lib/src/fnc/script/modules/surrealdb");
		}
	}
}
