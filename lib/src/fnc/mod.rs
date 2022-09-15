use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::args::Args;
use crate::sql::value::Value;

pub mod args;
pub mod array;
pub mod cast;
pub mod count;
pub mod crypto;
pub mod future;
pub mod geo;
pub mod http;
pub mod is;
pub mod math;
pub mod operate;
pub mod parse;
pub mod rand;
pub mod script;
pub mod session;
pub mod string;
pub mod time;
pub mod r#type;
pub mod util;

// Attempts to run any function
pub async fn run(ctx: &Context<'_>, name: &str, args: Vec<Value>) -> Result<Value, Error> {
	match name {
		v if v.starts_with("http") => {
			// HTTP functions are asynchronous
			asynchronous(ctx, name, args).await
		}
		_ => {
			// Other functions are synchronous
			synchronous(ctx, name, args)
		}
	}
}

// Attempts to run a synchronous function
pub fn synchronous(ctx: &Context<'_>, name: &str, args: Vec<Value>) -> Result<Value, Error> {
	match name {
		//
		"array::combine" => args::check(ctx, name, args, Args::Two, array::combine),
		"array::concat" => args::check(ctx, name, args, Args::Two, array::concat),
		"array::difference" => args::check(ctx, name, args, Args::Two, array::difference),
		"array::distinct" => args::check(ctx, name, args, Args::One, array::distinct),
		"array::intersect" => args::check(ctx, name, args, Args::Two, array::intersect),
		"array::len" => args::check(ctx, name, args, Args::One, array::len),
		"array::sort" => args::check(ctx, name, args, Args::OneTwo, array::sort),
		"array::union" => args::check(ctx, name, args, Args::Two, array::union),
		"array::sort::asc" => args::check(ctx, name, args, Args::One, array::sort::asc),
		"array::sort::desc" => args::check(ctx, name, args, Args::One, array::sort::desc),
		//
		"count" => args::check(ctx, name, args, Args::NoneOne, count::count),
		//
		"crypto::md5" => args::check(ctx, name, args, Args::One, crypto::md5),
		"crypto::sha1" => args::check(ctx, name, args, Args::One, crypto::sha1),
		"crypto::sha256" => args::check(ctx, name, args, Args::One, crypto::sha256),
		"crypto::sha512" => args::check(ctx, name, args, Args::One, crypto::sha512),
		"crypto::argon2::compare" => args::check(ctx, name, args, Args::Two, crypto::argon2::cmp),
		"crypto::argon2::generate" => args::check(ctx, name, args, Args::One, crypto::argon2::gen),
		"crypto::pbkdf2::compare" => args::check(ctx, name, args, Args::Two, crypto::pbkdf2::cmp),
		"crypto::pbkdf2::generate" => args::check(ctx, name, args, Args::One, crypto::pbkdf2::gen),
		"crypto::scrypt::compare" => args::check(ctx, name, args, Args::Two, crypto::scrypt::cmp),
		"crypto::scrypt::generate" => args::check(ctx, name, args, Args::One, crypto::scrypt::gen),
		//
		"geo::area" => args::check(ctx, name, args, Args::One, geo::area),
		"geo::bearing" => args::check(ctx, name, args, Args::Two, geo::bearing),
		"geo::centroid" => args::check(ctx, name, args, Args::One, geo::centroid),
		"geo::distance" => args::check(ctx, name, args, Args::Two, geo::distance),
		"geo::hash::decode" => args::check(ctx, name, args, Args::One, geo::hash::decode),
		"geo::hash::encode" => args::check(ctx, name, args, Args::OneTwo, geo::hash::encode),
		//
		"is::alphanum" => args::check(ctx, name, args, Args::One, is::alphanum),
		"is::alpha" => args::check(ctx, name, args, Args::One, is::alpha),
		"is::ascii" => args::check(ctx, name, args, Args::One, is::ascii),
		"is::domain" => args::check(ctx, name, args, Args::One, is::domain),
		"is::email" => args::check(ctx, name, args, Args::One, is::email),
		"is::hexadecimal" => args::check(ctx, name, args, Args::One, is::hexadecimal),
		"is::latitude" => args::check(ctx, name, args, Args::One, is::latitude),
		"is::longitude" => args::check(ctx, name, args, Args::One, is::longitude),
		"is::numeric" => args::check(ctx, name, args, Args::One, is::numeric),
		"is::semver" => args::check(ctx, name, args, Args::One, is::semver),
		"is::uuid" => args::check(ctx, name, args, Args::One, is::uuid),
		//
		"math::abs" => args::check(ctx, name, args, Args::One, math::abs),
		"math::bottom" => args::check(ctx, name, args, Args::Two, math::bottom),
		"math::ceil" => args::check(ctx, name, args, Args::One, math::ceil),
		"math::fixed" => args::check(ctx, name, args, Args::Two, math::fixed),
		"math::floor" => args::check(ctx, name, args, Args::One, math::floor),
		"math::interquartile" => args::check(ctx, name, args, Args::One, math::interquartile),
		"math::max" => args::check(ctx, name, args, Args::One, math::max),
		"math::mean" => args::check(ctx, name, args, Args::One, math::mean),
		"math::median" => args::check(ctx, name, args, Args::One, math::median),
		"math::midhinge" => args::check(ctx, name, args, Args::One, math::midhinge),
		"math::min" => args::check(ctx, name, args, Args::One, math::min),
		"math::mode" => args::check(ctx, name, args, Args::One, math::mode),
		"math::nearestrank" => args::check(ctx, name, args, Args::Two, math::nearestrank),
		"math::percentile" => args::check(ctx, name, args, Args::Two, math::percentile),
		"math::product" => args::check(ctx, name, args, Args::One, math::product),
		"math::round" => args::check(ctx, name, args, Args::One, math::round),
		"math::spread" => args::check(ctx, name, args, Args::One, math::spread),
		"math::sqrt" => args::check(ctx, name, args, Args::One, math::sqrt),
		"math::stddev" => args::check(ctx, name, args, Args::One, math::stddev),
		"math::sum" => args::check(ctx, name, args, Args::One, math::sum),
		"math::top" => args::check(ctx, name, args, Args::Two, math::top),
		"math::trimean" => args::check(ctx, name, args, Args::One, math::trimean),
		"math::variance" => args::check(ctx, name, args, Args::One, math::variance),
		//
		"parse::email::host" => args::check(ctx, name, args, Args::One, parse::email::host),
		"parse::email::user" => args::check(ctx, name, args, Args::One, parse::email::user),
		"parse::url::domain" => args::check(ctx, name, args, Args::One, parse::url::domain),
		"parse::url::fragment" => args::check(ctx, name, args, Args::One, parse::url::fragment),
		"parse::url::host" => args::check(ctx, name, args, Args::One, parse::url::host),
		"parse::url::path" => args::check(ctx, name, args, Args::One, parse::url::path),
		"parse::url::port" => args::check(ctx, name, args, Args::One, parse::url::port),
		"parse::url::query" => args::check(ctx, name, args, Args::One, parse::url::query),
		//
		"rand::bool" => args::check(ctx, name, args, Args::None, rand::bool),
		"rand::enum" => args::check(ctx, name, args, Args::Any, rand::r#enum),
		"rand::float" => args::check(ctx, name, args, Args::NoneTwo, rand::float),
		"rand::guid" => args::check(ctx, name, args, Args::NoneOne, rand::guid),
		"rand::int" => args::check(ctx, name, args, Args::NoneTwo, rand::int),
		"rand::string" => args::check(ctx, name, args, Args::NoneOneTwo, rand::string),
		"rand::time" => args::check(ctx, name, args, Args::NoneTwo, rand::time),
		"rand::uuid" => args::check(ctx, name, args, Args::None, rand::uuid),
		"rand" => args::check(ctx, name, args, Args::None, rand::rand),
		//
		"session::db" => args::check(ctx, name, args, Args::None, session::db),
		"session::id" => args::check(ctx, name, args, Args::None, session::id),
		"session::ip" => args::check(ctx, name, args, Args::None, session::ip),
		"session::ns" => args::check(ctx, name, args, Args::None, session::ns),
		"session::origin" => args::check(ctx, name, args, Args::None, session::origin),
		"session::sc" => args::check(ctx, name, args, Args::None, session::sc),
		"session::sd" => args::check(ctx, name, args, Args::None, session::sd),
		//
		"string::concat" => args::check(ctx, name, args, Args::Any, string::concat),
		"string::endsWith" => args::check(ctx, name, args, Args::Two, string::ends_with),
		"string::join" => args::check(ctx, name, args, Args::Any, string::join),
		"string::length" => args::check(ctx, name, args, Args::One, string::length),
		"string::lowercase" => args::check(ctx, name, args, Args::One, string::lowercase),
		"string::repeat" => args::check(ctx, name, args, Args::Two, string::repeat),
		"string::replace" => args::check(ctx, name, args, Args::Three, string::replace),
		"string::reverse" => args::check(ctx, name, args, Args::One, string::reverse),
		"string::slice" => args::check(ctx, name, args, Args::Three, string::slice),
		"string::slug" => args::check(ctx, name, args, Args::OneTwo, string::slug),
		"string::split" => args::check(ctx, name, args, Args::Two, string::split),
		"string::startsWith" => args::check(ctx, name, args, Args::Two, string::starts_with),
		"string::trim" => args::check(ctx, name, args, Args::One, string::trim),
		"string::uppercase" => args::check(ctx, name, args, Args::One, string::uppercase),
		"string::words" => args::check(ctx, name, args, Args::One, string::words),
		//
		"time::day" => args::check(ctx, name, args, Args::NoneOne, time::day),
		"time::floor" => args::check(ctx, name, args, Args::Two, time::floor),
		"time::group" => args::check(ctx, name, args, Args::Two, time::group),
		"time::hour" => args::check(ctx, name, args, Args::NoneOne, time::hour),
		"time::mins" => args::check(ctx, name, args, Args::NoneOne, time::mins),
		"time::month" => args::check(ctx, name, args, Args::NoneOne, time::month),
		"time::nano" => args::check(ctx, name, args, Args::NoneOne, time::nano),
		"time::now" => args::check(ctx, name, args, Args::None, time::now),
		"time::round" => args::check(ctx, name, args, Args::Two, time::round),
		"time::secs" => args::check(ctx, name, args, Args::NoneOne, time::secs),
		"time::unix" => args::check(ctx, name, args, Args::NoneOne, time::unix),
		"time::wday" => args::check(ctx, name, args, Args::NoneOne, time::wday),
		"time::week" => args::check(ctx, name, args, Args::NoneOne, time::week),
		"time::yday" => args::check(ctx, name, args, Args::NoneOne, time::yday),
		"time::year" => args::check(ctx, name, args, Args::NoneOne, time::year),
		//
		"type::bool" => args::check(ctx, name, args, Args::One, r#type::bool),
		"type::datetime" => args::check(ctx, name, args, Args::One, r#type::datetime),
		"type::decimal" => args::check(ctx, name, args, Args::One, r#type::decimal),
		"type::duration" => args::check(ctx, name, args, Args::One, r#type::duration),
		"type::float" => args::check(ctx, name, args, Args::One, r#type::float),
		"type::int" => args::check(ctx, name, args, Args::One, r#type::int),
		"type::number" => args::check(ctx, name, args, Args::One, r#type::number),
		"type::point" => args::check(ctx, name, args, Args::OneTwo, r#type::point),
		"type::regex" => args::check(ctx, name, args, Args::One, r#type::regex),
		"type::string" => args::check(ctx, name, args, Args::One, r#type::string),
		"type::table" => args::check(ctx, name, args, Args::One, r#type::table),
		"type::thing" => args::check(ctx, name, args, Args::OneTwo, r#type::thing),
		//
		_ => unreachable!(),
	}
}

// Attempts to run an asynchronous function
pub async fn asynchronous(ctx: &Context<'_>, name: &str, args: Vec<Value>) -> Result<Value, Error> {
	match name {
		//
		"http::head" => http::head(ctx, args).await,
		"http::get" => http::get(ctx, args).await,
		"http::put" => http::put(ctx, args).await,
		"http::post" => http::post(ctx, args).await,
		"http::patch" => http::patch(ctx, args).await,
		"http::delete" => http::delete(ctx, args).await,
		//
		_ => unreachable!(),
	}
}
