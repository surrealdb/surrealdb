use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::args::shim;
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
	macro_rules! dispatch {
		($name: ident, $ctx: expr, $args: ident, $($function: path),+, $((ctx) $ctx_function: path),+) => {
			{
				match $name {
					$(stringify!($function) => $function(shim($name, $args)?),)+
					$(stringify!($ctx_function) => $ctx_function($ctx, shim($name, $args)?),)+
					_ => unreachable!()
				}
			}
		}
	}

	dispatch!(
		name,
		ctx,
		args,
		array::combine,
		array::concat,
		array::difference,
		array::distinct,
		array::intersect,
		array::len,
		array::sort,
		array::union,
		array::sort::asc,
		array::sort::desc,
		count::count,
		crypto::md5,
		crypto::sha1,
		crypto::sha256,
		crypto::sha512,
		crypto::argon2::cmp,
		crypto::argon2::gen,
		crypto::pbkdf2::cmp,
		crypto::pbkdf2::gen,
		crypto::scrypt::cmp,
		crypto::scrypt::gen,
		geo::area,
		geo::bearing,
		geo::centroid,
		geo::distance,
		geo::hash::decode,
		geo::hash::encode,
		is::alphanum,
		is::alpha,
		is::ascii,
		is::domain,
		is::email,
		is::hexadecimal,
		is::latitude,
		is::longitude,
		is::numeric,
		is::semver,
		is::uuid,
		math::abs,
		math::bottom,
		math::ceil,
		math::fixed,
		math::floor,
		math::interquartile,
		math::max,
		math::mean,
		math::median,
		math::midhinge,
		math::min,
		math::mode,
		math::nearestrank,
		math::percentile,
		math::product,
		math::round,
		math::spread,
		math::sqrt,
		math::stddev,
		math::sum,
		math::top,
		math::trimean,
		math::variance,
		parse::email::host,
		parse::email::user,
		parse::url::domain,
		parse::url::fragment,
		parse::url::host,
		parse::url::path,
		parse::url::port,
		parse::url::query,
		rand::bool,
		rand::r#enum,
		rand::float,
		rand::guid,
		rand::int,
		rand::string,
		rand::time,
		rand::uuid,
		rand::rand,
		string::concat,
		string::endsWith,
		string::join,
		string::length,
		string::lowercase,
		string::repeat,
		string::replace,
		string::reverse,
		string::slice,
		string::slug,
		string::split,
		string::startsWith,
		string::trim,
		string::uppercase,
		string::words,
		time::day,
		time::floor,
		time::group,
		time::hour,
		time::mins,
		time::month,
		time::nano,
		time::now,
		time::round,
		time::secs,
		time::unix,
		time::wday,
		time::week,
		time::yday,
		time::year,
		r#type::bool,
		r#type::datetime,
		r#type::decimal,
		r#type::duration,
		r#type::float,
		r#type::int,
		r#type::number,
		r#type::point,
		r#type::regex,
		r#type::string,
		r#type::table,
		r#type::thing,
		(ctx) session::db,
		(ctx) session::id,
		(ctx) session::ip,
		(ctx) session::ns,
		(ctx) session::origin,
		(ctx) session::sc,
		(ctx) session::sd
	)
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
