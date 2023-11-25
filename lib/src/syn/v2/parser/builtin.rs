use super::{ParseResult, Parser};
use crate::{
	fnc::util::string::fuzzy::Fuzzy,
	sql::{Constant, Function, Ident, Value},
	syn::v2::{
		parser::{mac::expected, ParseError, ParseErrorKind},
		token::{t, Span},
	},
};
use phf::phf_map;

pub enum PathKind {
	Constant(Constant),
	Function,
}

pub(crate) static PATHS: phf::Map<&'static str, PathKind> = phf_map! {
		"array::add" => PathKind::Function,
		"array::all" => PathKind::Function,
		"array::any" => PathKind::Function,
		"array::append" => PathKind::Function,
		"array::at" => PathKind::Function,
		"array::boolean_and" => PathKind::Function,
		"array::boolean_not" => PathKind::Function,
		"array::boolean_or" => PathKind::Function,
		"array::boolean_xor" => PathKind::Function,
		"array::clump" => PathKind::Function,
		"array::combine" => PathKind::Function,
		"array::complement" => PathKind::Function,
		"array::concat" => PathKind::Function,
		"array::difference" => PathKind::Function,
		"array::distinct" => PathKind::Function,
		"array::filter_index" => PathKind::Function,
		"array::find_index" => PathKind::Function,
		"array::first" => PathKind::Function,
		"array::flatten" => PathKind::Function,
		"array::group" => PathKind::Function,
		"array::insert" => PathKind::Function,
		"array::intersect" => PathKind::Function,
		"array::join" => PathKind::Function,
		"array::last" => PathKind::Function,
		"array::len" => PathKind::Function,
		"array::logical_and" => PathKind::Function,
		"array::logical_or" => PathKind::Function,
		"array::logical_xor" => PathKind::Function,
		"array::matches" => PathKind::Function,
		"array::max" => PathKind::Function,
		"array::min" => PathKind::Function,
		"array::pop" => PathKind::Function,
		"array::prepend" => PathKind::Function,
		"array::push" => PathKind::Function,
		"array::remove" => PathKind::Function,
		"array::reverse" => PathKind::Function,
		"array::slice" => PathKind::Function,
		"array::sort" => PathKind::Function,
		"array::transpose" => PathKind::Function,
		"array::union" => PathKind::Function,
		"array::sort::asc" => PathKind::Function,
		"array::sort::desc" => PathKind::Function,
		//
		"bytes::len" => PathKind::Function,
		//
		"count" => PathKind::Function,
		//
		"crypto::md5" => PathKind::Function,
		"crypto::sha1" => PathKind::Function,
		"crypto::sha256" => PathKind::Function,
		"crypto::sha512" => PathKind::Function,
		//
		"duration::days" => PathKind::Function,
		"duration::hours" => PathKind::Function,
		"duration::micros" => PathKind::Function,
		"duration::millis" => PathKind::Function,
		"duration::mins" => PathKind::Function,
		"duration::nanos" => PathKind::Function,
		"duration::secs" => PathKind::Function,
		"duration::weeks" => PathKind::Function,
		"duration::years" => PathKind::Function,
		"duration::from::days" => PathKind::Function,
		"duration::from::hours" => PathKind::Function,
		"duration::from::micros" => PathKind::Function,
		"duration::from::millis" => PathKind::Function,
		"duration::from::mins" => PathKind::Function,
		"duration::from::nanos" => PathKind::Function,
		"duration::from::secs" => PathKind::Function,
		"duration::from::weeks" => PathKind::Function,
		//
		"encoding::base64::decode" => PathKind::Function,
		"encoding::base64::encode" => PathKind::Function,
		//
		"geo::area" => PathKind::Function,
		"geo::bearing" => PathKind::Function,
		"geo::centroid" => PathKind::Function,
		"geo::distance" => PathKind::Function,
		"geo::hash::decode" => PathKind::Function,
		"geo::hash::encode" => PathKind::Function,
		//
		"math::abs" => PathKind::Function,
		"math::bottom" => PathKind::Function,
		"math::ceil" => PathKind::Function,
		"math::fixed" => PathKind::Function,
		"math::floor" => PathKind::Function,
		"math::interquartile" => PathKind::Function,
		"math::max" => PathKind::Function,
		"math::mean" => PathKind::Function,
		"math::median" => PathKind::Function,
		"math::midhinge" => PathKind::Function,
		"math::min" => PathKind::Function,
		"math::mode" => PathKind::Function,
		"math::nearestrank" => PathKind::Function,
		"math::percentile" => PathKind::Function,
		"math::pow" => PathKind::Function,
		"math::product" => PathKind::Function,
		"math::round" => PathKind::Function,
		"math::spread" => PathKind::Function,
		"math::sqrt" => PathKind::Function,
		"math::stddev" => PathKind::Function,
		"math::sum" => PathKind::Function,
		"math::top" => PathKind::Function,
		"math::trimean" => PathKind::Function,
		"math::variance" => PathKind::Function,
		//
		"meta::id" => PathKind::Function,
		"meta::table" => PathKind::Function,
		"meta::tb" => PathKind::Function,
		//
		"not" => PathKind::Function,
		//
		"parse::email::host" => PathKind::Function,
		"parse::email::user" => PathKind::Function,
		"parse::url::domain" => PathKind::Function,
		"parse::url::fragment" => PathKind::Function,
		"parse::url::host" => PathKind::Function,
		"parse::url::path" => PathKind::Function,
		"parse::url::port" => PathKind::Function,
		"parse::url::query" => PathKind::Function,
		"parse::url::scheme" => PathKind::Function,
		//
		"rand" => PathKind::Function,
		"rand::bool" => PathKind::Function,
		"rand::enum" => PathKind::Function,
		"rand::float" => PathKind::Function,
		"rand::guid" => PathKind::Function,
		"rand::int" => PathKind::Function,
		"rand::string" => PathKind::Function,
		"rand::time" => PathKind::Function,
		"rand::ulid" => PathKind::Function,
		"rand::uuid::v4" => PathKind::Function,
		"rand::uuid::v7" => PathKind::Function,
		"rand::uuid" => PathKind::Function,
		//
		"session::db" => PathKind::Function,
		"session::id" => PathKind::Function,
		"session::ip" => PathKind::Function,
		"session::ns" => PathKind::Function,
		"session::origin" => PathKind::Function,
		"session::sc" => PathKind::Function,
		"session::sd" => PathKind::Function,
		"session::token" => PathKind::Function,
		//
		"string::concat" => PathKind::Function,
		"string::contains" => PathKind::Function,
		"string::endsWith" => PathKind::Function,
		"string::join" => PathKind::Function,
		"string::len" => PathKind::Function,
		"string::lowercase" => PathKind::Function,
		"string::repeat" => PathKind::Function,
		"string::replace" => PathKind::Function,
		"string::reverse" => PathKind::Function,
		"string::slice" => PathKind::Function,
		"string::slug" => PathKind::Function,
		"string::split" => PathKind::Function,
		"string::startsWith" => PathKind::Function,
		"string::trim" => PathKind::Function,
		"string::uppercase" => PathKind::Function,
		"string::words" => PathKind::Function,
		"string::distance::hamming" => PathKind::Function,
		"string::distance::levenshtein" => PathKind::Function,
		"string::is::alphanum" => PathKind::Function,
		"string::is::alpha" => PathKind::Function,
		"string::is::ascii" => PathKind::Function,
		"string::is::datetime" => PathKind::Function,
		"string::is::domain" => PathKind::Function,
		"string::is::email" => PathKind::Function,
		"string::is::hexadecimal" => PathKind::Function,
		"string::is::latitude" => PathKind::Function,
		"string::is::longitude" => PathKind::Function,
		"string::is::numeric" => PathKind::Function,
		"string::is::semver" => PathKind::Function,
		"string::is::url" => PathKind::Function,
		"string::is::uuid" => PathKind::Function,
		"string::similarity::fuzzy" => PathKind::Function,
		"string::similarity::jaro" => PathKind::Function,
		"string::similarity::smithwaterman" => PathKind::Function,
		//
		"time::ceil" => PathKind::Function,
		"time::day" => PathKind::Function,
		"time::floor" => PathKind::Function,
		"time::format" => PathKind::Function,
		"time::group" => PathKind::Function,
		"time::hour" => PathKind::Function,
		"time::max" => PathKind::Function,
		"time::min" => PathKind::Function,
		"time::minute" => PathKind::Function,
		"time::month" => PathKind::Function,
		"time::nano" => PathKind::Function,
		"time::micros" => PathKind::Function,
		"time::millis" => PathKind::Function,
		"time::now" => PathKind::Function,
		"time::round" => PathKind::Function,
		"time::second" => PathKind::Function,
		"time::timezone" => PathKind::Function,
		"time::unix" => PathKind::Function,
		"time::wday" => PathKind::Function,
		"time::week" => PathKind::Function,
		"time::yday" => PathKind::Function,
		"time::year" => PathKind::Function,
		"time::from::nanos" => PathKind::Function,
		"time::from::micros" => PathKind::Function,
		"time::from::millis" => PathKind::Function,
		"time::from::secs" => PathKind::Function,
		"time::from::unix" => PathKind::Function,
		//
		"type::bool" => PathKind::Function,
		"type::datetime" => PathKind::Function,
		"type::decimal" => PathKind::Function,
		"type::duration" => PathKind::Function,
		"type::float" => PathKind::Function,
		"type::int" => PathKind::Function,
		"type::number" => PathKind::Function,
		"type::point" => PathKind::Function,
		"type::string" => PathKind::Function,
		"type::table" => PathKind::Function,
		"type::thing" => PathKind::Function,
		"type::is::array" => PathKind::Function,
		"type::is::bool" => PathKind::Function,
		"type::is::bytes" => PathKind::Function,
		"type::is::collection" => PathKind::Function,
		"type::is::datetime" => PathKind::Function,
		"type::is::decimal" => PathKind::Function,
		"type::is::duration" => PathKind::Function,
		"type::is::float" => PathKind::Function,
		"type::is::geometry" => PathKind::Function,
		"type::is::int" => PathKind::Function,
		"type::is::line" => PathKind::Function,
		"type::is::null" => PathKind::Function,
		"type::is::multiline" => PathKind::Function,
		"type::is::multipoint" => PathKind::Function,
		"type::is::multipolygon" => PathKind::Function,
		"type::is::number" => PathKind::Function,
		"type::is::object" => PathKind::Function,
		"type::is::point" => PathKind::Function,
		"type::is::polygon" => PathKind::Function,
		"type::is::record" => PathKind::Function,
		"type::is::string" => PathKind::Function,
		"type::is::uuid" => PathKind::Function,
		//
		"vector::add" => PathKind::Function,
		"vector::angle" => PathKind::Function,
		"vector::cross" => PathKind::Function,
		"vector::dot" => PathKind::Function,
		"vector::divide" => PathKind::Function,
		"vector::magnitude" => PathKind::Function,
		"vector::multiply" => PathKind::Function,
		"vector::normalize" => PathKind::Function,
		"vector::project" => PathKind::Function,
		"vector::subtract" => PathKind::Function,
		"vector::distance::chebyshev" => PathKind::Function,
		"vector::distance::euclidean" => PathKind::Function,
		"vector::distance::hamming" => PathKind::Function,
		"vector::distance::mahalanobis" => PathKind::Function,
		"vector::distance::manhattan" => PathKind::Function,
		"vector::distance::minkowski" => PathKind::Function,
		"vector::similarity::cosine" => PathKind::Function,
		"vector::similarity::jaccard" => PathKind::Function,
		"vector::similarity::pearson" => PathKind::Function,
		"vector::similarity::spearman" => PathKind::Function,
		//
		"crypto::argon2::compare" => PathKind::Function,
		"crypto::argon2::generate" => PathKind::Function,
		"crypto::bcrypt::compare" => PathKind::Function,
		"crypto::bcrypt::generate" => PathKind::Function,
		"crypto::pbkdf2::compare" => PathKind::Function,
		"crypto::pbkdf2::generate" => PathKind::Function,
		"crypto::scrypt::compare" => PathKind::Function,
		"crypto::scrypt::generate" => PathKind::Function,
		//
		"http::head" => PathKind::Function,
		"http::get" => PathKind::Function,
		"http::put" => PathKind::Function,
		"http::post" => PathKind::Function,
		"http::patch" => PathKind::Function,
		"http::delete" => PathKind::Function,
		//
		"search::score" => PathKind::Function,
		"search::highlight" => PathKind::Function,
		"search::offsets" => PathKind::Function,
		//
		"sleep" => PathKind::Function,
		//
		"type::field" => PathKind::Function,
		"type::fields" => PathKind::Function,

		// constants
		"math::E" => PathKind::Constant(Constant::MathE),
		"math::FRAC_1_PI" => PathKind::Constant(Constant::MathFrac1Pi),
		"math::FRAC_1_SQRT_2" => PathKind::Constant(Constant::MathFrac1Sqrt2),
		"math::FRAC_2_PI" => PathKind::Constant(Constant::MathFrac2Pi),
		"math::FRAC_2_SQRT_PI" => PathKind::Constant(Constant::MathFrac2SqrtPi),
		"math::FRAC_PI_2" => PathKind::Constant(Constant::MathFracPi2),
		"math::FRAC_PI_3" => PathKind::Constant(Constant::MathFracPi3),
		"math::FRAC_PI_4" => PathKind::Constant(Constant::MathFracPi4),
		"math::FRAC_PI_6" => PathKind::Constant(Constant::MathFracPi6),
		"math::FRAC_PI_8" => PathKind::Constant(Constant::MathFracPi8),
		"math::INF" => PathKind::Constant(Constant::MathInf),
		"math::LN_10" => PathKind::Constant(Constant::MathLn10),
		"math::LN_2" => PathKind::Constant(Constant::MathLn2),
		"math::LOG10_2" => PathKind::Constant(Constant::MathLog102),
		"math::LOG10_E" => PathKind::Constant(Constant::MathLog10E),
		"math::LOG2_10" => PathKind::Constant(Constant::MathLog210),
		"math::LOG2_E" => PathKind::Constant(Constant::MathLog2E),
		"math::PI" => PathKind::Constant(Constant::MathPi),
		"math::SQRT_2" => PathKind::Constant(Constant::MathSqrt2),
		"math::TAU" => PathKind::Constant(Constant::MathTau),
};

impl Parser<'_> {
	pub fn parse_builtin(&mut self, start: Span) -> ParseResult<Value> {
		let mut last_span = start;
		while self.eat(t!("::")) {
			self.parse_token_value::<Ident>()?;
			last_span = self.last_span();
		}

		let span = start.covers(last_span);
		let slice = self.lexer.reader.span(span);

		// parser implementations guarentess that the slice is a valid utf8 string.
		debug_assert!(std::str::from_utf8(slice).is_ok());
		let str = unsafe { std::str::from_utf8_unchecked(slice) };

		match PATHS.get(str) {
			Some(PathKind::Constant(x)) => Ok(Value::Constant(x.clone())),
			Some(PathKind::Function) => {
				self.parse_builtin_function(str.to_owned()).map(|x| Value::Function(Box::new(x)))
			}
			None => {
				let possibly = PATHS.keys().copied().min_by_key(|x| x.fuzzy_score(str));
				Err(ParseError::new(
					ParseErrorKind::InvalidPath {
						possibly,
					},
					span,
				))
			}
		}
	}

	pub fn parse_builtin_function(&mut self, name: String) -> ParseResult<Function> {
		let start = expected!(self, "(").span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			args.push(self.parse_value_field()?);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		Ok(Function::Normal(name, args))
	}
}
