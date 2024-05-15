use super::{ParseResult, Parser};
use crate::{
	sql::{Constant, Function, Ident, Value},
	syn::{
		parser::{mac::expected, ParseError, ParseErrorKind},
		token::{t, Span},
	},
};
use phf::phf_map;
use reblessive::Stk;
use unicase::UniCase;

const MAX_LEVENSTHEIN_CUT_OFF: u8 = 4;
const MAX_FUNCTION_NAME_LEN: usize = 33;
const LEVENSTHEIN_ARRAY_SIZE: usize = 1 + MAX_FUNCTION_NAME_LEN + MAX_LEVENSTHEIN_CUT_OFF as usize;

/// simple function calculating levenshtein distance with a cut-off.
///
/// levenshtein distance seems fast enough for searching possible functions to suggest as the list
/// isn't that long and the function names aren't that long. Additionally this function also uses a
/// cut off for quick rejection of strings which won't lower the minimum searched distance.
///
/// Function uses stack allocated array's of size LEVENSTHEIN_ARRAY_SIZE. LEVENSTHEIN_ARRAY_SIZE should the largest size in the haystack +
/// maximum cut_off + 1 for the additional value required during calculation
fn levenshtein(a: &[u8], b: &[u8], cut_off: u8) -> u8 {
	debug_assert!(LEVENSTHEIN_ARRAY_SIZE < u8::MAX as usize);
	let mut distance_array = [[0u8; LEVENSTHEIN_ARRAY_SIZE]; 2];

	if a.len().abs_diff(b.len()) > cut_off as usize {
		// moving from a to b requires atleast more then cut off insertions or deletions so don't
		// even bother.
		return cut_off + 1;
	}

	// at this point a and b shouldn't be larger then LEVENSTHEIN_ARRAY_SIZE
	// because otherwise they would have been rejected by the previous if statement.
	assert!(a.len() < LEVENSTHEIN_ARRAY_SIZE);
	assert!(b.len() < LEVENSTHEIN_ARRAY_SIZE);

	for i in 1..=a.len() {
		distance_array[0][i] = i as u8;
	}

	for i in 1..=b.len() {
		let current = i & 1;
		let prev = current ^ 1;
		distance_array[current][0] = i as u8;

		let mut lowest = i as u8;

		for j in 1..=a.len() {
			let cost = (a.get(j - 1).map(|x| x.to_ascii_lowercase())
				!= b.get(j - 1).map(|x| x.to_ascii_lowercase())) as u8;

			let res = (distance_array[prev][j] + 1)
				.min(distance_array[current][j - 1] + 1)
				.min(distance_array[prev][j - 1] + cost);

			distance_array[current][j] = res;
			lowest = res.min(lowest)
		}

		// The lowest value in the next calculated row will always be equal or larger then the
		// lowest value of the current row. So we can cut off search early if the score can't equal
		// the cut_off.
		if lowest > cut_off {
			return cut_off + 1;
		}
	}
	distance_array[b.len() & 1][a.len()]
}

/// The kind of a parsed path.
#[non_exhaustive]
pub enum PathKind {
	Constant(Constant),
	Function,
}

/// A map of path strings for parsing paths.
pub(crate) static PATHS: phf::Map<UniCase<&'static str>, PathKind> = phf_map! {
		UniCase::ascii("array::add") => PathKind::Function,
		UniCase::ascii("array::all") => PathKind::Function,
		UniCase::ascii("array::any") => PathKind::Function,
		UniCase::ascii("array::append") => PathKind::Function,
		UniCase::ascii("array::at") => PathKind::Function,
		UniCase::ascii("array::boolean_and") => PathKind::Function,
		UniCase::ascii("array::boolean_not") => PathKind::Function,
		UniCase::ascii("array::boolean_or") => PathKind::Function,
		UniCase::ascii("array::boolean_xor") => PathKind::Function,
		UniCase::ascii("array::clump") => PathKind::Function,
		UniCase::ascii("array::combine") => PathKind::Function,
		UniCase::ascii("array::complement") => PathKind::Function,
		UniCase::ascii("array::concat") => PathKind::Function,
		UniCase::ascii("array::difference") => PathKind::Function,
		UniCase::ascii("array::distinct") => PathKind::Function,
		UniCase::ascii("array::filter_index") => PathKind::Function,
		UniCase::ascii("array::find_index") => PathKind::Function,
		UniCase::ascii("array::first") => PathKind::Function,
		UniCase::ascii("array::flatten") => PathKind::Function,
		UniCase::ascii("array::group") => PathKind::Function,
		UniCase::ascii("array::insert") => PathKind::Function,
		UniCase::ascii("array::intersect") => PathKind::Function,
		UniCase::ascii("array::join") => PathKind::Function,
		UniCase::ascii("array::last") => PathKind::Function,
		UniCase::ascii("array::len") => PathKind::Function,
		UniCase::ascii("array::logical_and") => PathKind::Function,
		UniCase::ascii("array::logical_or") => PathKind::Function,
		UniCase::ascii("array::logical_xor") => PathKind::Function,
		UniCase::ascii("array::matches") => PathKind::Function,
		UniCase::ascii("array::max") => PathKind::Function,
		UniCase::ascii("array::min") => PathKind::Function,
		UniCase::ascii("array::pop") => PathKind::Function,
		UniCase::ascii("array::prepend") => PathKind::Function,
		UniCase::ascii("array::push") => PathKind::Function,
		UniCase::ascii("array::remove") => PathKind::Function,
		UniCase::ascii("array::reverse") => PathKind::Function,
		UniCase::ascii("array::shuffle") => PathKind::Function,
		UniCase::ascii("array::slice") => PathKind::Function,
		UniCase::ascii("array::sort") => PathKind::Function,
		UniCase::ascii("array::transpose") => PathKind::Function,
		UniCase::ascii("array::union") => PathKind::Function,
		UniCase::ascii("array::sort::asc") => PathKind::Function,
		UniCase::ascii("array::sort::desc") => PathKind::Function,
		//
		UniCase::ascii("object::entries") => PathKind::Function,
		UniCase::ascii("object::from_entries") => PathKind::Function,
		UniCase::ascii("object::keys") => PathKind::Function,
		UniCase::ascii("object::len") => PathKind::Function,
		UniCase::ascii("object::values") => PathKind::Function,
		UniCase::ascii("object::matches") => PathKind::Function,
		//
		UniCase::ascii("bytes::len") => PathKind::Function,
		//
		UniCase::ascii("count") => PathKind::Function,
		//
		UniCase::ascii("crypto::md5") => PathKind::Function,
		UniCase::ascii("crypto::sha1") => PathKind::Function,
		UniCase::ascii("crypto::sha256") => PathKind::Function,
		UniCase::ascii("crypto::sha512") => PathKind::Function,
		//
		UniCase::ascii("duration::days") => PathKind::Function,
		UniCase::ascii("duration::hours") => PathKind::Function,
		UniCase::ascii("duration::micros") => PathKind::Function,
		UniCase::ascii("duration::millis") => PathKind::Function,
		UniCase::ascii("duration::mins") => PathKind::Function,
		UniCase::ascii("duration::nanos") => PathKind::Function,
		UniCase::ascii("duration::secs") => PathKind::Function,
		UniCase::ascii("duration::weeks") => PathKind::Function,
		UniCase::ascii("duration::years") => PathKind::Function,
		UniCase::ascii("duration::from::days") => PathKind::Function,
		UniCase::ascii("duration::from::hours") => PathKind::Function,
		UniCase::ascii("duration::from::micros") => PathKind::Function,
		UniCase::ascii("duration::from::millis") => PathKind::Function,
		UniCase::ascii("duration::from::mins") => PathKind::Function,
		UniCase::ascii("duration::from::nanos") => PathKind::Function,
		UniCase::ascii("duration::from::secs") => PathKind::Function,
		UniCase::ascii("duration::from::weeks") => PathKind::Function,
		//
		UniCase::ascii("encoding::base64::decode") => PathKind::Function,
		UniCase::ascii("encoding::base64::encode") => PathKind::Function,
		//
		UniCase::ascii("geo::area") => PathKind::Function,
		UniCase::ascii("geo::bearing") => PathKind::Function,
		UniCase::ascii("geo::centroid") => PathKind::Function,
		UniCase::ascii("geo::distance") => PathKind::Function,
		UniCase::ascii("geo::hash::decode") => PathKind::Function,
		UniCase::ascii("geo::hash::encode") => PathKind::Function,
		//
		UniCase::ascii("math::abs") => PathKind::Function,
		UniCase::ascii("math::bottom") => PathKind::Function,
		UniCase::ascii("math::ceil") => PathKind::Function,
		UniCase::ascii("math::fixed") => PathKind::Function,
		UniCase::ascii("math::floor") => PathKind::Function,
		UniCase::ascii("math::interquartile") => PathKind::Function,
		UniCase::ascii("math::max") => PathKind::Function,
		UniCase::ascii("math::mean") => PathKind::Function,
		UniCase::ascii("math::median") => PathKind::Function,
		UniCase::ascii("math::midhinge") => PathKind::Function,
		UniCase::ascii("math::min") => PathKind::Function,
		UniCase::ascii("math::mode") => PathKind::Function,
		UniCase::ascii("math::nearestrank") => PathKind::Function,
		UniCase::ascii("math::percentile") => PathKind::Function,
		UniCase::ascii("math::pow") => PathKind::Function,
		UniCase::ascii("math::product") => PathKind::Function,
		UniCase::ascii("math::round") => PathKind::Function,
		UniCase::ascii("math::spread") => PathKind::Function,
		UniCase::ascii("math::sqrt") => PathKind::Function,
		UniCase::ascii("math::stddev") => PathKind::Function,
		UniCase::ascii("math::sum") => PathKind::Function,
		UniCase::ascii("math::top") => PathKind::Function,
		UniCase::ascii("math::trimean") => PathKind::Function,
		UniCase::ascii("math::variance") => PathKind::Function,
		//
		UniCase::ascii("meta::id") => PathKind::Function,
		UniCase::ascii("meta::table") => PathKind::Function,
		UniCase::ascii("meta::tb") => PathKind::Function,
		//
		UniCase::ascii("not") => PathKind::Function,
		//
		UniCase::ascii("parse::email::host") => PathKind::Function,
		UniCase::ascii("parse::email::user") => PathKind::Function,
		UniCase::ascii("parse::url::domain") => PathKind::Function,
		UniCase::ascii("parse::url::fragment") => PathKind::Function,
		UniCase::ascii("parse::url::host") => PathKind::Function,
		UniCase::ascii("parse::url::path") => PathKind::Function,
		UniCase::ascii("parse::url::port") => PathKind::Function,
		UniCase::ascii("parse::url::query") => PathKind::Function,
		UniCase::ascii("parse::url::scheme") => PathKind::Function,
		//
		UniCase::ascii("rand") => PathKind::Function,
		UniCase::ascii("rand::bool") => PathKind::Function,
		UniCase::ascii("rand::enum") => PathKind::Function,
		UniCase::ascii("rand::float") => PathKind::Function,
		UniCase::ascii("rand::guid") => PathKind::Function,
		UniCase::ascii("rand::int") => PathKind::Function,
		UniCase::ascii("rand::string") => PathKind::Function,
		UniCase::ascii("rand::time") => PathKind::Function,
		UniCase::ascii("rand::ulid") => PathKind::Function,
		UniCase::ascii("rand::uuid::v4") => PathKind::Function,
		UniCase::ascii("rand::uuid::v7") => PathKind::Function,
		UniCase::ascii("rand::uuid") => PathKind::Function,
		//
		UniCase::ascii("session::db") => PathKind::Function,
		UniCase::ascii("session::id") => PathKind::Function,
		UniCase::ascii("session::ip") => PathKind::Function,
		UniCase::ascii("session::ns") => PathKind::Function,
		UniCase::ascii("session::origin") => PathKind::Function,
		UniCase::ascii("session::sc") => PathKind::Function,
		UniCase::ascii("session::sd") => PathKind::Function,
		UniCase::ascii("session::token") => PathKind::Function,
		//
		UniCase::ascii("string::concat") => PathKind::Function,
		UniCase::ascii("string::contains") => PathKind::Function,
		UniCase::ascii("string::endsWith") => PathKind::Function,
		UniCase::ascii("string::join") => PathKind::Function,
		UniCase::ascii("string::len") => PathKind::Function,
		UniCase::ascii("string::lowercase") => PathKind::Function,
		UniCase::ascii("string::repeat") => PathKind::Function,
		UniCase::ascii("string::replace") => PathKind::Function,
		UniCase::ascii("string::reverse") => PathKind::Function,
		UniCase::ascii("string::slice") => PathKind::Function,
		UniCase::ascii("string::slug") => PathKind::Function,
		UniCase::ascii("string::split") => PathKind::Function,
		UniCase::ascii("string::startsWith") => PathKind::Function,
		UniCase::ascii("string::trim") => PathKind::Function,
		UniCase::ascii("string::uppercase") => PathKind::Function,
		UniCase::ascii("string::words") => PathKind::Function,
		UniCase::ascii("string::distance::hamming") => PathKind::Function,
		UniCase::ascii("string::distance::levenshtein") => PathKind::Function,
		UniCase::ascii("string::is::alphanum") => PathKind::Function,
		UniCase::ascii("string::is::alpha") => PathKind::Function,
		UniCase::ascii("string::is::ascii") => PathKind::Function,
		UniCase::ascii("string::is::datetime") => PathKind::Function,
		UniCase::ascii("string::is::domain") => PathKind::Function,
		UniCase::ascii("string::is::email") => PathKind::Function,
		UniCase::ascii("string::is::hexadecimal") => PathKind::Function,
		UniCase::ascii("string::is::latitude") => PathKind::Function,
		UniCase::ascii("string::is::longitude") => PathKind::Function,
		UniCase::ascii("string::is::numeric") => PathKind::Function,
		UniCase::ascii("string::is::semver") => PathKind::Function,
		UniCase::ascii("string::is::url") => PathKind::Function,
		UniCase::ascii("string::is::uuid") => PathKind::Function,
		UniCase::ascii("string::semver::compare") => PathKind::Function,
		UniCase::ascii("string::semver::major") => PathKind::Function,
		UniCase::ascii("string::semver::minor") => PathKind::Function,
		UniCase::ascii("string::semver::patch") => PathKind::Function,
		UniCase::ascii("string::semver::inc::major") => PathKind::Function,
		UniCase::ascii("string::semver::inc::minor") => PathKind::Function,
		UniCase::ascii("string::semver::inc::patch") => PathKind::Function,
		UniCase::ascii("string::semver::set::major") => PathKind::Function,
		UniCase::ascii("string::semver::set::minor") => PathKind::Function,
		UniCase::ascii("string::semver::set::patch") => PathKind::Function,
		UniCase::ascii("string::similarity::fuzzy") => PathKind::Function,
		UniCase::ascii("string::similarity::jaro") => PathKind::Function,
		UniCase::ascii("string::similarity::smithwaterman") => PathKind::Function,
		UniCase::ascii("string::matches") => PathKind::Function,
		//
		UniCase::ascii("time::ceil") => PathKind::Function,
		UniCase::ascii("time::day") => PathKind::Function,
		UniCase::ascii("time::floor") => PathKind::Function,
		UniCase::ascii("time::format") => PathKind::Function,
		UniCase::ascii("time::group") => PathKind::Function,
		UniCase::ascii("time::hour") => PathKind::Function,
		UniCase::ascii("time::max") => PathKind::Function,
		UniCase::ascii("time::min") => PathKind::Function,
		UniCase::ascii("time::minute") => PathKind::Function,
		UniCase::ascii("time::month") => PathKind::Function,
		UniCase::ascii("time::nano") => PathKind::Function,
		UniCase::ascii("time::micros") => PathKind::Function,
		UniCase::ascii("time::millis") => PathKind::Function,
		UniCase::ascii("time::now") => PathKind::Function,
		UniCase::ascii("time::round") => PathKind::Function,
		UniCase::ascii("time::second") => PathKind::Function,
		UniCase::ascii("time::timezone") => PathKind::Function,
		UniCase::ascii("time::unix") => PathKind::Function,
		UniCase::ascii("time::wday") => PathKind::Function,
		UniCase::ascii("time::week") => PathKind::Function,
		UniCase::ascii("time::yday") => PathKind::Function,
		UniCase::ascii("time::year") => PathKind::Function,
		UniCase::ascii("time::from::nanos") => PathKind::Function,
		UniCase::ascii("time::from::micros") => PathKind::Function,
		UniCase::ascii("time::from::millis") => PathKind::Function,
		UniCase::ascii("time::from::secs") => PathKind::Function,
		UniCase::ascii("time::from::unix") => PathKind::Function,
		//
		UniCase::ascii("type::bool") => PathKind::Function,
		UniCase::ascii("type::datetime") => PathKind::Function,
		UniCase::ascii("type::decimal") => PathKind::Function,
		UniCase::ascii("type::duration") => PathKind::Function,
		UniCase::ascii("type::float") => PathKind::Function,
		UniCase::ascii("type::int") => PathKind::Function,
		UniCase::ascii("type::number") => PathKind::Function,
		UniCase::ascii("type::point") => PathKind::Function,
		UniCase::ascii("type::string") => PathKind::Function,
		UniCase::ascii("type::table") => PathKind::Function,
		UniCase::ascii("type::thing") => PathKind::Function,
		UniCase::ascii("type::range") => PathKind::Function,
		UniCase::ascii("type::is::array") => PathKind::Function,
		UniCase::ascii("type::is::bool") => PathKind::Function,
		UniCase::ascii("type::is::bytes") => PathKind::Function,
		UniCase::ascii("type::is::collection") => PathKind::Function,
		UniCase::ascii("type::is::datetime") => PathKind::Function,
		UniCase::ascii("type::is::decimal") => PathKind::Function,
		UniCase::ascii("type::is::duration") => PathKind::Function,
		UniCase::ascii("type::is::float") => PathKind::Function,
		UniCase::ascii("type::is::geometry") => PathKind::Function,
		UniCase::ascii("type::is::int") => PathKind::Function,
		UniCase::ascii("type::is::line") => PathKind::Function,
		UniCase::ascii("type::is::null") => PathKind::Function,
		UniCase::ascii("type::is::none") => PathKind::Function,
		UniCase::ascii("type::is::multiline") => PathKind::Function,
		UniCase::ascii("type::is::multipoint") => PathKind::Function,
		UniCase::ascii("type::is::multipolygon") => PathKind::Function,
		UniCase::ascii("type::is::number") => PathKind::Function,
		UniCase::ascii("type::is::object") => PathKind::Function,
		UniCase::ascii("type::is::point") => PathKind::Function,
		UniCase::ascii("type::is::polygon") => PathKind::Function,
		UniCase::ascii("type::is::record") => PathKind::Function,
		UniCase::ascii("type::is::string") => PathKind::Function,
		UniCase::ascii("type::is::uuid") => PathKind::Function,
		//
		UniCase::ascii("vector::add") => PathKind::Function,
		UniCase::ascii("vector::angle") => PathKind::Function,
		UniCase::ascii("vector::cross") => PathKind::Function,
		UniCase::ascii("vector::dot") => PathKind::Function,
		UniCase::ascii("vector::divide") => PathKind::Function,
		UniCase::ascii("vector::magnitude") => PathKind::Function,
		UniCase::ascii("vector::multiply") => PathKind::Function,
		UniCase::ascii("vector::normalize") => PathKind::Function,
		UniCase::ascii("vector::project") => PathKind::Function,
		UniCase::ascii("vector::subtract") => PathKind::Function,
		UniCase::ascii("vector::distance::chebyshev") => PathKind::Function,
		UniCase::ascii("vector::distance::euclidean") => PathKind::Function,
		UniCase::ascii("vector::distance::hamming") => PathKind::Function,
		UniCase::ascii("vector::distance::mahalanobis") => PathKind::Function,
		UniCase::ascii("vector::distance::manhattan") => PathKind::Function,
		UniCase::ascii("vector::distance::minkowski") => PathKind::Function,
		UniCase::ascii("vector::similarity::cosine") => PathKind::Function,
		UniCase::ascii("vector::similarity::jaccard") => PathKind::Function,
		UniCase::ascii("vector::similarity::pearson") => PathKind::Function,
		UniCase::ascii("vector::similarity::spearman") => PathKind::Function,
		//
		UniCase::ascii("crypto::argon2::compare") => PathKind::Function,
		UniCase::ascii("crypto::argon2::generate") => PathKind::Function,
		UniCase::ascii("crypto::bcrypt::compare") => PathKind::Function,
		UniCase::ascii("crypto::bcrypt::generate") => PathKind::Function,
		UniCase::ascii("crypto::pbkdf2::compare") => PathKind::Function,
		UniCase::ascii("crypto::pbkdf2::generate") => PathKind::Function,
		UniCase::ascii("crypto::scrypt::compare") => PathKind::Function,
		UniCase::ascii("crypto::scrypt::generate") => PathKind::Function,
		//
		UniCase::ascii("http::head") => PathKind::Function,
		UniCase::ascii("http::get") => PathKind::Function,
		UniCase::ascii("http::put") => PathKind::Function,
		UniCase::ascii("http::post") => PathKind::Function,
		UniCase::ascii("http::patch") => PathKind::Function,
		UniCase::ascii("http::delete") => PathKind::Function,
		//
		UniCase::ascii("search::analyze") => PathKind::Function,
		UniCase::ascii("search::score") => PathKind::Function,
		UniCase::ascii("search::highlight") => PathKind::Function,
		UniCase::ascii("search::offsets") => PathKind::Function,
		//
		UniCase::ascii("sleep") => PathKind::Function,
		//
		UniCase::ascii("type::field") => PathKind::Function,
		UniCase::ascii("type::fields") => PathKind::Function,

		// constants
		UniCase::ascii("math::E") => PathKind::Constant(Constant::MathE),
		UniCase::ascii("math::FRAC_1_PI") => PathKind::Constant(Constant::MathFrac1Pi),
		UniCase::ascii("math::FRAC_1_SQRT_2") => PathKind::Constant(Constant::MathFrac1Sqrt2),
		UniCase::ascii("math::FRAC_2_PI") => PathKind::Constant(Constant::MathFrac2Pi),
		UniCase::ascii("math::FRAC_2_SQRT_PI") => PathKind::Constant(Constant::MathFrac2SqrtPi),
		UniCase::ascii("math::FRAC_PI_2") => PathKind::Constant(Constant::MathFracPi2),
		UniCase::ascii("math::FRAC_PI_3") => PathKind::Constant(Constant::MathFracPi3),
		UniCase::ascii("math::FRAC_PI_4") => PathKind::Constant(Constant::MathFracPi4),
		UniCase::ascii("math::FRAC_PI_6") => PathKind::Constant(Constant::MathFracPi6),
		UniCase::ascii("math::FRAC_PI_8") => PathKind::Constant(Constant::MathFracPi8),
		UniCase::ascii("math::INF") => PathKind::Constant(Constant::MathInf),
		UniCase::ascii("math::LN_10") => PathKind::Constant(Constant::MathLn10),
		UniCase::ascii("math::LN_2") => PathKind::Constant(Constant::MathLn2),
		UniCase::ascii("math::LOG10_2") => PathKind::Constant(Constant::MathLog102),
		UniCase::ascii("math::LOG10_E") => PathKind::Constant(Constant::MathLog10E),
		UniCase::ascii("math::LOG2_10") => PathKind::Constant(Constant::MathLog210),
		UniCase::ascii("math::LOG2_E") => PathKind::Constant(Constant::MathLog2E),
		UniCase::ascii("math::PI") => PathKind::Constant(Constant::MathPi),
		UniCase::ascii("math::SQRT_2") => PathKind::Constant(Constant::MathSqrt2),
		UniCase::ascii("math::TAU") => PathKind::Constant(Constant::MathTau),
};

impl Parser<'_> {
	/// Parse a builtin path.
	pub async fn parse_builtin(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Value> {
		let mut last_span = start;
		while self.eat(t!("::")) {
			self.next_token_value::<Ident>()?;
			last_span = self.last_span();
		}

		let span = start.covers(last_span);
		let slice = self.lexer.reader.span(span);

		// parser implementations guarentess that the slice is a valid utf8 string.
		let str = std::str::from_utf8(slice).unwrap();

		match PATHS.get_entry(&UniCase::ascii(str)) {
			Some((_, PathKind::Constant(x))) => Ok(Value::Constant(x.clone())),
			Some((k, PathKind::Function)) => stk
				.run(|ctx| self.parse_builtin_function(ctx, k.into_inner().to_owned()))
				.await
				.map(|x| Value::Function(Box::new(x))),
			None => {
				// Generate an suggestion.
				// don't search further if the levenshtein distance is further then 10.
				let mut cut_off = MAX_LEVENSTHEIN_CUT_OFF;

				let possibly = PATHS
					.keys()
					.copied()
					.min_by_key(|x| {
						let res = levenshtein(str.as_bytes(), x.as_bytes(), cut_off);
						cut_off = res.min(cut_off);
						res
					})
					.map(|x| x.into_inner());

				if cut_off == MAX_LEVENSTHEIN_CUT_OFF {
					// couldn't find a value which lowered the cut off,
					// any suggestion probably will be nonsensical so don't give any.
					return Err(ParseError::new(
						ParseErrorKind::InvalidPath {
							possibly: None,
						},
						span,
					));
				}

				Err(ParseError::new(
					ParseErrorKind::InvalidPath {
						possibly,
					},
					span,
				))
			}
		}
	}

	/// Parse a call to a builtin function.
	pub async fn parse_builtin_function(
		&mut self,
		stk: &mut Stk,
		name: String,
	) -> ParseResult<Function> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = stk.run(|ctx| self.parse_value_field(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		Ok(Function::Normal(name, args))
	}
}

#[cfg(test)]
mod test {
	use super::{MAX_FUNCTION_NAME_LEN, PATHS};

	#[test]
	fn function_name_constant_up_to_date() {
		let max = PATHS.keys().map(|x| x.len()).max().unwrap();
		// These two need to be the same but the constant needs to manually be updated if PATHS
		// ever changes so that these two values are not the same.
		assert_eq!(
			MAX_FUNCTION_NAME_LEN, max,
			"the constant MAX_FUNCTION_NAME_LEN should be {} but is {}, please update the constant",
			max, MAX_FUNCTION_NAME_LEN
		);
	}
}
