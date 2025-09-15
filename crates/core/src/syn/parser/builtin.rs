use phf::phf_map;
use reblessive::Stk;
use unicase::UniCase;

use super::{ParseResult, Parser};
use crate::sql::{Constant, Expr, Function, FunctionCall};
use crate::syn::error::{MessageKind, bail};
use crate::syn::parser::mac::expected;
use crate::syn::parser::{SyntaxError, unexpected};
use crate::syn::token::{Span, t};

/// The kind of a parsed path.
pub enum PathKind {
	Constant(Constant),
	Function,
}

/// A map of path strings for parsing paths.
pub(crate) static PATHS: phf::Map<UniCase<&'static str>, PathKind> = phf_map! {
		UniCase::ascii("api::invoke") => PathKind::Function,
		//
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
		UniCase::ascii("array::every") => PathKind::Function,
		UniCase::ascii("array::fill") => PathKind::Function,
		UniCase::ascii("array::filter") => PathKind::Function,
		UniCase::ascii("array::filter_index") => PathKind::Function,
		UniCase::ascii("array::find") => PathKind::Function,
		UniCase::ascii("array::find_index") => PathKind::Function,
		UniCase::ascii("array::first") => PathKind::Function,
		UniCase::ascii("array::fold") => PathKind::Function,
		UniCase::ascii("array::flatten") => PathKind::Function,
		UniCase::ascii("array::group") => PathKind::Function,
		UniCase::ascii("array::includes") => PathKind::Function,
		UniCase::ascii("array::index_of") => PathKind::Function,
		UniCase::ascii("array::insert") => PathKind::Function,
		UniCase::ascii("array::intersect") => PathKind::Function,
		UniCase::ascii("array::is_empty") => PathKind::Function,
		UniCase::ascii("array::join") => PathKind::Function,
		UniCase::ascii("array::last") => PathKind::Function,
		UniCase::ascii("array::len") => PathKind::Function,
		UniCase::ascii("array::logical_and") => PathKind::Function,
		UniCase::ascii("array::logical_or") => PathKind::Function,
		UniCase::ascii("array::logical_xor") => PathKind::Function,
		UniCase::ascii("array::map") => PathKind::Function,
		UniCase::ascii("array::matches") => PathKind::Function,
		UniCase::ascii("array::max") => PathKind::Function,
		UniCase::ascii("array::min") => PathKind::Function,
		UniCase::ascii("array::pop") => PathKind::Function,
		UniCase::ascii("array::prepend") => PathKind::Function,
		UniCase::ascii("array::push") => PathKind::Function,
		UniCase::ascii("array::range") => PathKind::Function,
		UniCase::ascii("array::reduce") => PathKind::Function,
		UniCase::ascii("array::remove") => PathKind::Function,
		UniCase::ascii("array::repeat") => PathKind::Function,
		UniCase::ascii("array::reverse") => PathKind::Function,
		UniCase::ascii("array::shuffle") => PathKind::Function,
		UniCase::ascii("array::slice") => PathKind::Function,
		UniCase::ascii("array::some") => PathKind::Function,
		UniCase::ascii("array::sort") => PathKind::Function,
		UniCase::ascii("array::sort_natural") => PathKind::Function,
		UniCase::ascii("array::sort_lexical") => PathKind::Function,
		UniCase::ascii("array::sort_natural_lexical") => PathKind::Function,
		UniCase::ascii("array::swap") => PathKind::Function,
		UniCase::ascii("array::transpose") => PathKind::Function,
		UniCase::ascii("array::union") => PathKind::Function,
		UniCase::ascii("array::windows") => PathKind::Function,
		UniCase::ascii("array::sort::asc") => PathKind::Function,
		UniCase::ascii("array::sort::desc") => PathKind::Function,
		//
		UniCase::ascii("bytes::len") => PathKind::Function,
		//
		UniCase::ascii("count") => PathKind::Function,
		//
		UniCase::ascii("crypto::blake3") => PathKind::Function,
		UniCase::ascii("crypto::joaat") => PathKind::Function,
		UniCase::ascii("crypto::md5") => PathKind::Function,
		UniCase::ascii("crypto::sha1") => PathKind::Function,
		UniCase::ascii("crypto::sha256") => PathKind::Function,
		UniCase::ascii("crypto::sha512") => PathKind::Function,
		UniCase::ascii("crypto::argon2::compare") => PathKind::Function,
		UniCase::ascii("crypto::argon2::generate") => PathKind::Function,
		UniCase::ascii("crypto::bcrypt::compare") => PathKind::Function,
		UniCase::ascii("crypto::bcrypt::generate") => PathKind::Function,
		UniCase::ascii("crypto::pbkdf2::compare") => PathKind::Function,
		UniCase::ascii("crypto::pbkdf2::generate") => PathKind::Function,
		UniCase::ascii("crypto::scrypt::compare") => PathKind::Function,
		UniCase::ascii("crypto::scrypt::generate") => PathKind::Function,
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
		UniCase::ascii("encoding::cbor::decode") => PathKind::Function,
		UniCase::ascii("encoding::cbor::encode") => PathKind::Function,
		//
		UniCase::ascii("file::bucket") => PathKind::Function,
		UniCase::ascii("file::key") => PathKind::Function,
		UniCase::ascii("file::put") => PathKind::Function,
		UniCase::ascii("file::put_if_not_exists") => PathKind::Function,
		UniCase::ascii("file::get") => PathKind::Function,
		UniCase::ascii("file::head") => PathKind::Function,
		UniCase::ascii("file::delete") => PathKind::Function,
		UniCase::ascii("file::copy") => PathKind::Function,
		UniCase::ascii("file::copy_if_not_exists") => PathKind::Function,
		UniCase::ascii("file::rename") => PathKind::Function,
		UniCase::ascii("file::rename_if_not_exists") => PathKind::Function,
		UniCase::ascii("file::exists") => PathKind::Function,
		UniCase::ascii("file::list") => PathKind::Function,
		//
		UniCase::ascii("geo::area") => PathKind::Function,
		UniCase::ascii("geo::bearing") => PathKind::Function,
		UniCase::ascii("geo::centroid") => PathKind::Function,
		UniCase::ascii("geo::distance") => PathKind::Function,
		UniCase::ascii("geo::hash::decode") => PathKind::Function,
		UniCase::ascii("geo::hash::encode") => PathKind::Function,
		UniCase::ascii("geo::is::valid") => PathKind::Function,
		//
		UniCase::ascii("http::head") => PathKind::Function,
		UniCase::ascii("http::get") => PathKind::Function,
		UniCase::ascii("http::put") => PathKind::Function,
		UniCase::ascii("http::post") => PathKind::Function,
		UniCase::ascii("http::patch") => PathKind::Function,
		UniCase::ascii("http::delete") => PathKind::Function,
		//
		UniCase::ascii("math::abs") => PathKind::Function,
		UniCase::ascii("math::acos") => PathKind::Function,
		UniCase::ascii("math::acot") => PathKind::Function,
		UniCase::ascii("math::asin") => PathKind::Function,
		UniCase::ascii("math::atan") => PathKind::Function,
		UniCase::ascii("math::bottom") => PathKind::Function,
		UniCase::ascii("math::ceil") => PathKind::Function,
		UniCase::ascii("math::clamp") => PathKind::Function,
		UniCase::ascii("math::cos") => PathKind::Function,
		UniCase::ascii("math::cot") => PathKind::Function,
		UniCase::ascii("math::deg2rad") => PathKind::Function,
		UniCase::ascii("math::fixed") => PathKind::Function,
		UniCase::ascii("math::floor") => PathKind::Function,
		UniCase::ascii("math::interquartile") => PathKind::Function,
		UniCase::ascii("math::lerp") => PathKind::Function,
		UniCase::ascii("math::lerpangle") => PathKind::Function,
		UniCase::ascii("math::ln") => PathKind::Function,
		UniCase::ascii("math::log") => PathKind::Function,
		UniCase::ascii("math::log10") => PathKind::Function,
		UniCase::ascii("math::log2") => PathKind::Function,
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
		UniCase::ascii("math::rad2deg") => PathKind::Function,
		UniCase::ascii("math::round") => PathKind::Function,
		UniCase::ascii("math::sign") => PathKind::Function,
		UniCase::ascii("math::sin") => PathKind::Function,
		UniCase::ascii("math::spread") => PathKind::Function,
		UniCase::ascii("math::sqrt") => PathKind::Function,
		UniCase::ascii("math::stddev") => PathKind::Function,
		UniCase::ascii("math::sum") => PathKind::Function,
		UniCase::ascii("math::tan") => PathKind::Function,
		UniCase::ascii("math::top") => PathKind::Function,
		UniCase::ascii("math::trimean") => PathKind::Function,
		UniCase::ascii("math::variance") => PathKind::Function,
		//
		UniCase::ascii("meta::id") => PathKind::Function,
		UniCase::ascii("meta::tb") => PathKind::Function,
		//
		UniCase::ascii("not") => PathKind::Function,
		//
		UniCase::ascii("object::entries") => PathKind::Function,
		UniCase::ascii("object::extend") => PathKind::Function,
		UniCase::ascii("object::from_entries") => PathKind::Function,
		UniCase::ascii("object::is_empty") => PathKind::Function,
		UniCase::ascii("object::keys") => PathKind::Function,
		UniCase::ascii("object::len") => PathKind::Function,
		UniCase::ascii("object::matches") => PathKind::Function,
		UniCase::ascii("object::remove") => PathKind::Function,
		UniCase::ascii("object::values") => PathKind::Function,
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
		UniCase::ascii("rand::duration") => PathKind::Function,
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
		UniCase::ascii("record::exists") => PathKind::Function,
		UniCase::ascii("record::id") => PathKind::Function,
		UniCase::ascii("record::is::edge") => PathKind::Function,
		UniCase::ascii("record::table") => PathKind::Function,
		UniCase::ascii("record::tb") => PathKind::Function,
		//
		UniCase::ascii("search::analyze") => PathKind::Function,
		UniCase::ascii("search::linear") => PathKind::Function,
		UniCase::ascii("search::rrf") => PathKind::Function,
		UniCase::ascii("search::score") => PathKind::Function,
		UniCase::ascii("search::highlight") => PathKind::Function,
		UniCase::ascii("search::offsets") => PathKind::Function,
		//
		UniCase::ascii("sequence::nextval") => PathKind::Function,
		//
		UniCase::ascii("session::ac") => PathKind::Function,
		UniCase::ascii("session::db") => PathKind::Function,
		UniCase::ascii("session::id") => PathKind::Function,
		UniCase::ascii("session::ip") => PathKind::Function,
		UniCase::ascii("session::ns") => PathKind::Function,
		UniCase::ascii("session::origin") => PathKind::Function,
		UniCase::ascii("session::rd") => PathKind::Function,
		UniCase::ascii("session::token") => PathKind::Function,
		//
		UniCase::ascii("sleep") => PathKind::Function,
		//
		UniCase::ascii("string::concat") => PathKind::Function,
		UniCase::ascii("string::contains") => PathKind::Function,
		UniCase::ascii("string::ends_with") => PathKind::Function,
		UniCase::ascii("string::join") => PathKind::Function,
		UniCase::ascii("string::len") => PathKind::Function,
		UniCase::ascii("string::lowercase") => PathKind::Function,
		UniCase::ascii("string::repeat") => PathKind::Function,
		UniCase::ascii("string::replace") => PathKind::Function,
		UniCase::ascii("string::reverse") => PathKind::Function,
		UniCase::ascii("string::slice") => PathKind::Function,
		UniCase::ascii("string::slug") => PathKind::Function,
		UniCase::ascii("string::split") => PathKind::Function,
		UniCase::ascii("string::starts_with") => PathKind::Function,
		UniCase::ascii("string::trim") => PathKind::Function,
		UniCase::ascii("string::uppercase") => PathKind::Function,
		UniCase::ascii("string::words") => PathKind::Function,
		//
		UniCase::ascii("string::distance::damerau_levenshtein") => PathKind::Function,
		UniCase::ascii("string::distance::hamming") => PathKind::Function,
		UniCase::ascii("string::distance::levenshtein") => PathKind::Function,
		UniCase::ascii("string::distance::normalized_damerau_levenshtein") => PathKind::Function,
		UniCase::ascii("string::distance::normalized_levenshtein") => PathKind::Function,
		UniCase::ascii("string::distance::osa_distance") => PathKind::Function,
		//
		UniCase::ascii("string::html::encode") => PathKind::Function,
		UniCase::ascii("string::html::sanitize") => PathKind::Function,
		UniCase::ascii("string::is::alphanum") => PathKind::Function,
		UniCase::ascii("string::is::alpha") => PathKind::Function,
		UniCase::ascii("string::is::ascii") => PathKind::Function,
		UniCase::ascii("string::is::datetime") => PathKind::Function,
		UniCase::ascii("string::is::domain") => PathKind::Function,
		UniCase::ascii("string::is::email") => PathKind::Function,
		UniCase::ascii("string::is::hexadecimal") => PathKind::Function,
		UniCase::ascii("string::is::ip") => PathKind::Function,
		UniCase::ascii("string::is::ipv4") => PathKind::Function,
		UniCase::ascii("string::is::ipv6") => PathKind::Function,
		UniCase::ascii("string::is::latitude") => PathKind::Function,
		UniCase::ascii("string::is::longitude") => PathKind::Function,
		UniCase::ascii("string::is::numeric") => PathKind::Function,
		UniCase::ascii("string::is::semver") => PathKind::Function,
		UniCase::ascii("string::is::url") => PathKind::Function,
		UniCase::ascii("string::is::ulid") => PathKind::Function,
		UniCase::ascii("string::is::uuid") => PathKind::Function,
		UniCase::ascii("string::is::record") => PathKind::Function,
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
		//
		UniCase::ascii("string::similarity::fuzzy") => PathKind::Function,
		UniCase::ascii("string::similarity::jaro") => PathKind::Function,
		UniCase::ascii("string::similarity::jaro_winkler") => PathKind::Function,
		UniCase::ascii("string::similarity::smithwaterman") => PathKind::Function,
		UniCase::ascii("string::similarity::sorensen_dice") => PathKind::Function,
		UniCase::ascii("string::matches") => PathKind::Function,
		//
		UniCase::ascii("time::ceil") => PathKind::Function,
		UniCase::ascii("time::day") => PathKind::Function,
		UniCase::ascii("time::floor") => PathKind::Function,
		UniCase::ascii("time::format") => PathKind::Function,
		UniCase::ascii("time::group") => PathKind::Function,
		UniCase::ascii("time::hour") => PathKind::Function,
		UniCase::ascii("time::max") => PathKind::Function,
		UniCase::ascii("time::micros") => PathKind::Function,
		UniCase::ascii("time::millis") => PathKind::Function,
		UniCase::ascii("time::min") => PathKind::Function,
		UniCase::ascii("time::minute") => PathKind::Function,
		UniCase::ascii("time::month") => PathKind::Function,
		UniCase::ascii("time::nano") => PathKind::Function,
		UniCase::ascii("time::now") => PathKind::Function,
		UniCase::ascii("time::round") => PathKind::Function,
		UniCase::ascii("time::second") => PathKind::Function,
		UniCase::ascii("time::timezone") => PathKind::Function,
		UniCase::ascii("time::unix") => PathKind::Function,
		UniCase::ascii("time::wday") => PathKind::Function,
		UniCase::ascii("time::week") => PathKind::Function,
		UniCase::ascii("time::yday") => PathKind::Function,
		UniCase::ascii("time::year") => PathKind::Function,
		UniCase::ascii("time::from::micros") => PathKind::Function,
		UniCase::ascii("time::from::millis") => PathKind::Function,
		UniCase::ascii("time::from::nanos") => PathKind::Function,
		UniCase::ascii("time::from::secs") => PathKind::Function,
		UniCase::ascii("time::from::ulid") => PathKind::Function,
		UniCase::ascii("time::from::unix") => PathKind::Function,
		UniCase::ascii("time::from::uuid") => PathKind::Function,
		UniCase::ascii("time::is::leap_year") => PathKind::Function,
		//
		UniCase::ascii("type::array") => PathKind::Function,
		UniCase::ascii("type::bool") => PathKind::Function,
		UniCase::ascii("type::bytes") => PathKind::Function,
		UniCase::ascii("type::datetime") => PathKind::Function,
		UniCase::ascii("type::decimal") => PathKind::Function,
		UniCase::ascii("type::duration") => PathKind::Function,
		UniCase::ascii("type::field") => PathKind::Function,
		UniCase::ascii("type::fields") => PathKind::Function,
		UniCase::ascii("type::file") => PathKind::Function,
		UniCase::ascii("type::float") => PathKind::Function,
		UniCase::ascii("type::geometry") => PathKind::Function,
		UniCase::ascii("type::int") => PathKind::Function,
		UniCase::ascii("type::number") => PathKind::Function,
		UniCase::ascii("type::point") => PathKind::Function,
		UniCase::ascii("type::range") => PathKind::Function,
		UniCase::ascii("type::record") => PathKind::Function,
		UniCase::ascii("type::string") => PathKind::Function,
		UniCase::ascii("type::string_lossy") => PathKind::Function,
		UniCase::ascii("type::table") => PathKind::Function,
		UniCase::ascii("type::thing") => PathKind::Function,
		UniCase::ascii("type::uuid") => PathKind::Function,
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
		UniCase::ascii("type::is::multiline") => PathKind::Function,
		UniCase::ascii("type::is::multipoint") => PathKind::Function,
		UniCase::ascii("type::is::multipolygon") => PathKind::Function,
		UniCase::ascii("type::is::none") => PathKind::Function,
		UniCase::ascii("type::is::null") => PathKind::Function,
		UniCase::ascii("type::is::number") => PathKind::Function,
		UniCase::ascii("type::is::object") => PathKind::Function,
		UniCase::ascii("type::is::point") => PathKind::Function,
		UniCase::ascii("type::is::polygon") => PathKind::Function,
		UniCase::ascii("type::is::range") => PathKind::Function,
		UniCase::ascii("type::is::record") => PathKind::Function,
		UniCase::ascii("type::is::string") => PathKind::Function,
		UniCase::ascii("type::is::uuid") => PathKind::Function,
		//
		UniCase::ascii("value::diff") => PathKind::Function,
		UniCase::ascii("value::patch") => PathKind::Function,
		//
		UniCase::ascii("vector::add") => PathKind::Function,
		UniCase::ascii("vector::angle") => PathKind::Function,
		UniCase::ascii("vector::cross") => PathKind::Function,
		UniCase::ascii("vector::divide") => PathKind::Function,
		UniCase::ascii("vector::dot") => PathKind::Function,
		UniCase::ascii("vector::magnitude") => PathKind::Function,
		UniCase::ascii("vector::multiply") => PathKind::Function,
		UniCase::ascii("vector::normalize") => PathKind::Function,
		UniCase::ascii("vector::project") => PathKind::Function,
		UniCase::ascii("vector::scale") => PathKind::Function,
		UniCase::ascii("vector::subtract") => PathKind::Function,
		UniCase::ascii("vector::distance::chebyshev") => PathKind::Function,
		UniCase::ascii("vector::distance::euclidean") => PathKind::Function,
		UniCase::ascii("vector::distance::hamming") => PathKind::Function,
		UniCase::ascii("vector::distance::knn") => PathKind::Function,
		UniCase::ascii("vector::distance::mahalanobis") => PathKind::Function,
		UniCase::ascii("vector::distance::manhattan") => PathKind::Function,
		UniCase::ascii("vector::distance::minkowski") => PathKind::Function,
		UniCase::ascii("vector::similarity::cosine") => PathKind::Function,
		UniCase::ascii("vector::similarity::jaccard") => PathKind::Function,
		UniCase::ascii("vector::similarity::pearson") => PathKind::Function,
		UniCase::ascii("vector::similarity::spearman") => PathKind::Function,
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
		UniCase::ascii("math::NEG_INF") => PathKind::Constant(Constant::MathNegInf),
		UniCase::ascii("math::PI") => PathKind::Constant(Constant::MathPi),
		UniCase::ascii("math::SQRT_2") => PathKind::Constant(Constant::MathSqrt2),
		UniCase::ascii("math::TAU") => PathKind::Constant(Constant::MathTau),
		UniCase::ascii("time::EPOCH") => PathKind::Constant(Constant::TimeEpoch),
		UniCase::ascii("time::MINIMUM") => PathKind::Constant(Constant::TimeMin),
		UniCase::ascii("time::MAXIMUM") => PathKind::Constant(Constant::TimeMax),
		UniCase::ascii("duration::MAX") => PathKind::Constant(Constant::DurationMax),
		//
		UniCase::ascii("schema::table::exists") => PathKind::Function,
};

const MAX_LEVENSTHEIN_CUT_OFF: u8 = 4;
const MAX_FUNCTION_NAME_LEN: usize = 48;
const LEVENSTHEIN_ARRAY_SIZE: usize = 1 + MAX_FUNCTION_NAME_LEN + MAX_LEVENSTHEIN_CUT_OFF as usize;

/// simple function calculating levenshtein distance with a cut-off.
///
/// levenshtein distance seems fast enough for searching possible functions to
/// suggest as the list isn't that long and the function names aren't that long.
/// Additionally this function also uses a cut off for quick rejection of
/// strings which won't lower the minimum searched distance.
///
/// Function uses stack allocated array's of size LEVENSTHEIN_ARRAY_SIZE.
/// LEVENSTHEIN_ARRAY_SIZE should the largest size in the haystack +
/// maximum cut_off + 1 for the additional value required during calculation
fn levenshtein(a: &[u8], b: &[u8], cut_off: u8) -> u8 {
	debug_assert!(LEVENSTHEIN_ARRAY_SIZE < u8::MAX as usize);
	let mut distance_array = [[0u8; LEVENSTHEIN_ARRAY_SIZE]; 2];

	if a.len().abs_diff(b.len()) > cut_off as usize {
		// moving from a to b requires atleast more then cut off insertions or deletions
		// so don't even bother.
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
				!= b.get(i - 1).map(|x| x.to_ascii_lowercase())) as u8;

			let res = (distance_array[prev][j] + 1)
				.min(distance_array[current][j - 1] + 1)
				.min(distance_array[prev][j - 1] + cost);

			distance_array[current][j] = res;
			lowest = res.min(lowest)
		}

		// The lowest value in the next calculated row will always be equal or larger
		// then the lowest value of the current row. So we can cut off search early if
		// the score can't equal the cut_off.
		if lowest > cut_off {
			return cut_off + 1;
		}
	}
	distance_array[b.len() & 1][a.len()]
}

fn find_suggestion(got: &str) -> Option<&'static str> {
	// Generate a suggestion.
	// don't search further if the levenshtein distance is further then 10.
	let mut cut_off = MAX_LEVENSTHEIN_CUT_OFF;

	let possibly = PATHS
		.keys()
		.copied()
		.min_by_key(|x| {
			let res = levenshtein(got.as_bytes(), x.as_bytes(), cut_off);
			cut_off = res.min(cut_off);
			res
		})
		.map(|x| x.into_inner());

	if cut_off >= MAX_LEVENSTHEIN_CUT_OFF {
		return None;
	}

	possibly
}

impl Parser<'_> {
	/// Parse a builtin path.
	pub(super) async fn parse_builtin(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Expr> {
		let mut last_span = start;
		while self.eat(t!("::")) {
			let peek = self.peek();
			if !Self::kind_is_identifier(peek.kind) {
				unexpected!(self, peek, "an identifier")
			}
			self.pop_peek();
			last_span = self.last_span();
		}

		let span = start.covers(last_span);
		let str = self.lexer.span_str(span);

		match PATHS.get_entry(&UniCase::ascii(str)) {
			Some((_, PathKind::Constant(x))) => Ok(Expr::Constant(x.clone())),
			Some((k, PathKind::Function)) => {
				// TODO: Move this out of the parser.
				if k == &UniCase::ascii("api::invoke") && !self.settings.define_api_enabled {
					bail!("Cannot use the `api::invoke` method, as the experimental define api capability is not enabled", @span);
				}

				stk.run(|ctx| self.parse_builtin_function(ctx, k.into_inner().to_owned()))
					.await
					.map(|x| Expr::FunctionCall(Box::new(x)))
			}
			None => {
				if let Some(suggest) = find_suggestion(str) {
					Err(SyntaxError::new(format_args!(
						"Invalid function/constant path, did you maybe mean `{suggest}`"
					))
					.with_span(span, MessageKind::Error))
				} else {
					Err(SyntaxError::new("Invalid function/constant path")
						.with_span(span, MessageKind::Error))
				}
			}
		}
	}

	/// Parse a call to a builtin function.
	pub(super) async fn parse_builtin_function(
		&mut self,
		stk: &mut Stk,
		name: String,
	) -> ParseResult<FunctionCall> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		let receiver = Function::Normal(name);
		Ok(FunctionCall {
			receiver,
			arguments: args,
		})
	}
}

#[cfg(test)]
mod test {
	use super::{MAX_FUNCTION_NAME_LEN, PATHS};

	#[test]
	fn function_name_constant_up_to_date() {
		let max = PATHS.keys().map(|x| x.len()).max().unwrap();
		// These two need to be the same but the constant needs to manually be updated
		// if PATHS ever changes so that these two values are not the same.
		assert_eq!(
			MAX_FUNCTION_NAME_LEN, max,
			"the constant MAX_FUNCTION_NAME_LEN should be {} but is {}, please update the constant",
			max, MAX_FUNCTION_NAME_LEN
		);
	}

	#[test]
	fn function_suggestion() {
		assert_eq!(super::levenshtein(b"    book", b"    ook", 5), 1);
		assert_eq!(super::find_suggestion("string::start_with"), Some("string::starts_with"));
	}
}
