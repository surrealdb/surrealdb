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
/// The final item in the map is Some when a path has been renamed to the path on the left.
/// This is used to show users the exact new paths without needing string similarity to make an
/// educated guess.
pub(crate) static PATHS: phf::Map<
	UniCase<&'static str>,
	(PathKind, Option<UniCase<&'static str>>),
> = phf_map! {
		UniCase::ascii("api::invoke") => (PathKind::Function, None),
		UniCase::ascii("api::timeout") => (PathKind::Function, None),
		UniCase::ascii("api::req::body") => (PathKind::Function, None),
		UniCase::ascii("api::res::body") => (PathKind::Function, None),
		UniCase::ascii("api::res::status") => (PathKind::Function, None),
		UniCase::ascii("api::res::header") => (PathKind::Function, None),
		UniCase::ascii("api::res::headers") => (PathKind::Function, None),
		//
		UniCase::ascii("array::add") => (PathKind::Function, None),
		UniCase::ascii("array::all") => (PathKind::Function, None),
		UniCase::ascii("array::any") => (PathKind::Function, None),
		UniCase::ascii("array::append") => (PathKind::Function, None),
		UniCase::ascii("array::at") => (PathKind::Function, None),
		UniCase::ascii("array::boolean_and") => (PathKind::Function, None),
		UniCase::ascii("array::boolean_not") => (PathKind::Function, None),
		UniCase::ascii("array::boolean_or") => (PathKind::Function, None),
		UniCase::ascii("array::boolean_xor") => (PathKind::Function, None),
		UniCase::ascii("array::clump") => (PathKind::Function, None),
		UniCase::ascii("array::combine") => (PathKind::Function, None),
		UniCase::ascii("array::complement") => (PathKind::Function, None),
		UniCase::ascii("array::concat") => (PathKind::Function, None),
		UniCase::ascii("array::difference") => (PathKind::Function, None),
		UniCase::ascii("array::distinct") => (PathKind::Function, None),
		UniCase::ascii("array::every") => (PathKind::Function, None),
		UniCase::ascii("array::fill") => (PathKind::Function, None),
		UniCase::ascii("array::filter") => (PathKind::Function, None),
		UniCase::ascii("array::filter_index") => (PathKind::Function, None),
		UniCase::ascii("array::find") => (PathKind::Function, None),
		UniCase::ascii("array::find_index") => (PathKind::Function, None),
		UniCase::ascii("array::first") => (PathKind::Function, None),
		UniCase::ascii("array::fold") => (PathKind::Function, None),
		UniCase::ascii("array::flatten") => (PathKind::Function, None),
		UniCase::ascii("array::group") => (PathKind::Function, None),
		UniCase::ascii("array::includes") => (PathKind::Function, None),
		UniCase::ascii("array::index_of") => (PathKind::Function, None),
		UniCase::ascii("array::insert") => (PathKind::Function, None),
		UniCase::ascii("array::intersect") => (PathKind::Function, None),
		UniCase::ascii("array::is_empty") => (PathKind::Function, None),
		UniCase::ascii("array::join") => (PathKind::Function, None),
		UniCase::ascii("array::last") => (PathKind::Function, None),
		UniCase::ascii("array::len") => (PathKind::Function, None),
		UniCase::ascii("array::logical_and") => (PathKind::Function, None),
		UniCase::ascii("array::logical_or") => (PathKind::Function, None),
		UniCase::ascii("array::logical_xor") => (PathKind::Function, None),
		UniCase::ascii("array::map") => (PathKind::Function, None),
		UniCase::ascii("array::matches") => (PathKind::Function, None),
		UniCase::ascii("array::max") => (PathKind::Function, None),
		UniCase::ascii("array::min") => (PathKind::Function, None),
		UniCase::ascii("array::pop") => (PathKind::Function, None),
		UniCase::ascii("array::prepend") => (PathKind::Function, None),
		UniCase::ascii("array::push") => (PathKind::Function, None),
		UniCase::ascii("array::range") => (PathKind::Function, None),
		UniCase::ascii("array::reduce") => (PathKind::Function, None),
		UniCase::ascii("array::remove") => (PathKind::Function, None),
		UniCase::ascii("array::repeat") => (PathKind::Function, None),
		UniCase::ascii("array::reverse") => (PathKind::Function, None),
		UniCase::ascii("array::sequence") => (PathKind::Function, None),
		UniCase::ascii("array::shuffle") => (PathKind::Function, None),
		UniCase::ascii("array::slice") => (PathKind::Function, None),
		UniCase::ascii("array::some") => (PathKind::Function, None),
		UniCase::ascii("array::sort") => (PathKind::Function, None),
		UniCase::ascii("array::sort_natural") => (PathKind::Function, None),
		UniCase::ascii("array::sort_lexical") => (PathKind::Function, None),
		UniCase::ascii("array::sort_natural_lexical") => (PathKind::Function, None),
		UniCase::ascii("array::swap") => (PathKind::Function, None),
		UniCase::ascii("array::transpose") => (PathKind::Function, None),
		UniCase::ascii("array::union") => (PathKind::Function, None),
		UniCase::ascii("array::windows") => (PathKind::Function, None),
		UniCase::ascii("array::sort::asc") => (PathKind::Function, None),
		UniCase::ascii("array::sort::desc") => (PathKind::Function, None),
		//
		UniCase::ascii("bytes::len") => (PathKind::Function, None),
		//
		UniCase::ascii("count") => (PathKind::Function, None),
		//
		UniCase::ascii("crypto::blake3") => (PathKind::Function, None),
		UniCase::ascii("crypto::joaat") => (PathKind::Function, None),
		UniCase::ascii("crypto::md5") => (PathKind::Function, None),
		UniCase::ascii("crypto::sha1") => (PathKind::Function, None),
		UniCase::ascii("crypto::sha256") => (PathKind::Function, None),
		UniCase::ascii("crypto::sha512") => (PathKind::Function, None),
		UniCase::ascii("crypto::argon2::compare") => (PathKind::Function, None),
		UniCase::ascii("crypto::argon2::generate") => (PathKind::Function, None),
		UniCase::ascii("crypto::bcrypt::compare") => (PathKind::Function, None),
		UniCase::ascii("crypto::bcrypt::generate") => (PathKind::Function, None),
		UniCase::ascii("crypto::pbkdf2::compare") => (PathKind::Function, None),
		UniCase::ascii("crypto::pbkdf2::generate") => (PathKind::Function, None),
		UniCase::ascii("crypto::scrypt::compare") => (PathKind::Function, None),
		UniCase::ascii("crypto::scrypt::generate") => (PathKind::Function, None),
		//
		UniCase::ascii("duration::days") => (PathKind::Function, None),
		UniCase::ascii("duration::hours") => (PathKind::Function, None),
		UniCase::ascii("duration::micros") => (PathKind::Function, None),
		UniCase::ascii("duration::millis") => (PathKind::Function, None),
		UniCase::ascii("duration::mins") => (PathKind::Function, None),
		UniCase::ascii("duration::nanos") => (PathKind::Function, None),
		UniCase::ascii("duration::secs") => (PathKind::Function, None),
		UniCase::ascii("duration::weeks") => (PathKind::Function, None),
		UniCase::ascii("duration::years") => (PathKind::Function, None),
		UniCase::ascii("duration::from_days") => (PathKind::Function, Some(UniCase::ascii("duration::from::days"))),
		UniCase::ascii("duration::from_hours") => (PathKind::Function, Some(UniCase::ascii("duration::from::hours"))),
		UniCase::ascii("duration::from_micros") => (PathKind::Function, Some(UniCase::ascii("duration::from::micros"))),
		UniCase::ascii("duration::from_millis") => (PathKind::Function, Some(UniCase::ascii("duration::from::millis"))),
		UniCase::ascii("duration::from_mins") => (PathKind::Function, Some(UniCase::ascii("duration::from::mins"))),
		UniCase::ascii("duration::from_nanos") => (PathKind::Function, Some(UniCase::ascii("duration::from::nanos"))),
		UniCase::ascii("duration::from_secs") => (PathKind::Function, Some(UniCase::ascii("duration::from::secs"))),
		UniCase::ascii("duration::from_weeks") => (PathKind::Function, Some(UniCase::ascii("duration::from::weeks"))),
		//
		UniCase::ascii("encoding::base64::decode") => (PathKind::Function, None),
		UniCase::ascii("encoding::base64::encode") => (PathKind::Function, None),
		UniCase::ascii("encoding::cbor::decode") => (PathKind::Function, None),
		UniCase::ascii("encoding::cbor::encode") => (PathKind::Function, None),
		//
		UniCase::ascii("file::bucket") => (PathKind::Function, None),
		UniCase::ascii("file::key") => (PathKind::Function, None),
		UniCase::ascii("file::put") => (PathKind::Function, None),
		UniCase::ascii("file::put_if_not_exists") => (PathKind::Function, None),
		UniCase::ascii("file::get") => (PathKind::Function, None),
		UniCase::ascii("file::head") => (PathKind::Function, None),
		UniCase::ascii("file::delete") => (PathKind::Function, None),
		UniCase::ascii("file::copy") => (PathKind::Function, None),
		UniCase::ascii("file::copy_if_not_exists") => (PathKind::Function, None),
		UniCase::ascii("file::rename") => (PathKind::Function, None),
		UniCase::ascii("file::rename_if_not_exists") => (PathKind::Function, None),
		UniCase::ascii("file::exists") => (PathKind::Function, None),
		UniCase::ascii("file::list") => (PathKind::Function, None),
		//
		UniCase::ascii("geo::area") => (PathKind::Function, None),
		UniCase::ascii("geo::bearing") => (PathKind::Function, None),
		UniCase::ascii("geo::centroid") => (PathKind::Function, None),
		UniCase::ascii("geo::distance") => (PathKind::Function, None),
		UniCase::ascii("geo::hash::decode") => (PathKind::Function, None),
		UniCase::ascii("geo::hash::encode") => (PathKind::Function, None),
		UniCase::ascii("geo::is_valid") => (PathKind::Function, None),
		//
		UniCase::ascii("http::head") => (PathKind::Function, None),
		UniCase::ascii("http::get") => (PathKind::Function, None),
		UniCase::ascii("http::put") => (PathKind::Function, None),
		UniCase::ascii("http::post") => (PathKind::Function, None),
		UniCase::ascii("http::patch") => (PathKind::Function, None),
		UniCase::ascii("http::delete") => (PathKind::Function, None),
		//
		UniCase::ascii("math::abs") => (PathKind::Function, None),
		UniCase::ascii("math::acos") => (PathKind::Function, None),
		UniCase::ascii("math::acot") => (PathKind::Function, None),
		UniCase::ascii("math::asin") => (PathKind::Function, None),
		UniCase::ascii("math::atan") => (PathKind::Function, None),
		UniCase::ascii("math::bottom") => (PathKind::Function, None),
		UniCase::ascii("math::ceil") => (PathKind::Function, None),
		UniCase::ascii("math::clamp") => (PathKind::Function, None),
		UniCase::ascii("math::cos") => (PathKind::Function, None),
		UniCase::ascii("math::cot") => (PathKind::Function, None),
		UniCase::ascii("math::deg2rad") => (PathKind::Function, None),
		UniCase::ascii("math::fixed") => (PathKind::Function, None),
		UniCase::ascii("math::floor") => (PathKind::Function, None),
		UniCase::ascii("math::interquartile") => (PathKind::Function, None),
		UniCase::ascii("math::lerp") => (PathKind::Function, None),
		UniCase::ascii("math::lerpangle") => (PathKind::Function, None),
		UniCase::ascii("math::ln") => (PathKind::Function, None),
		UniCase::ascii("math::log") => (PathKind::Function, None),
		UniCase::ascii("math::log10") => (PathKind::Function, None),
		UniCase::ascii("math::log2") => (PathKind::Function, None),
		UniCase::ascii("math::max") => (PathKind::Function, None),
		UniCase::ascii("math::mean") => (PathKind::Function, None),
		UniCase::ascii("math::median") => (PathKind::Function, None),
		UniCase::ascii("math::midhinge") => (PathKind::Function, None),
		UniCase::ascii("math::min") => (PathKind::Function, None),
		UniCase::ascii("math::mode") => (PathKind::Function, None),
		UniCase::ascii("math::nearestrank") => (PathKind::Function, None),
		UniCase::ascii("math::percentile") => (PathKind::Function, None),
		UniCase::ascii("math::pow") => (PathKind::Function, None),
		UniCase::ascii("math::product") => (PathKind::Function, None),
		UniCase::ascii("math::rad2deg") => (PathKind::Function, None),
		UniCase::ascii("math::round") => (PathKind::Function, None),
		UniCase::ascii("math::sign") => (PathKind::Function, None),
		UniCase::ascii("math::sin") => (PathKind::Function, None),
		UniCase::ascii("math::spread") => (PathKind::Function, None),
		UniCase::ascii("math::sqrt") => (PathKind::Function, None),
		UniCase::ascii("math::stddev") => (PathKind::Function, None),
		UniCase::ascii("math::sum") => (PathKind::Function, None),
		UniCase::ascii("math::tan") => (PathKind::Function, None),
		UniCase::ascii("math::top") => (PathKind::Function, None),
		UniCase::ascii("math::trimean") => (PathKind::Function, None),
		UniCase::ascii("math::variance") => (PathKind::Function, None),
		//
		UniCase::ascii("meta::id") => (PathKind::Function, None),
		UniCase::ascii("meta::tb") => (PathKind::Function, None),
		//
		UniCase::ascii("not") => (PathKind::Function, None),
		//
		UniCase::ascii("object::entries") => (PathKind::Function, None),
		UniCase::ascii("object::extend") => (PathKind::Function, None),
		UniCase::ascii("object::from_entries") => (PathKind::Function, None),
		UniCase::ascii("object::is_empty") => (PathKind::Function, None),
		UniCase::ascii("object::keys") => (PathKind::Function, None),
		UniCase::ascii("object::len") => (PathKind::Function, None),
		UniCase::ascii("object::matches") => (PathKind::Function, None),
		UniCase::ascii("object::remove") => (PathKind::Function, None),
		UniCase::ascii("object::values") => (PathKind::Function, None),
		//
		UniCase::ascii("parse::email::host") => (PathKind::Function, None),
		UniCase::ascii("parse::email::user") => (PathKind::Function, None),
		UniCase::ascii("parse::url::domain") => (PathKind::Function, None),
		UniCase::ascii("parse::url::fragment") => (PathKind::Function, None),
		UniCase::ascii("parse::url::host") => (PathKind::Function, None),
		UniCase::ascii("parse::url::path") => (PathKind::Function, None),
		UniCase::ascii("parse::url::port") => (PathKind::Function, None),
		UniCase::ascii("parse::url::query") => (PathKind::Function, None),
		UniCase::ascii("parse::url::scheme") => (PathKind::Function, None),
		//
		UniCase::ascii("rand") => (PathKind::Function, None),
		UniCase::ascii("rand::bool") => (PathKind::Function, None),
		UniCase::ascii("rand::duration") => (PathKind::Function, None),
		UniCase::ascii("rand::enum") => (PathKind::Function, None),
		UniCase::ascii("rand::float") => (PathKind::Function, None),
		UniCase::ascii("rand::id") => (PathKind::Function, Some(UniCase::ascii("rand::guid"))),
		UniCase::ascii("rand::int") => (PathKind::Function, None),
		UniCase::ascii("rand::string") => (PathKind::Function, None),
		UniCase::ascii("rand::time") => (PathKind::Function, None),
		UniCase::ascii("rand::ulid") => (PathKind::Function, None),
		UniCase::ascii("rand::uuid::v4") => (PathKind::Function, None),
		UniCase::ascii("rand::uuid::v7") => (PathKind::Function, None),
		UniCase::ascii("rand::uuid") => (PathKind::Function, None),
		//
		UniCase::ascii("record::exists") => (PathKind::Function, None),
		UniCase::ascii("record::id") => (PathKind::Function, None),
		UniCase::ascii("record::is_edge") => (PathKind::Function, None),
		UniCase::ascii("record::table") => (PathKind::Function, None),
		UniCase::ascii("record::tb") => (PathKind::Function, None),
		//
		UniCase::ascii("search::analyze") => (PathKind::Function, None),
		UniCase::ascii("search::linear") => (PathKind::Function, None),
		UniCase::ascii("search::rrf") => (PathKind::Function, None),
		UniCase::ascii("search::score") => (PathKind::Function, None),
		UniCase::ascii("search::highlight") => (PathKind::Function, None),
		UniCase::ascii("search::offsets") => (PathKind::Function, None),
		//
		UniCase::ascii("set::add") => (PathKind::Function, None),
		UniCase::ascii("set::all") => (PathKind::Function, None),
		UniCase::ascii("set::any") => (PathKind::Function, None),
		UniCase::ascii("set::at") => (PathKind::Function, None),
		UniCase::ascii("set::complement") => (PathKind::Function, None),
		UniCase::ascii("set::contains") => (PathKind::Function, None),
		UniCase::ascii("set::difference") => (PathKind::Function, None),
		UniCase::ascii("set::filter") => (PathKind::Function, None),
		UniCase::ascii("set::find") => (PathKind::Function, None),
		UniCase::ascii("set::first") => (PathKind::Function, None),
		UniCase::ascii("set::flatten") => (PathKind::Function, None),
		UniCase::ascii("set::fold") => (PathKind::Function, None),
		UniCase::ascii("set::intersect") => (PathKind::Function, None),
		UniCase::ascii("set::is_empty") => (PathKind::Function, None),
		UniCase::ascii("set::join") => (PathKind::Function, None),
		UniCase::ascii("set::last") => (PathKind::Function, None),
		UniCase::ascii("set::len") => (PathKind::Function, None),
		UniCase::ascii("set::map") => (PathKind::Function, None),
		UniCase::ascii("set::max") => (PathKind::Function, None),
		UniCase::ascii("set::min") => (PathKind::Function, None),
		UniCase::ascii("set::reduce") => (PathKind::Function, None),
		UniCase::ascii("set::remove") => (PathKind::Function, None),
		UniCase::ascii("set::slice") => (PathKind::Function, None),
		UniCase::ascii("set::union") => (PathKind::Function, None),
		//
		UniCase::ascii("sequence::nextval") => (PathKind::Function, None),
		//
		UniCase::ascii("session::ac") => (PathKind::Function, None),
		UniCase::ascii("session::db") => (PathKind::Function, None),
		UniCase::ascii("session::id") => (PathKind::Function, None),
		UniCase::ascii("session::ip") => (PathKind::Function, None),
		UniCase::ascii("session::ns") => (PathKind::Function, None),
		UniCase::ascii("session::origin") => (PathKind::Function, None),
		UniCase::ascii("session::rd") => (PathKind::Function, None),
		UniCase::ascii("session::token") => (PathKind::Function, None),
		//
		UniCase::ascii("sleep") => (PathKind::Function, None),
		//
		UniCase::ascii("string::capitalize") => (PathKind::Function, None),
		UniCase::ascii("string::concat") => (PathKind::Function, None),
		UniCase::ascii("string::contains") => (PathKind::Function, None),
		UniCase::ascii("string::ends_with") => (PathKind::Function, None),
		UniCase::ascii("string::join") => (PathKind::Function, None),
		UniCase::ascii("string::len") => (PathKind::Function, None),
		UniCase::ascii("string::lowercase") => (PathKind::Function, None),
		UniCase::ascii("string::repeat") => (PathKind::Function, None),
		UniCase::ascii("string::replace") => (PathKind::Function, None),
		UniCase::ascii("string::reverse") => (PathKind::Function, None),
		UniCase::ascii("string::slice") => (PathKind::Function, None),
		UniCase::ascii("string::slug") => (PathKind::Function, None),
		UniCase::ascii("string::split") => (PathKind::Function, None),
		UniCase::ascii("string::starts_with") => (PathKind::Function, Some(UniCase::ascii("string::startsWith"))),
		UniCase::ascii("string::trim") => (PathKind::Function, None),
		UniCase::ascii("string::uppercase") => (PathKind::Function, None),
		UniCase::ascii("string::words") => (PathKind::Function, None),
		//
		UniCase::ascii("string::distance::damerau_levenshtein") => (PathKind::Function, None),
		UniCase::ascii("string::distance::hamming") => (PathKind::Function, None),
		UniCase::ascii("string::distance::levenshtein") => (PathKind::Function, None),
		UniCase::ascii("string::distance::normalized_damerau_levenshtein") => (PathKind::Function, None),
		UniCase::ascii("string::distance::normalized_levenshtein") => (PathKind::Function, None),
		UniCase::ascii("string::distance::osa") => (PathKind::Function, Some(UniCase::ascii("string::distance::osa_distance"))),
		//
		UniCase::ascii("string::html::encode") => (PathKind::Function, None),
		UniCase::ascii("string::html::sanitize") => (PathKind::Function, None),
		UniCase::ascii("string::is_alphanum") => (PathKind::Function, Some(UniCase::ascii("string::is::alphanum"))),
		UniCase::ascii("string::is_alpha") => (PathKind::Function, Some(UniCase::ascii("string::is::alpha"))),
		UniCase::ascii("string::is_ascii") => (PathKind::Function, Some(UniCase::ascii("string::is::ascii"))),
		UniCase::ascii("string::is_datetime") => (PathKind::Function, Some(UniCase::ascii("string::is::datetime"))),
		UniCase::ascii("string::is_domain") => (PathKind::Function, Some(UniCase::ascii("string::is::domain"))),
		UniCase::ascii("string::is_email") => (PathKind::Function, Some(UniCase::ascii("string::is::email"))),
		UniCase::ascii("string::is_hexadecimal") => (PathKind::Function, Some(UniCase::ascii("string::is::hexadecimal"))),
		UniCase::ascii("string::is_ip") => (PathKind::Function, Some(UniCase::ascii("string::is::ip"))),
		UniCase::ascii("string::is_ipv4") => (PathKind::Function, Some(UniCase::ascii("string::is::ipv4"))),
		UniCase::ascii("string::is_ipv6") => (PathKind::Function, Some(UniCase::ascii("string::is::ipv6"))),
		UniCase::ascii("string::is_latitude") => (PathKind::Function, Some(UniCase::ascii("string::is::latitude"))),
		UniCase::ascii("string::is_longitude") => (PathKind::Function, Some(UniCase::ascii("string::is::longitude"))),
		UniCase::ascii("string::is_numeric") => (PathKind::Function, Some(UniCase::ascii("string::is::numeric"))),
		UniCase::ascii("string::is_semver") => (PathKind::Function, Some(UniCase::ascii("string::is::semver"))),
		UniCase::ascii("string::is_url") => (PathKind::Function, Some(UniCase::ascii("string::is::url"))),
		UniCase::ascii("string::is_ulid") => (PathKind::Function, Some(UniCase::ascii("string::is::ulid"))),
		UniCase::ascii("string::is_uuid") => (PathKind::Function, Some(UniCase::ascii("string::is::uuid"))),
		UniCase::ascii("string::is_record") => (PathKind::Function, Some(UniCase::ascii("string::is::record"))),
		UniCase::ascii("string::semver::compare") => (PathKind::Function, None),
		UniCase::ascii("string::semver::major") => (PathKind::Function, None),
		UniCase::ascii("string::semver::minor") => (PathKind::Function, None),
		UniCase::ascii("string::semver::patch") => (PathKind::Function, None),
		UniCase::ascii("string::semver::inc::major") => (PathKind::Function, None),
		UniCase::ascii("string::semver::inc::minor") => (PathKind::Function, None),
		UniCase::ascii("string::semver::inc::patch") => (PathKind::Function, None),
		UniCase::ascii("string::semver::set::major") => (PathKind::Function, None),
		UniCase::ascii("string::semver::set::minor") => (PathKind::Function, None),
		UniCase::ascii("string::semver::set::patch") => (PathKind::Function, None),
		//
		UniCase::ascii("string::similarity::fuzzy") => (PathKind::Function, None),
		UniCase::ascii("string::similarity::jaro") => (PathKind::Function, None),
		UniCase::ascii("string::similarity::jaro_winkler") => (PathKind::Function, None),
		UniCase::ascii("string::similarity::smithwaterman") => (PathKind::Function, None),
		UniCase::ascii("string::similarity::sorensen_dice") => (PathKind::Function, None),
		UniCase::ascii("string::matches") => (PathKind::Function, None),
		//
		UniCase::ascii("time::ceil") => (PathKind::Function, None),
		UniCase::ascii("time::day") => (PathKind::Function, None),
		UniCase::ascii("time::floor") => (PathKind::Function, None),
		UniCase::ascii("time::format") => (PathKind::Function, None),
		UniCase::ascii("time::group") => (PathKind::Function, None),
		UniCase::ascii("time::hour") => (PathKind::Function, None),
		UniCase::ascii("time::max") => (PathKind::Function, None),
		UniCase::ascii("time::micros") => (PathKind::Function, None),
		UniCase::ascii("time::millis") => (PathKind::Function, None),
		UniCase::ascii("time::min") => (PathKind::Function, None),
		UniCase::ascii("time::minute") => (PathKind::Function, None),
		UniCase::ascii("time::month") => (PathKind::Function, None),
		UniCase::ascii("time::nano") => (PathKind::Function, None),
		UniCase::ascii("time::now") => (PathKind::Function, None),
		UniCase::ascii("time::round") => (PathKind::Function, None),
		UniCase::ascii("time::second") => (PathKind::Function, None),
		UniCase::ascii("time::timezone") => (PathKind::Function, None),
		UniCase::ascii("time::unix") => (PathKind::Function, None),
		UniCase::ascii("time::wday") => (PathKind::Function, None),
		UniCase::ascii("time::week") => (PathKind::Function, None),
		UniCase::ascii("time::yday") => (PathKind::Function, None),
		UniCase::ascii("time::year") => (PathKind::Function, None),
		UniCase::ascii("time::from_micros") => (PathKind::Function, Some(UniCase::ascii("time::from::micros"))),
		UniCase::ascii("time::from_millis") => (PathKind::Function, Some(UniCase::ascii("time::from::millis"))),
		UniCase::ascii("time::from_nanos") => (PathKind::Function, Some(UniCase::ascii("time::from::nanos"))),
		UniCase::ascii("time::from_secs") => (PathKind::Function, Some(UniCase::ascii("time::from::secs"))),
		UniCase::ascii("time::from_ulid") => (PathKind::Function, Some(UniCase::ascii("time::from::ulid"))),
		UniCase::ascii("time::from_unix") => (PathKind::Function, Some(UniCase::ascii("time::from::unix"))),
		UniCase::ascii("time::from_uuid") => (PathKind::Function, Some(UniCase::ascii("time::from::uuid"))),
		UniCase::ascii("time::is_leap_year") => (PathKind::Function, Some(UniCase::ascii("time::is::leap_year"))),
		//
		UniCase::ascii("type::array") => (PathKind::Function, None),
		UniCase::ascii("type::bool") => (PathKind::Function, None),
		UniCase::ascii("type::bytes") => (PathKind::Function, None),
		UniCase::ascii("type::datetime") => (PathKind::Function, None),
		UniCase::ascii("type::decimal") => (PathKind::Function, None),
		UniCase::ascii("type::duration") => (PathKind::Function, None),
		UniCase::ascii("type::field") => (PathKind::Function, None),
		UniCase::ascii("type::fields") => (PathKind::Function, None),
		UniCase::ascii("type::file") => (PathKind::Function, None),
		UniCase::ascii("type::float") => (PathKind::Function, None),
		UniCase::ascii("type::geometry") => (PathKind::Function, None),
		UniCase::ascii("type::int") => (PathKind::Function, None),
		UniCase::ascii("type::number") => (PathKind::Function, None),
		UniCase::ascii("type::of") => (PathKind::Function, None),
		UniCase::ascii("type::point") => (PathKind::Function, None),
		UniCase::ascii("type::range") => (PathKind::Function, None),
		UniCase::ascii("type::record") => (PathKind::Function, Some(UniCase::ascii("type::thing"))),
		UniCase::ascii("type::set") => (PathKind::Function, None),
		UniCase::ascii("type::string") => (PathKind::Function, None),
		UniCase::ascii("type::string_lossy") => (PathKind::Function, None),
		UniCase::ascii("type::table") => (PathKind::Function, None),
		UniCase::ascii("type::uuid") => (PathKind::Function, None),
		UniCase::ascii("type::is_array") => (PathKind::Function, Some(UniCase::ascii("type::is::array"))),
		UniCase::ascii("type::is_bool") => (PathKind::Function, Some(UniCase::ascii("type::is::bool"))),
		UniCase::ascii("type::is_bytes") => (PathKind::Function, Some(UniCase::ascii("type::is::bytes"))),
		UniCase::ascii("type::is_collection") => (PathKind::Function, Some(UniCase::ascii("type::is::collection"))),
		UniCase::ascii("type::is_datetime") => (PathKind::Function, Some(UniCase::ascii("type::is::datetime"))),
		UniCase::ascii("type::is_decimal") => (PathKind::Function, Some(UniCase::ascii("type::is::decimal"))),
		UniCase::ascii("type::is_duration") => (PathKind::Function, Some(UniCase::ascii("type::is::duration"))),
		UniCase::ascii("type::is_float") => (PathKind::Function, Some(UniCase::ascii("type::is::float"))),
		UniCase::ascii("type::is_geometry") => (PathKind::Function, Some(UniCase::ascii("type::is::geometry"))),
		UniCase::ascii("type::is_int") => (PathKind::Function, Some(UniCase::ascii("type::is::int"))),
		UniCase::ascii("type::is_line") => (PathKind::Function, Some(UniCase::ascii("type::is::line"))),
		UniCase::ascii("type::is_multiline") => (PathKind::Function, Some(UniCase::ascii("type::is::multiline"))),
		UniCase::ascii("type::is_multipoint") => (PathKind::Function, Some(UniCase::ascii("type::is::multipoint"))),
		UniCase::ascii("type::is_multipolygon") => (PathKind::Function, Some(UniCase::ascii("type::is::multipolygon"))),
		UniCase::ascii("type::is_none") => (PathKind::Function, Some(UniCase::ascii("type::is::none"))),
		UniCase::ascii("type::is_null") => (PathKind::Function, Some(UniCase::ascii("type::is::null"))),
		UniCase::ascii("type::is_number") => (PathKind::Function, Some(UniCase::ascii("type::is::number"))),
		UniCase::ascii("type::is_object") => (PathKind::Function, Some(UniCase::ascii("type::is::object"))),
		UniCase::ascii("type::is_point") => (PathKind::Function, Some(UniCase::ascii("type::is::point"))),
		UniCase::ascii("type::is_polygon") => (PathKind::Function, Some(UniCase::ascii("type::is::polygon"))),
		UniCase::ascii("type::is_range") => (PathKind::Function, Some(UniCase::ascii("type::is::range"))),
		UniCase::ascii("type::is_record") => (PathKind::Function, Some(UniCase::ascii("type::is::record"))),
		UniCase::ascii("type::is_set") => (PathKind::Function, None),
		UniCase::ascii("type::is_string") => (PathKind::Function, Some(UniCase::ascii("type::is::string"))),
		UniCase::ascii("type::is_uuid") => (PathKind::Function, Some(UniCase::ascii("type::is::uuid"))),
		//
		UniCase::ascii("value::diff") => (PathKind::Function, None),
		UniCase::ascii("value::patch") => (PathKind::Function, None),
		//
		UniCase::ascii("vector::add") => (PathKind::Function, None),
		UniCase::ascii("vector::angle") => (PathKind::Function, None),
		UniCase::ascii("vector::cross") => (PathKind::Function, None),
		UniCase::ascii("vector::divide") => (PathKind::Function, None),
		UniCase::ascii("vector::dot") => (PathKind::Function, None),
		UniCase::ascii("vector::magnitude") => (PathKind::Function, None),
		UniCase::ascii("vector::multiply") => (PathKind::Function, None),
		UniCase::ascii("vector::normalize") => (PathKind::Function, None),
		UniCase::ascii("vector::project") => (PathKind::Function, None),
		UniCase::ascii("vector::scale") => (PathKind::Function, None),
		UniCase::ascii("vector::subtract") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::chebyshev") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::euclidean") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::hamming") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::knn") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::mahalanobis") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::manhattan") => (PathKind::Function, None),
		UniCase::ascii("vector::distance::minkowski") => (PathKind::Function, None),
		UniCase::ascii("vector::similarity::cosine") => (PathKind::Function, None),
		UniCase::ascii("vector::similarity::jaccard") => (PathKind::Function, None),
		UniCase::ascii("vector::similarity::pearson") => (PathKind::Function, None),
		UniCase::ascii("vector::similarity::spearman") => (PathKind::Function, None),
		// constants
		UniCase::ascii("math::E") => (PathKind::Constant(Constant::MathE), None),
		UniCase::ascii("math::FRAC_1_PI") => (PathKind::Constant(Constant::MathFrac1Pi), None),
		UniCase::ascii("math::FRAC_1_SQRT_2") => (PathKind::Constant(Constant::MathFrac1Sqrt2), None),
		UniCase::ascii("math::FRAC_2_PI") => (PathKind::Constant(Constant::MathFrac2Pi), None),
		UniCase::ascii("math::FRAC_2_SQRT_PI") => (PathKind::Constant(Constant::MathFrac2SqrtPi), None),
		UniCase::ascii("math::FRAC_PI_2") => (PathKind::Constant(Constant::MathFracPi2), None),
		UniCase::ascii("math::FRAC_PI_3") => (PathKind::Constant(Constant::MathFracPi3), None),
		UniCase::ascii("math::FRAC_PI_4") => (PathKind::Constant(Constant::MathFracPi4), None),
		UniCase::ascii("math::FRAC_PI_6") => (PathKind::Constant(Constant::MathFracPi6), None),
		UniCase::ascii("math::FRAC_PI_8") => (PathKind::Constant(Constant::MathFracPi8), None),
		UniCase::ascii("math::INF") => (PathKind::Constant(Constant::MathInf), None),
		UniCase::ascii("math::LN_10") => (PathKind::Constant(Constant::MathLn10), None),
		UniCase::ascii("math::LN_2") => (PathKind::Constant(Constant::MathLn2), None),
		UniCase::ascii("math::LOG10_2") => (PathKind::Constant(Constant::MathLog102), None),
		UniCase::ascii("math::LOG10_E") => (PathKind::Constant(Constant::MathLog10E), None),
		UniCase::ascii("math::LOG2_10") => (PathKind::Constant(Constant::MathLog210), None),
		UniCase::ascii("math::LOG2_E") => (PathKind::Constant(Constant::MathLog2E), None),
		UniCase::ascii("math::NEG_INF") => (PathKind::Constant(Constant::MathNegInf), None),
		UniCase::ascii("math::PI") => (PathKind::Constant(Constant::MathPi), None),
		UniCase::ascii("math::SQRT_2") => (PathKind::Constant(Constant::MathSqrt2), None),
		UniCase::ascii("math::TAU") => (PathKind::Constant(Constant::MathTau), None),
		UniCase::ascii("time::EPOCH") => (PathKind::Constant(Constant::TimeEpoch), None),
		UniCase::ascii("time::MINIMUM") => (PathKind::Constant(Constant::TimeMin), None),
		UniCase::ascii("time::MAXIMUM") => (PathKind::Constant(Constant::TimeMax), None),
		UniCase::ascii("duration::MAX") => (PathKind::Constant(Constant::DurationMax), None),
		//
		UniCase::ascii("schema::table::exists") => (PathKind::Function, None),
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

	for (i, item) in distance_array[0].iter_mut().enumerate().take(a.len() + 1).skip(1) {
		*item = i as u8;
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
	// Generate a suggestion. First look for deprecated paths.
	if let Some(surely) = PATHS.into_iter().find_map(|(path, (_, old_path))| match old_path {
		Some(s) if s.into_inner() == got => Some(path),
		_ => None,
	}) {
		return Some(surely.into_inner());
	}

	// No deprecated paths found, now use string similarity.
	// Don't search further if the levenshtein distance is greater than 4.
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
			Some((_, (PathKind::Constant(x), _))) => Ok(Expr::Constant(x.clone())),
			Some((k, (PathKind::Function, _))) => {
				// TODO: Move this out of the parser.
				if k.to_lowercase().starts_with("api::") && !self.settings.define_api_enabled {
					bail!("Cannot use the `{k}` method, as the experimental define api capability is not enabled", @span);
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
