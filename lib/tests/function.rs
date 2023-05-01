mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

macro_rules! check {
	($function: expr, $argument: expr, $result: literal, $actual: expr) => {
		let msg = concat!(" test case ", $function, "(", $argument, ") => ", $result);
		match ($actual) {
			Ok(actual) => assert_eq!(actual, Value::parse($result), "{}", msg),
			Err(err) => assert_eq!(err.to_string(), $result.to_owned(), "{}", msg),
		}
	};
	($function: expr, $argument: expr, $result: expr, $actual: expr) => {
		let msg = concat!(" test case ", $function, "(", $argument, ") => ", stringify!($result));
		match ($actual) {
			Ok(actual) => assert!($result(actual), "{}", msg),
			Err(err) => panic!("{err} : {msg}"),
		}
	};
}

macro_rules! function {
	($name: ident, $function: expr, $($argument: expr => $result: expr),*) => {
		#[tokio::test]
		async fn $name() -> Result<(), Error> {
			let sql = concat!($("RETURN ", $function, "(", $argument, ");\n"),*);
			let dbs = Datastore::new("memory").await?;
			let ses = Session::for_kv().with_ns("test").with_db("test");
			let res = &mut dbs.execute(&sql, &ses, None, false).await?;
			assert_eq!(res.len(), 0usize $(.saturating_add({
				let _ = $result;
				1
			}))*);

			$(
				let actual = res.remove(0).result;
				check!($function, $argument, $result, actual);
			)*
			Ok(())
		}
	}
}

// --------------------------------------------------
// array
// --------------------------------------------------

function!(
	function_array_add,
	"array::add",
	"[], 3" => "[3]",
	"3, true" => "Incorrect arguments for function array::add(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2], 2" => "[1,2]",
	"[1,2], 3" => "[1,2,3]",
	"[1,2], [2,3]" => "[1,2,3]"
);
function!(
	function_array_all,
	"array::all",
	"[]" => "true",
	"'some text'" => "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,'text',3,NONE,3,4]" => "false"
);
function!(
	function_array_any,
	"array::any",
	"[]" => "false",
	"'some text'" => "Incorrect arguments for function array::any(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,'text',3,NONE,3,4]" => "true"
);
function!(
	function_array_append,
	"array::append",
	"[], 3" => "[3]",
	"3, true" => "Incorrect arguments for function array::append(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2], [2,3]" => "[1,2,[2,3]]"
);
function!(
	function_array_combine,
	"array::combine",
	"[], []" => "[]",
	"3, true" => "Incorrect arguments for function array::combine(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2], [2,3]" => "[ [1,2], [1,3], [2,2], [2,3] ]"
);
function!(
	function_array_complement,
	"array::complement",
	"[], []" => "[]",
	"3, true" => "Incorrect arguments for function array::complement(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2,3,4], [3,4,5,6]" => "[1,2]"
);
function!(
	function_array_concat,
	"array::concat",
	"[], []" => "[]",
	"3, true" => "Incorrect arguments for function array::concat(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2,3,4], [3,4,5,6]" => "[1,2,3,4,3,4,5,6]"
);
function!(
	function_array_difference,
	"array::difference",
	"[], []" => "[]",
	"3, true" => "Incorrect arguments for function array::difference(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
);
function!(
	function_array_distinct,
	"array::distinct",
	"[]" => "[]",
	"'some text'" => "Incorrect arguments for function array::distinct(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,1,3,3,4]" => "[1,2,3,4]"
);
function!(
	function_array_flatten,
	"array::flatten",
	"[]" => "[]",
	"'some text'" => "Incorrect arguments for function array::flatten(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[[1,2], [3,4]]" => "[1,2,3,4]",
	"[[1,2], [3, 4], 'SurrealDB', [5, 6, [7, 8]]]" => "[1, 2, 3, 4, 'SurrealDB', 5, 6, [7, 8]]"
);
function!(
	function_array_group,
	"array::group",
	"[]" => "[]",
	"3" => "Incorrect arguments for function array::group(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[ [1,2,3,4], [3,4,5,6] ]" => "[1,2,3,4,5,6]"
);
function!(
	function_array_insert,
	"array::insert",
	"[], 1" => "[1]",
	"[3], 1, 5" => "[3]",
	"[3], 1, 1" => "[3,1]",
	"[1,2,3,4], 5, -1" => "[1,2,3,5,4]"
);
function!(
	function_array_intersect,
	"array::intersect",
	"[], []" => "[]",
	"3, true" => "Incorrect arguments for function array::intersect(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2,3,4], [3,4,5,6]" => "[3,4]"
);
function!(
	function_array_len,
	"array::len",
	"[]" => "0",
	"'some text'" => "Incorrect arguments for function array::len(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,'text',3,3,4]" => "6"
);
function!(
	function_array_max,
	"array::max",
	"[]" => "NONE",
	"'some text'" => "Incorrect arguments for function array::max(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,'text',3,3,4]" => "'text'"
);
function!(
	function_array_min,
	"array::min",
	"[]" => "NONE",
	"'some text'" => "Incorrect arguments for function array::min(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,'text',3,3,4]" => "1"
);
function!(
	function_array_pop,
	"array::pop",
	"[]" => "NONE",
	"'some text'" => "Incorrect arguments for function array::pop(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array",
	"[1,2,'text',3,3,4]" => "4"
);
function!(
	function_array_prepend,
	"array::prepend",
	"[], 3" => "[3]",
	"3, true" => "Incorrect arguments for function array::prepend(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2], [2,3]" => "[[2,3],1,2]"
);
function!(
	function_array_push,
	"array::push",
	"[], 3" => "[3]",
	"3, true" => "Incorrect arguments for function array::push(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2], [2,3]" => "[1,2,[2,3]]"
);
function!(
	function_array_remove,
	"array::remove",
	"[3], 0" => "[]",
	"[3], 2" => "[3]",
	"[3,4,5], 1" => "[3,5]",
	"[1,2,3,4], -1" => "[1,2,3]"
);
function!(
	function_array_reverse,
	"array::reverse",
	"[]" => "[]",
	"3" => "Incorrect arguments for function array::reverse(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2,'text',3,3,4]" => "[4,3,3,'text',2,1]"
);
function!(
	function_array_slice,
	"array::slice",
	"[]" => "[]",
	"3" => "Incorrect arguments for function array::slice(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2,'text',3,3,4]" => "[1,2,'text',3,3,4]",
	"[1,2,'text',3,3,4], 1" => "[2,'text',3,3,4]",
	"[1,2,'text',3,3,4], 3" => "[3,3,4]",
	"[1,2,'text',3,3,4], 3, -1" => "[3,3]",
	"[1,2,'text',3,3,4], -1" => "[4]"
);
function!(
	function_array_sort,
	"array::sort",
	"[]" => "[]",
	"3, false" => "Incorrect arguments for function array::sort(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[4,2,'text',1,3,4]" => "[1,2,3,4,4,'text']",
	"[4,2,'text',1,3,4], true" => "[1,2,3,4,4,'text']",
	"[4,2,'text',1,3,4], false" => "['text',4,4,3,2,1]",
	"[4,2,'text',1,3,4], 'asc'" => "[1,2,3,4,4,'text']",
	"[4,2,'text',1,3,4], 'desc'" => "['text',4,4,3,2,1]"
);
function!(
	function_array_sort_asc,
	"array::sort::asc",
	"[]" => "[]",
	"3" => "Incorrect arguments for function array::sort::asc(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	r#"[4,2,"text",1,3,4]"# => "[1,2,3,4,4,'text']"
);
function!(
	function_array_sort_desc,
	"array::sort::desc",
	"[]" => "[]",
	"3" => "Incorrect arguments for function array::sort::desc(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	r#"[4,2,"text",1,3,4]"# => "['text',4,4,3,2,1]"
);
function!(
	function_array_union,
	"array::union",
	"[], []" => "[]",
	"3, true" => "Incorrect arguments for function array::union(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array",
	"[1,2,1,6], [1,3,4,5,6]" => "[1,2,6,3,4,5]"
);

// --------------------------------------------------
// count
// --------------------------------------------------

function!(
	function_count,
	"count",
	"" => "1",
	"true" => "1",
	"false" => "0",
	"15 > 10" => "1",
	"15 < 10" => "0"
);

// --------------------------------------------------
// crypto
// --------------------------------------------------

function!(
	function_crypto_md5,
	"crypto::md5",
	"'tobie'" => "'4768b3fc7ac751e03a614e2349abf3bf'"
);
function!(
	function_crypto_sha1,
	"crypto::sha1",
	"'tobie'" => "'c6be709a1b6429472e0c5745b411f1693c4717be'"
);
function!(
	function_crypto_sha256,
	"crypto::sha256",
	"'tobie'" => "'33fe1859daba927ea5674813adc1cf34b9e2795f2b7e91602fae19c0d0c493af'"
);
function!(
	function_crypto_sha512,
	"crypto::sha512",
	"'tobie'" => "'39f0160c946c4c53702112d6ef3eea7957ea8e1c78787a482a89f8b0a8860a20ecd543432e4a187d9fdcd1c415cf61008e51a7e8bf2f22ac77e458789c9cdccc'"
);

// --------------------------------------------------
// duration
// --------------------------------------------------

function!(
	function_duration_days,
	"duration::days",
	"7d" => "7",
	"4w3d" => "31",
	"4h" => "0"
);
function!(
	function_duration_hours,
	"duration::hours",
	"7h" => "7",
	"4d3h" => "99",
	"30m" => "0"
);
function!(
	function_duration_micros,
	"duration::micros",
	"150µs" => "150",
	"1m100µs" => "60000100",
	"100ns" => "0"
);
function!(
	function_duration_millis,
	"duration::millis",
	"150ms" => "150",
	"1m100ms" => "60100",
	"100µs" => "0"
);
function!(
	function_duration_mins,
	"duration::mins",
	"30m" => "30",
	"1h30m" => "90",
	"45s" => "0"
);
function!(
	function_duration_nanos,
	"duration::nanos",
	"200ns" => "200",
	"30ms100ns" => "30000100",
	"0ns" => "0"
);
function!(
	function_duration_secs,
	"duration::secs",
	"25s" => "25",
	"1m25s" => "85",
	"350ms" => "0"
);
function!(
	function_duration_weeks,
	"duration::weeks",
	"7w" => "7",
	"1y3w" => "55",
	"4d" => "0"
);
function!(
	function_duration_years,
	"duration::years",
	"7y" => "7",
	"7y4w30d" => "7",
	"4w" => "0"
);
function!(
	function_duration_from_days,
	"duration::from::days",
	"3" => "3d",
	"50" => "7w1d"
);
function!(
	function_duration_from_hours,
	"duration::from::hours",
	"3" => "3h",
	"30" => "1d6h"
);
function!(
	function_duration_from_micros,
	"duration::from::micros",
	"300" => "300µs",
	"50500" => "50ms500µs"
);
function!(
	function_duration_from_millis,
	"duration::from::millis",
	"30" => "30ms",
	"1500" => "1s500ms"
);
function!(
	function_duration_from_mins,
	"duration::from::mins",
	"3" => "3m",
	"100" => "1h40m"
);
function!(
	function_duration_from_nanos,
	"duration::from::nanos",
	"30" => "30ns",
	"5005000" => "5ms5µs"
);
function!(
	function_duration_from_secs,
	"duration::from::secs",
	"3" => "3s",
	"100" => "1m40s"
);
function!(
	function_duration_from_weeks,
	"duration::from::weeks",
	"3" => "3w",
	"60" => "1y7w6d"
);

// --------------------------------------------------
// geo
// --------------------------------------------------

function!(
	function_parse_geo_area,
	"geo::area",
	r#"{
		type: 'Polygon',
		coordinates: [[
			[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
			[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
			[-0.38314819, 51.37692386]
		]]
	}"# => |tmp: Value| tmp == Value::from(1029944667.4192368)
);
function!(
	function_parse_geo_bearing,
	"geo::bearing",
	r#"
		{
			type: 'Point',
			coordinates: [-0.136439, 51.509865]
		},
		{
			type: 'Point',
			coordinates: [ -73.971321, 40.776676]
		}
	"# => "-71.63409590760736"
);
function!(
	function_parse_geo_centroid,
	"geo::centroid",
	r#"{
		type: 'Polygon',
		coordinates: [[
			[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
			[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
			[-0.38314819, 51.37692386]
		]]
	}"# => r#"{
		type: 'Point',
		coordinates: [
			-0.10231019499999999,
			51.49576478
		]
	}"#
);
function!(
	function_parse_geo_distance,
	"geo::distance",
	r#"
		{
			type: 'Point',
			coordinates: [-0.136439, 51.509865]
		},
		{
			type: 'Point',
			coordinates: [ -73.971321, 40.776676]
		}
	"# => "5562851.11270021"
);
function!(
	function_parse_geo_hash_encode,
	"geo::hash::encode",
	r#"{
		type: 'Point',
		coordinates: [-0.136439, 51.509865]
	}"# => "'gcpvhchdswz9'"
);
function!(
	function_parse_geo_hash_decode,
	"geo::hash::decode",
	"'gcpvhchdswz9'" => r#"{
		type: 'Point',
		coordinates: [
			-0.13643911108374596,
			51.50986502878368
		]
	}"#
);

// --------------------------------------------------
// is
// --------------------------------------------------

function!(
	function_parse_is_alphanum,
	"is::alphanum",
	"'abcdefg123'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_alpha,
	"is::alpha",
	"'abcdefg'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_ascii,
	"is::ascii",
	"'abcdefg123'" => "true",
	"'this is a test 😀'" => "false"
);
function!(
	function_parse_is_datetime,
	"is::datetime",
	"'2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S'" => "true",
	"'2012-06-22 23:56:04', '%T'" => "false"
);
function!(
	function_parse_is_domain,
	"is::domain",
	"'surrealdb.com'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_email,
	"is::email",
	"'info@surrealdb.com'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_hexadecimal,
	"is::hexadecimal",
	"'ff009e'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_latitude,
	"is::latitude",
	"'51.509865'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_longitude,
	"is::longitude",
	"'-0.136439'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_numeric,
	"is::numeric",
	"'13136439'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_semver,
	"is::semver",
	"'1.0.0-rc.1'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_url,
	"is::url",
	"'https://surrealdb.com/docs'" => "true",
	"'this is a test!'" => "false"
);
function!(
	function_parse_is_uuid,
	"is::uuid",
	"'e72bee20-f49b-11ec-b939-0242ac120002'" => "true",
	"'this is a test!'" => "false"
);

// --------------------------------------------------
// math
// --------------------------------------------------

function!(
	function_math_abs,
	"math::abs",
	"0" => "0",
	"100" => "100",
	"-100" => "100"
);
function!(
	function_math_bottom,
	"math::bottom",
	"[1,2,3], 0" => "Incorrect arguments for function math::bottom(). The second argument must be an integer greater than 0.",
	"[1,2,3], 1" => "[1]",
	"[1,2,3], 2" => "[2,1]"
);
function!(
	function_math_ceil,
	"math::ceil",
	"101" => "101",
	"101.5" => "102"
);
function!(
	function_math_fixed,
	"math::fixed",
	"101, 0" => "Incorrect arguments for function math::fixed(). The second argument must be an integer greater than 0.",
	"101, 2" => "101",
	"101.5, 2" => "101.50"
);
function!(
	function_math_floor,
	"math::floor",
	"101" => "101",
	"101.5" => "101"
);
function!(
	function_math_interquartile,
	"math::interquartile",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "207.5",
	"[101.5, 213.5, 202.5]" => "208.0"
);
function!(
	function_math_max,
	"math::max",
	"[]" => "NONE",
	"[101, 213, 202]" => "213",
	"[101.5, 213.5, 202.5]" => "213.5"
);
function!(
	function_math_mean,
	"math::mean",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "172",
	"[101.5, 213.5, 202.5]" => "172.5"
);
function!(
	function_math_median,
	"math::median",
	"[]" => "NONE",
	"[101, 213, 202]" => "202",
	"[101.5, 213.5, 202.5]" => "202.5"
);
function!(
	function_math_midhinge,
	"math::midhinge",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "103.75",
	"[101.5, 213.5, 202.5]" => "104.0"
);
function!(
	function_math_min,
	"math::min",
	"[]" => "NONE",
	"[101, 213, 202]" => "101",
	"[101.5, 213.5, 202.5]" => "101.5"
);
function!(
	function_math_mode,
	"math::mode",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "213",
	"[101.5, 213.5, 202.5]" => "213.5"
);
function!(
	function_math_nearestrank,
	"math::nearestrank",
	"[], 75" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202], 75" => "213",
	"[101.5, 213.5, 202.5], 75" => "213.5"
);
function!(
	function_math_percentile,
	"math::percentile",
	"[], 99" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202], 99" => "207.5",
	"[101.5, 213.5, 202.5], 99" => "208.0"
);
function!(
	function_math_pow,
	"math::pow",
	"101, 3" => "1030301",
	"101.5, 3" => "1045678.375"
);
function!(
	function_math_product,
	"math::product",
	"[]" => "1",
	"[101, 213, 202]" => "4345626",
	"[101.5, 213.5, 202.5]" => "4388225.625"
);
function!(
	function_math_round,
	"math::round",
	"101" => "101",
	"101.5" => "102"
);
function!(
	function_math_spread,
	"math::spread",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "112",
	"[101.5, 213.5, 202.5]" => "112.0"
);
function!(
	function_math_sqrt,
	"math::sqrt",
	"101" => "10.04987562112089",
	"101.5" => "10.07472083980494220820325739456714210123675076934383520155548236146713380225253351613768233376490240"
);
function!(
	function_math_stddev,
	"math::stddev",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "61.73329733620260786466504830446900810163706056134726969779498735043443723773086343343420617365104296",
	"[101.5, 213.5, 202.5]" => "61.73329733620260786466504830446900810163706056134726969779498735043443723773086343343420617365104296"
);
function!(
	function_math_sum,
	"math::sum",
	"[]" => "0",
	"[101, 213, 202]" => "516",
	"[101.5, 213.5, 202.5]" => "517.5"
);
function!(
	function_math_top,
	"math::top",
	"[1,2,3], 0" => "Incorrect arguments for function math::top(). The second argument must be an integer greater than 0.",
	"[1,2,3], 1" => "[3]",
	"[1,2,3], 2" => "[2,3]"
);
function!(
	function_math_trimean,
	"math::trimean",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "152.875",
	"[101.5, 213.5, 202.5]" => "153.25"
);
function!(
	function_math_variance,
	"math::variance",
	"[]" => |tmp: Value| tmp.is_nan(),
	"[101, 213, 202]" => "3811",
	"[101.5, 213.5, 202.5]" => "3811.0"
);

// --------------------------------------------------
// meta
// --------------------------------------------------

function!(
	function_parse_meta_id,
	"meta::id",
	"'person:tobie'" => "'tobie'"
);
function!(
	function_parse_meta_table,
	"meta::table",
	"'person:tobie'" => "'person'"
);
function!(
	function_parse_meta_tb,
	"meta::tb",
	"'person:tobie'" => "'person'"
);

// --------------------------------------------------
// not
// --------------------------------------------------

function!(
	function_not,
	"not",
	"true" => "false",
	"not(true)" => "true",
	"false" => "true",
	"not(false)" => "false",
	"0" => "true",
	"1" => "false",
	"'hello'" => "false"
);

// --------------------------------------------------
// parse
// --------------------------------------------------

function!(
	function_parse_email_host,
	"parse::email::host",
	"'john.doe@example.com'" => "'example.com'"
);
function!(
	function_parse_email_user,
	"parse::email::user",
	"'john.doe@example.com'" => "'john.doe'"
);
function!(
	function_parse_url_domain,
	"parse::url::domain",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "'www.surrealdb.com'"
);
function!(
	function_parse_url_fragment,
	"parse::url::fragment",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "'somefragment'"
);
function!(
	function_parse_url_host,
	"parse::url::host",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "'www.surrealdb.com'"
);
function!(
	function_parse_url_path,
	"parse::url::path",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "'/path/to/page'"
);
function!(
	function_parse_url_port,
	"parse::url::port",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "80"
);
function!(
	function_parse_url_query,
	"parse::url::query",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "'query=param'"
);
function!(
	function_parse_url_scheme,
	"parse::url::scheme",
	"'https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment'" => "'https'"
);

// --------------------------------------------------
// rand
// --------------------------------------------------

function!(
	function_rand,
	"rand",
	"" => |tmp: Value| tmp.is_float()
);
function!(
	function_rand_bool,
	"rand::bool",
	"" => |tmp: Value| tmp.is_bool()
);
function!(
	function_rand_enum,
	"rand::enum",
	r#"["one", "two", "three"]"# => |tmp: Value| tmp.is_strand()
);
function!(
	function_rand_float,
	"rand::float",
	"" => |tmp: Value| tmp.is_float(),
	"5, 10" => |tmp: Value| tmp.is_float()
);
function!(
	function_rand_guid,
	"rand::guid",
	"" => |tmp: Value| tmp.is_strand(),
	"10" => |tmp: Value| tmp.is_strand(),
	"10, 15" => |tmp: Value| tmp.is_strand()
);
function!(
	function_rand_int,
	"rand::int",
	"" => |tmp: Value| tmp.is_int(),
	"5, 10" => |tmp: Value| tmp.is_int()
);
function!(
	function_rand_string,
	"rand::string",
	"" => |tmp: Value| tmp.is_strand(),
	"10" => |tmp: Value| tmp.is_strand(),
	"10, 15" => |tmp: Value| tmp.is_strand()
);
function!(
	function_rand_time,
	"rand::time",
	"" => |tmp: Value| tmp.is_datetime(),
	"1577836800, 1893456000" => |tmp: Value| tmp.is_datetime()
);
function!(
	function_rand_ulid,
	"rand::ulid",
	"" => |tmp: Value| tmp.is_strand()
);
function!(
	function_rand_uuid,
	"rand::uuid",
	"" => |tmp: Value| tmp.is_uuid()
);
function!(
	function_rand_uuid_v4,
	"rand::uuid::v4",
	"" => |tmp: Value| tmp.is_uuid()
);
function!(
	function_rand_uuid_v7,
	"rand::uuid::v7",
	"" => |tmp: Value| tmp.is_uuid()
);

// --------------------------------------------------
// string
// --------------------------------------------------

function!(
	function_string_concat,
	"string::concat",
	"" => "''",
	"'test'" => "'test'",
	"'this', ' ', 'is', ' ', 'a', ' ', 'test'" => "'this is a test'"
);
function!(
	function_string_ends_with,
	"string::endsWith",
	"'', ''" => "true",
	"'', 'test'" => "false",
	"'this is a test', 'test'" => "true"
);
function!(
	function_string_join,
	"string::join",
	"''" => "''",
	"'test'" => "''",
	"' ', 'this', 'is', 'a', 'test'" => "'this is a test'"
);
function!(
	function_string_len,
	"string::len",
	"''" => "0",
	"'test'" => "4",
	"'test this string'" => "16"
);
function!(
	function_string_lowercase,
	"string::lowercase",
	"''" => "''",
	"'TeSt'" => "'test'",
	"'THIS IS A TEST'" => "'this is a test'"
);
function!(
	function_string_repeat,
	"string::repeat",
	"'', 3" => "''",
	"'test', 3" => "'testtesttest'",
	"'test this', 3" => "'test thistest thistest this'"
);
function!(
	function_string_replace,
	"string::replace",
	"'', '', ''" => "''",
	"'this is a test', 'a test', 'awesome'" => "'this is awesome'",
	"'this is an 😀 emoji test', '😀', 'awesome 👍'" => "'this is an awesome 👍 emoji test'"
);
function!(
	function_string_reverse,
	"string::reverse",
	"''" => "''",
	"'test'" => "'tset'",
	"'test this string'" => "'gnirts siht tset'"
);
function!(
	function_string_slice,
	"string::slice",
	"'the quick brown fox jumps over the lazy dog.'" => "'the quick brown fox jumps over the lazy dog.'",
	"'the quick brown fox jumps over the lazy dog.', 16" => "'fox jumps over the lazy dog.'",
	"'the quick brown fox jumps over the lazy dog.', 0, 60" => "'the quick brown fox jumps over the lazy dog.'",
	"'the quick brown fox jumps over the lazy dog.', 0, -1" => "'the quick brown fox jumps over the lazy dog'",
	"'the quick brown fox jumps over the lazy dog.', 16, -1" => "'fox jumps over the lazy dog'",
	"'the quick brown fox jumps over the lazy dog.', -9, -1" => "'lazy dog'",
	"'the quick brown fox jumps over the lazy dog.', -100, -100" => "''"
);
function!(
	function_string_slug,
	"string::slug",
	"''" => "''",
	"'this is a test'" => "'this-is-a-test'",
	"'blog - this is a test with 😀 emojis'" => "'blog-this-is-a-test-with-grinning-emojis'"
);
function!(
	function_string_split,
	"string::split",
	"'', ''" => "['', '']",
	"'this, is, a, list', ', '" => "['this', 'is', 'a', 'list']",
	"'this - is - another - test', ' - '" => "['this', 'is', 'another', 'test']"
);
function!(
	function_string_starts_with,
	"string::startsWith",
	"'', ''" => "true",
	"'', 'test'" => "false",
	"'test this string', 'test'" => "true"
);
function!(
	function_string_trim,
	"string::trim",
	"''" => "''",
	"'test'" => "'test'",
	"'   this is a test with text   '" => "'this is a test with text'"
);
function!(
	function_string_uppercase,
	"string::uppercase",
	"''" => "''",
	"'tEsT'" => "'TEST'",
	"'this is a test'" => "'THIS IS A TEST'"
);
function!(
	function_string_words,
	"string::words",
	"''" => "[]",
	"'test'" => "['test']",
	"'this is a test'" => "['this', 'is', 'a', 'test']"
);
function!(
	function_time_day,
	"time::day",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "22"
);

// --------------------------------------------------
// time
// --------------------------------------------------

function!(
	function_time_floor,
	"time::floor",
	"'1987-06-22T08:30:45Z', 1w" => "'1987-06-18T00:00:00Z'",
	"'1987-06-22T08:30:45Z', 1y" => "'1986-12-28T00:00:00Z'"
);
function!(
	function_time_format,
	"time::format",
	"'1987-06-22T08:30:45Z', '%Y-%m-%d'" => "'1987-06-22'",
	"'1987-06-22T08:30:45Z', '%T'" => "'08:30:45'"
);
function!(
	function_time_group,
	"time::group",
	"'1987-06-22T08:30:45Z', 'hour'" => "'1987-06-22T08:00:00Z'",
	"'1987-06-22T08:30:45Z', 'month'" => "'1987-06-01T00:00:00Z'"
);
function!(
	function_time_hour,
	"time::hour",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "8"
);
function!(
	function_time_minute,
	"time::minute",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "30"
);
function!(
	function_time_month,
	"time::month",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "6"
);
function!(
	function_time_nano,
	"time::nano",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => |tmp: Value| tmp == Value::from(551349045000000000i64)
);
function!(
	function_time_now,
	"time::now",
	"" => |tmp: Value| tmp.is_datetime()
);
function!(
	function_time_round,
	"time::round",
	"'1987-06-22T08:30:45Z', 1w" => "'1987-06-25T00:00:00Z'",
	"'1987-06-22T08:30:45Z', 1y" => "'1986-12-28T00:00:00Z'"
);
function!(
	function_time_second,
	"time::second",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "45"
);
function!(
	function_time_unix,
	"time::unix",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "551349045"
);
function!(
	function_time_wday,
	"time::wday",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "1"
);
function!(
	function_time_week,
	"time::week",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "26"
);
function!(
	function_time_yday,
	"time::yday",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "173"
);
function!(
	function_time_year,
	"time::year",
	"" => |tmp: Value| tmp.is_number(),
	"'1987-06-22T08:30:45Z'" => "1987"
);
function!(
	function_time_from_micros,
	"time::from::micros",
	"384025770384840" => "'1982-03-03T17:49:30.384840Z'",
	"2840257704384440" => "'2060-01-02T08:28:24.384440Z'"
);
function!(
	function_time_from_millis,
	"time::from::millis",
	"384025773840" => "'1982-03-03T17:49:33.840Z'",
	"2840257704440" => "'2060-01-02T08:28:24.440Z'"
);
function!(
	function_time_from_secs,
	"time::from::secs",
	"384053840" => "'1982-03-04T01:37:20Z'",
	"2845704440" => "'2060-03-05T09:27:20Z'"
);
function!(
	function_time_from_unix,
	"time::from::unix",
	"384053840" => "'1982-03-04T01:37:20Z'",
	"2845704440" => "'2060-03-05T09:27:20Z'"
);

// --------------------------------------------------
// type
// --------------------------------------------------

function!(
	function_type_bool,
	"type::bool",
	"'true'" => "true",
	"'false'" => "false"
);
function!(
	function_type_datetime,
	"type::datetime",
	"'1987-06-22'" => "'1987-06-22T00:00:00Z'",
	"'2022-08-01'"=> "'2022-08-01T00:00:00Z'"
);
function!(
	function_type_decimal,
	"type::decimal",
	"'13.1043784018'" => "13.1043784018",
	"'13.5719384719384719385639856394139476937756394756'" => "13.5719384719384719385639856394139476937756394756"
);
function!(
	function_type_duration,
	"type::duration",
	"'1h30m'" => "1h30m",
	"'1h30m30s50ms'" => "1h30m30s50ms"
);
function!(
	function_type_float,
	"type::float",
	"'13.1043784018'" => |tmp: Value| tmp == Value::from(13.1043784018f64),
	"'13.5719384719384719385639856394139476937756394756'" => |tmp: Value| tmp == Value::from(13.571938471938472f64)
);
function!(
	function_type_int,
	"type::int",
	"'194719'" => |tmp: Value| tmp == Value::from(194719i64),
	"'1457105732053058'" => |tmp: Value| tmp == Value::from(1457105732053058i64)
);
function!(
	function_type_number,
	"type::number",
	"'194719.1947104740'" => "194719.1947104740",
	"'1457105732053058.3957394823281756381849375'" => "1457105732053058.3957394823281756381849375"
);
function!(
	function_type_point,
	"type::point",
	"[1.345, 6.789]" => r#"{
			type: 'Point',
			coordinates: [
				1.345,
				6.789
			]
		}"#,
	"[-0.136439, 51.509865]" => r#"{
			type: 'Point',
			coordinates: [
				-0.136439,
				51.509865
			]
		}"#
);
function!(
	function_type_string,
	"type::string",
	"30s" => "'30s'",
	"13.58248" => "'13.58248'"
);
function!(
	function_type_table,
	"type::table",
	"'person'" => |tmp: Value| tmp == Value::Table("person".into()),
	"'animal'" => |tmp: Value| tmp == Value::Table("animal".into())
);

#[tokio::test]
async fn function_type_thing() -> Result<(), Error> {
	let sql = r#"
		CREATE type::thing('person', 'test');
		CREATE type::thing('person', 1434619);
		CREATE type::thing('city', '8e60244d-95f6-4f95-9e30-09a98977efb0');
		CREATE type::thing('temperature', ['London', '2022-09-30T20:25:01.406828Z']);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:1434619,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:⟨8e60244d-95f6-4f95-9e30-09a98977efb0⟩,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: temperature:['London', '2022-09-30T20:25:01.406828Z'],
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
