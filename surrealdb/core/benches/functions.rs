#![allow(clippy::unwrap_used)]

mod common;

use common::{block_on, setup_datastore};
use criterion::{Criterion, criterion_group, criterion_main};

// ============================================================================
// Benchmark: Array functions
// ============================================================================

fn bench_array_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("array_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, array_add, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::add([1, 2, 3], 4);");
	bench!(group, array_all, &dbs, &ses, expected: |result| result.is_bool(), "RETURN array::all([true, true, true]);");
	bench!(group, array_any, &dbs, &ses, expected: |result| result.is_bool(), "RETURN array::any([false, true, false]);");
	bench!(group, array_append, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::append([1, 2, 3], 4);");
	bench!(group, array_at, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::at([1, 2, 3], 1);");
	bench!(group, array_boolean_and, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::boolean_and([true, false, true], [true, true, false]);");
	bench!(group, array_boolean_not, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::boolean_not([true, false, true]);");
	bench!(group, array_boolean_or, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::boolean_or([true, false, true], [false, true, false]);");
	bench!(group, array_boolean_xor, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::boolean_xor([true, false, true], [true, true, false]);");
	bench!(group, array_clump, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::clump([1, 2, 3, 4, 5, 6], 2);");
	bench!(group, array_combine, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::combine([1, 2, 3], [4, 5, 6]);");
	bench!(group, array_complement, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::complement([1, 2, 3, 4], [3, 4, 5, 6]);");
	bench!(group, array_concat, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::concat([1, 2], [3, 4], [5, 6]);");
	bench!(group, array_difference, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::difference([1, 2, 3, 4], [3, 4, 5, 6]);");
	bench!(group, array_distinct, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::distinct([1, 2, 2, 3, 3, 3, 4, 4, 4, 4]);");
	bench!(group, array_fill, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::fill([1, 2, 3, 4, 5], 0);");
	bench!(group, array_filter_index, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::filter_index([1, 2, 3, 4, 5], |$v, $i| $i > 2);");
	bench!(group, array_find_index, &dbs, &ses, expected: |result| result.is_number() || result.is_none(), "RETURN array::find_index([1, 2, 3, 4, 5], |$v| $v > 3);");
	bench!(group, array_first, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::first([1, 2, 3, 4, 5]);");
	bench!(group, array_flatten, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::flatten([[1, 2], [3, 4], [5, 6]]);");
	bench!(group, array_group, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::group([1, 2, 1, 3, 2, 4]);");
	bench!(group, array_insert, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::insert([1, 2, 4, 5], 3, 2);");
	bench!(group, array_intersect, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::intersect([1, 2, 3, 4], [3, 4, 5, 6]);");
	bench!(group, array_is_empty, &dbs, &ses, expected: |result| result.is_bool(), "RETURN array::is_empty([]);");
	bench!(group, array_join, &dbs, &ses, expected: |result| result.is_string(), "RETURN array::join([1, 2, 3], ', ');");
	bench!(group, array_last, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::last([1, 2, 3, 4, 5]);");
	bench!(group, array_len, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::len([1, 2, 3, 4, 5]);");
	bench!(group, array_logical_and, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::logical_and([0, 1, 2, 3], [0, 1, 4, 5]);");
	bench!(group, array_logical_or, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::logical_or([0, 1, 2], [0, 2, 4]);");
	bench!(group, array_logical_xor, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::logical_xor([0, 1, 2, 3], [0, 2, 3, 4]);");
	bench!(group, array_matches, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::matches([1, 2, 3, 4], 3);");
	bench!(group, array_max, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::max([5, 2, 8, 1, 9, 3]);");
	bench!(group, array_min, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::min([5, 2, 8, 1, 9, 3]);");
	bench!(group, array_pop, &dbs, &ses, expected: |result| result.is_number(), "RETURN array::pop([1, 2, 3, 4, 5]);");
	bench!(group, array_prepend, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::prepend([2, 3, 4], 1);");
	bench!(group, array_push, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::push([1, 2, 3], 4);");
	bench!(group, array_range, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::range(1..5);");
	bench!(group, array_remove, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::remove([1, 2, 3, 4, 5], 2);");
	bench!(group, array_repeat, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::repeat(5, 3);");
	bench!(group, array_reverse, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::reverse([1, 2, 3, 4, 5]);");
	bench!(group, array_sequence, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sequence(0, 10);");
	bench!(group, array_shuffle, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::shuffle([1, 2, 3, 4, 5]);");
	bench!(group, array_slice, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::slice([1, 2, 3, 4, 5], 1, 3);");
	bench!(group, array_sort, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sort([5, 2, 8, 1, 9, 3]);");
	bench!(group, array_sort_asc, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sort::asc([5, 2, 8, 1, 9, 3]);");
	bench!(group, array_sort_desc, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sort::desc([5, 2, 8, 1, 9, 3]);");
	bench!(group, array_sort_lexical, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sort_lexical(['Álvares', 'senhor', 'Obrigado']);");
	bench!(group, array_sort_natural, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sort_natural([8, 9, 10, '3', '2.2', '11']);");
	bench!(group, array_sort_natural_lexical, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::sort_natural_lexical(['Obrigado', 'senhor', 'Álvares', 8, 9, 10, '3', '2.2', '11']);");
	bench!(group, array_swap, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::swap([1, 2, 3, 4, 5], 0, 4);");
	bench!(group, array_transpose, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::transpose([[0, 1], [2, 3]]);");
	bench!(group, array_union, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::union([1, 2, 3], [3, 4, 5]);");
	bench!(group, array_windows, &dbs, &ses, expected: |result| result.is_array(), "RETURN array::windows([1, 2, 3, 4], 2);");

	group.finish();
}

// ============================================================================
// Benchmark: Crypto functions
// ============================================================================

fn bench_crypto_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("crypto_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	// Note: Intentionally slow hash functions (argon2, bcrypt, pbkdf2, scrypt) are excluded
	bench!(group, crypto_md5, &dbs, &ses, expected: |result| result.is_string(), "RETURN crypto::md5('hello world');");
	bench!(group, crypto_sha1, &dbs, &ses, expected: |result| result.is_string(), "RETURN crypto::sha1('hello world');");
	bench!(group, crypto_sha256, &dbs, &ses, expected: |result| result.is_string(), "RETURN crypto::sha256('hello world');");
	bench!(group, crypto_sha512, &dbs, &ses, expected: |result| result.is_string(), "RETURN crypto::sha512('hello world');");

	group.finish();
}

// ============================================================================
// Benchmark: Duration functions
// ============================================================================

fn bench_duration_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("duration_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, duration_days, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::days(2d);");
	bench!(group, duration_from_days, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_days(7);");
	bench!(group, duration_from_hours, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_hours(24);");
	bench!(group, duration_from_micros, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_micros(1000000);");
	bench!(group, duration_from_millis, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_millis(1000);");
	bench!(group, duration_from_mins, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_mins(60);");
	bench!(group, duration_from_nanos, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_nanos(1000000000);");
	bench!(group, duration_from_secs, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_secs(3600);");
	bench!(group, duration_from_weeks, &dbs, &ses, expected: |result| result.is_duration(), "RETURN duration::from_weeks(1);");
	bench!(group, duration_hours, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::hours(3h);");
	bench!(group, duration_micros, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::micros(1s);");
	bench!(group, duration_millis, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::millis(1s);");
	bench!(group, duration_mins, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::mins(45m);");
	bench!(group, duration_nanos, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::nanos(1s);");
	bench!(group, duration_secs, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::secs(120s);");
	bench!(group, duration_weeks, &dbs, &ses, expected: |result| result.is_number(), "RETURN duration::weeks(2w);");

	group.finish();
}

// ============================================================================
// Benchmark: Geo functions
// ============================================================================

fn bench_geo_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("geo_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, geo_area, &dbs, &ses, expected: |result| result.is_number(), "RETURN geo::area({{ type: 'Polygon', coordinates: [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]] }});");
	bench!(group, geo_bearing, &dbs, &ses, expected: |result| result.is_number(), "RETURN geo::bearing((0.0, 0.0), (1.0, 1.0));");
	bench!(group, geo_centroid, &dbs, &ses, expected: |result| result.is_geometry(), "RETURN geo::centroid({{ type: 'Polygon', coordinates: [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]] }});");
	bench!(group, geo_distance, &dbs, &ses, expected: |result| result.is_number(), "RETURN geo::distance((0.0, 0.0), (1.0, 1.0));");
	bench!(group, geo_hash_decode, &dbs, &ses, expected: |result| result.is_geometry(), "RETURN geo::hash::decode('9q8yyk');");
	bench!(group, geo_hash_encode, &dbs, &ses, expected: |result| result.is_string(), "RETURN geo::hash::encode((37.7749, -122.4194));");

	group.finish();
}

// ============================================================================
// Benchmark: Math functions
// ============================================================================

fn bench_math_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("math_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, math_abs, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::abs(-42);");
	bench!(group, math_acos, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::acos(0.5);");
	bench!(group, math_acot, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::acot(1);");
	bench!(group, math_asin, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::asin(0.5);");
	bench!(group, math_atan, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::atan(1);");
	bench!(group, math_bottom, &dbs, &ses, expected: |result| result.is_array(), "RETURN math::bottom([1, 2, 3, 4, 5], 3);");
	bench!(group, math_ceil, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::ceil(3.14);");
	bench!(group, math_clamp, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::clamp(5, 0, 10);");
	bench!(group, math_cos, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::cos(0);");
	bench!(group, math_cot, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::cot(1);");
	bench!(group, math_deg2rad, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::deg2rad(180);");
	bench!(group, math_fixed, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::fixed(3.14159, 2);");
	bench!(group, math_floor, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::floor(3.14);");
	bench!(group, math_interquartile, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::interquartile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_lerp, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::lerp(0, 100, 0.5);");
	bench!(group, math_lerpangle, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::lerpangle(0, 360, 0.5);");
	bench!(group, math_ln, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::ln(2.71828);");
	bench!(group, math_log, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::log(100, 10);");
	bench!(group, math_max, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::max([5, 2, 8, 1, 9, 3]);");
	bench!(group, math_mean, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::mean([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_median, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::median([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_midhinge, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::midhinge([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_min, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::min([5, 2, 8, 1, 9, 3]);");
	bench!(group, math_mode, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::mode([1, 2, 2, 3, 3, 3]);");
	bench!(group, math_nearestrank, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::nearestrank([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50);");
	bench!(group, math_percentile, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50);");
	bench!(group, math_pow, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::pow(2, 8);");
	bench!(group, math_product, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::product([1, 2, 3, 4, 5]);");
	bench!(group, math_rad2deg, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::rad2deg(3.14159);");
	bench!(group, math_round, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::round(3.14);");
	bench!(group, math_sign, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::sign(-42);");
	bench!(group, math_sin, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::sin(1.57);");
	bench!(group, math_spread, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::spread([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_sqrt, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::sqrt(16);");
	bench!(group, math_stddev, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::stddev([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_sum, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::sum([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_tan, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::tan(0.785);");
	bench!(group, math_top, &dbs, &ses, expected: |result| result.is_array(), "RETURN math::top([1, 2, 3, 4, 5], 3);");
	bench!(group, math_trimean, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::trimean([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");
	bench!(group, math_variance, &dbs, &ses, expected: |result| result.is_number(), "RETURN math::variance([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);");

	group.finish();
}

// ============================================================================
// Benchmark: Object functions
// ============================================================================

fn bench_object_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("object_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, object_entries, &dbs, &ses, expected: |result| result.is_array(), "RETURN object::entries({{ a: 1, b: 2, c: 3 }});");
	bench!(group, object_from_entries, &dbs, &ses, expected: |result| result.is_object(), "RETURN object::from_entries([['a', 1], ['b', 2], ['c', 3]]);");
	bench!(group, object_keys, &dbs, &ses, expected: |result| result.is_array(), "RETURN object::keys({{ a: 1, b: 2, c: 3 }});");
	bench!(group, object_len, &dbs, &ses, expected: |result| result.is_number(), "RETURN object::len({{ a: 1, b: 2, c: 3 }});");
	bench!(group, object_values, &dbs, &ses, expected: |result| result.is_array(), "RETURN object::values({{ a: 1, b: 2, c: 3 }});");

	group.finish();
}

// ============================================================================
// Benchmark: Parse functions
// ============================================================================

fn bench_parse_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("parse_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, parse_email_host, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::email::host('test@example.com');");
	bench!(group, parse_email_user, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::email::user('test@example.com');");
	bench!(group, parse_url_domain, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::url::domain('https://example.com/path');");
	bench!(group, parse_url_fragment, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::url::fragment('https://example.com/path#section');");
	bench!(group, parse_url_host, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::url::host('https://example.com:8080/path');");
	bench!(group, parse_url_path, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::url::path('https://example.com/path/to/page');");
	bench!(group, parse_url_port, &dbs, &ses, expected: |result| result.is_number(), "RETURN parse::url::port('https://example.com:8080/path');");
	bench!(group, parse_url_query, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::url::query('https://example.com/path?key=value');");
	bench!(group, parse_url_scheme, &dbs, &ses, expected: |result| result.is_string(), "RETURN parse::url::scheme('https://example.com/path');");

	group.finish();
}

// ============================================================================
// Benchmark: Rand functions
// ============================================================================

fn bench_rand_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("rand_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, rand_bool, &dbs, &ses, expected: |result| result.is_bool(), "RETURN rand::bool();");
	bench!(group, rand_enum, &dbs, &ses, expected: |result| !result.is_none(), "RETURN rand::enum('a', 'b', 'c');");
	bench!(group, rand_float, &dbs, &ses, expected: |result| result.is_number(), "RETURN rand::float();");
	bench!(group, rand_guid, &dbs, &ses, expected: |result| result.is_string(), "RETURN rand::id();");
	bench!(group, rand_int, &dbs, &ses, expected: |result| result.is_number(), "RETURN rand::int(1, 100);");
	bench!(group, rand_string, &dbs, &ses, expected: |result| result.is_string(), "RETURN rand::string(20);");
	bench!(group, rand_time, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN rand::time();");
	bench!(group, rand_ulid, &dbs, &ses, expected: |result| result.is_string(), "RETURN rand::ulid();");
	bench!(group, rand_uuid, &dbs, &ses, expected: |result| result.is_uuid(), "RETURN rand::uuid();");

	group.finish();
}

// ============================================================================
// Benchmark: String functions
// ============================================================================

fn bench_string_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("string_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, string_capitalize, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::capitalize('hello world');");
	bench!(group, string_concat, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::concat('hello', ' ', 'world');");
	bench!(group, string_contains, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::contains('hello world', 'world');");
	bench!(group, string_ends_with, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::ends_with('hello world', 'world');");
	bench!(group, string_is_alphanum, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_alphanum('abc123');");
	bench!(group, string_is_alpha, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_alpha('abc');");
	bench!(group, string_is_ascii, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_ascii('hello');");
	bench!(group, string_is_datetime, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_datetime('2023-10-15T12:30:45Z');");
	bench!(group, string_is_domain, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_domain('example.com');");
	bench!(group, string_is_email, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_email('test@example.com');");
	bench!(group, string_is_hexadecimal, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_hexadecimal('1a2b3c');");
	bench!(group, string_is_latitude, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_latitude('51.509865');");
	bench!(group, string_is_longitude, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_longitude('-0.118092');");
	bench!(group, string_is_numeric, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_numeric('12345');");
	bench!(group, string_is_semver, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_semver('1.2.3');");
	bench!(group, string_is_ulid, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_ulid('01ARZ3NDEKTSV4RRFFQ69G5FAV');");
	bench!(group, string_is_url, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_url('https://example.com');");
	bench!(group, string_is_uuid, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::is_uuid('550e8400-e29b-41d4-a716-446655440000');");
	bench!(group, string_join, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::join(', ', 'one', 'two', 'three');");
	bench!(group, string_len, &dbs, &ses, expected: |result| result.is_number(), "RETURN string::len('hello world');");
	bench!(group, string_lowercase, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::lowercase('HELLO WORLD');");
	bench!(group, string_matches, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::matches('hello123', /[0-9]+/);");
	bench!(group, string_repeat, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::repeat('ab', 3);");
	bench!(group, string_replace, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::replace('hello world', 'world', 'there');");
	bench!(group, string_reverse, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::reverse('hello world');");
	bench!(group, string_slice, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::slice('hello world', 0, 5);");
	bench!(group, string_slug, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::slug('Hello World Example');");
	bench!(group, string_split, &dbs, &ses, expected: |result| result.is_array(), "RETURN string::split('one,two,three', ',');");
	bench!(group, string_starts_with, &dbs, &ses, expected: |result| result.is_bool(), "RETURN string::starts_with('hello world', 'hello');");
	bench!(group, string_trim, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::trim('  hello world  ');");
	bench!(group, string_uppercase, &dbs, &ses, expected: |result| result.is_string(), "RETURN string::uppercase('hello world');");
	bench!(group, string_words, &dbs, &ses, expected: |result| result.is_array(), "RETURN string::words('hello world example');");

	group.finish();
}

// ============================================================================
// Benchmark: Time functions
// ============================================================================

fn bench_time_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("time_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, time_ceil, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::ceil(d'2023-10-15T12:30:45Z', 1h);");
	bench!(group, time_day, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::day(d'2023-10-15T12:30:45Z');");
	bench!(group, time_floor, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::floor(d'2023-10-15T12:30:45Z', 1h);");
	bench!(group, time_format, &dbs, &ses, expected: |result| result.is_string(), "RETURN time::format(d'2023-10-15T12:30:45Z', '%Y-%m-%d');");
	bench!(group, time_from_micros, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::from_micros(1000000);");
	bench!(group, time_from_millis, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::from_millis(1000);");
	bench!(group, time_from_nanos, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::from_nanos(1000000);");
	bench!(group, time_from_secs, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::from_secs(1000);");
	bench!(group, time_from_unix, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::from_unix(1000);");
	bench!(group, time_group, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::group(d'2023-10-15T12:30:45Z', 'year');");
	bench!(group, time_hour, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::hour(d'2023-10-15T12:30:45Z');");
	bench!(group, time_is_leap_year, &dbs, &ses, expected: |result| result.is_bool(), "RETURN time::is_leap_year(d'2024-01-01T00:00:00Z');");
	bench!(group, time_max, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::max([d'2023-10-15T12:30:45Z', d'2024-10-15T12:30:45Z']);");
	bench!(group, time_micros, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::micros(d'2023-10-15T12:30:45Z');");
	bench!(group, time_millis, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::millis(d'2023-10-15T12:30:45Z');");
	bench!(group, time_min, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::min([d'2023-10-15T12:30:45Z', d'2024-10-15T12:30:45Z']);");
	bench!(group, time_minute, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::minute(d'2023-10-15T12:30:45Z');");
	bench!(group, time_month, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::month(d'2023-10-15T12:30:45Z');");
	bench!(group, time_nano, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::nano(d'2023-10-15T12:30:45Z');");
	bench!(group, time_now, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::now();");
	bench!(group, time_round, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN time::round(d'2023-10-15T12:30:45Z', 1h);");
	bench!(group, time_second, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::second(d'2023-10-15T12:30:45Z');");
	bench!(group, time_timezone, &dbs, &ses, expected: |result| result.is_string(), "RETURN time::timezone();");
	bench!(group, time_unix, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::unix(d'2023-10-15T12:30:45Z');");
	bench!(group, time_wday, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::wday(d'2023-10-15T12:30:45Z');");
	bench!(group, time_week, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::week(d'2023-10-15T12:30:45Z');");
	bench!(group, time_yday, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::yday(d'2023-10-15T12:30:45Z');");
	bench!(group, time_year, &dbs, &ses, expected: |result| result.is_number(), "RETURN time::year(d'2023-10-15T12:30:45Z');");

	group.finish();
}

// ============================================================================
// Benchmark: Type functions
// ============================================================================

fn bench_type_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("type_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, type_array, &dbs, &ses, expected: |result| result.is_array(), "RETURN type::array([1,2,3]);");
	bench!(group, type_bool, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::bool('true');");
	bench!(group, type_bytes, &dbs, &ses, expected: |result| result.is_bytes(), "RETURN type::bytes('hello');");
	bench!(group, type_datetime, &dbs, &ses, expected: |result| result.is_datetime(), "RETURN type::datetime('2023-10-15T12:30:45Z');");
	bench!(group, type_decimal, &dbs, &ses, expected: |result| result.is_number(), "RETURN type::decimal('123.456');");
	bench!(group, type_duration, &dbs, &ses, expected: |result| result.is_duration(), "RETURN type::duration('1h30m');");
	bench!(group, type_float, &dbs, &ses, expected: |result| result.is_number(), "RETURN type::float('123.456');");
	bench!(group, type_int, &dbs, &ses, expected: |result| result.is_number(), "RETURN type::int('123');");
	bench!(group, type_is_array, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_array([1, 2, 3]);");
	bench!(group, type_is_bool, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_bool(true);");
	bench!(group, type_is_bytes, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_bytes(<bytes>'hello');");
	bench!(group, type_is_collection, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_collection('table:id');");
	bench!(group, type_is_datetime, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_datetime(d'2023-10-15T12:30:45Z');");
	bench!(group, type_is_decimal, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_decimal(123.456dec);");
	bench!(group, type_is_duration, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_duration(1h);");
	bench!(group, type_is_float, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_float(123.456f);");
	bench!(group, type_is_geometry, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_geometry((1.23, 4.56));");
	bench!(group, type_is_int, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_int(123);");
	bench!(group, type_is_line, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_line('not a line');");
	bench!(group, type_is_multiline, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_multiline('not a multiline');");
	bench!(group, type_is_multipoint, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_multipoint('not a multipoint');");
	bench!(group, type_is_multipolygon, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_multipolygon('not a multipolygon');");
	bench!(group, type_is_none, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_none(NONE);");
	bench!(group, type_is_null, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_null(NULL);");
	bench!(group, type_is_number, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_number(42);");
	bench!(group, type_is_object, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_object({{ name: 'test' }});");
	bench!(group, type_is_point, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_point((1.23, 4.56));");
	bench!(group, type_is_polygon, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_polygon('not a polygon');");
	bench!(group, type_is_range, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_range(1..10);");
	bench!(group, type_is_record, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_record(person:test);");
	bench!(group, type_is_string, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_string('hello');");
	bench!(group, type_is_uuid, &dbs, &ses, expected: |result| result.is_bool(), "RETURN type::is_uuid(u'018a6680-bef9-701b-9025-e1754f296a0f');");
	bench!(group, type_number, &dbs, &ses, expected: |result| result.is_number(), "RETURN type::number('42');");
	bench!(group, type_of, &dbs, &ses, expected: |result| result.is_string(), "RETURN type::of(42);");
	bench!(group, type_point, &dbs, &ses, expected: |result| result.is_geometry(), "RETURN type::point([1.23, 4.56]);");
	bench!(group, type_range, &dbs, &ses, expected: |result| result.is_range(), "RETURN type::range([1,10]);");
	bench!(group, type_record, &dbs, &ses, expected: |result| result.is_record(), "RETURN type::record('person', 'test');");
	bench!(group, type_string, &dbs, &ses, expected: |result| result.is_string(), "RETURN type::string(42);");
	bench!(group, type_table, &dbs, &ses, expected: |result| result.is_table(), "RETURN type::table('person');");
	bench!(group, type_uuid, &dbs, &ses, expected: |result| result.is_uuid(), "RETURN type::uuid('0191f946-936f-7223-bef5-aebbc527ad80');");

	group.finish();
}

// ============================================================================
// Benchmark: Vector functions
// ============================================================================

fn bench_vector_functions(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("vector_functions");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, vector_add, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::add([1, 2, 3], [1, 2, 3]);");
	bench!(group, vector_angle, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::angle([5, 10, 15], [10, 5, 20]);");
	bench!(group, vector_cross, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::cross([1, 2, 3], [4, 5, 6]);");
	bench!(group, vector_distance_chebyshev, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::distance::chebyshev([2, 4, 5, 3, 8, 2], [3, 1, 5, -3, 7, 2]);");
	bench!(group, vector_distance_euclidean, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::distance::euclidean([10, 50, 200], [400, 100, 20]);");
	bench!(group, vector_distance_hamming, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::distance::hamming([1, 2, 2], [1, 2, 3]);");
	bench!(group, vector_distance_manhattan, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::distance::manhattan([10, 20, 15, 10, 5], [12, 24, 18, 8, 7]);");
	bench!(group, vector_distance_minkowski, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::distance::minkowski([10, 20, 15, 10, 5], [12, 24, 18, 8, 7], 3);");
	bench!(group, vector_divide, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::divide([10, -20, 30, 0], [0, -1, 2, -3]);");
	bench!(group, vector_dot, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::dot([1, 2, 3], [1, 2, 3]);");
	bench!(group, vector_magnitude, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::magnitude([1, 2, 3, 3, 3, 4, 5]);");
	bench!(group, vector_multiply, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::multiply([1, 2, 3], [1, 2, 3]);");
	bench!(group, vector_normalize, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::normalize([4, 3]);");
	bench!(group, vector_project, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::project([1, 2, 3], [4, 5, 6]);");
	bench!(group, vector_scale, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::scale([3, 1, 5, -3, 7, 2], 5);");
	bench!(group, vector_similarity_cosine, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::similarity::cosine([10, 50, 200], [400, 100, 20]);");
	bench!(group, vector_similarity_jaccard, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::similarity::jaccard([0, 1, 2, 5, 6], [0, 2, 3, 4, 5, 7, 9]);");
	bench!(group, vector_similarity_pearson, &dbs, &ses, expected: |result| result.is_number(), "RETURN vector::similarity::pearson([1, 2, 3], [1, 5, 7]);");
	bench!(group, vector_subtract, &dbs, &ses, expected: |result| result.is_array(), "RETURN vector::subtract([4, 5, 6], [3, 2, 1]);");

	group.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default();
	targets = bench_array_functions,
		bench_crypto_functions,
		bench_duration_functions,
		bench_geo_functions,
		bench_math_functions,
		bench_object_functions,
		bench_parse_functions,
		bench_rand_functions,
		bench_string_functions,
		bench_time_functions,
		bench_type_functions,
		bench_vector_functions,
);
criterion_main!(benches);
