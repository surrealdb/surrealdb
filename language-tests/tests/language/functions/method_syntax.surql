/**
[test]
reason = "Asserts that no errors are produced when every function registered for method syntax is called in that way."

[[test.results]]
value = "NONE"
*/

{
	-- =====================================================================
	-- Generic methods - type conversion
	-- =====================================================================
	{123,}.to_array();
	true.to_bool();
	"abc".to_bytes();
	"2025-01-01T00:00:00Z".to_datetime();
	123.45.to_decimal();
	"1h".to_duration();
	123.to_float();
	{ type: "Point", coordinates: [0, 0] }
    .to_geometry();
	"123".to_int();
	123.to_number();
	[1, 2].to_point();
	[1,10].to_range();
	"person:test".to_record();
	[1, 2, 2].to_set();
	123.to_string();
	123.to_string_lossy();
	"019535d9-3df7-79fb-b466-fa907fa17f9e".to_uuid();

	-- =====================================================================
	-- Generic methods - type checking
	-- =====================================================================
	[].is_array();
	true.is_bool();
	"abc".to_bytes().is_bytes();
	[].is_collection();
	time::now().is_datetime();
	123.45.to_decimal().is_decimal();
	"1h".to_duration().is_duration();
	123.4.is_float();
	{ type: "Point", coordinates: [0, 0] }.is_geometry();
	123.is_int();
	{ type: "LineString", coordinates: [[0,0],[1,1]] }.is_line();
	NONE.is_none();
	NULL.is_null();
	{ type: "MultiLineString", coordinates: [[[0,0],[1,1]]] }.is_multiline();
	{ type: "MultiPoint", coordinates: [[0,0],[1,1]] }.is_multipoint();
	{ type: "MultiPolygon", coordinates: [[[[0,0],[1,0],[1,1],[0,0]]]] }.is_multipolygon();
	123.is_number();
	{ a: 1 }.is_object();
	{ type: "Point", coordinates: [0, 0] }.is_point();
	{ type: "Polygon", coordinates: [[[0,0],[1,0],[1,1],[0,0]]] }.is_polygon();
	(1..10).is_range();
	"person:test".to_record().is_record();
	[1, 2, 2].to_set().is_set();
	"abc".is_string();
	"019535d9-3df7-79fb-b466-fa907fa17f9e".to_uuid().is_uuid();

	-- =====================================================================
	-- Universal methods
	-- =====================================================================
	123.chain(|$x| $x);
	{ one: "object" }.diff({ another: "object" });
	{ one: "object" }.patch([
		{ op: "remove", path: "/one" },
		{ op: "add", path: "/another", value: "object" }
	]);
	[1, 2].repeat(2);

	-- =====================================================================
	-- String methods
	-- =====================================================================
	"hello".capitalize();
	"hello".concat(" world");
	"hello".contains("ell");
	"hello".ends_with("lo");
	["a", "b"].join("-");
	"hello".len();
	"HELLO".lowercase();
	"hello".matches("ell");
	"ha".repeat(2);
	"hello".replace("l", "x");
	"hello".reverse();
	"hello".slice(1, 3);
	"Hello World".slug();
	"a,b,c".split(",");
	"hello".starts_with("he");
	"  hello  ".trim();
	"hello".uppercase();
	"one two".words();

	-- String distance methods
	"kitten".distance_damerau_levenshtein("sitting");
	"karolin".distance_hamming("kathrin");
	"kitten".distance_levenshtein("sitting");
	"kitten".distance_normalized_damerau_levenshtein("sitting");
	"kitten".distance_normalized_levenshtein("sitting");

	-- String HTML methods
	"<b>x</b>".html_encode();
	"<b>x</b>".html_sanitize();

	-- String format/is_* methods
	"abc123".is_alphanum();
	"abc".is_alpha();
	"abc".is_ascii();
	"2025-01-01T00:00:00Z".is_datetime();
	"example.com".is_domain();
	"a@example.com".is_email();
	"deadbeef".is_hexadecimal();
	"127.0.0.1".is_ip();
	"127.0.0.1".is_ipv4();
	"::1".is_ipv6();
	"51.5074".is_latitude();
	"-0.1278".is_longitude();
	"12345".is_numeric();
	"1.2.3".is_semver();
	"https://surrealdb.com".is_url();
	"01ARZ3NDEKTSV4RRFFQ69G5FAV".is_ulid();
	"019535d9-3df7-79fb-b466-fa907fa17f9e".is_uuid();
	"person:test".is_record();

	-- String similarity methods
	"hello".similarity_fuzzy("hallo");
	"hello".similarity_jaro("hallo");
	"hello".similarity_jaro_winkler("hallo");
	"hello".similarity_smithwaterman("hallo");
	"night".similarity_sorensen_dice("nacht");

	-- String semver methods
	"1.2.3".semver_compare("1.2.4");
	"1.2.3".semver_major();
	"1.2.3".semver_minor();
	"1.2.3".semver_patch();
	"1.2.3".semver_inc_major();
	"1.2.3".semver_inc_minor();
	"1.2.3".semver_inc_patch();
	"1.2.3".semver_set_major(2);
	"1.2.3".semver_set_minor(5);
	"1.2.3".semver_set_patch(9);

	-- =====================================================================
	-- Array methods
	-- =====================================================================
	[1, 2].add(3);
	[1, 2, 3].all(|$x| $x > 0);
	[1, 2, 3].any(|$x| $x = 2);
	[1, 2].append(3);
	[1, 2, 3].at(1);
	[true,true].boolean_and([true, true]);
	[true, false].boolean_not();
	[true, false].boolean_or([true, true]);
	[true, false].boolean_xor([true, true]);
	[1, 2, 3, 4].clump(2);
	[1, 2].combine([3, 4]);
	[1, 2].complement([2, 3]);
	[1, 2].concat([3, 4]);
	[1, 2].difference([2, 3]);
	[1, 1, 2].distinct();
	[1, 2].fill(0, 1, 2);
	[1, 2, 3].filter(|$x| $x > 1);
	[1, 2, 3].filter_index(|$x| $x > 1);
	[1, 2, 3].find(|$x| $x = 2);
	[1, 2, 3].find_index(|$x| $x = 2);
	[1, 2, 3].first();
	[1, 2, 3].fold(0, |$a, $x| $a + $x);
	[[1], [2]].flatten();
	[1, 2, 2].group();
	[1, 3].insert(1, 2);
	[1, 2].intersect([2, 3]);
	[].is_empty();
	["a", "b"].join("-");
	[1, 2, 3].last();
	[1, 2, 3].len();
	[true, false].logical_and([true, true]);
	[true, false].logical_or([true, true]);
	[true, false].logical_xor([true, true]);
	["abc"].matches("a");
	[1, 2].map(|$x| $x + 1);
	[1, 3, 2].max();
	[1, 3, 2].min();
	[1, 2, 3].pop();
	[2, 3].prepend(1);
	[1, 2].push(3);
	[1, 2, 3].reduce(|$a, $x| $a + $x);
	[1, 2, 3].remove(1);
	[1, 2, 3].reverse();
	[1, 2, 3].shuffle();
	[1, 2, 3].slice(0, 2);
	[3, 1, 2].sort();
	["b", "a"].sort_natural();
	["b", "a"].sort_lexical();
	["b2", "b10", "b1"].sort_natural_lexical();
	[1, 2, 3].swap(0, 2);
	[[1, 2], [3, 4]].transpose();
	[1, 2].union([2, 3]);
	[3, 1, 2].sort_asc();
	[1, 3, 2].sort_desc();
	[1, 2, 3].windows(2);

	-- Array aliases
	[1, 2, 3].every(|$x| $x > 0);
	[1, 2, 3].includes(|$x| $x = 2);
	[1, 2, 3].some(|$x| $x = 2);
	[1, 2, 3].index_of(|$x| $x = 2);

	-- Array vector methods
	[1, 2].vector_add([3, 4]);
	[1, 0].vector_angle([0, 1]);
	[1, 0, 0].vector_cross([0, 1, 0]);
	[1, 2].vector_dot([3, 4]);
	[2, 4].vector_divide([1, 2]);
	[3, 4].vector_magnitude();
	[2, 3].vector_multiply([4, 5]);
	[3, 4].vector_normalize();
	[1, 2].vector_project([3, 4]);
	[1, 2].vector_scale(2);
	[3, 4].vector_subtract([1, 2]);
	[1, 2].vector_distance_chebyshev([2, 4]);
	[1, 2].vector_distance_euclidean([2, 4]);
	[1, 2].vector_distance_hamming([1, 3]);
	[1, 2].vector_distance_manhattan([2, 4]);
	[1, 2].vector_distance_minkowski([2, 4], 3);
	[1, 0].vector_similarity_cosine([0, 1]);
	[1, 2].vector_similarity_jaccard([2, 3]);
	[1, 2, 3].vector_similarity_pearson([1, 2, 3]);

	-- =====================================================================
	-- Set methods
	-- =====================================================================
	{1, 2}.add(3);
	{1, 2, 3}.all(|$x| $x > 0);
	{1, 2, 3}.any(|$x| $x = 2);
	{1, 2, 3}.at(1);
	{1, 2}.complement({2, 3});
	{1, 2}.contains(2);
	{1, 2}.difference({2, 3});
	{1, 2, 3}.filter(|$x| $x > 1);
	{1, 2, 3}.find(|$x| $x = 2);
	{1, 2, 3}.first();
	{{1,}, {2,}}.flatten();
	{1, 2, 3}.fold(0, |$a, $x| $a + $x);
	{1, 2}.intersect({2, 3});
	{,}.is_empty();
	{"a", "b"}.join("-");
	{1, 2, 3}.last();
	{1, 2, 3}.len();
	{1, 2}.map(|$x| $x + 1);
	{1, 3, 2}.max();
	{1, 3, 2}.min();
	{1, 2, 3}.reduce(|$a, $x| $a + $x);
	{1, 2, 3}.remove(2);
	{1, 2, 3}.slice(0, 2);
	{1, 2}.union({2, 3});

	-- =====================================================================
	-- Object methods
	-- =====================================================================
	{ a: 1, b: 2 }.entries();
	{ a: 1 }.extend({ b: 2 });
	{}.is_empty();
	{ a: 1, b: 2 }.keys();
	{ a: 1, b: 2 }.len();
	{ a: 1, b: 2 }.remove("a");
	{ a: 1, b: 2 }.values();

	-- =====================================================================
	-- Bytes methods
	-- =====================================================================
	encoding::base64::decode("aGVsbG8=").len();

	-- =====================================================================
	-- Duration methods
	-- =====================================================================
	"1w2d3h4m5s".to_duration().days();
	"1w2d3h4m5s".to_duration().hours();
	"1w2d3h4m5s".to_duration().micros();
	"1w2d3h4m5s".to_duration().millis();
	"1w2d3h4m5s".to_duration().mins();
	"1w2d3h4m5s".to_duration().nanos();
	"1w2d3h4m5s".to_duration().secs();
	"1w2d3h4m5s".to_duration().weeks();
	"1w2d3h4m5s".to_duration().years();

	-- =====================================================================
	-- Number methods
	-- =====================================================================
	(-1).abs();
	1.acos();
	1.acot();
	0.asin();
	1.atan();
	1.2.ceil();
	1.cos();
	1.cot();
	180.deg2rad();
	1.8.floor();
	10.ln();
	10.log(10);
	10.log10();
	8.log2();
	3.1415926535.rad2deg();
	1.4.round();
	(-1).sign();
	0.sin();
	1.tan();

	-- =====================================================================
	-- Geometry methods
	-- =====================================================================
	{ type: "Polygon", coordinates: [[[0,0],[1,0],[1,1],[0,0]]] }.area();
	{ type: "Point", coordinates: [0,0] }.bearing({ type: "Point", coordinates: [1,1] });
	{ type: "Polygon", coordinates: [[[0,0],[1,0],[1,1],[0,0]]] }.centroid();
	{ type: "Point", coordinates: [0,0] }.distance({ type: "Point", coordinates: [1,1] });
	{ type: "Point", coordinates: [0,0] }.hash_encode();
	{ type: "Point", coordinates: [0,0] }.is_valid();

	-- =====================================================================
	-- Record methods
	-- =====================================================================
    DEFINE TABLE IF NOT EXISTS person;
	"person:test".to_record().exists();
	"person:test".to_record().id();
	"person:test".to_record().tb();
	"person:test".to_record().table();
	"person:test".to_record().is_edge();

	-- =====================================================================
	-- Datetime methods
	-- =====================================================================
	time::now().ceil(1s);
	time::now().day();
	time::now().floor(1s);
	time::now().format("%Y-%m-%d");
	time::now().group("day");
	time::now().hour();
	time::now().is_leap_year();
	time::now().micros();
	time::now().millis();
	time::now().minute();
	time::now().month();
	time::now().nano();
	time::now().round(1s);
	time::now().second();
	time::now().unix();
	time::now().wday();
	time::now().week();
	time::now().yday();
	time::now().year();
	time::now().set_year(2025);
	time::now().set_month(1);
	time::now().set_day(1);
	time::now().set_hour(0);
	time::now().set_minute(0);
	time::now().set_second(0);
	time::now().set_nanosecond(0);

	RETURN NONE;
};