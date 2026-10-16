/**
[env]
namespace = true
database = true

[test]
reason = """
	A statement in value position, correlated with the row of the enclosing statement by an equality
	over an indexed field, evaluated for more rows than a lookup serves before it reads the table
	once and keeps it by key. The keys are integers, floats and missing values; the statements are
	a count, a grouped count, a maximum, a mean, an array aggregate whose order is the order of the
	rows, a sum that meets a string, and a count in a `WHERE`. Every answer is the answer of the
	statement run for each row.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[0, 6, 6, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 6, 6, 6, 0, 6, 6, 6, 0, 0, 6, 6, 6, 0, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 6, 6, 6, 0, 6, 6, 6, 0, 0, 6, 6, 6, 0, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 6, 6, 6, 0, 6, 6, 6, 0, 0, 6, 6, 6, 0]"

[[test.results]]
value = "[[{ count: 0 }], [{ count: 6 }], [{ count: 6 }]]"

[[test.results]]
value = "[[], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [], [], [], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }], [{ count: 2, g: 0 }, { count: 2, g: 1 }, { count: 2, g: 2 }]]"

[[test.results]]
value = "[-Infinity, 51, 52, 53, 54, 55, 56, 57, 58, -Infinity, -Infinity, -Infinity, 50, 51, 52, 53, 54, 55, -Infinity, 57, 58, 59, -Infinity, -Infinity, 50, 51, 52, -Infinity, 54, 55, 56, 57, 58, 59, -Infinity, -Infinity, -Infinity, 51, 52, 53, 54, 55, 56, 57, 58, -Infinity, -Infinity, -Infinity, 50, 51, 52, 53, 54, 55, -Infinity, 57, 58, 59, -Infinity, -Infinity, 50, 51, 52, -Infinity, 54, 55, 56, 57, 58, 59, -Infinity, -Infinity, -Infinity, 51, 52, 53, 54, 55, 56, 57, 58, -Infinity, -Infinity, -Infinity, 50, 51, 52, 53, 54, 55, -Infinity, 57, 58, 59, -Infinity, -Infinity, 50, 51, 52, -Infinity]"

[[test.results]]
value = "[NaN, 26f, 27f, 28f, 29f, 30f, 31f, 32f, 33f, NaN, NaN, NaN, 25f, 26f, 27f, 28f, 29f, 30f, NaN, 32f, 33f, 34f, NaN, NaN, 25f, 26f, 27f, NaN, 29f, 30f, 31f, 32f, 33f, 34f, NaN, NaN, NaN, 26f, 27f, 28f, 29f, 30f, 31f, 32f, 33f, NaN, NaN, NaN, 25f, 26f, 27f, 28f, 29f, 30f, NaN, 32f, 33f, 34f, NaN, NaN, 25f, 26f, 27f, NaN, 29f, 30f, 31f, 32f, 33f, 34f, NaN, NaN, NaN, 26f, 27f, 28f, 29f, 30f, 31f, 32f, 33f, NaN, NaN, NaN, 25f, 26f, 27f, 28f, 29f, 30f, NaN, 32f, 33f, 34f, NaN, NaN, 25f, 26f, 27f, NaN]"

[[test.results]]
value = "[[], [1, 11, 21, 31, 41, 51], [2, 12, 22, 32, 42, 52], [3, 13, 23, 33, 43, 53], [4, 14, 24, 34, 44, 54], [5, 15, 25, 35, 45, 55], [6, 16, 26, 36, 46, 56], [7, 17, 27, 37, 47, 57], [8, 18, 28, 38, 48, 58], [], [], [], [0, 10, 20, 30, 40, 50], [1, 11, 21, 31, 41, 51]]"

[[test.results]]
value = "[12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12, 12, 12, 12, 0, 0, 12, 12]"

[[test.results]]
value = "[55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56, 57, 58, 59, -Infinity, -Infinity, 55, 56]"

[[test.results]]
value = "[[{ count: 4, g: 0 }, { count: 4, g: 1 }, { count: 4, g: 2 }], [{ count: 4, g: 0 }, { count: 4, g: 1 }, { count: 4, g: 2 }], [{ count: 4, g: 0 }, { count: 4, g: 1 }, { count: 4, g: 2 }], [{ count: 4, g: 0 }, { count: 4, g: 1 }, { count: 4, g: 2 }], [{ count: 4, g: 0 }, { count: 4, g: 1 }, { count: 4, g: 2 }], [], [], [{ count: 4, g: 0 }, { count: 4, g: 1 }, { count: 4, g: 2 }]]"

[[test.results]]
value = "[src:1, src:2, src:3, src:4, src:5, src:6, src:7, src:8, src:12, src:13, src:14, src:15, src:16, src:17, src:19, src:20, src:21, src:24, src:25, src:26, src:28, src:29, src:30, src:31, src:32, src:33, src:37, src:38, src:39, src:40, src:41, src:42, src:43, src:44, src:48, src:49, src:50, src:51, src:52, src:53, src:55, src:56, src:57, src:60, src:61, src:62, src:64, src:65, src:66, src:67, src:68, src:69, src:73, src:74, src:75, src:76, src:77, src:78, src:79, src:80, src:84, src:85, src:86, src:87, src:88, src:89, src:91, src:92, src:93, src:96, src:97, src:98]"

[[test.results]]
value = "[src:0, src:1, src:2, src:3, src:4, src:7, src:8, src:9, src:10, src:11, src:14, src:15, src:16, src:17, src:18, src:21, src:22, src:23, src:24, src:25, src:28, src:29, src:30, src:31, src:32, src:35, src:36, src:37, src:38, src:39, src:42, src:43, src:44, src:45, src:46, src:49, src:50, src:51, src:52, src:53, src:56, src:57, src:58, src:59, src:60, src:63, src:64, src:65, src:66, src:67, src:70, src:71, src:72, src:73, src:74, src:77, src:78, src:79, src:80, src:81, src:84, src:85, src:86, src:87, src:88, src:91, src:92, src:93, src:94, src:95, src:98, src:99]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 156, 162, 168, 174, 180, 186, 192, 198, 0, 0, 0, 150, 156, 162, 168, 174, 180, 0, 192, 198, 204, 0, 0, 150, 156, 162, 0, 174, 180, 186, 192, 198, 204, 0, 0, 0, 156, 162, 168, 174, 180, 186, 192, 198, 0, 0, 0, 150, 156, 162, 168, 174, 180, 0, 192, 198, 204, 0, 0, 150, 156, 162, 0, 174, 180, 186, 192, 198, 204, 0, 0, 0, 156, 162, 168, 174, 180, 186, 192, 198, 0, 0, 0, 150, 156, 162, 168, 174, 180, 0, 192, 198, 204, 0, 0, 150, 156, 162, 0]"

[[test.results]]
value = "[0, 6, 6, 7, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 7, 6, 6, 0, 6, 6, 6, 0, 0, 6, 6, 6, 0, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 7, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 7, 6, 6, 0, 6, 6, 6, 0, 0, 6, 6, 6, 0, 6, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 7, 6, 6, 6, 6, 6, 0, 0, 0, 6, 6, 6, 7, 6, 6, 0, 6, 6, 6, 0, 0, 6, 6, 6, 0]"
*/

{
	DEFINE TABLE src SCHEMALESS;
	DEFINE TABLE item SCHEMALESS;
	DEFINE INDEX item_n ON item FIELDS n;
	DEFINE INDEX item_f ON item FIELDS f;
	FOR $i IN 0..100 {
		IF $i % 9 = 0 {
			CREATE type::record('src', $i) SET f = <float> ($i % 7) + 0.5 RETURN NONE;
		} ELSE {
			CREATE type::record('src', $i) SET n = $i % 12, f = <float> ($i % 7) + 0.5 RETURN NONE;
		};
	};
	FOR $i IN 0..60 {
		CREATE type::record('item', $i) SET n = $i % 10, f = <float> ($i % 5) + 0.5, v = $i, g = $i % 3 RETURN NONE;
	};
	RETURN 'OK';
};

-- Integer keys: some rows own no item, some have no key.
SELECT VALUE (SELECT count() FROM item WHERE n = $parent.n GROUP ALL)[0].count FROM src ORDER BY id;
SELECT VALUE (SELECT count() FROM item WHERE n = $parent.n GROUP ALL) FROM src ORDER BY id LIMIT 3;
SELECT VALUE (SELECT g, count() FROM item WHERE n = $parent.n GROUP BY g ORDER BY g) FROM src ORDER BY id LIMIT 14;
SELECT VALUE (SELECT math::max(v) AS m FROM item WHERE n = $parent.n GROUP ALL)[0].m FROM src ORDER BY id;
SELECT VALUE (SELECT math::mean(v) AS m FROM item WHERE n = $parent.n GROUP ALL)[0].m FROM src ORDER BY id;
SELECT VALUE (SELECT array::group(v) AS vs FROM item WHERE n = $parent.n GROUP ALL)[0].vs FROM src ORDER BY id LIMIT 14;

-- Float keys are hashed from the first evaluation.
SELECT VALUE (SELECT count() FROM item WHERE f = $parent.f GROUP ALL)[0].count FROM src ORDER BY id;
SELECT VALUE (SELECT math::max(v) AS m FROM item WHERE f = $parent.f GROUP ALL)[0].m FROM src ORDER BY id;
SELECT VALUE (SELECT g, count() FROM item WHERE f = $parent.f GROUP BY g ORDER BY g) FROM src ORDER BY id LIMIT 8;

-- In a `WHERE`.
SELECT VALUE id FROM src WHERE (SELECT count() FROM item WHERE n = $parent.n GROUP ALL)[0].count > 5 ORDER BY id;
SELECT VALUE id FROM src WHERE (SELECT count() FROM item WHERE f = $parent.f GROUP ALL)[0].count = 12 ORDER BY id;

-- A sum that meets a string, and the counts after the table changes.
CREATE item:text SET n = 3, f = 1.5, v = 'x', g = 0 RETURN NONE;
SELECT VALUE (SELECT math::sum(v) AS s FROM item WHERE n = $parent.n GROUP ALL)[0].s FROM src ORDER BY id;
SELECT VALUE (SELECT count() FROM item WHERE n = $parent.n GROUP ALL)[0].count FROM src ORDER BY id;
