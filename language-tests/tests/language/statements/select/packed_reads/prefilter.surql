/**
[env]
namespace = true
database = true

[test]
reason = "A filter over the columns of a packed row of a table with a computed field, which decodes its rows after judging them from their columns, keeps the rows the whole predicate keeps, over every kind of column, absent and NULL values and nested paths."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[x:3, x:43]"

[[test.results]]
value = "[x:0, x:1, x:2, x:4, x:5, x:6, x:7, x:8, x:9, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:18, x:19, x:20, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:34, x:35, x:36, x:37, x:38, x:39, x:40, x:41, x:42, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:51, x:52, x:53, x:54, x:55, x:56, x:57, x:58, x:59]"

[[test.results]]
value = "[x:0, x:1, x:2, x:3, x:40, x:41, x:42, x:43]"

[[test.results]]
value = "[x:38, x:39]"

[[test.results]]
value = "[x:1, x:5, x:9, x:41, x:45, x:49]"

[[test.results]]
value = "[x:0, x:2, x:3, x:4, x:6, x:7, x:8, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:18, x:19, x:20, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:34, x:35, x:36, x:37, x:38, x:39, x:40, x:42, x:43, x:44, x:46, x:47, x:48, x:50, x:51, x:52, x:53, x:54, x:55, x:56, x:57, x:58, x:59]"

[[test.results]]
value = "[x:3, x:20, x:37, x:54]"

[[test.results]]
value = "[x:0, x:1, x:2, x:4, x:5, x:6, x:7, x:8, x:9, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:18, x:19, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:34, x:35, x:36, x:38, x:39, x:40, x:41, x:42, x:43, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:51, x:52, x:53, x:55, x:56, x:57, x:58, x:59]"

[[test.results]]
value = "[x:1, x:2, x:18, x:19, x:35, x:36, x:52, x:53]"

[[test.results]]
value = "[x:1, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:18, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:35, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:52]"

[[test.results]]
value = "[x:21, x:22, x:44, x:45]"

[[test.results]]
value = "[x:6, x:29, x:52]"

[[test.results]]
value = "[x:0, x:3, x:6, x:9, x:12, x:15, x:18, x:21, x:24, x:27, x:30, x:33, x:36, x:39, x:42, x:45, x:48, x:51, x:54, x:57]"

[[test.results]]
value = "[x:1, x:2, x:4, x:5, x:7, x:8, x:10, x:11, x:13, x:14, x:16, x:17, x:19, x:20, x:22, x:23, x:25, x:26, x:28, x:29, x:31, x:32, x:34, x:35, x:37, x:38, x:40, x:41, x:43, x:44, x:46, x:47, x:49, x:50, x:52, x:53, x:55, x:56, x:58, x:59]"

[[test.results]]
value = "[x:0, x:3, x:6, x:9, x:12, x:15, x:18, x:21, x:24, x:27, x:30, x:33, x:36, x:39, x:42, x:45, x:48, x:51, x:54, x:57]"

[[test.results]]
value = "[x:3, x:34]"

[[test.results]]
value = "[x:30]"

[[test.results]]
value = "[x:46, x:47, x:48, x:49]"

[[test.results]]
value = "[x:4, x:54]"

[[test.results]]
value = "[x:7, x:8, x:16, x:17, x:25, x:26, x:34, x:35, x:43, x:44, x:52, x:53]"

[[test.results]]
value = "[x:0, x:9, x:18, x:27, x:36, x:45, x:54]"

[[test.results]]
value = "[x:0, x:4, x:8, x:12, x:16, x:20, x:24, x:28, x:32, x:36, x:40, x:44, x:48, x:52, x:56]"

[[test.results]]
value = "[x:1, x:2, x:3, x:5, x:6, x:7, x:9, x:10, x:11, x:13, x:14, x:15, x:17, x:18, x:19, x:21, x:22, x:23, x:25, x:26, x:27, x:29, x:30, x:31, x:33, x:34, x:35, x:37, x:38, x:39, x:41, x:42, x:43, x:45, x:46, x:47, x:49, x:50, x:51, x:53, x:54, x:55, x:57, x:58, x:59]"

[[test.results]]
value = "[x:5, x:18, x:31, x:57]"

[[test.results]]
value = "[x:0, x:1, x:2, x:3, x:4, x:6, x:7, x:8, x:9, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:19, x:20, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:32, x:33, x:34, x:35, x:36, x:37, x:38, x:39, x:40, x:41, x:42, x:43, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:51, x:52, x:53, x:54, x:55, x:56, x:58, x:59]"

[[test.results]]
value = "[x:0, x:5, x:10, x:15, x:20, x:25, x:30, x:35, x:40, x:45, x:50, x:55]"

[[test.results]]
value = "[x:1, x:2, x:3, x:4, x:6, x:7, x:8, x:9, x:11, x:12, x:13, x:14, x:16, x:17, x:18, x:19, x:21, x:22, x:23, x:24, x:26, x:27, x:28, x:29, x:31, x:32, x:33, x:34, x:36, x:37, x:38, x:39, x:41, x:42, x:43, x:44, x:46, x:47, x:48, x:49, x:51, x:52, x:53, x:54, x:56, x:57, x:58, x:59]"

[[test.results]]
value = "[x:9, x:21, x:31, x:32, x:42, x:43, x:53, x:54]"

[[test.results]]
value = "[x:0, x:5, x:10, x:15, x:20, x:25, x:30, x:35, x:40, x:45, x:50, x:55]"

[[test.results]]
value = "[x:1, x:3, x:5, x:7, x:9, x:11, x:13, x:15, x:17, x:19, x:21, x:23, x:25, x:27, x:29, x:31, x:33, x:35, x:37, x:39, x:41, x:43, x:45, x:47, x:49, x:51, x:53, x:55, x:57, x:59]"

[[test.results]]
value = "[x:0, x:2, x:4, x:6, x:8, x:10, x:12, x:14, x:16, x:18, x:20, x:22, x:24, x:26, x:28, x:30, x:32, x:34, x:36, x:38, x:40, x:42, x:44, x:46, x:48, x:50, x:52, x:54, x:56, x:58]"

[[test.results]]
value = "[x:0, x:2, x:4, x:6, x:8, x:10, x:12, x:14, x:16, x:18, x:20, x:22, x:24, x:26, x:28, x:30, x:32, x:34, x:36, x:38, x:40, x:42, x:44, x:46, x:48, x:50, x:52, x:54, x:56, x:58]"

[[test.results]]
value = "[x:3, x:12, x:21, x:30, x:39, x:48, x:57]"

[[test.results]]
value = "[x:0, x:6, x:12, x:18, x:24, x:30, x:36, x:42, x:48, x:54]"

[[test.results]]
value = "[x:1, x:4, x:7, x:10, x:13, x:16, x:19, x:22, x:25, x:28, x:31, x:34, x:37, x:40, x:43, x:46, x:49, x:52, x:55, x:58]"

[[test.results]]
value = "[x:7, x:42]"

[[test.results]]
value = "[x:1, x:2, x:3, x:4, x:5, x:6, x:8, x:9, x:10, x:11, x:12, x:13, x:15, x:16, x:17, x:18, x:19, x:20, x:22, x:23, x:24, x:25, x:26, x:27, x:29, x:30, x:31, x:32, x:33, x:34, x:36, x:37, x:38, x:39, x:40, x:41, x:43, x:44, x:45, x:46, x:47, x:48, x:50, x:51, x:52, x:53, x:54, x:55, x:57, x:58, x:59]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[x:0, x:1, x:2, x:3, x:4, x:5, x:6, x:7, x:8, x:9, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:18, x:19, x:20, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:34, x:35, x:36, x:37, x:38, x:39, x:40, x:41, x:42, x:43, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:51, x:52, x:53, x:54, x:55, x:56, x:57, x:58, x:59]"

[[test.results]]
value = "[x:20, x:22, x:23, x:25, x:26, x:28, x:29, x:31, x:32, x:34, x:35, x:37, x:38]"

[[test.results]]
value = "[x:1, x:3]"

[[test.results]]
value = "[x:0, x:1, x:2, x:3, x:4, x:5, x:18, x:31, x:40, x:41, x:42, x:43, x:44, x:57]"

[[test.results]]
value = "[x:6, x:18]"

*/

DEFINE TABLE x SCHEMAFULL;
DEFINE FIELD i ON x TYPE int;
DEFINE FIELD s ON x TYPE string;
DEFINE FIELD f ON x TYPE float;
DEFINE FIELD b ON x TYPE bool;
DEFINE FIELD dec ON x TYPE decimal;
DEFINE FIELD t ON x TYPE datetime;
DEFINE FIELD du ON x TYPE duration;
DEFINE FIELD o ON x TYPE option<int>;
DEFINE FIELD nl ON x TYPE int | null;
DEFINE FIELD tags ON x TYPE array<string>;
DEFINE FIELD obj ON x TYPE object;
DEFINE FIELD obj.x ON x TYPE int;
DEFINE FIELD obj.y ON x TYPE option<string>;
DEFINE FIELD flex ON x TYPE object FLEXIBLE;
DEFINE FIELD c ON x TYPE int COMPUTED i + 1;
FOR $n IN 0..60 {
	CREATE type::record('x', $n) CONTENT {
		i: $n % 40,
		s: 's' + <string> ($n % 17),
		f: <float> ($n % 23) / 4,
		b: $n % 3 = 0,
		dec: <decimal> ($n % 31),
		t: d'2020-01-01T00:00:00Z' + <duration> (<string> ($n % 50) + 'd'),
		du: <duration> (<string> ($n % 9) + 'h'),
		o: IF $n % 4 = 0 { NONE } ELSE { $n % 13 },
		nl: IF $n % 5 = 0 { NULL } ELSE { $n % 11 },
		tags: IF $n % 2 = 0 { ['a', 'b'] } ELSE { ['c'] },
		obj: { x: $n % 9, y: IF $n % 6 = 0 { NONE } ELSE { 'y' + <string> ($n % 3) } },
		flex: IF $n % 7 = 0 { { k: $n % 5 } } ELSE { {} }
	} RETURN NONE;
};
SELECT VALUE id FROM x WHERE i = 3;
SELECT VALUE id FROM x WHERE i != 3;
SELECT VALUE id FROM x WHERE i < 4;
SELECT VALUE id FROM x WHERE i >= 38;
SELECT VALUE id FROM x WHERE i IN [1, 5, 9];
SELECT VALUE id FROM x WHERE i NOT IN [1, 5, 9];
SELECT VALUE id FROM x WHERE s = 's3';
SELECT VALUE id FROM x WHERE s != 's3';
SELECT VALUE id FROM x WHERE s IN ['s1', 's2'];
SELECT VALUE id FROM x WHERE s CONTAINS '1';
SELECT VALUE id FROM x WHERE f > 5.0;
SELECT VALUE id FROM x WHERE f = 1.5f;
SELECT VALUE id FROM x WHERE b = true;
SELECT VALUE id FROM x WHERE b = false;
SELECT VALUE id FROM x WHERE b;
SELECT VALUE id FROM x WHERE dec = 3dec;
SELECT VALUE id FROM x WHERE dec > 29;
SELECT VALUE id FROM x WHERE t > d'2020-02-15T00:00:00Z';
SELECT VALUE id FROM x WHERE t = d'2020-01-05T00:00:00Z';
SELECT VALUE id FROM x WHERE du >= 7h;
SELECT VALUE id FROM x WHERE du = 0s;
SELECT VALUE id FROM x WHERE o IS NONE;
SELECT VALUE id FROM x WHERE o IS NOT NONE;
SELECT VALUE id FROM x WHERE o = 5;
SELECT VALUE id FROM x WHERE o != 5;
SELECT VALUE id FROM x WHERE nl IS NULL;
SELECT VALUE id FROM x WHERE nl IS NOT NULL;
SELECT VALUE id FROM x WHERE nl > 8;
SELECT VALUE id FROM x WHERE nl = NULL;
SELECT VALUE id FROM x WHERE tags CONTAINS 'c';
SELECT VALUE id FROM x WHERE tags CONTAINS 'a';
SELECT VALUE id FROM x WHERE tags CONTAINSALL ['a', 'b'];
SELECT VALUE id FROM x WHERE obj.x = 3;
SELECT VALUE id FROM x WHERE obj.y IS NONE;
SELECT VALUE id FROM x WHERE obj.y = 'y1';
SELECT VALUE id FROM x WHERE flex.k = 2;
SELECT VALUE id FROM x WHERE flex.k IS NONE;
SELECT VALUE id FROM x WHERE flex.zz = 1;
SELECT VALUE id FROM x WHERE absent = 1;
SELECT VALUE id FROM x WHERE absent IS NONE;
SELECT VALUE id FROM x WHERE NOT (i < 20 OR b);
SELECT VALUE id FROM x WHERE i < 5 AND (s = 's1' OR s = 's3');
SELECT VALUE id FROM x WHERE i < 5 OR o = 5;
SELECT VALUE id FROM x WHERE o > 4 AND nl IS NOT NULL AND obj.y IS NONE;
