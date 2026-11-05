/**
[env]
namespace = true
database = true

[test]
reason = "A range comparison over the columns of a packed row of a table with a computed field, which is never judged from the columns before the decode, keeps the rows the whole predicate keeps: one that keeps few rows, one that keeps most, ranges of float, decimal, optional and nullable columns, and ranges combined with an equality and negated."

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
value = "[x:38, x:39]"

[[test.results]]
value = "[x:38, x:39]"

[[test.results]]
value = "[x:0, x:1, x:40, x:41]"

[[test.results]]
value = "[x:0, x:1, x:40, x:41]"

[[test.results]]
value = "[x:3, x:4, x:5, x:6, x:7, x:8, x:9, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:18, x:19, x:20, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:34, x:35, x:36, x:37, x:38, x:39, x:43, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:51, x:52, x:53, x:54, x:55, x:56, x:57, x:58, x:59]"

[[test.results]]
value = "[x:38, x:39]"

[[test.results]]
value = "[x:38, x:39]"

[[test.results]]
value = "[x:38, x:39]"

[[test.results]]
value = "[x:38]"

[[test.results]]
value = "[x:0, x:38, x:39, x:40]"

[[test.results]]
value = "[x:0, x:1, x:2, x:40, x:41, x:42]"

[[test.results]]
value = "[x:39]"

[[test.results]]
value = "[x:35]"

[[test.results]]
value = "[x:21, x:22, x:44, x:45]"

[[test.results]]
value = "[x:0, x:1, x:23, x:24, x:46, x:47]"

[[test.results]]
value = "[x:30]"

[[test.results]]
value = "[x:0, x:1, x:31, x:32]"

[[test.results]]
value = "[x:25, x:38, x:51]"

[[test.results]]
value = "[x:0, x:4, x:8, x:12, x:13, x:16, x:20, x:24, x:26, x:28, x:32, x:36, x:39, x:40, x:44, x:48, x:52, x:56]"

[[test.results]]
value = "[x:21, x:32, x:43, x:54]"

[[test.results]]
value = "[x:0, x:5, x:10, x:11, x:15, x:20, x:22, x:25, x:30, x:33, x:35, x:40, x:44, x:45, x:50, x:55]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[x:0, x:1, x:2, x:3, x:4, x:5, x:6, x:7, x:8, x:9, x:10, x:11, x:12, x:13, x:14, x:15, x:16, x:17, x:18, x:19, x:20, x:21, x:22, x:23, x:24, x:25, x:26, x:27, x:28, x:29, x:30, x:31, x:32, x:33, x:34, x:35, x:36, x:37, x:38, x:39, x:40, x:41, x:42, x:43, x:44, x:45, x:46, x:47, x:48, x:49, x:50, x:51, x:52, x:53, x:54, x:55, x:56, x:57, x:58, x:59]"
*/

DEFINE TABLE x SCHEMAFULL;
DEFINE FIELD i ON x TYPE int;
DEFINE FIELD f ON x TYPE float;
DEFINE FIELD dec ON x TYPE decimal;
DEFINE FIELD o ON x TYPE option<int>;
DEFINE FIELD nl ON x TYPE int | null;
DEFINE FIELD c ON x TYPE int COMPUTED i + 1;
FOR $n IN 0..60 {
	CREATE type::record('x', $n) CONTENT {
		i: $n % 40,
		f: <float> ($n % 23) / 4,
		dec: <decimal> ($n % 31),
		o: IF $n % 4 = 0 { NONE } ELSE { $n % 13 },
		nl: IF $n % 5 = 0 { NULL } ELSE { $n % 11 }
	} RETURN NONE;
};
SELECT VALUE id FROM x WHERE i > 37;
SELECT VALUE id FROM x WHERE i >= 38;
SELECT VALUE id FROM x WHERE i < 2;
SELECT VALUE id FROM x WHERE i <= 1;
SELECT VALUE id FROM x WHERE i > 2;
SELECT VALUE id FROM x WHERE 37 < i;
SELECT VALUE id FROM x WHERE i > 37.5;
SELECT VALUE id FROM x WHERE i > 37dec;
SELECT VALUE id FROM x WHERE i > 37 AND i < 39;
SELECT VALUE id FROM x WHERE i > 37 OR i < 1;
SELECT VALUE id FROM x WHERE NOT (i > 2);
SELECT VALUE id FROM x WHERE i > 37 AND i != 38;
SELECT VALUE id FROM x WHERE i > 30 AND i = 35;
SELECT VALUE id FROM x WHERE f > 5;
SELECT VALUE id FROM x WHERE f <= 0.25;
SELECT VALUE id FROM x WHERE dec >= 30;
SELECT VALUE id FROM x WHERE dec < 1.5;
SELECT VALUE id FROM x WHERE o > 11;
SELECT VALUE id FROM x WHERE o < 1;
SELECT VALUE id FROM x WHERE nl > 9;
SELECT VALUE id FROM x WHERE nl < 1;
SELECT VALUE id FROM x WHERE absent > 1;
SELECT VALUE id FROM x WHERE absent < 1;
