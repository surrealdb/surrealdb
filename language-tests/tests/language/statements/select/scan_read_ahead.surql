/**
[test]
reason = """
	A scan of a table or a record range that holds more than one chunk of
	records answers the same whether the chunk after the one it runs is read
	while it runs or after: a plain scan, a filter, a sort, a scan backward, a
	range of record ids, a scan that fails part-way through, and a filter in a
	transaction that has written the records it reads, before and after it
	rewrites them. A group, a count and a sort on a computed key read decoded
	rows and are held to the same answers.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "1500"

[[test.results]]
value = "500"

[[test.results]]
value = "[0, 1, 2]"

[[test.results]]
value = "1499"

[[test.results]]
value = "[1499, 1498, 1497]"

[[test.results]]
value = "0"

[[test.results]]
value = "1299"

[[test.results]]
value = "[{ c: 215, g: 0, s: 161035 }, { c: 215, g: 1, s: 161250 }, { c: 214, g: 2, s: 159965 }, { c: 214, g: 3, s: 160179 }, { c: 214, g: 4, s: 160393 }, { c: 214, g: 5, s: 160607 }, { c: 214, g: 6, s: 160821 }]"

[[test.results]]
value = "[{ c: 1500, s: 1124250 }]"

[[test.results]]
value = "[1499, 1498, 1497]"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '1'"

[[test.results]]
value = "[1497, 1498, 1499]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1200, s: 719400 }]"

[[test.results]]
value = "[0, 300, 600, 900]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 1200, s: 720600 }]"

[[test.results]]
value = "[300, 600, 900, 1200]"

[[test.results]]
value = "[1200, 1199]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1200, s: 720600 }]"

*/

{
	FOR $i IN 0..1500 {
		CREATE type::record('rec', $i) SET n = $i, g = $i % 7, name = <string>$i RETURN NONE;
	};
	RETURN 'OK';
};

RETURN array::len(SELECT * FROM rec);
RETURN array::len(SELECT * FROM rec WHERE n % 3 = 0);
RETURN array::slice((SELECT VALUE n FROM rec), 0, 3);
RETURN array::last(SELECT VALUE n FROM rec);
RETURN array::slice((SELECT VALUE n FROM rec ORDER BY id DESC), 0, 3);
RETURN array::last(SELECT VALUE n FROM rec ORDER BY id DESC);
RETURN array::len(SELECT * FROM rec WHERE id > rec:100 AND id < rec:1400);
SELECT g, count() AS c, math::sum(n) AS s FROM rec GROUP BY g ORDER BY g;
SELECT count() AS c, math::sum(n) AS s FROM rec GROUP ALL;
RETURN array::slice((SELECT VALUE n FROM rec ORDER BY n DESC), 0, 3);
SELECT * FROM rec WHERE 9223372036854775807 + (n - 1199) > 0;
SELECT VALUE n FROM rec WHERE n > 1496;

BEGIN;
FOR $i IN 0..1200 {
	CREATE type::record('written', $i) SET n = $i RETURN NONE;
};
SELECT count() AS c, math::sum(n) AS s FROM written GROUP ALL;
SELECT VALUE n FROM written WHERE n % 300 = 0;
UPDATE written SET n += 1 RETURN NONE;
SELECT count() AS c, math::sum(n) AS s FROM written GROUP ALL;
SELECT VALUE n FROM written WHERE n % 300 = 0;
SELECT VALUE n FROM written WHERE n > 1198 ORDER BY id DESC;
COMMIT;

SELECT count() AS c, math::sum(n) AS s FROM written GROUP ALL;
