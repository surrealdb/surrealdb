/**
[env]
namespace = true
database = true

[test]
reason = """
	A bound that is a bare parameter is read once when the scan opens. A parameter holding a record
	id of the table bounds the scan, and one holding anything else bounds nothing, so the
	predicate answers as it does over the whole table. Paging a table by the last id read, the
	shape this serves, returns each page in order with no row repeated or lost.
"""

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, n: 2 }]"

[[test.results]]
value = "[{ id: t:5, n: 5 }]"

[[test.results]]
value = "[{ id: t:a, n: 'a' }]"

[[test.results]]
value = "[{ id: t:[1, 2], n: 'array' }]"

[[test.results]]
value = "[{ id: u:1, n: 'other' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "[t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "[t:[1, 2], t:a, t:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:5]"

[[test.results]]
value = "[t:2, t:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "[t:1, t:2, t:5, t:a, t:[1, 2]]"

[[test.results]]
value = "[]"

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
value = "[p:1, p:50, p:51, p:100, p:101, p:150, p:151, p:200, []]"

[[test.results]]
value = "true"

[[test.results]]
value = "[p:200, p:199, p:198]"

[[test.results]]
value = "[p:50, p:49, p:48]"
*/

CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
CREATE t:5 SET n = 5;
CREATE t:a SET n = 'a';
CREATE t:[1, 2] SET n = 'array';
CREATE u:1 SET n = 'other';
LET $c = t:2;
SELECT VALUE id FROM t WHERE id > $c;
SELECT VALUE id FROM t WHERE id >= $c;
SELECT VALUE id FROM t WHERE id < $c;
SELECT VALUE id FROM t WHERE $c <= id;
SELECT VALUE id FROM t WHERE id > $c ORDER BY id DESC;
LET $hi = t:a;
SELECT VALUE id FROM t WHERE id > $c AND id < $hi;
SELECT VALUE id FROM t WHERE id >= $c AND id <= t:5 AND id < $hi;
LET $other = u:1;
SELECT VALUE id FROM t WHERE id > $other;
SELECT VALUE id FROM t WHERE id < $other;
LET $number = 5;
SELECT VALUE id FROM t WHERE id > $number;
SELECT VALUE id FROM t WHERE id < $number;
LET $none = NONE;
SELECT VALUE id FROM t WHERE id > $none;
LET $null = NULL;
SELECT VALUE id FROM t WHERE id < $null;
LET $array = t:[1, 2];
SELECT VALUE id FROM t WHERE id > $array;
LET $text = 't:2';
SELECT VALUE id FROM t WHERE id > $text;
SELECT VALUE id FROM t WHERE id > $undefined;
SELECT VALUE id FROM t WHERE id < $undefined;
-- Paging by the last id read.
FOR $i IN 1..=200 { CREATE type::record('p', $i) SET n = $i; };
LET $page1 = SELECT VALUE id FROM p ORDER BY id LIMIT 50;
LET $page2 = SELECT VALUE id FROM p WHERE id > $page1[49] ORDER BY id LIMIT 50;
LET $page3 = SELECT VALUE id FROM p WHERE id > $page2[49] ORDER BY id LIMIT 50;
LET $page4 = SELECT VALUE id FROM p WHERE id > $page3[49] ORDER BY id LIMIT 50;
LET $page5 = SELECT VALUE id FROM p WHERE id > $page4[49] ORDER BY id LIMIT 50;
RETURN [$page1[0], $page1[49], $page2[0], $page2[49], $page3[0], $page3[49], $page4[0], $page4[49], $page5];
RETURN array::len(array::concat($page1, $page2, $page3, $page4)) = 200;
SELECT VALUE id FROM p WHERE id > $page3[49] ORDER BY id DESC LIMIT 3;
SELECT VALUE id FROM p WHERE id < $page2[0] ORDER BY id DESC LIMIT 3;
