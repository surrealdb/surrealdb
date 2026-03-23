/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "DynamicScan (type::table) handles ORDER BY + LIMIT correctly for various orderings"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:2, item:3]"

[[test.results]]
value = "[item:10, item:9, item:8]"

[[test.results]]
value = "[item:1, item:2, item:3]"

[[test.results]]
value = "[item:10, item:9, item:8]"

[[test.results]]
value = "[item:1, item:2, item:3]"

[[test.results]]
value = "[item:3, item:4, item:5]"
*/

FOR $i IN 1..=10 {
    CREATE type::record('item', $i) SET ts = d'2025-01-01T00:00:00Z' + duration::from_days($i - 1);
};

LET $name = "item";

-- ORDER BY non-id ASC + LIMIT via type::table()
(SELECT * FROM type::table($name) ORDER BY ts ASC LIMIT 3).id;

-- ORDER BY non-id DESC + LIMIT via type::table()
(SELECT * FROM type::table($name) ORDER BY ts DESC LIMIT 3).id;

-- ORDER BY id ASC + LIMIT via type::table() (limit pushdown safe)
(SELECT id FROM type::table($name) ORDER BY id LIMIT 3).id;

-- ORDER BY id DESC + LIMIT via type::table() (limit pushdown safe)
(SELECT id FROM type::table($name) ORDER BY id DESC LIMIT 3).id;

-- No ORDER BY + LIMIT via type::table() (limit pushdown safe)
(SELECT id FROM type::table($name) LIMIT 3).id;

-- ORDER BY non-id + LIMIT + START via type::table()
(SELECT * FROM type::table($name) ORDER BY ts ASC LIMIT 3 START 2).id;
