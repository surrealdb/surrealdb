/**
[env]
namespace = true
database = true

[test]
reason = """
	A component may be named with a keyword. Such a name is a plain identifier,
	so the component is a field under exactly that name everywhere the record's
	fields are read: the RETURN image, a VALUE clause and an ASSERT clause of
	another field, an event's WHEN (bare and through $after) and its THEN, a
	foreign view, an index, a key range, and UPDATE, UPSERT and MERGE, which may
	restate a component and may not change it.
"""

# Definitions.
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

# Writes.
[[test.results]]
value = "[{ id: kw:['London', 5, d'2024-01-01T00:00:00Z'], label: 'London-5', select: 'London', temp: 1, time: d'2024-01-01T00:00:00Z', value: 5 }]"

[[test.results]]
error = "Found 1 for field `temp`, with record `kw:['London', 0, d'2024-01-01T00:00:00Z']`, but field must conform to: `value` > 0"

[[test.results]]
value = "[{ id: kw:['London', 5, d'2024-01-01T00:00:00Z'], label: 'London-5', select: 'London', temp: 2, time: d'2024-01-01T00:00:00Z', value: 5 }]"

[[test.results]]
value = "[{ id: kw:['Paris', 7, d'2024-01-02T00:00:00Z'], label: 'Paris-7', select: 'Paris', temp: 3, time: d'2024-01-02T00:00:00Z', value: 7 }]"

[[test.results]]
value = "[{ id: kw:['Paris', 7, d'2024-01-02T00:00:00Z'], label: 'Paris-7', select: 'Paris', temp: 4, time: d'2024-01-02T00:00:00Z', value: 7 }]"

[[test.results]]
error = "Found 8 for the `value` field, but it is part of the record id of `kw` and does not match it"

# Reads.
[[test.results]]
value = "[{ id: kw:['London', 5, d'2024-01-01T00:00:00Z'], label: 'London-5', select: 'London', temp: 2, time: d'2024-01-01T00:00:00Z', value: 5 }, { id: kw:['Paris', 7, d'2024-01-02T00:00:00Z'], label: 'Paris-7', select: 'Paris', temp: 4, time: d'2024-01-02T00:00:00Z', value: 7 }]"

[[test.results]]
value = "[{ sel: 'London', t: d'2024-01-01T00:00:00Z', v: 5 }, { sel: 'London', t: d'2024-01-01T00:00:00Z', v: 5 }]"

[[test.results]]
value = "[{ id: kv:['London'], n: 1, select: 'London', top: 5 }, { id: kv:['Paris'], n: 1, select: 'Paris', top: 7 }]"

[[test.results]]
value = "[{ id: kw:['Paris', 7, d'2024-01-02T00:00:00Z'], value: 7 }]"

[[test.results]]
value = "[{ id: kw:['London', 5, d'2024-01-01T00:00:00Z'] }]"

[[test.results]]
value = "[{ select: 'London', time: d'2024-01-01T00:00:00Z', value: 5 }]"

# Literal keywords as names.
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
value = "[{ NULL: true, from: 'f', id: k2:['a', 1, 'f', 'w', true], label: 'a-1fwtrue', none: 1, true: 'a', where: 'w' }]"

[[test.results]]
value = "[{ NULL: false, from: 'f', id: k2:['a', 2, 'f', 'w', false], label: 'a-2fwfalse', none: 2, true: 'a', where: 'w' }]"

[[test.results]]
value = "[{ f: 'f', n: 1, nl: true, t: 'a' }, { f: 'f', n: 2, nl: false, t: 'a' }]"

[[test.results]]
value = "[{ id: kv2:['a'], n: 2, true: 'a' }]"
*/

DEFINE TABLE kw SCHEMALESS;
DEFINE FIELD id ON kw TYPE [select: string, value: int, time: datetime];
DEFINE FIELD label ON kw VALUE string::concat(`select`, '-', <string> `value`);
DEFINE FIELD temp ON kw ASSERT `value` > 0;
DEFINE EVENT ev ON kw WHEN $after.`select` = 'London' AND `time` > d'2000-01-01T00:00:00Z' THEN {
	CREATE log SET sel = $after.`select`, v = $after.`value`, t = $after.`time`
};
DEFINE TABLE kv AS SELECT `select`, count() AS n, math::max(`value`) AS top FROM kw GROUP BY `select`;
DEFINE INDEX by_value ON kw FIELDS `value`;

CREATE kw SET `select` = 'London', `value` = 5, `time` = d'2024-01-01T00:00:00Z', temp = 1 RETURN AFTER;
-- The ASSERT of `temp` reads the component `value`, which is 0 here.
CREATE kw SET `select` = 'London', `value` = 0, `time` = d'2024-01-01T00:00:00Z', temp = 1 RETURN AFTER;
UPDATE kw:['London', 5, d'2024-01-01T00:00:00Z'] SET temp = 2 RETURN AFTER;
UPSERT kw:['Paris', 7, d'2024-01-02T00:00:00Z'] SET temp = 3 RETURN AFTER;
UPDATE kw:['Paris', 7, d'2024-01-02T00:00:00Z'] MERGE { temp: 4, `value`: 7 } RETURN AFTER;
UPDATE kw:['Paris', 7, d'2024-01-02T00:00:00Z'] SET `value` = 8 RETURN AFTER;

SELECT * FROM kw;
SELECT sel, v, t FROM log ORDER BY sel;
SELECT * FROM kv;
SELECT id, `value` FROM kw WITH INDEX by_value WHERE `value` = 7;
SELECT id FROM kw WHERE `select` = 'London' AND `value` >= 5;
SELECT `select`, `value`, `time` FROM kw WHERE `value` = 5;

DEFINE TABLE k2 SCHEMALESS;
DEFINE FIELD id ON k2 TYPE [true: string, none: int, from: string, where: string, NULL: bool];
DEFINE FIELD label ON k2 VALUE string::concat(`true`, '-', <string> `none`, `from`, `where`, <string> `NULL`);
DEFINE EVENT ev ON k2 WHEN `true` = 'a' AND $after.`none` > 0 THEN {
	CREATE log2 SET t = $after.`true`, n = $after.`none`, f = $after.`from`, nl = $after.`NULL`
};
DEFINE TABLE kv2 AS SELECT `true`, count() AS n FROM k2 GROUP BY `true`;
CREATE k2 SET `true` = 'a', `none` = 1, `from` = 'f', `where` = 'w', `NULL` = true RETURN AFTER;
UPSERT k2 SET `true` = 'a', `none` = 2, `from` = 'f', `where` = 'w', `NULL` = false RETURN AFTER;
SELECT t, n, f, nl FROM log2 ORDER BY n;
SELECT * FROM kv2;
