/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A component of a named record id is a field the table derives from the id,
	with no definition of its own, and an index may hold it as a column: by its
	name, or as `id[i]`, on a SCHEMAFULL table and on a schemaless one. A path
	beneath a component is refused as a computed field's is, and a name the id
	does not give is refused on a SCHEMAFULL table as any undefined field is. An
	index on a component is maintained like any other: a UNIQUE one refuses a
	second row with the same value, and a query over it returns what a scan of
	the table returns. The id cannot be removed while an index reads one of its
	components.
"""

# SCHEMAFULL table, id with three components and a stored field.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A component by its name, in a compound index, as a UNIQUE index, as `id[1]`.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A stored field next to them.
[[test.results]]
value = "NONE"

# A path beneath a component, and a name that is no field.
[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_beneath' - Field: 'city'"

[[test.results]]
error = "The field 'nothing' does not exist"

# The definitions are kept as written.
[[test.results]]
match = '''$result.indexes.by_time = "DEFINE INDEX by_time ON sf FIELDS time" AND $result.indexes.by_city_time = "DEFINE INDEX by_city_time ON sf FIELDS city, time" AND $result.indexes.by_seq = "DEFINE INDEX by_seq ON sf FIELDS seq UNIQUE" AND $result.indexes.by_second = /^DEFINE INDEX by_second ON sf FIELDS id\[1\]$/ AND $result.indexes.by_v = "DEFINE INDEX by_v ON sf FIELDS v" AND object::len($result.indexes) = 5'''
error = false

# Rows written with the indexes in place.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# A second row with the UNIQUE component's value is refused, and is not kept.
[[test.results]]
match = '''$error = /^Database index `by_seq` already contains 1, with record `sf:/'''
error = true

[[test.results]]
value = "[10, 20, 30]"

# Queries over the indexed components return what a scan returns.
[[test.results]]
value = "[20]"

[[test.results]]
value = "[30]"

[[test.results]]
value = "[20]"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "true"

# The id cannot be removed while an index reads its components.
[[test.results]]
match = '''$error = /^Cannot change the record id of `sf`: the index `by_[a-z_]+` reads its component `(city|time|seq)`$/'''
error = true

# Schemaless table: the same column refusals and acceptances.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_beneath' - Field: 'site'"

# A name no component gives is an ordinary field of a schemaless table.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[30]"

[[test.results]]
value = "[50, 60]"

[[test.results]]
value = "[20, 30, 40]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_n") AND !string::contains($result, "TableScan")'''
error = false
*/

DEFINE TABLE sf SCHEMAFULL;
DEFINE FIELD id ON sf TYPE [city: string, time: datetime, seq: int];
DEFINE FIELD v ON sf TYPE int;

DEFINE INDEX by_time ON sf FIELDS time;
DEFINE INDEX by_city_time ON sf FIELDS city, time;
DEFINE INDEX by_seq ON sf FIELDS seq UNIQUE;
DEFINE INDEX by_second ON sf FIELDS id[1];
DEFINE INDEX by_v ON sf FIELDS v;

DEFINE INDEX by_beneath ON sf FIELDS city.x;
DEFINE INDEX by_nothing ON sf FIELDS nothing;

INFO FOR TABLE sf;

CREATE sf SET city = 'a', time = d'2024-01-01T00:00:00Z', seq = 1, v = 10 RETURN NONE;
CREATE sf SET city = 'a', time = d'2024-01-01T00:00:00.500Z', seq = 2, v = 20 RETURN NONE;
CREATE sf SET city = 'b', time = d'2024-01-01T00:00:01Z', seq = 3, v = 30 RETURN NONE;
CREATE sf SET city = 'b', time = d'2024-01-01T00:00:02Z', seq = 1, v = 40 RETURN NONE;
SELECT VALUE v FROM sf ORDER BY v;

SELECT VALUE v FROM sf WHERE seq = 2;
SELECT VALUE v FROM sf WHERE time = d'2024-01-01T00:00:01Z';
SELECT VALUE v FROM sf WHERE city = 'a' AND time >= d'2024-01-01T00:00:00.500Z' ORDER BY v;
SELECT VALUE v FROM sf WHERE seq < 3 ORDER BY v;
(SELECT VALUE v FROM sf WHERE seq < 3 ORDER BY v) = (SELECT VALUE v FROM sf WITH NOINDEX WHERE seq < 3 ORDER BY v);

REMOVE FIELD id ON sf;

DEFINE TABLE sl SCHEMALESS;
DEFINE FIELD id ON sl TYPE [site: string, n: int];
DEFINE INDEX by_n ON sl FIELDS n;
DEFINE INDEX by_beneath ON sl FIELDS site.x;
DEFINE INDEX loose ON sl FIELDS other;
FOR $i IN 1..7 {
	CREATE sl SET site = ['a', 'b'][$i % 2], n = $i, v = $i * 10 RETURN NONE;
};
SELECT VALUE v FROM sl WHERE n = 3;
SELECT VALUE v FROM sl WHERE n > 4 ORDER BY v;
SELECT VALUE v FROM sl WHERE n >= 2 AND n < 5 ORDER BY v;
(SELECT VALUE v FROM sl WHERE n >= 2 AND n < 5 ORDER BY v) = (SELECT VALUE v FROM sl WITH NOINDEX WHERE n >= 2 AND n < 5 ORDER BY v);
SELECT count() FROM sl WHERE n > 4 GROUP ALL;
EXPLAIN SELECT VALUE v FROM sl WHERE n = 3;
