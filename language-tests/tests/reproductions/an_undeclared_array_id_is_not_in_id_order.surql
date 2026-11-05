/**
[env]
namespace = true
database = true

[test]
reason = """
A record is stored under the storekey encoding of its id, and a table scan
reads in that byte order. `ORDER BY id` over a table scan is therefore
answered by the scan alone, with no sort, on the claim that key order is `id`
order.

The claim holds for a number, a string or a uuid id, whose encodings order as
their values do. It does not hold for every id an undeclared table can hold,
because an array or object id encodes each element as a `Value`, and two
element kinds are encoded in an order that is not their value order:

- a datetime is its RFC 3339 text, in which the sub-second form sorts before the whole second it
  belongs to (`.500Z` compares `'.'` against `'Z'`), so two readings half a second apart come back
  reversed;
- a number is an order-preserving decimal buffer followed by the tag of its number kind, so two
  elements whose values compare equal (`5` and `5dec`) order by kind, and the elements after them
  never decide.

Neither is reachable by the planner on a table that declares nothing: there is
no key structure to consult, so the scan is taken to deliver `id` order and the
sort is dropped. A `DESC` read is the same claim backwards and is wrong the
same way. Where the key is declared (`DEFINE FIELD id ON tick TYPE [at:
datetime]`) the property is known -- a datetime component orders coarsely, by
the whole second -- and the sort is kept, which is the control this file
carries, as is the same order read through a subquery, where the sort is not
over a scan.

Fixing it is a design question rather than a patch. A schemaless table gives no
static knowledge of what its ids hold, so refusing to deliver `id` order unless
a key structure is declared would keep a sort on essentially every `ORDER BY
id` -- which costs a streaming read of the first page its whole table -- and
correcting the encoding would change a persisted key format without rewriting
the keys already written. This file pins the current answers.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[d'2024-01-01T00:00:00.500Z'], [d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[[d'2024-01-01T00:00:00Z'], [d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: reading, direction: Forward]
''''

[[test.results]]
value = "[[d'2024-01-01T00:00:00Z'], [d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00.500Z' }, { at: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[5, 2], [5dec, 1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[-5, 1, 'a', u'018f3f1a-0000-7000-8000-000000000000']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[d'2024-01-01T00:00:00Z'], [d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        TableScan [ctx: Db] [table: tick, direction: Forward]
''''
*/

-- An undeclared table whose ids hold a datetime: the scan is taken to deliver
-- `id` order, and the rows come back in key order, which is not that order.
DEFINE TABLE reading SCHEMALESS;
CREATE reading:[d'2024-01-01T00:00:00.500Z'] RETURN NONE;
CREATE reading:[d'2024-01-01T00:00:00Z'] RETURN NONE;
SELECT VALUE id.id() FROM reading ORDER BY id;
SELECT VALUE id.id() FROM reading ORDER BY id DESC;
EXPLAIN SELECT VALUE id FROM reading ORDER BY id;
-- The order those ids compare in, which the two reads above owe: the sort is
-- over a subquery's rows rather than over a scan, so it is kept.
SELECT VALUE id.id() FROM (SELECT id FROM reading) ORDER BY id;

-- An object id holds its values the same way, and is read the same way.
DEFINE TABLE sample SCHEMALESS;
CREATE sample:{ at: d'2024-01-01T00:00:00.500Z' } RETURN NONE;
CREATE sample:{ at: d'2024-01-01T00:00:00Z' } RETURN NONE;
SELECT VALUE id.id() FROM sample ORDER BY id;

-- `5` and `5dec` are one value and two keys, so the element after them never
-- decides: `[5dec, 1]` is owed first and is stored second.
DEFINE TABLE amount SCHEMALESS;
CREATE amount:[5, 2] RETURN NONE;
CREATE amount:[5dec, 1] RETURN NONE;
SELECT VALUE id.id() FROM amount ORDER BY id;

-- A number, a string and a uuid id order as they are stored.
DEFINE TABLE plain SCHEMALESS;
CREATE plain:-5, plain:1, plain:⟨a⟩, plain:u'018f3f1a-0000-7000-8000-000000000000' RETURN NONE;
SELECT VALUE id.id() FROM plain ORDER BY id;

-- The control: the same datetime key declared. The component is known to
-- order by the whole second, so the sort is kept and the rows are right.
DEFINE TABLE tick SCHEMALESS;
DEFINE FIELD id ON tick TYPE [at: datetime];
CREATE tick:[d'2024-01-01T00:00:00.500Z'] RETURN NONE;
CREATE tick:[d'2024-01-01T00:00:00Z'] RETURN NONE;
SELECT VALUE id.id() FROM tick ORDER BY id;
EXPLAIN SELECT VALUE id FROM tick ORDER BY id;
