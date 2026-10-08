/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A subquery's rows name the records a write is applied to; they are not
	those records. A row that is a record id, or an object carrying one, names
	the record, which the write reads from storage and writes whole: a field
	the subquery did not select keeps its stored value, under UPDATE, UPSERT
	and DELETE alike, and a DELETE returns the record as it was stored. Each
	record is judged by its own table's schema, whichever table the subquery
	read. A row naming no record is refused, as the same value written as a
	target is.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ balance: 100, id: acct:1, name: 'x', note: 'keep' }]"

[[test.results]]
value = "[{ id: person:alice, name: 'alice' }]"

[[test.results]]
value = "[{ author: person:alice, id: post:1 }]"

[[test.results]]
value = "[{ balance: 100, id: acct:1, name: 'x', note: 'keep', touched: true }]"

[[test.results]]
value = "[{ balance: 100, id: acct:1, name: 'x', note: 'keep', touched: true }]"

[[test.results]]
value = "[{ balance: 100, id: acct:1, name: 'x', note: 'keep', touched: true, upserted: true }]"

[[test.results]]
value = "[{ balance: 100, id: acct:1, name: 'x', note: 'keep', touched: true, upserted: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 41, id: person:alice, name: 'alice' }]"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:alice`: Expected `none | int` but found `'old'`"

[[test.results]]
value = "[{ age: 41, id: person:alice, name: 'alice' }]"

[[test.results]]
error = "Cannot execute UPDATE statement using value: { name: 'alice' }"

[[test.results]]
error = "Cannot execute UPDATE statement using value: 'alice'"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE option<int>;
CREATE acct:1 SET name = 'x', balance = 100, note = 'keep';
CREATE person:alice SET name = 'alice';
CREATE post:1 SET author = person:alice;

-- A projection names the record, and the fields it left out keep their values.
UPDATE (SELECT id, name FROM acct WHERE id = acct:1) SET touched = true;
SELECT * FROM acct;
UPSERT (SELECT id FROM acct WHERE id = acct:1) SET upserted = true;
DELETE (SELECT id, name FROM acct) RETURN BEFORE;
SELECT * FROM acct;

-- Rows of another table name records of their own table, judged by its schema.
UPDATE (SELECT VALUE author.* FROM post WHERE id = post:1) SET age = 41;
UPDATE (SELECT VALUE author.* FROM post WHERE id = post:1) SET age = 'old';
SELECT * FROM person;

-- A row naming no record is refused.
UPDATE (SELECT name FROM person) SET age = 1;
UPDATE (SELECT VALUE name FROM person) SET age = 1;
