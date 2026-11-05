/**
[env]
imports = ["language/write/subquery_rows_keep_hidden_fields_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[test]
reason = """
	A record user's subquery returns rows cut by the caller's field
	permissions, and a write through it stores the record, not that cut: a
	field the caller may not select keeps its stored value, and the record's
	events see it before and after the write. Each record is judged by its own
	table's permissions and schema, whichever table the subquery read: the
	caller updates the records of another table that it may update, and none of
	that table's records that it may not, however the subquery reached them.
"""

[[test.results]]
value = "[{ age: 41 }]"

[[test.results]]
value = "[{ after: 'hidden', before: 'hidden' }]"

[[test.results]]
value = "[{ age: 42 }]"

[[test.results]]
value = "[{ after: 'hidden', before: 'hidden' }, { after: 'hidden', before: 'hidden' }]"

[[test.results]]
value = "[{ age: 99, id: person:bob }]"

[[test.results]]
value = "[{ age: NONE, id: person:alice }, { age: 99, id: person:bob }]"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:bob`: Expected `none | int` but found `'old'`"

[[test.results]]
value = "[{ age: 7, id: person:bob }]"

[[test.results]]
value = "[{ age: NONE, id: person:alice }, { age: 7, id: person:bob }]"

*/

-- `SELECT *` cannot return `notes` to this caller, and the write keeps it.
UPDATE (SELECT * FROM member) SET age = 41 RETURN age;
SELECT before, after FROM audit;
-- Nor can a projection, which also leaves out `owner`.
UPDATE (SELECT id, name FROM member) SET age = 42 RETURN age;
SELECT before, after FROM audit;

-- The rows of `post` name `person` records, each judged by `person`'s own
-- permission and schema.
UPDATE (SELECT VALUE author.* FROM post) SET age = 99 RETURN id, age;
SELECT id, age FROM person ORDER BY id;
UPDATE (SELECT VALUE author.* FROM post WHERE author = person:bob) SET age = 'old';
UPDATE (SELECT VALUE author FROM post) SET age = 7 RETURN id, age;
SELECT id, age FROM person ORDER BY id;
