/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:q3v4t5sdkp7ipv2wobbe, one: 'something', three: 'something', two: 'something' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: person:q3v4t5sdkp7ipv2wobbe, one: 'something', three: 'something', two: 'something' }]"
skip-record-id-key = true

[[test.results]]
match = """
	$error.starts_with("Database index `testing` already contains ['something', 'something', 'something'], with record `person:")
"""

[[test.results]]
value = "[{ count: 1 }]"

*/
-- This will define a unique index on the table
DEFINE INDEX OVERWRITE testing ON person FIELDS one, two, three UNIQUE;
-- This will create a record, and populate the unique index with this record id
UPSERT person SET one = 'something', two = 'something', three = 'something';
-- This will update the record, returning the same record id created in the statement above
UPSERT person SET one = 'something', two = 'something', three = 'something';
-- This will throw an error, because the unique index already has a record with a different record id
UPSERT person:test SET one = 'something', two = 'something', three = 'something';
-- Select all created records
SELECT count() FROM person GROUP ALL;
