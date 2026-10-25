/**
[env]
imports = ["upgrade/3.03.02_record_scalar_content_import.surql"]

[test]
upgrade = true
introduced = "3.3.2"
# The import writes a scalar record that this build refuses to write, so only a
# released 3.3 writer can produce the state. The gate also keeps the ordinary
# runner away, since it matches the import's version against its own.
importing-version = "<3.4.0-a"
# A released reader leaves the scalar as it is when a data clause sets a field,
# where this build turns it into an object, so only this build meets the
# expectations below.
version = ">=3.4.0-a"
reason = "A record whose stored content is a scalar, written by 3.3.2, must stay readable, updatable and deletable by this build, and the other rows of its table and the index must keep working. A data clause that edits the scalar record treats its content as the object of its id, so the record ends as an object."

[[test.results]]
value = "[5]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: t:1, x: 1 }]"

[[test.results]]
value = "[{ id: t:2, x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

*/
-- The scalar record, read by id.
SELECT * FROM t:2;
-- Both records, counted over the table scan.
SELECT count() FROM t GROUP ALL;
-- The ordinary record, found through the index.
SELECT * FROM t WHERE a = 1;
-- The data clause edits both records.
UPDATE t SET x = 1 RETURN NONE;
SELECT * FROM t:1;
SELECT * FROM t:2;
-- The table delete removes the scalar record too.
DELETE t RETURN NONE;
SELECT count() FROM t GROUP ALL;
