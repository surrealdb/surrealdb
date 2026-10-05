/**
[env]
imports = ["reproductions/7539_restricted_array_elements_stay_out_of_index_answers_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:1" }

[test]
issue = 7539
reason = """
A SELECT-denied `acl.*` hides an array's elements from a record user, so to
that user `acl` reads as `[]` and `acl CONTAINS 'secret'` holds for no row.
An index on the bare column `acl` stores those same elements as its entries,
as an index on `acl.*` does, so neither index may answer the predicate: an
index-only count would report how many rows hold the hidden value. A read or
an index column touches a restricted field at, beneath or above it, and
every query here agrees with its WITH NOINDEX form.
"""

[[test.results]]
value = "[{ acl: [], id: d:1 }, { acl: [], id: d:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

SELECT * FROM d ORDER BY id;
SELECT VALUE id FROM d WHERE acl CONTAINS 'secret';
SELECT VALUE id FROM d WITH NOINDEX WHERE acl CONTAINS 'secret';
SELECT count() FROM d WHERE acl CONTAINS 'secret' GROUP ALL;
SELECT count() FROM d WITH NOINDEX WHERE acl CONTAINS 'secret' GROUP ALL;
SELECT VALUE id FROM e WHERE acl CONTAINS 'secret';
SELECT VALUE id FROM e WITH NOINDEX WHERE acl CONTAINS 'secret';
SELECT count() FROM e WHERE acl CONTAINS 'secret' GROUP ALL;
SELECT count() FROM e WITH NOINDEX WHERE acl CONTAINS 'secret' GROUP ALL;
{ LET $t = type::table('d'); SELECT VALUE id FROM $t WHERE acl CONTAINS 'secret' };
