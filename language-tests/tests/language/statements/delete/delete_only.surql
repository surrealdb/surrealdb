/**
[test]
reason = """
The DELETE counterpart to `update_only`. DELETE returns no rows by default, so
`DELETE ONLY` yields NONE on a record that existed and NONE again on one that
did not — the keyword changes the shape of the result, not whether the delete
happened.

`RETURN BEFORE` is what makes the difference observable: it returns the deleted
record itself, unwrapped rather than in an array.
"""

[[test.results]]
value = "[{ id: test:a, name: 'A' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:b, name: 'B' }]"

[[test.results]]
value = "{ id: test:b, name: 'B' }"

[[test.results]]
value = "[]"
*/

CREATE test:a SET name = 'A';
DELETE ONLY test:a;
DELETE ONLY test:a RETURN BEFORE;
CREATE test:b SET name = 'B';
DELETE ONLY test:b RETURN BEFORE;
SELECT * FROM test;
