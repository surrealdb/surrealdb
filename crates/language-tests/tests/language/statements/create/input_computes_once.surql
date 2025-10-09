/**
[test]

[[test.results]]
value = "[{ id: test:vid2w42m31exfu8wlqz5, other: other:agaysdhbron0stov8q2v }]"
skip-record-id-key = true

[[test.results]]
value = "[{ count: 1 }]"

*/

CREATE test SET other = (CREATE ONLY other).id;
SELECT count() FROM other GROUP ALL;