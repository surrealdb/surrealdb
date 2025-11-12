/**
[test]

[[test.results]]
value = "[{ id: test:vid2w42m31exfu8wlqz5, other: other:agaysdhbron0stov8q2v }]"
skip-record-id-key = true

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }]"

[[test.results]]
value = "[{ id: test:2 }]"

[[test.results]]
value = "[{ id: test:3 }]"

*/

CREATE test SET other = (CREATE ONLY other).id;
SELECT count() FROM other GROUP ALL;

DEFINE FUNCTION fn::increment($name: string) {
    RETURN (UPSERT ONLY type::record('counter', $name) SET value += 1).value;
};

CREATE test SET id = fn::increment('test');
CREATE test SET id = fn::increment('test');
CREATE test SET id = fn::increment('test');