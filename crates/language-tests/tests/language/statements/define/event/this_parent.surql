/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:foo }]"

[[test.results]]
value = "[{ id: activity:33teaj8bmm3mfyz2f6zw, parent: { id: user:foo }, this: { id: activity:33teaj8bmm3mfyz2f6zw } }]"
skip-record-id-key = true

*/
DEFINE EVENT event ON user WHEN true THEN (
    CREATE activity SET this = $this, parent = $parent
);
CREATE user:foo;
SELECT * FROM activity;
