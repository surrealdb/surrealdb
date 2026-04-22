/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Events defined on a vertex table must not fire during cascade cleanup of edge records from related tables"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ foo: 'bar', id: test1:1 }]"

[[test.results]]
value = "[{ id: test1rel:1, in: test1:1, out: test1:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"
*/

DEFINE TABLE OVERWRITE test1;
DEFINE TABLE OVERWRITE test1rel;
DEFINE TABLE OVERWRITE test1b;

DELETE test1;
DELETE test1rel;
DELETE test1b;

UPSERT test1:1 CONTENT { foo: "bar" };
RELATE test1:1->test1rel:1->test1:1;

DEFINE EVENT OVERWRITE testevent ON TABLE test1 WHEN $before != $after THEN ({
    CREATE test1b SET
        ref = $before.id || $after.id,
        action = $event;
});

DELETE test1:1;

-- The event on test1 must fire exactly once (for deleting test1:1),
-- not also for the cascade cleanup of edge test1rel:1.
SELECT count() FROM test1b GROUP ALL;
