/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:bob }]"

[[test.results]]
value = "[{ id: doc:1, owner: 'bob', visible_for: ['bob', 'carol'] }]"

[[test.results]]
value = "[{ id: doc:2, owner: 'alice', visible_for: ['alice'] }]"

[[test.results]]
value = "[{ id: doc:3, owner: 'carol' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;

-- The field permission references `$value` — must be bound to the
-- field's picked value at evaluation time.
DEFINE FIELD visible_for ON doc
    PERMISSIONS FOR SELECT WHERE $auth.id.id() IN $value;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

CREATE person:bob;
CREATE doc:1 SET owner = 'bob', visible_for = ['bob', 'carol'];
CREATE doc:2 SET owner = 'alice', visible_for = ['alice'];
-- doc:3 has no `visible_for` field. `Value::each` on the
-- non-wildcard idiom yields zero paths, so the permission predicate
-- never runs and the row passes through unmodified — locking in
-- behaviour for the absent-field case.
CREATE doc:3 SET owner = 'carol';
