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
value = "NONE"

[[test.results]]
value = "[{ id: person:bob, role: 'user' }]"

[[test.results]]
value = "[{ id: foo:1, outer: { a: 'secret', b: 'public' } }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) SET role = $role )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE foo SCHEMALESS PERMISSIONS FULL;

-- Permission gates the *nested* path `outer.a`. The streaming runtime
-- must evaluate this against each concrete-path expansion of the field.
DEFINE FIELD outer.a ON foo
    PERMISSIONS FOR SELECT WHERE $auth.role = 'admin';

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD role ON person TYPE string;

CREATE person:bob SET role = 'user';
CREATE foo:1 SET outer = { a: 'secret', b: 'public' };
