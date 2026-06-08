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
value = "[{ id: person:guest, role: 'guest' }]"

[[test.results]]
value = "[{ id: cart:1, items: [{ id: 'a', name: 'apple', secret: 'red' }, { id: 'b', name: 'banana', secret: 'yellow' }] }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('person', $id) SET role = $role )
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE cart SCHEMALESS PERMISSIONS FULL;

-- Wildcard in the field path. Each concrete `items[i].secret` should be
-- treated independently at evaluation time.
DEFINE FIELD items[*].secret ON cart
    PERMISSIONS FOR SELECT WHERE $auth.role = 'admin';

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD role ON person TYPE string;

CREATE person:guest SET role = 'guest';
CREATE cart:1 SET items = [
    { id: 'a', name: 'apple', secret: 'red' },
    { id: 'b', name: 'banana', secret: 'yellow' },
];
