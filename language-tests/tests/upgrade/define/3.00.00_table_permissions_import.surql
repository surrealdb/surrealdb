/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, owner: 'alice', title: 'first' }]"

[[test.results]]
value = "[{ id: doc:2, owner: 'bob', title: 'second' }]"

*/
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS
        FOR select WHERE owner = $auth.id OR $access = 'public'
        FOR create, update WHERE owner = $auth.id
        FOR delete NONE;
CREATE doc:1 SET title = 'first', owner = 'alice';
CREATE doc:2 SET title = 'second', owner = 'bob';
