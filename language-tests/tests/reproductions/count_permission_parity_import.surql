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
value = "[{ id: person:bob }]"

[[test.results]]
value = "[{ id: grant:g1, holder: person:bob }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

-- The inner table a permission predicate reads. Its OWN select permission
-- denies a record session outright. Inside a permission the definer-authored
-- predicate reads with permissions bypassed, so the subquery below still
-- finds this row; the count fast paths must do the same.
DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select NONE;

-- The outer table. Visibility depends entirely on the subquery above.
DEFINE TABLE item SCHEMALESS PERMISSIONS
    FOR select WHERE (SELECT * FROM grant WHERE holder = $auth.id LIMIT 1);

DEFINE INDEX item_n ON item FIELDS n;

CREATE person:bob;
CREATE grant:g1 SET holder = person:bob;

FOR $i IN 0..5 { CREATE type::record('item', $i) SET n = $i; };

-- A table this identity must NOT see: no grant row points at anyone else.
DEFINE TABLE secret SCHEMALESS PERMISSIONS
    FOR select WHERE (SELECT * FROM grant WHERE holder = $auth.id AND scope = 'secret' LIMIT 1);
