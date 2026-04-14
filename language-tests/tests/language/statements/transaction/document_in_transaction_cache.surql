/**
[test]
reason = "Test document in transaction cache should include document id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: parent:2, name: 'Alice' }]"

[[test.results]]
value = "[{ id: parent:2, name: 'Alice' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: parent:1, name: 'Alice', children: [] }]"

[[test.results]]
value = "[{ id: child:a, label: 'X', parent: parent:1 }]"

[[test.results]]
value = "[{ id: child:b, label: 'Y', parent: parent:1 }]"

[[test.results]]
value = "[{ id: parent:1, name: 'Alice', children: [child:a, child:b] }]"

[[test.results]]
value = "NONE"
*/

BEGIN;
    CREATE parent:2 SET name = "Alice";
    SELECT * FROM parent:2;
COMMIT;

{
    DEFINE TABLE OVERWRITE parent SCHEMAFULL;
    DEFINE FIELD OVERWRITE name     ON TABLE parent TYPE string;
    DEFINE FIELD OVERWRITE children ON TABLE parent COMPUTED <~child;

    DEFINE TABLE OVERWRITE child SCHEMAFULL;
    DEFINE FIELD OVERWRITE parent ON TABLE child TYPE record<parent> REFERENCE ON DELETE CASCADE;
    DEFINE FIELD OVERWRITE label  ON TABLE child TYPE string;
    NONE;
};

BEGIN;
  CREATE parent:1 SET name = 'Alice';
  CREATE child:a  SET parent = parent:1, label = 'X';
  CREATE child:b  SET parent = parent:1, label = 'Y';
  SELECT * FROM parent:1;
COMMIT;