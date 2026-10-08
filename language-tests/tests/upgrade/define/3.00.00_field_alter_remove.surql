/**
[env]
imports = ["upgrade/define/3.00.00_field_alter_remove_import.surql"]

[test]
upgrade = true
reason = """
Fields a release before 3.3 wrote store their TYPE as a `Kind`, a revision the
current checkout converts on read. `ALTER FIELD` and `REMOVE FIELD` read the
stored definition before rewriting or deleting it, so each must accept that
revision: the altered field keeps its converted type, the removed one is gone,
and writes afterwards are checked against what is left. Removing `tags`
leaves the `tags.*` field its definition implied, as it does for a field the
current checkout defines."""
# ALTER FIELD and the current INFO rendering are the current checkout's.
version = ">=3.4.0-a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ age: "DEFINE FIELD age ON person TYPE none | int COMMENT 'years' PERMISSIONS FULL", name: 'DEFINE FIELD name ON person TYPE string | int PERMISSIONS FULL', "tags.*": 'DEFINE FIELD tags.* ON person TYPE string PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ age: 4, id: person:2, name: 2 }]"

[[test.results]]
value = "[{ age: 3, id: person:1, name: 'a', tags: ['x'] }, { age: 4, id: person:2, name: 2 }]"
*/
ALTER FIELD age ON person COMMENT 'years';
ALTER FIELD name ON person TYPE string | int;
REMOVE FIELD tags ON person;
RETURN (INFO FOR TABLE person).fields;
CREATE person:2 SET name = 2, age = 4;
SELECT * FROM person ORDER BY id;
