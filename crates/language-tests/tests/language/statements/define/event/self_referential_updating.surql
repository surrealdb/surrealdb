/**
[test]
reason = "Test self-referential updating with DEFINE EVENT and nested UPSERT operations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ animals: [animal:test], id: child:test, name: 'John' }]"

[[test.results]]
value = "[{ animals: [{ id: animal:test, toy: toy:test, toys: [{ id: animal:test, toy: toy:test, toys: [animal:test] }] }], id: child:test, name: 'John' }]"

[env]
namespace = "test"
database = "test"

*/

DEFINE EVENT updateOwner ON TABLE toy WHEN $before.owner != $after.owner THEN {
    UPSERT $after.owner SET toys += $this.id;
};

DEFINE EVENT updateOwner ON TABLE animal WHEN $before.owner != $after.owner THEN {
    UPSERT $after.owner SET animals += $this.id;
};

UPSERT child:test SET
    name = 'John',
    animals = [
        (
           UPSERT ONLY animal:test SET toy = (
               UPSERT ONLY toy:test SET owner=$parent.id, name = 'doll'
           ).id
        ).id
    ]
;

SELECT * FROM child FETCH animals, animals.toys;
