/**
[test]
issue = 4781
wip = true

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ color: NONE }, { color: NONE }, { color: NONE }, { color: NONE }]"

[[test.results]]
value = "[{ color: NONE }, { color: NONE }, { color: NONE }, { color: NONE }]"

[[test.results]]
value = "[{ color: NONE }, { color: NONE }, { color: NONE }, { color: NONE }]"

*/

BEGIN;
    DEFINE TABLE UserTag SCHEMAFULL PERMISSIONS FULL;
    DEFINE FIELD name ON TABLE UserTag TYPE option<string>;
    DEFINE FIELD color ON TABLE UserTag VALUE COMPUTED {RETURN SELECT VALUE group.color FROM ONLY $this};
    DEFINE FIELD group ON TABLE UserTag TYPE option<record<UserTagGroup>>;


    INSERT INTO ⟨UserTag⟩ [
      { id: "tag-1", group: UserTagGroup:`utg-1`},
        { id: "tag-2", group: UserTagGroup:`utg-1` },
        { id: "tag-3", group: UserTagGroup:`utg-2` },
        { id: "tag-4" }
    ];

    INSERT INTO ⟨Color⟩ [
      { id: "yellow" },
      { id: "blue" },
      { id: "red" }
    ];

RETURN "OK";
COMMIT;


SELECT color FROM UserTag; //this works
SELECT * FROM UserTag WHERE color; //this does not
SELECT * FROM UserTag WHERE [color][WHERE id == Color:yellow ]
