/**
[test]
issue = 4781

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[Color:yellow, Color:yellow, Color:blue, NONE]"

[[test.results]]
value = "[UserTag:`tag-1`, UserTag:`tag-2`, UserTag:`tag-3`]"

[[test.results]]
value = "[UserTag:`tag-1`, UserTag:`tag-2`]"

*/

{
    DEFINE TABLE UserTag SCHEMAFULL PERMISSIONS FULL;
    DEFINE FIELD name ON TABLE UserTag TYPE option<string>;
    DEFINE FIELD color ON TABLE UserTag COMPUTED group.color;
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

    INSERT INTO ⟨UserTagGroup⟩ [
      { id: "utg-1", color: Color:yellow },
      { id: "utg-2", color: Color:blue }
    ];

    RETURN "OK";
};

SELECT VALUE color FROM UserTag;
SELECT VALUE id FROM UserTag WHERE color;
SELECT VALUE id FROM UserTag WHERE color = Color:yellow;
