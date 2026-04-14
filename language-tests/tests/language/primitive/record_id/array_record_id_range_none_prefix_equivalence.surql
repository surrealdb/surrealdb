/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Open-ended range starts city:[\"London\"].., city:[\"London\", NONE].., and city:[\"London\", NONE, NONE].. must be equivalent; stable under default dual planner and benign field indexes"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: city:['London', 7], tag: 'l' }, { id: city:['London', 9], tag: 'l' }, { id: city:['Paris', 0], tag: 'p' }, { id: city:['Zagreb', 15, 3], tag: 'z' }]"

[[test.results]]
value = "[{ id: city:['London', 7], tag: 'l' }, { id: city:['London', 9], tag: 'l' }, { id: city:['Paris', 0], tag: 'p' }, { id: city:['Zagreb', 15, 3], tag: 'z' }]"

[[test.results]]
value = "[{ id: city:['London', 7], tag: 'l' }, { id: city:['London', 9], tag: 'l' }, { id: city:['Paris', 0], tag: 'p' }, { id: city:['Zagreb', 15, 3], tag: 'z' }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"
*/

{
    DEFINE TABLE OVERWRITE city SCHEMALESS;
    DEFINE FIELD OVERWRITE tag ON city TYPE string;
    DEFINE INDEX OVERWRITE city_tag ON city FIELDS tag;

    CREATE city:["Belgrade", 1] SET tag = 'b';
    CREATE city:["London", 7] SET tag = 'l';
    CREATE city:["London", 9] SET tag = 'l';
    CREATE city:["Paris", 0] SET tag = 'p';
    CREATE city:["Zagreb", 15, 3] SET tag = 'z';

    RETURN "OK";
};

-- Core: trailing NONE in the range start must not change the scanned set
SELECT * FROM city:["London"].. ORDER BY id;
SELECT * FROM city:["London", NONE].. ORDER BY id;
SELECT * FROM city:["London", NONE, NONE].. ORDER BY id;

-- Count must match (open end)
SELECT count() FROM city:["London"].. GROUP ALL;
SELECT count() FROM city:["London", NONE].. GROUP ALL;
SELECT count() FROM city:["London", NONE, NONE].. GROUP ALL;
