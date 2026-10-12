/**
[env]
namespace = true
database = true

[test]
reason = """
	A view that selects every field of its source (`SELECT *`, alone or beside
	other fields, over one table or several) holds rows that carry the fields
	of the records they were written from, by name. Naming the components of
	the record id of a source would give rows written afterwards `city` and
	`time` while the rows already held keep none, so a filter on a component
	would miss them. While such a view holds a row, naming the components is
	refused, naming the view, and the table and the view are left as they
	were.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "[{ id: v:['London', d'2024-01-01T00:00:00Z'], temp: 10 }, { id: v:['Paris', d'2024-01-01T00:00:00Z'], temp: 20 }]"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot change the record id of `early`: the view `doubled` selects every field of `early` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "[{ id: doubled:['Paris', d'2024-01-01T00:00:00Z'], temp: 20, x: 40 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot change the record id of `north`: the view `both` selects every field of `north` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `south`: the view `both` selects every field of `south` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"
*/

-- A view defined after the data.
DEFINE TABLE reading SCHEMALESS;
INSERT INTO reading [
	{ id: ['London', d'2024-01-01T00:00:00Z'], temp: 10 },
	{ id: ['Paris', d'2024-01-01T00:00:00Z'], temp: 20 }
] RETURN NONE;
DEFINE TABLE v AS SELECT * FROM reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
SELECT * FROM v ORDER BY temp;
(INFO FOR TABLE reading).fields;

-- A view defined before the data, selecting every field beside a computed one,
-- under a condition.
DEFINE TABLE early SCHEMALESS;
DEFINE TABLE doubled AS SELECT *, temp * 2 AS x FROM early WHERE temp > 15;
INSERT INTO early [
	{ id: ['London', d'2024-01-01T00:00:00Z'], temp: 10 },
	{ id: ['Paris', d'2024-01-01T00:00:00Z'], temp: 20 }
] RETURN NONE;
DEFINE FIELD id ON early TYPE [city: string, time: datetime];
SELECT * FROM doubled;

-- A view over two tables reads every field of each.
DEFINE TABLE north SCHEMALESS;
DEFINE TABLE south SCHEMALESS;
DEFINE TABLE both AS SELECT * FROM north, south;
CREATE north:['London', 1] SET temp = 10 RETURN NONE;
CREATE south:['Paris', 2] SET temp = 20 RETURN NONE;
DEFINE FIELD id ON north TYPE [city: string, n: int];
DEFINE FIELD id ON south TYPE [city: string, n: int];
