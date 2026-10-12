/**
[env]
namespace = true
database = true

[test]
reason = """
	Naming a component changes what a view over the table, or the COUNT
	condition of an index, reads under that name. One that hands the record
	to a function may read the name, so naming is refused, naming the
	definition, as a view or index that spells the name is. A field of the
	table is not checked: it reads the component from then on.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `city` in the record id of `k1`: the view `v` reads fields of the record by names that cannot be determined, so it may already read a stored `city` field. Remove it, then define it again after changing the record id"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `city` in the record id of `k2`: the index `cnt` reads fields of the record by names that cannot be determined, so it may already read a stored `city` field. Remove it, then define it again after changing the record id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['London']"
*/

DEFINE FUNCTION fn::city_of($r: any) { RETURN $r.city; };

-- A view that hands its source record to a function.
DEFINE TABLE k1 SCHEMALESS;
CREATE k1:['London', d'2024-01-01T00:00:00Z'] RETURN NONE;
DEFINE TABLE v AS SELECT fn::city_of($this) AS c FROM k1;
DEFINE FIELD id ON k1 TYPE [city: string, time: datetime];
(INFO FOR TABLE k1).fields;

-- The COUNT condition of an index.
DEFINE TABLE k2 SCHEMALESS;
DEFINE INDEX cnt ON k2 COUNT WHERE fn::city_of($this) = 'London';
DEFINE FIELD id ON k2 TYPE [city: string, time: datetime];

-- A field of the table is not checked when names are added: from then on it
-- reads the components the record id gives.
DEFINE TABLE k3 SCHEMALESS;
CREATE k3:['London', d'2024-01-01T00:00:00Z'] RETURN NONE;
DEFINE FIELD c ON k3 COMPUTED fn::city_of($this);
DEFINE FIELD id ON k3 TYPE [city: string, time: datetime];
SELECT VALUE c FROM k3;
