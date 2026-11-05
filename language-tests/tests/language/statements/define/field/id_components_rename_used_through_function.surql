/**
[env]
namespace = true
database = true

[test]
reason = """
	A clause that hands the record to a function, or calls a function whose
	body is evaluated against the record, may read any field of it by a name
	the clause does not spell: `fn::city_of($this)` returning `$r.city`, or
	`fn::hidden()` returning `city`. The bodies are not read, so while such a
	clause is defined on the table, renaming or removing the components of its
	record id is refused, naming the clause, the same in a field, a table
	permission and an event. Removing the clause lets the record id change.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `f1`: the field `via_arg` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `f1`: the field `via_arg` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `f2`: the field `up` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `f2`: the field `up` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `f3`: the field `note` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `f3`: the field `note` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `f4`: the field `via_body` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `f4`: the field `via_body` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `p1`: the table permission `select` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `p1`: the table permission `select` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `e1`: the event `ev` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `e1`: the event `ev` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `e2`: the event `ev` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `e2`: the event `ev` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FUNCTION fn::city_of($r: any) { RETURN $r.city; };
DEFINE FUNCTION fn::hidden() { RETURN city; };

-- A function handed the record, in a COMPUTED, a VALUE and an ASSERT clause.
DEFINE FIELD id ON f1 TYPE [city: string, time: datetime];
DEFINE FIELD via_arg ON f1 COMPUTED fn::city_of($this);
ALTER FIELD id ON f1 TYPE [place: string, time: datetime];
REMOVE FIELD id ON f1;
DEFINE FIELD id ON f2 TYPE [city: string, time: datetime];
DEFINE FIELD up ON f2 VALUE string::uppercase(fn::city_of($this));
ALTER FIELD id ON f2 TYPE [place: string, time: datetime];
REMOVE FIELD id ON f2;
DEFINE FIELD id ON f3 TYPE [city: string, time: datetime];
DEFINE FIELD note ON f3 TYPE option<string> ASSERT fn::city_of($this) = 'London';
ALTER FIELD id ON f3 TYPE [place: string, time: datetime];
REMOVE FIELD id ON f3;

-- A function whose body reads a field of the record it is evaluated against.
DEFINE FIELD id ON f4 TYPE [city: string, time: datetime];
DEFINE FIELD via_body ON f4 COMPUTED fn::hidden();
ALTER FIELD id ON f4 TYPE [place: string, time: datetime];
REMOVE FIELD id ON f4;

-- A table permission.
DEFINE TABLE p1 SCHEMALESS PERMISSIONS FOR select WHERE fn::city_of($this) = 'London' FOR create, update, delete FULL;
DEFINE FIELD id ON p1 TYPE [city: string, time: datetime];
ALTER FIELD id ON p1 TYPE [place: string, time: datetime];
REMOVE FIELD id ON p1;

-- An event that hands its record to a function, and one whose condition calls
-- a function evaluated against the record the event fires for.
DEFINE FIELD id ON e1 TYPE [city: string, time: datetime];
DEFINE EVENT ev ON e1 THEN { CREATE audit SET seen = fn::city_of($after) };
ALTER FIELD id ON e1 TYPE [place: string, time: datetime];
REMOVE FIELD id ON e1;
DEFINE FIELD id ON e2 TYPE [city: string, time: datetime];
DEFINE EVENT ev ON e2 WHEN fn::hidden() = 'London' THEN { CREATE audit SET how = 'when' };
ALTER FIELD id ON e2 TYPE [place: string, time: datetime];
REMOVE FIELD id ON e2;

-- Once the reader is gone, the record id can change.
REMOVE FIELD via_arg ON f1;
ALTER FIELD id ON f1 TYPE [place: string, time: datetime];
REMOVE FIELD id ON f1;
