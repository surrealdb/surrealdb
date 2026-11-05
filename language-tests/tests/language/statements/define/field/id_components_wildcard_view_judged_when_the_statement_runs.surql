/**
[env]
namespace = true
database = true

[test]
reason = """
	DEFINE FIELD and REMOVE FIELD of the `id` judge the views over the table
	against the rows they hold when the statement runs, not when the query is
	planned. A block, an IF branch or a function body that writes a row into a
	view that selects every field and then renames the components or removes
	the `id` is refused, and the whole statement is undone. One that empties
	the view first is accepted.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `stars`: the view `everything` selects every field of `stars` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `stars`: the view `everything` selects every field of `stars` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `stars`: the view `everything` selects every field of `stars` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `stars`: the view `everything` selects every field of `stars` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `stars`: the view `everything` selects every field of `stars` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `stars`: the view `everything` selects every field of `stars` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "[]"

[[test.results]]
value = "'DEFINE FIELD id ON stars TYPE [city: string, n: int] PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD id ON s2 TYPE [place: string, n: int] PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON stars TYPE [city: string, n: int];
DEFINE TABLE everything AS SELECT * FROM stars;

-- The row is written by the statement before the change.
{ CREATE stars:['London', 1] SET x = 1 RETURN NONE; DEFINE FIELD OVERWRITE id ON stars TYPE [place: string, n: int]; };
{ CREATE stars:['London', 1] SET x = 1 RETURN NONE; REMOVE FIELD id ON stars; };
IF true { CREATE stars:['London', 1] SET x = 1 RETURN NONE; REMOVE FIELD id ON stars; };
IF true { CREATE stars:['London', 1] SET x = 1 RETURN NONE; DEFINE FIELD OVERWRITE id ON stars TYPE [place: string, n: int]; };
DEFINE FUNCTION fn::fill_then_remove() { CREATE stars:['London', 1] SET x = 1 RETURN NONE; REMOVE FIELD id ON stars; RETURN true; };
fn::fill_then_remove();
DEFINE FUNCTION fn::fill_then_rename() { CREATE stars:['London', 1] SET x = 1 RETURN NONE; DEFINE FIELD OVERWRITE id ON stars TYPE [place: string, n: int]; RETURN true; };
fn::fill_then_rename();
-- Each refusal undid its statement's write.
SELECT * FROM everything;
(INFO FOR TABLE stars).fields.id;

-- The row is deleted by the statement before the change.
DEFINE FIELD id ON s1 TYPE [city: string, n: int];
DEFINE TABLE e1 AS SELECT * FROM s1;
CREATE s1:['London', 1] RETURN NONE;
{ DELETE s1; REMOVE FIELD id ON s1; };
(INFO FOR TABLE s1).fields.id;

DEFINE FIELD id ON s2 TYPE [city: string, n: int];
DEFINE TABLE e2 AS SELECT * FROM s2;
CREATE s2:['London', 1] RETURN NONE;
IF true { DELETE s2; DEFINE FIELD OVERWRITE id ON s2 TYPE [place: string, n: int]; };
(INFO FOR TABLE s2).fields.id;

DEFINE FIELD id ON s3 TYPE [city: string, n: int];
DEFINE TABLE e3 AS SELECT * FROM s3;
CREATE s3:['London', 1] RETURN NONE;
DEFINE FUNCTION fn::empty_then_remove() { DELETE s3; REMOVE FIELD id ON s3; RETURN true; };
fn::empty_then_remove();
(INFO FOR TABLE s3).fields.id;
