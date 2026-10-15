/**
[test]
reason = "ALTER FIELD that sets a TYPE is judged against the fields the field is nested in, as DEFINE FIELD is: a kind the parent's kind does not admit is refused, with or without a migration and whether or not the table requires one, and the rows keep reading as the parent declares them."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot set field `labels.*` with type `int` as it mismatched with field `labels` with type `none | set<string>`"

[[test.results]]
error = "Cannot set field `labels.*` with type `int` as it mismatched with field `labels` with type `none | set<string>`"

[[test.results]]
error = "Cannot set field `labels.*` with type `int` as it mismatched with field `labels` with type `none | set<string>`"

[[test.results]]
error = "Cannot set field `xs.*` with type `string` as it mismatched with field `xs` with type `array<int>`"

[[test.results]]
error = "Cannot set field `xs.*` with type `string` as it mismatched with field `xs` with type `array<int>`"

[[test.results]]
value = "[{ labels: {'1', 'z'}, xs: [1, 2] }]"

[[test.results]]
value = """{ labels: 'DEFINE FIELD labels ON post TYPE none | set<string> PERMISSIONS FULL', "labels.*": 'DEFINE FIELD labels.* ON post TYPE string PERMISSIONS FULL', xs: 'DEFINE FIELD xs ON post TYPE array<int> PERMISSIONS FULL', "xs.*": 'DEFINE FIELD xs.* ON post TYPE int PERMISSIONS FULL' }"""

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot set field `labels.*` with type `int` as it mismatched with field `labels` with type `none | set<string>`"

[[test.results]]
value = "[{ labels: {'1', 'z'} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD labels ON post TYPE option<set<string>>;
DEFINE FIELD xs ON post TYPE array<int>;
CREATE post:1 SET labels = <set>['1', 'z'], xs = [1, 2] RETURN NONE;
ALTER FIELD labels.* ON post TYPE int;
ALTER FIELD labels.* ON post TYPE int MIGRATE EXISTING ROWS AS labels_int USING <int>$value FALLBACK 0;
DEFINE FIELD OVERWRITE labels.* ON post TYPE int;
ALTER FIELD xs.* ON post TYPE string;
ALTER FIELD xs.* ON post TYPE string MIGRATE EXISTING ROWS AS xs_str USING <string>$value FALLBACK '';
SELECT labels, xs FROM post;
(INFO FOR TABLE post).fields;
(INFO FOR TABLE post).migrations.migrations;
-- A table that requires no migration judges the kind against the parent too.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD labels ON loose TYPE option<set<string>>;
CREATE loose:1 SET labels = <set>['1', 'z'] RETURN NONE;
ALTER FIELD labels.* ON loose TYPE int;
SELECT labels FROM loose;
-- A kind the parent admits is accepted.
ALTER FIELD labels.* ON post TYPE string;
ALTER FIELD xs.* ON post TYPE int;
