/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A field whose name is a reserved keyword (e.g. `function`) must round-trip. Field names are persisted as SurrealQL text and re-parsed whenever the schema is read back — INFO FOR TABLE, and the schema re-validation that runs before any subsequent DEFINE FIELD. Before the fix, Idiom serialized via to_raw_string(), whose leading-field escaper (EscapeKwFreeIdent) does not escape keywords, so the bare `function` failed to re-parse via Idiom::from_str with a Conversion/SyntaxError ('Unexpected end of file, expected («'), making the table schema unreadable and un-extendable. Idioms now serialize via to_sql() (like Expr), which escapes the leading field with EscapeIdent."
issue = 7408

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON t TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ another: 1, function: 'hello', id: t:x }]"

[[test.results]]
value = "[{ another: 1, function: 'hello' }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD `function` ON t TYPE string;
-- Reading the schema back must NOT error (this is what regressed):
INFO FOR TABLE t;
-- Re-validation before a further DEFINE FIELD must also succeed:
DEFINE FIELD another ON t TYPE int;
-- Data must round-trip through the keyword-named field:
CREATE t:x SET `function` = 'hello', another = 1;
SELECT `function`, another FROM t;
