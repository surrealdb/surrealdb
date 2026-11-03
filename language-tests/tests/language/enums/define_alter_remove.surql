/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE, ALTER and REMOVE TYPE, and their listing in INFO FOR DB"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'mood' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['sad', 'ok', 'happy']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['angry', 'sad', 'meh', 'ok', 'fine', 'happy', 'ecstatic']"

[[test.results]]
error = "The type 'mood' already has the value 'meh'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'mood' has no value 'nope'; its values are 'angry', 'sad', 'meh', 'ok', 'fine', 'happy', 'ecstatic'"

[[test.results]]
error = "The type 'mood' already has the value 'fine'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'mood' already has the value 'fine'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['angry', 'sorrowful', 'meh', 'okay', 'fine', 'happy', 'ecstatic']"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: { mood: "DEFINE TYPE mood AS 'angry' | 'sorrowful' | 'meh' | 'okay' | 'fine' | 'happy' | 'ecstatic' COMMENT 'feelings' PERMISSIONS FULL" }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'nope' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'nope' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: { dup: "DEFINE TYPE dup AS 'a' | 'b' PERMISSIONS FULL" }, users: {  } }"""

*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TYPE IF NOT EXISTS mood AS 'x';
DEFINE TYPE OVERWRITE mood AS 'sad' | 'ok' | 'happy' COMMENT 'feelings';
enum::values('mood');
ALTER TYPE mood ADD VALUE 'ecstatic' AFTER 'happy';
ALTER TYPE mood ADD VALUE 'angry' BEFORE 'sad';
ALTER TYPE mood ADD VALUE 'meh' AFTER 'sad' ADD VALUE 'fine' BEFORE 'happy';
enum::values('mood');
ALTER TYPE mood ADD VALUE 'meh';
ALTER TYPE mood ADD VALUE IF NOT EXISTS 'meh';
ALTER TYPE mood ADD VALUE 'zzz' AFTER 'nope';
ALTER TYPE mood RENAME VALUE 'ok' TO 'fine';
ALTER TYPE mood RENAME VALUE 'ok' TO 'okay';
ALTER TYPE mood RENAME VALUE 'okay' TO 'fine';
ALTER TYPE mood RENAME VALUE 'sad' TO 'sorrowful';
enum::values('mood');
INFO FOR DB;
DEFINE TYPE dup AS 'a' | 'b' | 'a';
ALTER TYPE nope ADD VALUE 'x';
ALTER TYPE IF EXISTS nope ADD VALUE 'x';
REMOVE TYPE nope;
REMOVE TYPE IF EXISTS nope;
REMOVE TYPE mood;
INFO FOR DB;
