/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE, ALTER and REMOVE ENUM, and their listing in INFO FOR DB"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The enum 'mood' already exists"

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
error = "The enum 'mood' already has the value 'meh'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The enum 'mood' has no value 'nope'; its values are 'angry', 'sad', 'meh', 'ok', 'fine', 'happy', 'ecstatic'"

[[test.results]]
error = "The enum 'mood' already has the value 'fine'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The enum 'mood' already has the value 'fine'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['angry', 'sorrowful', 'meh', 'okay', 'fine', 'happy', 'ecstatic']"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, enums: { mood: "DEFINE ENUM mood VALUES 'angry', 'sorrowful', 'meh', 'okay', 'fine', 'happy', 'ecstatic' COMMENT 'feelings' PERMISSIONS FULL" }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
error = "Invalid statement: The enum 'dup' lists the label 'a' more than once"

[[test.results]]
error = "The enum 'nope' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The enum 'nope' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, enums: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

*/

DEFINE ENUM mood VALUES 'sad', 'ok', 'happy';
DEFINE ENUM mood VALUES 'sad', 'ok', 'happy';
DEFINE ENUM IF NOT EXISTS mood VALUES 'x';
DEFINE ENUM OVERWRITE mood VALUES 'sad', 'ok', 'happy' COMMENT 'feelings';
enum::values('mood');
ALTER ENUM mood ADD VALUE 'ecstatic' AFTER 'happy';
ALTER ENUM mood ADD VALUE 'angry' BEFORE 'sad';
ALTER ENUM mood ADD VALUE 'meh' AFTER 'sad' ADD VALUE 'fine' BEFORE 'happy';
enum::values('mood');
ALTER ENUM mood ADD VALUE 'meh';
ALTER ENUM mood ADD VALUE IF NOT EXISTS 'meh';
ALTER ENUM mood ADD VALUE 'zzz' AFTER 'nope';
ALTER ENUM mood RENAME VALUE 'ok' TO 'fine';
ALTER ENUM mood RENAME VALUE 'ok' TO 'okay';
ALTER ENUM mood RENAME VALUE 'okay' TO 'fine';
ALTER ENUM mood RENAME VALUE 'sad' TO 'sorrowful';
enum::values('mood');
INFO FOR DB;
DEFINE ENUM dup VALUES 'a', 'b', 'a';
ALTER ENUM nope ADD VALUE 'x';
ALTER ENUM IF EXISTS nope ADD VALUE 'x';
REMOVE ENUM nope;
REMOVE ENUM IF EXISTS nope;
REMOVE ENUM mood;
INFO FOR DB;
