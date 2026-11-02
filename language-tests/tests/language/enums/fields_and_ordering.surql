/**
[env]
namespace = true
database = true

[test]
reason = "Enum typed fields validate membership, order by declaration, and keep equality and IN on the index"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a, mood: 'happy', name: 'a', size: 'l' }]"

[[test.results]]
value = "[{ id: person:b, mood: 'sad', name: 'b', size: 'm' }]"

[[test.results]]
value = "[{ id: person:c, mood: 'ok', name: 'c', note: 'ok', size: 's' }]"

[[test.results]]
error = "Couldn't coerce value for field `mood` of `person:bad`: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'ecstatic'`"

[[test.results]]
error = "Couldn't coerce value for field `size` of `person:bad`: Expected `enum<'s', 'm', 'l'>` but found `'xl'`"

[[test.results]]
error = "Couldn't coerce value for field `mood` of `person:bad`: Expected `enum<mood>` but found `5`"

[[test.results]]
value = "['b', 'c', 'a']"

[[test.results]]
value = "['a', 'c', 'b']"

[[test.results]]
value = "['c', 'b', 'a']"

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = "['b', 'c']"

[[test.results]]
value = "['c']"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
error = "'nope' is not a value of the enum 'mood'; its values are 'sad', 'ok', 'happy'"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
error = "'nope' is not a value of the enum 'mood'; its values are 'sad', 'ok', 'happy'"

[[test.results]]
value = "'sad'"

[[test.results]]
value = "'happy'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The enum 'missing' does not exist"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "Could not cast into `enum<mood>` using input `'nope'`; its values are 'sad', 'ok', 'happy'"

[[test.results]]
value = "'happy'"

[[test.results]]
value = "'string'"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ attributes: { expr: 'name' }, children: [{ attributes: { access: "= 'ok'", direction: 'Forward', index: 'idx_mood', table: 'person' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = """{ attributes: { expr: 'name' }, children: [{ attributes: { sort_keys: 'name ASC' }, children: [{ attributes: { predicate: "enum::rank('mood', mood) > enum::rank('mood', 'sad')" }, children: [{ attributes: { direction: 'Forward', table: 'person' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a, mood: 'ecstatic', name: 'a', size: 'l' }]"

[[test.results]]
value = "['a']"

[[test.results]]
value = "['a', 'c', 'b']"

[[test.results]]
error = "Invalid statement: Cannot remove the enum 'mood': the field 'mood' on the table 'person' has a type that names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the enum 'mood': the field 'note' on the table 'person' has a type that names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, enums: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { person: 'DEFINE TABLE person TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }, users: {  } }"

*/

DEFINE ENUM mood VALUES 'sad', 'ok', 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD mood ON person TYPE enum<mood>;
DEFINE FIELD note ON person TYPE option<enum<mood>>;
DEFINE FIELD size ON person TYPE enum<'s', 'm', 'l'>;
CREATE person:a SET name = 'a', mood = 'happy', size = 'l';
CREATE person:b SET name = 'b', mood = 'sad', size = 'm';
CREATE person:c SET name = 'c', mood = 'ok', size = 's', note = 'ok';
CREATE person:bad SET name = 'x', mood = 'ecstatic', size = 's';
CREATE person:bad SET name = 'x', mood = 'ok', size = 'xl';
CREATE person:bad SET name = 'x', mood = 5, size = 's';
SELECT value name FROM person ORDER BY mood;
SELECT value name FROM person ORDER BY mood DESC;
SELECT value name FROM person ORDER BY size;
SELECT value name FROM person WHERE mood > 'sad' ORDER BY name;
SELECT value name FROM person WHERE mood <= 'ok' ORDER BY name;
SELECT value name FROM person WHERE mood = 'ok';
SELECT value name FROM person WHERE mood IN ['sad', 'happy'] ORDER BY name;
SELECT value name FROM person WHERE mood < 'nope';
enum::rank('mood', 'happy');
enum::rank('mood', NONE);
enum::rank('mood', 'nope');
enum::min('mood', ['happy', 'ok', 'sad']);
enum::max('mood', ['happy', 'ok', 'sad']);
enum::min('mood', []);
enum::values('missing');
<enum<mood>> 'ok';
<enum<mood>> 'nope';
<string> person:a.mood;
type::of(person:a.mood);
DEFINE INDEX idx_mood ON person FIELDS mood;
SELECT value name FROM person WHERE mood = 'ok' EXPLAIN;
SELECT value name FROM person WHERE mood > 'sad' ORDER BY name EXPLAIN;
ALTER ENUM mood ADD VALUE 'ecstatic' AFTER 'happy';
UPDATE person:a SET mood = 'ecstatic';
SELECT value name FROM person WHERE mood >= 'happy' ORDER BY name;
SELECT value name FROM person ORDER BY mood DESC;
REMOVE ENUM mood;
REMOVE INDEX idx_mood ON person;
REMOVE FIELD mood ON person;
REMOVE ENUM mood;
REMOVE FIELD note ON person;
REMOVE ENUM mood;
INFO FOR DB;
