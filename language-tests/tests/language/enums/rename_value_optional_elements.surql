/**
[env]
namespace = true
database = true

[test]
reason = "RENAME VALUE writes only the records that hold the old label in a list of optional enum labels, and is refused while a field holds the enum two lists deep"

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
value = "NONE"

# The rewrite writes person:a again, and person:b only at its create.
[[test.results]]
match = '''
array::len(array::filter(array::flatten($result.changes), |$c| $c.update.id == person:a)) == 2
&& array::len(array::filter(array::flatten($result.changes), |$c| $c.update.id == person:b)) == 1
'''
error = false

[[test.results]]
value = "[{ id: person:a, past: ['fine', NONE] }, { id: person:b, past: ['happy'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "the type 'mood' is held by the field 'days.*.*' on the table 'pet', whose path a rename does not reach"

[[test.results]]
value = "[{ id: person:a, past: ['fine', NONE] }, { id: person:b, past: ['happy'] }]"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL CHANGEFEED 1h;
DEFINE FIELD past ON person TYPE array<option<mood>>;
CREATE person:a SET past = ['ok', NONE] RETURN NONE;
CREATE person:b SET past = ['happy'] RETURN NONE;
ALTER TYPE mood RENAME VALUE 'ok' TO 'fine';
SHOW CHANGES FOR TABLE person SINCE 1;
SELECT id, past FROM person ORDER BY id;
DEFINE TABLE pet SCHEMAFULL;
DEFINE FIELD days ON pet TYPE array<array<mood>>;
ALTER TYPE mood RENAME VALUE 'fine' TO 'okay';
SELECT id, past FROM person ORDER BY id;
