/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE TYPE OVERWRITE of a union keeps the same labels freely, and changes them only while nothing names the type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a, mood: 'ok' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['sad', 'ok', 'happy']"

[[test.results]]
error = "Invalid statement: Cannot change the values of the type 'mood': the field 'mood' on the table 'person' has a type that names it"

[[test.results]]
error = "Invalid statement: Cannot change the values of the type 'mood': the field 'mood' on the table 'person' has a type that names it"

[[test.results]]
value = "['sad', 'ok', 'happy']"

[[test.results]]
value = "[{ id: person:a, mood: 'ok' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot change the values of the type 'mood': the function 'fn::cheer' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['sad', 'happy', 'ecstatic']"
*/
DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE FIELD mood ON person TYPE option<mood>;
CREATE person:a SET mood = 'ok';
-- The same values in the same order: only the comment changes.
DEFINE TYPE OVERWRITE mood AS 'sad' | 'ok' | 'happy' COMMENT 'feelings';
enum::values('mood');
-- Other values, or the same ones reordered, while a field names the type.
DEFINE TYPE OVERWRITE mood AS 'sad' | 'happy';
DEFINE TYPE OVERWRITE mood AS 'happy' | 'ok' | 'sad';
enum::values('mood');
SELECT * FROM person;
-- A function naming the type holds it too.
REMOVE FIELD mood ON person;
DEFINE FUNCTION fn::cheer($m: mood) { $m };
DEFINE TYPE OVERWRITE mood AS 'sad' | 'happy';
REMOVE FUNCTION fn::cheer;
-- Once nothing names it, the values may change.
DEFINE TYPE OVERWRITE mood AS 'sad' | 'happy' | 'ecstatic';
enum::values('mood');
