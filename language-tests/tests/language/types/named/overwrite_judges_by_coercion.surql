/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE TYPE OVERWRITE judges a stored value by coercion to the replacement's body, as a write of it is judged: a body a cast would reach but coercion refuses is refused, and the stored definition stays"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, x: 5 }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'label': the field 'x' on the table 't' holds a value it does not admit: Expected `label (string)` but found `5`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, y: '42' }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'text': the field 'y' on the table 'u' holds a value it does not admit: Expected `text (int)` but found `'42'`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `x` of `t:1`: Expected `label (float)` but found `'five'`"

[[test.results]]
value = "[{ id: t:1, x: 5 }]"

[[test.results]]
value = "[{ id: u:1, y: '42' }]"

*/
DEFINE TYPE label AS int;
DEFINE FIELD x ON t TYPE label;
CREATE t:1 SET x = 5;
DEFINE TYPE OVERWRITE label AS string;
DEFINE TYPE text AS string;
DEFINE FIELD y ON u TYPE text;
CREATE u:1 SET y = '42';
DEFINE TYPE OVERWRITE text AS int;
DEFINE TYPE OVERWRITE label AS float;
UPDATE t:1 SET x = 'five';
SELECT * FROM t;
SELECT * FROM u;
