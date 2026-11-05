/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE TYPE OVERWRITE judges every stored value the replacement would hold, wherever a field's kind holds the type: an array or set element, an optional member, a literal object field or tuple position, and an alias that names it"

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
value = "NONE"

[[test.results]]
value = "[{ id: t:1, xs: [5] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'xs' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `5`"

[[test.results]]
value = "[{ id: t:1, xs: [1] }]"

[[test.results]]
value = "[{ id: t:2, s: {7,}, xs: [] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 's' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `7`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:3, o: { n: 8 }, xs: [] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'o' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `8`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:4, p: ['x', 9], xs: [] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'p' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `9`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 6, id: t:5, xs: [] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'a' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `6`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, xs: [1] }]"

*/
DEFINE TYPE posint AS int ASSERT $value > 0;
DEFINE TYPE amount AS posint;
DEFINE FIELD xs ON t TYPE array<posint>;
DEFINE FIELD s ON t TYPE option<set<posint>>;
DEFINE FIELD o ON t TYPE option<{ n: posint }>;
DEFINE FIELD p ON t TYPE option<[string, posint]>;
DEFINE FIELD a ON t TYPE option<amount>;
CREATE t:1 SET xs = [5];
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
UPDATE t:1 SET xs = [1];
CREATE t:2 SET xs = [], s = <set>[7];
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:2;
CREATE t:3 SET xs = [], o = { n: 8 };
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:3;
CREATE t:4 SET xs = [], p = ['x', 9];
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:4;
CREATE t:5 SET xs = [], a = 6;
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:5;
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
SELECT * FROM t;
