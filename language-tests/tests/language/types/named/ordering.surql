/**
[env]
namespace = true
database = true

[test]
reason = "A field of a union type orders by declaration, a field of another named type orders by value, and enum::* functions refuse a type that is not a union"

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
value = "[{ id: t:1, m: 'happy', n: 3 }]"

[[test.results]]
value = "[{ id: t:2, m: 'sad', n: 1 }]"

[[test.results]]
value = "[{ id: t:3, m: 'ok', n: 2 }]"

[[test.results]]
value = "['sad', 'ok', 'happy']"

[[test.results]]
value = "['ok', 'happy']"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "['sad', 'ok', 'happy']"

[[test.results]]
value = "1"

[[test.results]]
value = "'happy'"

[[test.results]]
error = "The type 'small' is not a union of string labels, so it has no order: it is int"

[[test.results]]
error = "The type 'small' is not a union of string labels, so it has no order: it is int"

*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TYPE small AS int ASSERT $value < 100;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD m ON t TYPE mood;
DEFINE FIELD n ON t TYPE small;
CREATE t:1 SET m = 'happy', n = 3;
CREATE t:2 SET m = 'sad', n = 1;
CREATE t:3 SET m = 'ok', n = 2;
SELECT VALUE m FROM t ORDER BY m;
SELECT VALUE m FROM t WHERE m > 'sad' ORDER BY m;
SELECT VALUE n FROM t ORDER BY n;
SELECT VALUE n FROM t WHERE n > 1 ORDER BY n;
enum::values('mood');
enum::rank('mood', 'ok');
enum::max('mood', ['sad', 'happy', 'ok']);
enum::values('small');
enum::rank('small', 3);
