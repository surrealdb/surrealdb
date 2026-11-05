/**
[env]
namespace = true
database = true

[test]
reason = "A field, a cast, a function argument and return coerce to a named type and run its checks, and the type's DEFAULT fills a field written NONE"

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
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `p` of `t:1`: Expected `percent (int)` but found `'7'`"

[[test.results]]
value = "[{ id: t:2, p: 50 }]"

[[test.results]]
error = "Couldn't coerce value for field `p` of `t:3`: Expected `percent (constraint in_range failed)` but found `101`"

[[test.results]]
error = "Couldn't coerce value for field `p` of `t:4`: Expected `percent (ASSERT $value != 13 failed)` but found `13`"

[[test.results]]
error = "Couldn't coerce value for field `m` of `t:5`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'meh'`"

[[test.results]]
value = "[{ id: t:6, m: 'ok', p: 1, r: { level: 30, mood: 'happy' } }]"

[[test.results]]
error = "Couldn't coerce value for field `r` of `t:7`: Expected `percent (constraint in_range failed)` but found `300`"

[[test.results]]
error = "Couldn't coerce value for field `ps` of `t:8`: Expected `percent (constraint in_range failed)` but found `200`"

[[test.results]]
value = "42"

[[test.results]]
error = "Could not cast into `percent`: Expected `percent (constraint in_range failed)` but found `420`"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "Could not cast into `mood`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "15"

[[test.results]]
error = "Couldn't coerce return value from function `fn::bump`: Expected `percent (constraint in_range failed)` but found `105`"

[[test.results]]
error = "Incorrect arguments for function fn::bump(). Failed to coerce argument `$x`: Expected `percent (constraint in_range failed)` but found `-1`"

[[test.results]]
value = "[1, 50]"

*/

DEFINE TYPE percent AS int CONSTRAINT in_range ASSERT $value >= 0 AND $value <= 100 ASSERT $value != 13 DEFAULT 50;
DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TYPE reading AS { level: percent, mood: option<mood> };
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD p ON t TYPE percent;
DEFINE FIELD m ON t TYPE option<mood>;
DEFINE FIELD r ON t TYPE option<reading>;
DEFINE FIELD ps ON t TYPE option<array<percent>>;
CREATE t:1 SET p = '7';
CREATE t:2;
CREATE t:3 SET p = 101;
CREATE t:4 SET p = 13;
CREATE t:5 SET p = 1, m = 'meh';
CREATE t:6 SET p = 1, m = 'ok', r = { level: 30, mood: 'happy' };
CREATE t:7 SET p = 1, r = { level: 300 };
CREATE t:8 SET p = 1, ps = [1, 2, 200];
<percent> '42';
<percent> 420;
<mood> 'ok';
<mood> 'nope';
<option<mood>> NONE;
DEFINE FUNCTION fn::bump($x: percent) -> percent { RETURN $x + 10 };
fn::bump(5);
fn::bump(95);
fn::bump(-1);
SELECT VALUE p FROM t ORDER BY p;
