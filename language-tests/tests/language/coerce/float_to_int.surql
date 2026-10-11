/**
[env]
namespace = true
database = true

[test]
reason = "A float or decimal coerces to an int only when it is a whole number within the i64 range. One beyond the range is refused rather than clamped to i64::MAX or i64::MIN, in a field, a function argument, an array element and a union."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:whole, n: 5 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: t:neg_zero, n: 0 }]"

[[test.results]]
value = "0"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: t:exact_limit, n: 9007199254740992 }]"

[[test.results]]
value = "9007199254740992"

[[test.results]]
value = "[{ id: t:lowest, n: -9223372036854775808 }]"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
value = "true"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:two_pow_63`: Expected `int` but found `9223372036854776000f`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:huge`: Expected `int` but found `1000000000000000000000000000000f`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:negative_huge`: Expected `int` but found `-1000000000000000000000000000000f`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:below_lowest`: Expected `int` but found `-9223372036854778000f`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:fraction`: Expected `int` but found `0.5f`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:nan`: Expected `int` but found `NaN`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:infinity`: Expected `int` but found `Infinity`"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:negative_infinity`: Expected `int` but found `-Infinity`"

[[test.results]]
value = "[t:exact_limit, t:lowest, t:neg_zero, t:whole]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 5, id: dt:whole }]"

[[test.results]]
value = "true"

[[test.results]]
error = "Couldn't coerce value for field `d` of `dt:fraction`: Expected `int` but found `1.5dec`"

[[test.results]]
error = "Couldn't coerce value for field `d` of `dt:beyond`: Expected `int` but found `10000000000000000000dec`"

[[test.results]]
value = "[dt:whole]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
error = "Incorrect arguments for function fn::takes_int(). Failed to coerce argument `$x`: Expected `int` but found `9223372036854776000f`"

[[test.results]]
error = "Incorrect arguments for function fn::takes_int(). Failed to coerce argument `$x`: Expected `int` but found `1000000000000000000000000000000f`"

[[test.results]]
error = "Incorrect arguments for function fn::takes_int(). Failed to coerce argument `$x`: Expected `int` but found `-1000000000000000000000000000000f`"

[[test.results]]
value = "[2]"

# The legacy engine words this error with "function slice()" and the
# planner with "method slice()", so only the refusal is pinned here.
[[test.results]]
error = true

[[test.results]]
error = "Incorrect arguments for function string::slice(). Argument 3 was the wrong type. Expected `int` but found `1000000000000000000000000000000f`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ arr: [1, 2, 0], id: a:ok }]"

[[test.results]]
value = "[1, 2, 0]"

[[test.results]]
error = "Couldn't coerce value for field `arr` of `a:huge`: Expected `int` but found `1000000000000000000000000000000f` when coercing element at index 1 of `array<int>`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `opt` of `o:huge`: Expected `none | int` but found `1000000000000000000000000000000f`"

[[test.results]]
error = "Couldn't coerce value for field `opt` of `o:fraction`: Expected `none | int` but found `1.5dec`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ either: 5, id: e:whole }]"

[[test.results]]
value = "true"

[[test.results]]
error = "Couldn't coerce value for field `either` of `e:huge`: Expected `int | string` but found `1000000000000000000000000000000f`"

[[test.results]]
error = "Couldn't coerce value for field `either` of `e:fraction`: Expected `int | string` but found `1.5dec`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:huge, wide: 1000000000000000000000000000000f }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: w:fraction, wide: 1.5f }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: w:whole, wide: 5 }]"

[[test.results]]
value = "true"
*/

-- A field typed `int`: whole floats within the range coerce to an int
DEFINE FIELD n ON t TYPE int;
CREATE t:whole SET n = 5.0;
type::is_int((SELECT VALUE n FROM ONLY t:whole));
CREATE t:neg_zero SET n = -0.0;
(SELECT VALUE n FROM ONLY t:neg_zero);
type::is_int((SELECT VALUE n FROM ONLY t:neg_zero));
CREATE t:exact_limit SET n = 9007199254740992.0;
(SELECT VALUE n FROM ONLY t:exact_limit);
-- -2^63 is i64::MIN, the lowest value an int holds
CREATE t:lowest SET n = -9.223372036854775808e18;
(SELECT VALUE n FROM ONLY t:lowest);
type::is_int((SELECT VALUE n FROM ONLY t:lowest));

-- Out of range, fractional and non-finite floats are refused
CREATE t:two_pow_63 SET n = 9.223372036854775807e18;
CREATE t:huge SET n = 1e30;
CREATE t:negative_huge SET n = -1e30;
CREATE t:below_lowest SET n = -9.223372036854777856e18;
CREATE t:fraction SET n = 0.5;
CREATE t:nan SET n = <float>"NaN";
CREATE t:infinity SET n = <float>"inf";
CREATE t:negative_infinity SET n = <float>"-inf";
-- Only the four rows that coerced were written
SELECT VALUE id FROM t ORDER BY id;

-- A decimal follows the same rule
DEFINE FIELD d ON dt TYPE int;
CREATE dt:whole SET d = 5dec;
type::is_int((SELECT VALUE d FROM ONLY dt:whole));
CREATE dt:fraction SET d = 1.5dec;
CREATE dt:beyond SET d = 10000000000000000000dec;
SELECT VALUE id FROM dt;

-- A user-defined function argument typed `int`
DEFINE FUNCTION fn::takes_int($x: int) { RETURN $x; };
type::is_int(fn::takes_int(5.0));
fn::takes_int(-9.223372036854775808e18);
fn::takes_int(9.223372036854775807e18);
fn::takes_int(1e30);
fn::takes_int(-1e30);

-- A built-in function argument typed as an int
[1, 2, 3].slice(1, 2.0);
[1, 2, 3].slice(1, 1e30);
string::slice("abc", 0, 1e30);

-- Elements, optionals and unions take the same rule
DEFINE FIELD arr ON a TYPE array<int>;
CREATE a:ok SET arr = [1.0, 2.0, -0.0];
(SELECT VALUE arr FROM ONLY a:ok);
CREATE a:huge SET arr = [1, 1e30];
DEFINE FIELD opt ON o TYPE option<int>;
CREATE o:huge SET opt = 1e30;
CREATE o:fraction SET opt = 1.5dec;
DEFINE FIELD either ON e TYPE int | string;
CREATE e:whole SET either = 5.0;
type::is_int((SELECT VALUE either FROM ONLY e:whole));
CREATE e:huge SET either = 1e30;
CREATE e:fraction SET either = 1.5dec;
-- A union that has a member able to hold the value still reaches it
DEFINE FIELD wide ON w TYPE int | float;
CREATE w:huge SET wide = 1e30;
type::is_float((SELECT VALUE wide FROM ONLY w:huge));
CREATE w:fraction SET wide = 1.5dec;
type::is_float((SELECT VALUE wide FROM ONLY w:fraction));
CREATE w:whole SET wide = 5.0;
type::is_int((SELECT VALUE wide FROM ONLY w:whole));
