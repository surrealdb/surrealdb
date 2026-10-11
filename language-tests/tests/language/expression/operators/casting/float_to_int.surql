/**
[env]
namespace = true
database = true

[test]
reason = "Casting a float or decimal to an int succeeds only for a whole number within the i64 range. One beyond the range is refused rather than clamped to i64::MAX or i64::MIN."

[[test.results]]
value = "true"

[[test.results]]
value = "5"

[[test.results]]
value = "0"

[[test.results]]
value = "9007199254740992"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
value = "true"

[[test.results]]
error = "Could not cast into `int` using input `9223372036854776000f`"

[[test.results]]
error = "Could not cast into `int` using input `1000000000000000000000000000000f`"

[[test.results]]
error = "Could not cast into `int` using input `-1000000000000000000000000000000f`"

[[test.results]]
error = "Could not cast into `int` using input `-9223372036854778000f`"

[[test.results]]
error = "Could not cast into `int` using input `0.5f`"

[[test.results]]
error = "Could not cast into `int` using input `NaN`"

[[test.results]]
error = "Could not cast into `int` using input `Infinity`"

[[test.results]]
error = "Could not cast into `int` using input `-Infinity`"

[[test.results]]
value = "true"

[[test.results]]
value = "5"

[[test.results]]
error = "Could not cast into `int` using input `1.5dec`"

[[test.results]]
value = "9223372036854775807"

[[test.results]]
error = "Could not cast into `int` using input `10000000000000000000dec`"

[[test.results]]
value = "true"

[[test.results]]
value = "5"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
error = "Could not cast into `int` using input `9223372036854776000f`"

[[test.results]]
error = "Could not cast into `int` using input `1000000000000000000000000000000f`"

[[test.results]]
value = "[5, 0]"

[[test.results]]
error = "Could not cast into `int` using input `1000000000000000000000000000000f` when casting element at index 1 of `array<int>`"

[[test.results]]
value = "1000000000000000000000000000000f"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Could not cast into `int | bool` using input `1.5dec`"

[[test.results]]
error = "Could not cast into `int | bool` using input `1000000000000000000000000000000f`"
*/

-- Whole floats within the range cast to an int
type::is_int(<int>5.0);
<int>5.0;
<int>-0.0;
<int>9007199254740992.0;
-- -2^63 is i64::MIN, the lowest value an int holds
<int>-9.223372036854775808e18;
type::is_int(<int>-9.223372036854775808e18);

-- Out of range, fractional and non-finite floats are refused
<int>9.223372036854775807e18;
<int>1e30;
<int>-1e30;
<int>-9.223372036854777856e18;
<int>0.5;
<int><float>"NaN";
<int><float>"inf";
<int><float>"-inf";

-- A decimal follows the same rule
type::is_int(<int>5dec);
<int>5dec;
<int>1.5dec;
<int>9223372036854775807dec;
<int>10000000000000000000dec;

-- type::int casts the same way
type::is_int(type::int(5.0));
type::int(5.0);
type::int(-9.223372036854775808e18);
type::int(9.223372036854775807e18);
type::int(1e30);

-- Elements and unions
<array<int>>[5.0, -0.0];
<array<int>>[5.0, 1e30];
<int | float>1e30;
type::is_float(<int | float>1e30);
type::is_float(<int | float>1.5dec);
type::is_int(<int | float>5.0);
<int | bool>1.5dec;
<int | bool>1e30;
