/**
[env]
namespace = true
database = true

[test]
reason = "A closure's parameters and declared return type, and a function's parameters, are judged against the named types they declare; a parameter left out is judged as NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Failed to coerce argument $x: Expected `percent (ASSERT $value <= 100 failed)` but found `101`"

[[test.results]]
value = "50"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce return value from function `ANONYMOUS`: Expected `percent (ASSERT $value <= 100 failed)` but found `101`"

[[test.results]]
value = "5"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Failed to coerce argument $x: Expected `percent (ASSERT $value <= 100 failed)` but found `500`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Failed to coerce argument $x: Expected `percent (int)` but found `NONE`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function fn::pct(). Failed to coerce argument `$x`: Expected `percent (int)` but found `NONE`"

[[test.results]]
value = "7"
*/

DEFINE TYPE percent AS int ASSERT $value <= 100;
LET $f = |$x: percent| $x;
$f(101);
$f(50.0);
LET $g = |$x: int| -> percent { $x };
$g(101);
$g(5);
LET $h = |$x: option<percent>| $x;
$h();
$h(500);
LET $k = |$x: percent| $x;
$k();
DEFINE FUNCTION fn::pct($x: percent) { RETURN $x; };
fn::pct();
fn::pct(7);
