/**
[env]
namespace = true
database = true

[test]
reason = "Named enum membership is judged for function arguments and returns, LET bindings and casts to optional and array types, and REMOVE ENUM is refused while a function names the enum"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'happy'"

[[test.results]]
error = "Incorrect arguments for function fn::cheer(). Failed to coerce argument `$m`: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'ecstatic'`"

[[test.results]]
error = "Couldn't coerce return value from function `fn::pick`: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'nope'`"

[[test.results]]
value = "['sad', 'ok']"

[[test.results]]
error = "Incorrect arguments for function fn::all(). Failed to coerce argument `$ms`: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'nope'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ok'"

[[test.results]]
value = "'happy'"

[[test.results]]
error = "Tried to set `$n`, but couldn't coerce value: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'nope'`"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "Could not cast into `none | enum<mood>`: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'nope'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['sad', 'happy']"

[[test.results]]
error = "Could not cast into `array<enum<mood>>`: Expected `enum<mood>, one of 'sad', 'ok', 'happy'` but found `'nope'`"

[[test.results]]
error = "Could not cast into `enum<mood>` using input `'nope'`; its values are 'sad', 'ok', 'happy'"

[[test.results]]
error = "Invalid statement: Cannot remove the enum 'mood': the function 'fn::all' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE ENUM mood VALUES 'sad', 'ok', 'happy';
DEFINE FUNCTION fn::cheer($m: enum<mood>) { RETURN $m; };
DEFINE FUNCTION fn::pick() -> enum<mood> { RETURN 'nope'; };
DEFINE FUNCTION fn::all($ms: array<enum<mood>>) { RETURN $ms; };
RETURN fn::cheer('happy');
RETURN fn::cheer('ecstatic');
RETURN fn::pick();
RETURN fn::all(['sad', 'ok']);
RETURN fn::all(['sad', 'nope']);
LET $m: enum<mood> = 'ok';
RETURN $m;
{ LET $k: enum<mood> = 'happy'; RETURN $k; };
{ LET $n: enum<mood> = 'nope'; RETURN $n; };
RETURN <option<enum<mood>>> 'ok';
RETURN <option<enum<mood>>> 'nope';
RETURN <option<enum<mood>>> NONE;
RETURN <array<enum<mood>>> ['sad', 'happy'];
RETURN <array<enum<mood>>> ['sad', 'nope'];
RETURN <enum<mood>> 'nope';
REMOVE ENUM mood;
REMOVE FUNCTION fn::cheer;
REMOVE FUNCTION fn::all;
REMOVE FUNCTION fn::pick;
REMOVE ENUM mood;
