/**
[env]
namespace = true
database = true

[test]
reason = "Named enum membership is judged for function arguments and returns, LET bindings and casts to optional and array types, and REMOVE TYPE is refused while a function names the enum"

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
error = "Incorrect arguments for function fn::cheer(). Failed to coerce argument `$m`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'ecstatic'`"

[[test.results]]
error = "Couldn't coerce return value from function `fn::pick`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "['sad', 'ok']"

[[test.results]]
error = "Incorrect arguments for function fn::all(). Failed to coerce argument `$ms`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ok'"

[[test.results]]
value = "'happy'"

[[test.results]]
error = "Tried to set `$n`, but couldn't coerce value: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "Could not cast into `none | mood`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['sad', 'happy']"

[[test.results]]
error = "Could not cast into `array<mood>`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
error = "Could not cast into `mood`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the function 'fn::all' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE FUNCTION fn::cheer($m: mood) { RETURN $m; };
DEFINE FUNCTION fn::pick() -> mood { RETURN 'nope'; };
DEFINE FUNCTION fn::all($ms: array<mood>) { RETURN $ms; };
RETURN fn::cheer('happy');
RETURN fn::cheer('ecstatic');
RETURN fn::pick();
RETURN fn::all(['sad', 'ok']);
RETURN fn::all(['sad', 'nope']);
LET $m: mood = 'ok';
RETURN $m;
{ LET $k: mood = 'happy'; RETURN $k; };
{ LET $n: mood = 'nope'; RETURN $n; };
RETURN <option<mood>> 'ok';
RETURN <option<mood>> 'nope';
RETURN <option<mood>> NONE;
RETURN <array<mood>> ['sad', 'happy'];
RETURN <array<mood>> ['sad', 'nope'];
RETURN <mood> 'nope';
REMOVE TYPE mood;
REMOVE FUNCTION fn::cheer;
REMOVE FUNCTION fn::all;
REMOVE FUNCTION fn::pick;
REMOVE TYPE mood;
