/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/statements/define/function/_small_body_functions.surql"]

[test]
reason = """
A function whose body is a small leaf — one run of LET, IF and RETURN over its
arguments that reads no storage, no parameter and calls no other unit — is
called by the lighter convention: the body runs in the frame the call entered,
with its arguments in its slots, and no frame is built for the call.
The call keeps its boundary whole: the arguments are coerced to their kinds,
the value returned to the declared kind, a THROW and an arithmetic error in
the body are raised as a called body raises them, and the body reads the
caller's row as its document.
"""

[[test.results]]
value = "[false, true, true]"

[[test.results]]
value = "['low', 'mid', 'high']"

[[test.results]]
error = "Incorrect arguments for function fn::band(). Failed to coerce argument `$x`: Expected `int` but found `'seven'`"

[[test.results]]
error = "Couldn't coerce return value from function `fn::wrong`: Expected `int` but found `'4'`"

[[test.results]]
value = "[1, 2]"

[[test.results]]
error = "An error occurred: too big: 3"

[[test.results]]
error = "Cannot perform multiplication with '2' and '9223372036854775807'"

[[test.results]]
value = "[{ band: 'mid', doc: 'a!', id: t:1, over: true }, { band: 'low', doc: 'b!', id: t:2, over: false }]"

[[test.results]]
value = "[t:1]"
*/

RETURN [fn::over(10), fn::over(100), fn::over(75.5)];
RETURN [fn::band(3), fn::band(30), fn::band(300)];
RETURN fn::band('seven');
RETURN fn::wrong(4);
RETURN [fn::refuse(1), fn::refuse(2)];
RETURN fn::refuse(3);
RETURN fn::overflow(2);
SELECT id, fn::over(n) AS over, fn::band(n) AS band, fn::doc() AS doc FROM t ORDER BY id;
SELECT VALUE id FROM t WHERE fn::over(n);
