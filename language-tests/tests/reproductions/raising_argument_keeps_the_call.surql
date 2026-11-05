/**
[env]
namespace = true
database = true

[test]
reason = "a call evaluates every argument before its body, so inlining an argument that can raise must not move or skip its evaluation"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `NONE`"
*/

-- The inline rule substitutes an argument wherever the body names its
-- parameter. Naming is not evaluating: a branch the body does not take skips
-- the argument the call had already evaluated and coerced. `fn::pick(false,
-- <int> NONE)` therefore has to report the cast, as the call does.

DEFINE FUNCTION fn::pick($use: bool, $x: any) { RETURN IF $use THEN $x ELSE 1 END; };

RETURN fn::pick(false, <int> NONE);
