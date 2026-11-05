/**
[env]
namespace = true
database = true

[test]
reason = "A `ORDER BY` target is a basic idiom, and its parts must be charged against the expression-depth limit like every other idiom's. A path lowers to one expression node per part and the passes over that tree recurse per node, so an unbounded part run overflows the stack and aborts the process instead of reporting a parse error."

[test.results]
parsing-error = true
*/

-- ~130 parts, past the default expression depth limit (128); a few hundred
-- bytes of text, far under any size limit.
SELECT * FROM t ORDER BY a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a;
