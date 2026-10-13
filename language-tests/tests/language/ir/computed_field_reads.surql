/**
[env]
namespace = true
database = true

[test]
reason = """
A COMPUTED field is derived once per row and every read of it in that row sees that one
derivation: `[a, a]` draws the sequence once. The right operand of AND is read only when the left
operand does not decide the result, a COMPUTED field whose definition raises raises at the read that
reaches it, and a direct call of the same non-deterministic function draws the next value after the
derivations above.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[0, 0]]"

[[test.results]]
value = "[false]"

[[test.results]]
value = "[false]"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "[2]"
*/

DEFINE SEQUENCE s;
DEFINE FIELD a ON doc COMPUTED sequence::nextval('s');
DEFINE FIELD c ON doc COMPUTED 1 + "1";
CREATE doc:1 RETURN NONE;
SELECT VALUE [a, a] FROM doc;
SELECT VALUE (a AND false) FROM doc;
SELECT VALUE (false AND a) FROM doc;
SELECT VALUE (c AND false) FROM doc;
SELECT VALUE sequence::nextval('s') FROM doc;
