/**
[test]
reason = """
The values and errors the expression operators produce, as the bytecode
instructions for them must reproduce: what AND, OR, ?? and ?: return and
which operands they evaluate, integer overflow and division by zero, NONE
and NULL through arithmetic and comparison, string ordering, field access
past a missing value, and which error a composite value reports first.
"""

[[test.results]]
value = "2"

[[test.results]]
value = "0"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'b'"

[[test.results]]
value = "3"

[[test.results]]
value = "NULL"

[[test.results]]
value = "[1]"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "'x'"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '1'"

[[test.results]]
error = "Cannot perform subtraction with '-9223372036854775808' and '1'"

[[test.results]]
error = "Cannot perform multiplication with '9223372036854775807' and '2'"

[[test.results]]
value = "3"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Cannot perform remainder with '7' and '0'"

[[test.results]]
value = "2.5f"

[[test.results]]
value = "2dec"

[[test.results]]
error = "Cannot perform addition with 'none' and 'int'"

[[test.results]]
error = "Cannot perform addition with 'null' and 'int'"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Cannot perform addition with 'string' and 'int'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

RETURN 1 AND 2;
RETURN 0 AND 2;
RETURN 'a' OR 'b';
RETURN '' OR 'b';
RETURN NONE OR 3;
RETURN NULL AND 1;
RETURN [] OR [1];
RETURN false AND <int> 'x';
RETURN true OR <int> 'x';
RETURN true AND <int> 'x';
RETURN NONE ?? 1;
RETURN 0 ?? 1;
RETURN 0 ?: 1;
RETURN NULL ?: 'x';
RETURN 9223372036854775807 + 1;
RETURN -9223372036854775808 - 1;
RETURN 9223372036854775807 * 2;
RETURN 7 / 2;
RETURN 7 / 0;
RETURN 7 % 0;
RETURN 1 + 1.5;
RETURN 1 + 1dec;
RETURN NONE + 1;
RETURN NULL + 1;
RETURN NONE = NULL;
RETURN NONE < 1;
RETURN NULL < 1;
RETURN 'a' + 1;
RETURN 1 + 'a';
RETURN 'B' < 'a';
RETURN 10 < '9';
RETURN 1 = 1.0;
RETURN { a: 1 }.b;
RETURN { a: 1 }.b.c;
RETURN [<int> 'x', <float> 'y'];
RETURN { a: <int> 'x', b: <float> 'y' };
