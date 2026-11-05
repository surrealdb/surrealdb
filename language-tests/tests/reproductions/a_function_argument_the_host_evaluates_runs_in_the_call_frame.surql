/**
[test]
reason = """
A user-defined function's arguments are evaluated in the frame the call opens,
including an argument no engine instruction covers and that is handed to the
host to evaluate: a builtin over a field read. The argument's own error, or its
value, is the statement's answer, with or without a row to read the field from.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, s: 'ab' }]"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function record::id(). Argument 1 was the wrong type. Expected `record` but found `NONE`"

[[test.results]]
value = "[{ n: 3 }]"

[[test.results]]
value = "[{ n: 2 }]"
*/

DEFINE FUNCTION fn::inc($x: any) { RETURN $x + 1 };
DEFINE FUNCTION fn::name($p: any) { RETURN $p.name };
CREATE t:1 SET s = 'ab';
RETURN fn::inc(string::len(id));
RETURN fn::name(record::id(id));
SELECT fn::inc(string::len(s)) AS n FROM t;
SELECT fn::inc(record::id(id)) AS n FROM t;
