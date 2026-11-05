/**
[test]
reason = "Setup for omitted_named_argument_is_judged_when_inlined: a named type and small pure functions whose parameters name it"
run = false
*/

DEFINE TYPE posint AS int ASSERT $value > 0;
DEFINE FUNCTION fn::f($n: posint) { RETURN 1 };
DEFINE FUNCTION fn::g($n: option<posint>) { RETURN 1 };
