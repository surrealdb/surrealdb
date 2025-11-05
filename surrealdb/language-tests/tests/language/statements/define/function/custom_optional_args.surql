/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'test'"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function fn::one_arg(). The function expects 1 argument."

[[test.results]]
error = "Incorrect arguments for function fn::last_option(). The function expects 1 to 2 arguments."

[[test.results]]
error = "Incorrect arguments for function fn::middle_option(). The function expects 3 arguments."

[[test.results]]
error = "Incorrect arguments for function fn::zero_arg(). The function expects 0 arguments."

[[test.results]]
value = "'other'"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[true, NONE]"

[[test.results]]
value = "[true, false]"

[[test.results]]
value = "[true, false, true]"

[[test.results]]
value = "[true, NONE, true]"

*/
DEFINE FUNCTION fn::any_arg($a: any) { $a || 'test' };
DEFINE FUNCTION fn::zero_arg() { [] };
DEFINE FUNCTION fn::one_arg($a: bool) { [$a] };
DEFINE FUNCTION fn::last_option($a: bool, $b: option<bool>) { [$a, $b] };
DEFINE FUNCTION fn::middle_option($a: bool, $b: option<bool>, $c: bool) { [$a, $b, $c] };

RETURN fn::any_arg();
RETURN fn::zero_arg();
RETURN fn::one_arg();
RETURN fn::last_option();
RETURN fn::middle_option();

RETURN fn::zero_arg(true);
RETURN fn::any_arg('other');
RETURN fn::one_arg(true);
RETURN fn::last_option(true);
RETURN fn::last_option(true, false);
RETURN fn::middle_option(true, false, true);
RETURN fn::middle_option(true, NONE, true);
