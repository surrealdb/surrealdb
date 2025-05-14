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
