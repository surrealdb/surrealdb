/**
[env]
namespace = true
database = true

[test]
reason = "A `LET` that is a whole statement of a request binds a request variable, the same variable a client binds with `set` or with a query's parameters, so it refuses a protected name (`$auth`, `$access`, `$token`, `$session`) and no other. `$value`, `$input`, `$before`, `$after` and `$event` are bound per frame and shadowed by the frame that binds them, so a statement `LET` binds them and a later statement reads them back."

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ ok: true }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ after: 8, before: 7, event: 6, input: 5, value: { ok: true } }"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'access' is a protected variable and cannot be set"

[[test.results]]
error = "'token' is a protected variable and cannot be set"

[[test.results]]
error = "'session' is a protected variable and cannot be set"
*/

LET $value = { ok: true };
RETURN $value;

LET $input = 5;
LET $event = 6;
LET $before = 7;
LET $after = 8;
RETURN { after: $after, before: $before, event: $event, input: $input, value: $value };

LET $auth = 1;
LET $access = 1;
LET $token = 1;
LET $session = 1;
