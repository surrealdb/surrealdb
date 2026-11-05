/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 6734
reason = "SECURITY, INVOKER and DEFINER are keywords of DEFINE API and ALTER API only; wherever an identifier is expected they stay ordinary names, so schemas that already use them for tables, fields, params, function arguments and object keys keep parsing, and a handler expression that is itself the identifier `security` does not swallow the clause after it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ definer: 2, id: security:invoker, invoker: 1, security: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ definer: 2, invoker: 1, security: 3 }]"

[[test.results]]
value = "[{ definer: 2, id: security:invoker, invoker: 1, security: 3 }]"

[[test.results]]
value = "{ definer: 3, invoker: 2, security: 1 }"

[[test.results]]
value = "60"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ "/edge": "DEFINE API '/edge' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN security SECURITY INVOKER" }"""

*/

DEFINE TABLE security SCHEMALESS;
DEFINE FIELD invoker ON security TYPE int;
DEFINE FIELD definer ON security TYPE int;
CREATE security:invoker SET invoker = 1, definer = 2, security = 3;
LET $security = 10;
LET $invoker = 20;
LET $definer = 30;
SELECT invoker, definer, security FROM security;
SELECT * FROM security WHERE invoker = 1 AND definer = 2;
{ security: 1, invoker: 2, definer: 3 };
RETURN $security + $invoker + $definer;
DEFINE FUNCTION fn::security($invoker: int, $definer: int) { RETURN $invoker + $definer; };
fn::security(1, 2);
DEFINE API "/edge" FOR get THEN security SECURITY INVOKER;
(INFO FOR DB).apis;
