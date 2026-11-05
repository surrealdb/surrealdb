/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE FUNCTION IF NOT EXISTS that meets the function stores nothing, so the types its body names are not judged; a definition that is stored is judged"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
error = "Invalid statement: The function 'fn::g' cannot be defined: the type 'missing' it names does not exist"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION IF NOT EXISTS fn::f() { RETURN <missing>'x' };
fn::f();
DEFINE FUNCTION IF NOT EXISTS fn::g() { RETURN <missing>'x' };
