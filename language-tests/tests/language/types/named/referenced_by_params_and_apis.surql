/**
[env]
namespace = true
database = true

[test]
reason = "REMOVE TYPE is refused while a parameter's permission, or an API's handler, fallback or permission, names the type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the permission of the parameter '$p' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the API '/feel' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the API '/feel' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE PARAM $p VALUE 1 PERMISSIONS WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
REMOVE PARAM $p;
DEFINE API "/feel" FOR get THEN { RETURN { status: 200, body: <mood>'ok' } };
REMOVE TYPE mood;
REMOVE API "/feel";
DEFINE API "/feel" FOR get PERMISSIONS WHERE <mood>'ok' = 'ok' THEN { RETURN { status: 200 } };
REMOVE TYPE mood;
REMOVE API "/feel";
REMOVE TYPE mood;
