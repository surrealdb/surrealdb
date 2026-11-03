/**
[env]
namespace = true
database = true

[test]
reason = "A type's PERMISSIONS predicate is judged for the types it names: a missing one refuses the definition, and one it names cannot be removed while it does; the predicate may name the type itself"

[[test.results]]
error = "Invalid statement: The type 'visible' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the type 'visible' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TYPE visible AS 'yes' PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE TYPE visible AS 'yes' PERMISSIONS WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
DEFINE TYPE selfish AS 'me' PERMISSIONS WHERE <selfish>'me' = 'me';
REMOVE TYPE visible;
REMOVE TYPE mood;
