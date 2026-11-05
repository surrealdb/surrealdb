/**
[env]
namespace = true
database = true

[test]
reason = "REMOVE TYPE is refused while a field's REFERENCE ON DELETE THEN or MATERIALIZE WHERE, an index's COUNT condition, a bucket's permission or the API config's permission names the type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': an expression of the field 'p' on the table 'k' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': an expression of the field 'p' on the table 'k' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the COUNT WHERE condition of the index 'c' on the table 'k' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the permission of the bucket 'b' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the permission of the API config names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE TABLE u;
DEFINE FIELD kids ON u TYPE option<array<record<k>>> READONLY;
DEFINE FIELD p ON k TYPE option<record<u>> REFERENCE ON DELETE THEN (UPDATE $this SET m = <mood>'ok');
REMOVE TYPE mood;
REMOVE FIELD p ON k;
DEFINE FIELD p ON k TYPE option<record<u>> REFERENCE MATERIALIZE INTO u.kids WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
REMOVE FIELD p ON k;
DEFINE INDEX c ON k COUNT WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
REMOVE INDEX c ON k;
DEFINE BUCKET b BACKEND "memory" PERMISSIONS WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
REMOVE BUCKET b;
DEFINE CONFIG API PERMISSIONS WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
REMOVE CONFIG API;
REMOVE TYPE mood;
