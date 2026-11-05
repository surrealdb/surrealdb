/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE TYPE OVERWRITE judges a stored value of an id field, or of a field at a path inside id, by the record's key: an array key judged against a tuple type, an object key's field and an array key's element. A replacement every key admits is stored; one a key does not admit is refused"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'pos': the field 'id' on the table 'u' holds a value it does not admit: Expected `pos (ASSERT $value > 0 failed)` but found `-5`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'pos': the field 'id.n' on the table 'v' holds a value it does not admit: Expected `pos (ASSERT $value > 0 failed)` but found `-5`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'pos': the field 'id[0]' on the table 'w' holds a value it does not admit: Expected `pos (ASSERT $value > 0 failed)` but found `-5`"

[[test.results]]
value = "{ pos: 'DEFINE TYPE pos AS int PERMISSIONS FULL' }"

*/
DEFINE TYPE pos AS int;
DEFINE FIELD id ON u TYPE [pos, string];
CREATE u:[5, 'x'] RETURN NONE;
DEFINE TYPE OVERWRITE pos AS int ASSERT $value > 0;
DEFINE TYPE OVERWRITE pos AS int;
CREATE u:[-5, 'x'] RETURN NONE;
DEFINE TYPE OVERWRITE pos AS int ASSERT $value > 0;
REMOVE TABLE u;
DEFINE FIELD id ON v TYPE object;
DEFINE FIELD id.n ON v TYPE pos;
CREATE v:{ n: 5 } RETURN NONE;
DEFINE TYPE OVERWRITE pos AS int ASSERT $value > 0;
DEFINE TYPE OVERWRITE pos AS int;
CREATE v:{ n: -5 } RETURN NONE;
DEFINE TYPE OVERWRITE pos AS int ASSERT $value > 0;
REMOVE TABLE v;
DEFINE FIELD id ON w TYPE array;
DEFINE FIELD id[0] ON w TYPE pos;
CREATE w:[5, 'x'] RETURN NONE;
DEFINE TYPE OVERWRITE pos AS int ASSERT $value > 0;
DEFINE TYPE OVERWRITE pos AS int;
CREATE w:[-5, 'x'] RETURN NONE;
DEFINE TYPE OVERWRITE pos AS int ASSERT $value > 0;
(INFO FOR DB).types;
