/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE/ALTER/REMOVE FIELD accept parameterized bracket indices and substitute them to literals before storing on disk"

# 0: LET $idx = 0
[[test.results]]
value = "NONE"

# 1: DEFINE FIELD data — the array the index is in
[[test.results]]
value = "NONE"

# 2: DEFINE FIELD data[$idx]
[[test.results]]
value = "NONE"

# 3: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields['data[0]'] = /DEFINE FIELD data\\[0\\].*TYPE number/"

# 4: ALTER FIELD data[$idx]
[[test.results]]
value = "NONE"

# 5: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields['data[0]'] = /DEFINE FIELD data\\[0\\].*COMMENT 'altered'/"

# 6: REMOVE FIELD data[$idx]
[[test.results]]
value = "NONE"

# 7: INFO FOR TABLE thing
[[test.results]]
match = "object::keys($result.fields) = ['data']"

# 8: LET $key = "city"
[[test.results]]
value = "NONE"

# 9: DEFINE FIELD addr — the object the key is in
[[test.results]]
value = "NONE"

# 10: DEFINE FIELD addr[$key]
[[test.results]]
value = "NONE"

# 11: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields['addr.city'] = /DEFINE FIELD addr\\.city.*TYPE string/"

# 12: DEFINE FIELD bad[true]
[[test.results]]
match = "$error = /Field path index must evaluate to an integer/"
error = true

# 13: DEFINE FIELD bad[?id > 0]
[[test.results]]
match = "$error = /not allowed in a field path/"
error = true
*/

-- DEFINE FIELD with a parameterized integer index resolves to a static path.
LET $idx = 0;
DEFINE FIELD data ON TABLE thing TYPE array;
DEFINE FIELD data[$idx] ON TABLE thing TYPE number;
INFO FOR TABLE thing;

-- ALTER FIELD with the same parameter must find and update the same key.
ALTER FIELD data[$idx] ON TABLE thing COMMENT 'altered';
INFO FOR TABLE thing;

-- REMOVE FIELD with the same parameter must find and remove the same key.
REMOVE FIELD data[$idx] ON TABLE thing;
INFO FOR TABLE thing;

-- A parameterized string key resolves to an object key.
LET $key = "city";
DEFINE FIELD addr ON TABLE thing TYPE object;
DEFINE FIELD addr[$key] ON TABLE thing TYPE string;
INFO FOR TABLE thing;

-- Booleans are not valid index/key values.
DEFINE FIELD bad[true] ON TABLE thing TYPE number;

-- Where-clauses inside a field path are not allowed.
DEFINE FIELD bad[?id > 0] ON TABLE thing TYPE number;
