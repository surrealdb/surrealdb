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

# 1: DEFINE FIELD data[$idx]
[[test.results]]
value = "NONE"

# 2: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields['data[0]'] = /DEFINE FIELD data\\[0\\].*TYPE number/"

# 3: ALTER FIELD data[$idx]
[[test.results]]
value = "NONE"

# 4: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields['data[0]'] = /DEFINE FIELD data\\[0\\].*COMMENT 'altered'/"

# 5: REMOVE FIELD data[$idx]
[[test.results]]
value = "NONE"

# 6: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields = {}"

# 7: LET $key = "city"
[[test.results]]
value = "NONE"

# 8: DEFINE FIELD addr[$key]
[[test.results]]
value = "NONE"

# 9: INFO FOR TABLE thing
[[test.results]]
match = "$result.fields['addr.city'] = /DEFINE FIELD addr\\.city.*TYPE string/"

# 10: DEFINE FIELD bad[true]
[[test.results]]
match = "$error = /Field path index must evaluate to an integer/"
error = true

# 11: DEFINE FIELD bad[?id > 0]
[[test.results]]
match = "$error = /not allowed in a field path/"
error = true
*/

-- DEFINE FIELD with a parameterized integer index resolves to a static path.
LET $idx = 0;
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
DEFINE FIELD addr[$key] ON TABLE thing TYPE string;
INFO FOR TABLE thing;

-- Booleans are not valid index/key values.
DEFINE FIELD bad[true] ON TABLE thing TYPE number;

-- Where-clauses inside a field path are not allowed.
DEFINE FIELD bad[?id > 0] ON TABLE thing TYPE number;
