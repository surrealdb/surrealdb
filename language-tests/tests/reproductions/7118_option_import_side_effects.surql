/**
[env]
namespace = true
database = true

[test]
reason = "Users should be able to use OPTION IMPORT for schema/data, then disable it for side-effect queries in a single file"
issue = 7118

[[test.results]]
value = "NONE"

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
value = "[{ id: t:2, name: 'second' }]"

[[test.results]]
value = "[{ id: t:1, name: 'first' }, { id: t:2, name: 'second' }]"

[[test.results]]
match = "array::len($result) == 1 AND $result[0].what == t:2"
error = false

*/

-- Phase 1: Import schema and data with OPTION IMPORT (suppresses side effects)
OPTION IMPORT;
DEFINE TABLE t TYPE NORMAL SCHEMALESS;
DEFINE FIELD name ON t TYPE string;
DEFINE EVENT log ON t WHEN $event = "CREATE" THEN { CREATE log SET what = $value.id };
CREATE t:1 SET name = 'first';

-- Phase 2: Disable import mode so side effects trigger
OPTION IMPORT = FALSE;
CREATE t:2 SET name = 'second';

-- Phase 3: Verify - t:1 was created during import (no event fired), t:2 should trigger event
SELECT * FROM t ORDER BY id;
SELECT * FROM log;
