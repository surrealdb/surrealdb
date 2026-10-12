/**
[env]
namespace = true
database = true

[test]
reason = "An import replays field definitions as written, including fields whose parent was never defined, which a 3.x export can hold; outside an import they are refused, and a replayed one can still be redefined and removed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `c.d`: the field `c` it is nested in is not defined. Define `c` first"

[[test.results]]
value = "['a.b', 'n', 'n.m']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

OPTION IMPORT;
DEFINE TABLE legacy SCHEMALESS;
DEFINE FIELD a.b ON legacy TYPE string;
DEFINE FIELD n ON legacy TYPE int;
DEFINE FIELD n.m ON legacy;
OPTION IMPORT = false;
DEFINE FIELD c.d ON legacy TYPE string;
object::keys((INFO FOR TABLE legacy).fields);
DEFINE FIELD OVERWRITE a.b ON legacy TYPE int;
REMOVE FIELD a.b ON legacy;
REMOVE FIELD n ON legacy;
object::keys((INFO FOR TABLE legacy).fields);
