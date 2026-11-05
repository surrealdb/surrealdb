/**
[env]
namespace = true
database = true

[test]
reason = """
`DEFINE FIELD` refuses a change to the value a field derives only where an
aggregated view could hold a group keyed by the value it derived before. These
leave every group as it is, so they are not refused, and the view keeps being
maintained afterwards:

- the table holds no record yet, whatever order the view and the field are
  defined in;
- the definition of the field is the one it replaces;
- the field is one the view does not depend on, or is stored, even where a `COMPUTED`
  field that reads the record by a name it computes sits beside it;
- the view that reads the field is a materialized one, which keys its rows by the
  record's own key.

The tables carry an event, so their writes are observed and `COMPUTED` fields
are derived onto the images the views read.
"""

# The setup of `t`: the view is defined before the field it groups by.
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

# The table holds no record: the field is defined, redefined and defined again as it was.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

# The same definition, another computed field, a new one, and a stored field.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# The view is maintained through all of it.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'z', id: v:['z'], n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# The setup of `u`, and its materialized view.
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
value = "[]"

[[test.results]]
value = "NONE"

# The field the materialized view reads is redefined while `u` holds a record.
[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'y', id: m:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# The setup of `p`: the view reads neither `c` nor the field that reads by a computed name.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

# `c` is redefined while `p` holds a record, and the view is maintained.
[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pv:['h'], k: 'h', n: 1 }]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE option<string>;
DEFINE FIELD b ON t TYPE option<string>;
DEFINE FIELD d ON t COMPUTED b;
DEFINE EVENT seen ON t THEN {};
DEFINE TABLE v AS SELECT count() AS n, c FROM t GROUP BY c;
DEFINE FIELD c ON t COMPUTED b;
DEFINE FIELD OVERWRITE c ON t COMPUTED a;
DEFINE FIELD OVERWRITE c ON t COMPUTED a;
CREATE t:1 SET a = 'x', b = 'y' RETURN NONE;
DEFINE FIELD OVERWRITE c ON t COMPUTED a;
DEFINE FIELD OVERWRITE d ON t COMPUTED a;
DEFINE FIELD e ON t COMPUTED b;
DEFINE FIELD OVERWRITE a ON t TYPE option<string> COMMENT 'stored';
UPDATE t:1 SET b = 'q' RETURN NONE;
UPDATE t:1 SET a = 'z' RETURN NONE;
SELECT * FROM v;
DELETE t:1 RETURN NONE;
SELECT * FROM v;

DEFINE TABLE u SCHEMALESS;
DEFINE FIELD a ON u TYPE option<string>;
DEFINE FIELD b ON u TYPE option<string>;
DEFINE FIELD c ON u COMPUTED a;
DEFINE EVENT seen ON u THEN {};
CREATE u:1 SET a = 'x', b = 'y' RETURN NONE;
DEFINE TABLE m AS SELECT c FROM u;
DEFINE FIELD OVERWRITE c ON u COMPUTED b;
UPDATE u:1 SET a = 'z' RETURN NONE;
SELECT * FROM m;
DELETE u:1 RETURN NONE;
SELECT * FROM m;

DEFINE TABLE p SCHEMALESS;
DEFINE FIELD a ON p TYPE option<string>;
DEFINE FIELD b ON p TYPE option<string>;
DEFINE FIELD k ON p TYPE option<string>;
DEFINE FIELD c ON p COMPUTED a;
DEFINE FIELD d ON p COMPUTED type::field(IF true { 'c' } ELSE { 'x' });
DEFINE EVENT seen ON p THEN {};
DEFINE TABLE pv AS SELECT count() AS n, k FROM p GROUP BY k;
CREATE p:1 SET a = 'x', b = 'y', k = 'g' RETURN NONE;
DEFINE FIELD OVERWRITE c ON p COMPUTED b;
UPDATE p:1 SET k = 'h' RETURN NONE;
SELECT * FROM pv;
