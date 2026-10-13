/**
[env]
namespace = true
database = true

[test]
reason = "Issue #7408: a field whose name is a reserved keyword must round-trip through the catalog, so INFO FOR TABLE, later DEFINE FIELD on the same table, and reads of the stored data all keep working. A leading keyword is rendered bare by the idiom rendering that stored text uses, so the re-parse has to read it as a field rather than as the statement or call that word would start."
issue = 7408

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON t TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON t TYPE string PERMISSIONS FULL', other: 'DEFINE FIELD other ON t TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON t TYPE string PERMISSIONS FULL', other: 'DEFINE FIELD other ON t TYPE string PERMISSIONS FULL', select: 'DEFINE FIELD `select` ON t TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ function: 'f', id: t:1, other: 'o', select: 3 }]"

[[test.results]]
value = "[{ function: 'f', id: t:1, other: 'o', select: 3 }]"

[[test.results]]
value = "[{ function: 'f', select: 3 }]"

[[test.results]]
value = "[{ function: 'f', id: t:1, other: 'o', select: 3 }]"

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
value = """{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON n TYPE object PERMISSIONS FULL', "function.sub": 'DEFINE FIELD `function`.sub ON n TYPE string PERMISSIONS FULL', meta: 'DEFINE FIELD meta ON n TYPE object PERMISSIONS FULL', "meta.function": 'DEFINE FIELD meta.function ON n TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "[{ function: { sub: 'x' }, id: n:1, meta: { function: 'y' } }]"

[[test.results]]
value = "[{ function: { sub: 'x' }, meta: { function: 'y' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON n TYPE object PERMISSIONS FULL', "function.sub": 'DEFINE FIELD `function`.sub ON n TYPE string PERMISSIONS FULL', meta: 'DEFINE FIELD meta ON n TYPE object PERMISSIONS FULL', "meta.function": 'DEFINE FIELD meta.function ON n TYPE string PERMISSIONS FULL' }, indexes: { idx_kw: 'DEFINE INDEX idx_kw ON n FIELDS `function`.sub' }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "[{ function: { sub: 'x' }, id: n:1, meta: { function: 'y' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON n TYPE object PERMISSIONS FULL', "function.sub": 'DEFINE FIELD `function`.sub ON n TYPE none | string PERMISSIONS FULL', meta: 'DEFINE FIELD meta ON n TYPE object PERMISSIONS FULL', "meta.function": 'DEFINE FIELD meta.function ON n TYPE string PERMISSIONS FULL' }, indexes: { idx_kw: 'DEFINE INDEX idx_kw ON n FIELDS `function`.sub' }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON n TYPE object PERMISSIONS FULL', meta: 'DEFINE FIELD meta ON n TYPE object PERMISSIONS FULL', "meta.function": 'DEFINE FIELD meta.function ON n TYPE string PERMISSIONS FULL' }, indexes: { idx_kw: 'DEFINE INDEX idx_kw ON n FIELDS `function`.sub' }, lives: {  }, tables: {  } }"""

*/

-- The reproduction exactly as reported: defining the field succeeded, and then
-- every read of the table's schema failed.
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD `function` ON t TYPE string;
INFO FOR TABLE t;

-- The schema re-validation that runs before every DEFINE FIELD re-reads the
-- stored fields, so a broken keyword field also broke every later field on the
-- same table.
DEFINE FIELD other ON t TYPE string;
INFO FOR TABLE t;

-- `select` fails differently as an expression (a statement wanting a field
-- list, rather than a call wanting `(`), so it is pinned separately.
DEFINE FIELD `select` ON t TYPE int;
INFO FOR TABLE t;

-- The stored data has to be writable and readable through the keyword names.
CREATE t:1 SET `function` = 'f', other = 'o', `select` = 3;
SELECT * FROM t;
SELECT `function`, `select` FROM t;
SELECT * FROM t WHERE `select` = 3;

-- Only a *leading* keyword is rendered bare; after a dot the `.` disambiguates
-- it. Both halves must work, and the rendering shows the difference.
DEFINE TABLE n SCHEMAFULL;
DEFINE FIELD `function` ON n TYPE object;
DEFINE FIELD `function`.sub ON n TYPE string;
DEFINE FIELD meta ON n TYPE object;
DEFINE FIELD meta.`function` ON n TYPE string;
INFO FOR TABLE n;
CREATE n:1 SET `function` = { sub: 'x' }, meta = { `function`: 'y' };
SELECT `function`.sub, meta.`function` FROM n;

-- An index whose column is a keyword path stores that path as text too, and
-- has to keep resolving well enough for the planner to pick it.
DEFINE INDEX idx_kw ON n FIELDS `function`.sub;
INFO FOR TABLE n;
SELECT * FROM n WHERE `function`.sub = 'x';

-- ALTER and REMOVE both re-read the stored field before rewriting it.
ALTER FIELD `function`.sub ON n TYPE option<string>;
INFO FOR TABLE n;
REMOVE FIELD `function`.sub ON n;
INFO FOR TABLE n;
