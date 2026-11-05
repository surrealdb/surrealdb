/**
[test]
reason = "While an enforced table's rows are behind a migration step, a change of its TYPE, or into a view, is refused for the step only once every refusal that does not lift by itself has passed: the step's refusal lifts when the rewrite job finishes, so a caller told to wait must not be refused for something else afterwards. A table whose record id names its components cannot become a view or a relation, and a PERMISSIONS clause with an effect is refused; both are raised ahead of the step, on DEFINE TABLE as on ALTER TABLE, and the table, its rows and its steps are left as they were."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot make `rd` a view: its record id has named components"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "Cannot migrate table `rd`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `rd`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
value = "[{ a: 5, city: 'ldn', id: rd:['ldn', 1], n: 1 }, { a: -1, city: 'ldn', id: rd:['ldn', 2], n: 2 }]"

[[test.results]]
value = "'DEFINE TABLE rd TYPE NORMAL SCHEMAFULL PERMISSIONS NONE'"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot make `em` a relation table: its record id has named components"

[[test.results]]
error = "Cannot make `em` a relation table: its record id has named components"

[[test.results]]
error = "Cannot make `em` a view: its record id has named components"

[[test.results]]
error = "Cannot migrate table `em`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `em`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
value = "'DEFINE TABLE em TYPE NORMAL SCHEMAFULL PERMISSIONS NONE'"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"
*/
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
CREATE src:1 SET a = '7' RETURN NONE;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
-- An enforced table whose record id names its components, with rows behind a step.
DEFINE TABLE rd SCHEMAFULL;
DEFINE FIELD id ON rd TYPE [city: string, n: int];
DEFINE FIELD a ON rd TYPE string;
CREATE rd:['ldn', 1] SET a = '5' RETURN NONE;
CREATE rd:['ldn', 2] SET a = 'junk' RETURN NONE;
ALTER FIELD a ON rd TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
-- Becoming a view is refused for the named record id, which waiting would not change.
DEFINE TABLE OVERWRITE rd AS SELECT a FROM src;
-- A TYPE the rows admit, with a PERMISSIONS clause whose call writes, is refused for the clause.
ALTER TABLE rd TYPE ANY PERMISSIONS FOR select WHERE fn::writes();
DEFINE TABLE OVERWRITE rd TYPE ANY SCHEMAFULL PERMISSIONS FOR select WHERE fn::writes();
-- With nothing else to refuse, the step refuses the TYPE.
ALTER TABLE rd TYPE ANY;
DEFINE TABLE OVERWRITE rd TYPE ANY SCHEMAFULL;
-- The table, its rows and its steps are as they were.
SELECT * FROM rd ORDER BY id;
(INFO FOR DB).tables.rd;
(INFO FOR TABLE rd).migrations.migrations;
SELECT * FROM log;
-- With no rows, its steps awaiting the job: becoming a relation is refused for the named record id.
DEFINE TABLE em SCHEMAFULL;
DEFINE FIELD id ON em TYPE [city: string, n: int];
DEFINE FIELD a ON em TYPE string;
ALTER FIELD a ON em TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER TABLE em TYPE RELATION;
DEFINE TABLE OVERWRITE em TYPE RELATION SCHEMAFULL;
DEFINE TABLE OVERWRITE em AS SELECT a FROM src;
ALTER TABLE em TYPE ANY;
DEFINE TABLE OVERWRITE em TYPE ANY SCHEMAFULL;
(INFO FOR DB).tables.em;
(INFO FOR TABLE em).migrations.migrations;
