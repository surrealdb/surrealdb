/**
[env]
imports = ["upgrade/define/3.00.00_field_keyword_name_import.surql"]

[test]
upgrade = true
# The upgrade suite hops through adjacent version pairs, and every release below
# 3.4.0 reads a stored keyword field name as an expression and keys the element
# definitions of a keyword-named array field where no statement addresses them —
# so a hop that ends below it, such as 3.0.0 => 3.0.1, would be asserting the
# schema loads on a version that cannot load it. Only a hop that lands on 3.4.0
# or later can.
version = ">=3.4.0"
reason = """
Issue #7408: a field name that is a reserved keyword is stored bare, so a version that re-reads
the stored schema as an expression cannot load the table at all. Reading such a definition back
must work, and the table must be writable again, so that a database an older version left
unreadable recovers on upgrade.

The definition an array field's kind implies for its elements was keyed by the escaped rendering
of its path, `` `select`.* ``, where every statement addresses the definition of a path under
its storage name, `select.*`. Such a definition must be addressable after the upgrade:
overwritten in place rather than defined a second time beside the one the old binary wrote,
removed by REMOVE FIELD on its path, and removed with the field it belongs to."""

[[test.results]]
value = "{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON kwfn TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: {  }, fields: { select: 'DEFINE FIELD `select` ON kwsel TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ function: 'f', id: kwfn:1 }]"

[[test.results]]
value = "[{ function: 'f', id: kwfn:1 }]"

[[test.results]]
value = "['`select`', '`select`.*']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['`select`', '`select`.*']"

[[test.results]]
value = "[{ id: kwarr:1, select: ['a'] }]"

[[test.results]]
error = "Found '' for field ``select`.*`, with record `kwarr:2`, but field must conform to: $value != ''"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['`select`']"

[[test.results]]
value = "[{ id: kwarr:3, select: [''] }]"

[[test.results]]
value = "['`value`', '`value`.*']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
INFO FOR TABLE kwfn;
INFO FOR TABLE kwsel;
CREATE kwfn:1 SET `function` = 'f';
SELECT * FROM kwfn;
-- The element definition the old binary wrote is read back: one definition of
-- each path.
array::sort((INFO FOR TABLE kwarr STRUCTURE).fields.name);
-- Overwriting it replaces it, leaving one definition of each path.
DEFINE FIELD OVERWRITE `select`.* ON kwarr TYPE string ASSERT $value != '';
array::sort((INFO FOR TABLE kwarr STRUCTURE).fields.name);
-- A write is checked against the definition that replaced it.
CREATE kwarr:1 SET `select` = ['a'];
CREATE kwarr:2 SET `select` = [''];
-- Removing it leaves no definition of the path, and writes are no longer
-- checked against one.
REMOVE FIELD `select`.* ON kwarr;
array::sort((INFO FOR TABLE kwarr STRUCTURE).fields.name);
CREATE kwarr:3 SET `select` = [''];
-- Removing the field removes the element definition the old binary wrote for it.
array::sort((INFO FOR TABLE kwrm STRUCTURE).fields.name);
REMOVE FIELD `value` ON kwrm;
(INFO FOR TABLE kwrm STRUCTURE).fields;
