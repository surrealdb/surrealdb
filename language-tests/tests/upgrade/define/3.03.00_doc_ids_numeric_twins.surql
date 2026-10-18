/**
[env]
imports = ["upgrade/define/3.03.00_doc_ids_numeric_twins_import.surql"]

[test]
upgrade = true
introduced = ">=3.3.0"
reason = """
Records an earlier release mapped in a table's shared doc-ID space keep their
doc-IDs on the new binary, which resolves through `!dj` alone, and two whose ids
differ only in a nested number's kind stay apart: each matches its own terms,
deleting one leaves the other found, and the deleted one can be re-created and
found again.

Kinds are asserted with `type::is_int`, because the runner compares record ids
across numeric kinds."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, true], [2, true]]"

[[test.results]]
value = "[[1, false]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1, false]]"

[[test.results]]
value = "[[2, true]]"

[[test.results]]
value = "[{ body: 'banana cherry', id: t:[1] }]"

[[test.results]]
value = "[[1, false], [1, true]]"

[[test.results]]
value = "[[1, true], [2, true]]"

*/
-- Each match as [id component, whether it is an int], sorted.
DEFINE FUNCTION fn::matches($term: string) {
	RETURN array::sort(
		SELECT VALUE [record::id(id)[0], type::is_int(record::id(id)[0])] FROM t WHERE body @@ $term
	);
};
fn::matches('apple');
fn::matches('banana');
DELETE t:[1];
fn::matches('banana');
fn::matches('apple');
UPSERT t:[1] SET body = 'banana cherry';
fn::matches('banana');
fn::matches('cherry');
