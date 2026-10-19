/**
[env]
timeout = 5000
namespace = true
database = true

[test]
reason = "A DELETE whose WHERE casts each array id to a string removes every matched record, with the report's CREATE/UPDATE event defined on the table, and the event does not run for the deletes. This pins the results at a small size only. The report's crash at two million rows was checked by hand at 300k rows on the memory and surrealkv backends; a DELETE holds its whole write set in one transaction by design, so its memory is not bounded and no assertion here could pin it."
issue = 6327

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "310"

[[test.results]]
value = "310"

[[test.results]]
value = "[]"

[[test.results]]
value = "10"

[[test.results]]
value = "['kept']"

[[test.results]]
value = "310"
*/

DEFINE FUNCTION fn::project::recomputeItem($item: any) { RETURN NONE; };
DEFINE EVENT itemUpdate ON TABLE item WHEN $event IN ['UPDATE', 'CREATE'] THEN {
	LET $bef = $before AND (SELECT * OMIT updatedAt FROM ONLY $before);
	LET $aft = SELECT * OMIT updatedAt FROM ONLY $after;
	IF $bef != $aft {
		CREATE itemHistory:[($before OR $after).id[0], rand::ulid()] CONTENT (SELECT *, id AS item OMIT id FROM ONLY ($before OR $after)) RETURN NONE;
		IF (time::now() - $after.updatedAt) > 1s {
			fn::project::recomputeItem($aft);
		};
	};
};

FOR $p IN 0..3 {
	INSERT INTO item (array::map(<array> 0..100, |$i| { id: [type::record('project', $p), $i], name: 'n', updatedAt: d'2020-01-01T00:00:00Z' })) RETURN NONE;
};
INSERT INTO item (array::map(<array> 0..10, |$i| { id: ['other', $i], name: 'kept', updatedAt: d'2020-01-01T00:00:00Z' })) RETURN NONE;

count(SELECT id FROM item);
count(SELECT id FROM itemHistory);
DELETE item WHERE (<string> id) CONTAINS 'project' RETURN NONE;
count(SELECT id FROM item);
SELECT VALUE name FROM item LIMIT 1;
count(SELECT id FROM itemHistory);
