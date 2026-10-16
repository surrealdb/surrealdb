/**
[test]
reason = """
	A FETCH of a link field per row over a scan reads the same rows, in the
	same order, whatever the paging before it skips or admits, and whether the
	link is a record that exists, one that does not, a table that is not
	defined, or no link at all. The same holds beside a filter, a projection,
	an ordering, an OMIT, a link field the table computes or restricts, a
	path that is not a link field, a write in the statement, a statement in a
	transaction that has written the records it fetches, and a read of a
	record range.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1200, NONE, 1, 199, 200, NONE, [user:299, user:0]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[700, comment:100, comment:799, 101, 199]"

[[test.results]]
value = "[{ author: { id: user:198, n: 198, name: '198' }, id: comment:498 }, { author: { id: user:199, n: 199, name: '199' }, id: comment:499 }, { author: { id: user:200, n: 200, name: '200' }, id: comment:500 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: { id: user:298, n: 298, name: '298' }, id: comment:1198 }, { author: NONE, id: comment:1199 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: comment:1 }, { author: { id: user:2, n: 2, name: '2' }, id: comment:2 }]"

[[test.results]]
value = "[{ author: { id: user:297, n: 297, name: '297' }, id: comment:1197 }, { author: { id: user:298, n: 298, name: '298' }, id: comment:1198 }, { author: NONE, id: comment:1199 }]"

[[test.results]]
value = "[{ author: NONE, id: comment:1199 }, { author: { id: user:298, n: 298, name: '298' }, id: comment:1198 }, { author: { id: user:297, n: 297, name: '297' }, id: comment:1197 }]"

[[test.results]]
value = "[{ id: comment:5, name: '5' }, { id: comment:6, name: '6' }, { id: comment:7, name: '7' }]"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: comment:1 }, { author: { id: user:2, n: 2, name: '2' }, id: comment:2 }, { author: { id: user:3, n: 3, name: '3' }, id: comment:3 }]"

[[test.results]]
value = "[{ author: { id: user:197, n: 197, name: '197' }, id: comment:497 }, { author: { id: user:198, n: 198, name: '198' }, id: comment:498 }, { author: { id: user:199, n: 199, name: '199' }, id: comment:499 }, { author: { id: user:200, n: 200, name: '200' }, id: comment:500 }, { author: { id: user:201, n: 201, name: '201' }, id: comment:501 }, { author: { id: user:202, n: 202, name: '202' }, id: comment:502 }]"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: derived:1, text: 'a', upper: 'A' }, { author: { id: user:2, n: 2, name: '2' }, id: derived:2, text: 'b', upper: 'B' }]"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: walled:1 }, { author: { id: user:2, n: 2, name: '2' }, id: walled:2 }]"

[[test.results]]
error = "The table 'missing_table' does not exist"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: bad:1 }]"

[[test.results]]
value = "[{ author: { id: user:3, n: 3, name: '3' }, friends: [{ id: user:3, n: 3, name: '3' }, { id: user:4, n: 4, name: '4' }], id: comment:3, text: '3' }, { author: { id: user:4, n: 4, name: '4' }, friends: [{ id: user:4, n: 4, name: '4' }, { id: user:5, n: 5, name: '5' }], id: comment:4, text: '4' }]"

[[test.results]]
value = "[{ author: { id: user:3, n: 3, name: '3' }, friends: [user:3, user:4], id: comment:3, text: '3' }, { author: { id: user:4, n: 4, name: '4' }, friends: [user:4, user:5], id: comment:4, text: '4' }]"

[[test.results]]
value = "[{ author: { id: user:3, n: 3, name: '3' }, friends: [user:3, user:4], id: comment:3, text: '3', users: [300] }, { author: { id: user:4, n: 4, name: '4' }, friends: [user:4, user:5], id: comment:4, text: '4', users: [300] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:7, n: 7, name: 'seven' }]"

[[test.results]]
value = "[{ id: user:4000, name: 'fresh' }]"

[[test.results]]
value = "[{ author: user:4000, id: comment:fresh, text: 'fresh' }]"

[[test.results]]
value = "[{ author: { id: user:6, n: 6, name: '6' }, id: comment:6 }, { author: { id: user:7, n: 7, name: 'seven' }, id: comment:7 }]"

[[test.results]]
value = "[{ author: { id: user:4000, name: 'fresh' }, id: comment:fresh }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: { id: user:7, n: 7, name: 'seven' }, id: comment:7 }, { author: NONE, id: comment:8 }, { author: { id: user:9, n: 9, name: '9' }, id: comment:9 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ author: { id: user:3, n: 3, name: '3' }, friends: [user:3, user:4], id: comment:3 }, { author: { id: user:4, n: 4, name: '4' }, friends: [user:4, user:5], id: comment:4 }]"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: linked:1, n: 1 }, { author: { id: user:2, n: 2, name: '2' }, id: linked:2, n: 2 }]"

[[test.results]]
value = "[{ author: { id: user:5, n: 5, name: '5' }, friends: [user:5, user:6], id: comment:5, text: '5', w: 1 }, { author: { id: user:6, n: 6, name: '6' }, friends: [user:6, user:7], id: comment:6, text: '6', w: 2 }, { author: { id: user:7, n: 7, name: 'seven' }, friends: [user:7, user:8], id: comment:7, text: '7', w: 3 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ author: { id: user:1, n: 1, name: '1' }, id: capped:1 }, { author: { id: user:2, n: 2, name: '2' }, id: capped:2 }]"

*/

{
	DEFINE TABLE user SCHEMALESS;
	DEFINE TABLE comment SCHEMALESS;
	DEFINE TABLE derived SCHEMALESS;
	DEFINE FIELD upper ON derived COMPUTED string::uppercase(text);
	DEFINE TABLE walled SCHEMALESS;
	DEFINE FIELD author ON walled PERMISSIONS FOR select FULL;
	FOR $i IN 0..300 {
		CREATE type::record('user', $i) SET n = $i, name = <string>$i RETURN NONE;
	};
	FOR $i IN 0..1200 {
		CREATE type::record('comment', $i) SET text = <string>$i,
			author = IF $i % 11 = 0 { NONE } ELSE IF $i % 13 = 0 { type::record('user', 9999) } ELSE { type::record('user', $i % 300) },
			friends = [type::record('user', $i % 300), type::record('user', ($i + 1) % 300)]
			RETURN NONE;
	};
	CREATE bad:1 SET author = user:1 RETURN NONE;
	CREATE bad:2 SET author = type::record('missing_table', 1) RETURN NONE;
	CREATE bad:3 SET author = user:3 RETURN NONE;
	CREATE derived:1 SET text = 'a', author = user:1 RETURN NONE;
	CREATE derived:2 SET text = 'b', author = user:2 RETURN NONE;
	CREATE walled:1 SET author = user:1 RETURN NONE;
	CREATE walled:2 SET author = user:2 RETURN NONE;
	RETURN 'OK';
};

LET $all = SELECT * FROM comment FETCH author;
RETURN [array::len($all), $all[0].author, $all[1].author.n, $all[499].author.n, $all[500].author.n, $all[1199].author.n, $all[1199].friends];
LET $page = SELECT * FROM comment LIMIT 700 START 100 FETCH author, friends;
RETURN [array::len($page), $page[0].id, $page[699].id, $page[0].friends[1].n, $page[699].author.n];
SELECT id, author FROM comment LIMIT 3 START 498 FETCH author;
SELECT id, author FROM comment LIMIT 0 FETCH author;
SELECT id, author FROM comment START 1198 FETCH author;
SELECT id, author FROM comment START 5000 FETCH author;
SELECT id, author FROM comment LIMIT 2 START 1 FETCH author;
SELECT id, author FROM comment WHERE id > comment:1196 FETCH author;
SELECT id, author FROM comment ORDER BY id DESC LIMIT 3 FETCH author;
SELECT id, author.name AS name FROM comment LIMIT 3 START 5 FETCH author;
SELECT id, author FROM comment WHERE author != NONE LIMIT 3 FETCH author;
SELECT id, author FROM comment:497..503 FETCH author;
SELECT * FROM derived FETCH author;
SELECT * FROM walled FETCH author;
SELECT * FROM bad FETCH author;
SELECT * FROM bad LIMIT 1 FETCH author;
SELECT * FROM comment LIMIT 2 START 3 FETCH author, friends.*;
SELECT * FROM comment LIMIT 2 START 3 FETCH author.name;
SELECT *, (SELECT VALUE count() FROM user GROUP ALL) AS users FROM comment LIMIT 2 START 3 FETCH author;
BEGIN;
UPDATE user:7 SET name = 'seven';
CREATE type::record('user', 4000) SET name = 'fresh';
CREATE comment:fresh SET author = user:4000, text = 'fresh';
SELECT id, author FROM comment LIMIT 2 START 6 FETCH author;
SELECT id, author FROM comment WHERE id = comment:fresh FETCH author;
DELETE user:8;
SELECT id, author FROM comment LIMIT 3 START 7 FETCH author;
COMMIT;
{
	DEFINE TABLE linked SCHEMALESS;
	DEFINE FIELD author ON linked COMPUTED type::record('user', n);
	CREATE linked:1 SET n = 1 RETURN NONE;
	CREATE linked:2 SET n = 2 RETURN NONE;
	RETURN 'OK';
};
SELECT * OMIT text FROM comment LIMIT 2 START 3 FETCH author;
SELECT * FROM linked FETCH author;
SELECT *, (UPSERT audit:1 SET hits = (hits ?? 0) + 1 RETURN VALUE hits)[0] AS w FROM comment LIMIT 3 START 5 FETCH author;
{
	DEFINE TABLE capped SCHEMALESS;
	DEFINE FIELD author ON capped PERMISSIONS FOR select WHERE true;
	CREATE capped:1 SET author = user:1 RETURN NONE;
	CREATE capped:2 SET author = user:2 RETURN NONE;
	RETURN 'OK';
};
SELECT * FROM capped FETCH author;
