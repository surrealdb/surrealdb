/**
[env]
namespace = true
database = true
timeout = 30000
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a column of the outermost query read two subqueries down from its select list, from its ORDER BY and from a comparison with a count, is the column of the row the outermost query is on"

[[test.results]]
value = "[{ id: 1, n: 2 }, { id: 2, n: 1 }, { id: 3, n: 0 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 2 }, { id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"
*/

select u.id, (select count(*) from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.owner_id = u.id)) as n from users u order by 1;
select u.id from users u order by (select count(*) from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.id = p.id and q.owner_id = u.id)), u.id desc;
select u.id from users u where (select count(*) from posts p where exists (select 1 from posts q where q.owner_id = u.id and q.id = p.id)) >= 2 order by 1;
