/**
[env]
namespace = true
database = true
timeout = 30000
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a column of the outermost query read inside an IN subquery inside an IN subquery, and inside = ANY inside an EXISTS, is the column of the row the outermost query is on"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 2 }]"
*/

select u.id from users u where u.id in (select p.owner_id from posts p where p.id in (select q.id from posts q where q.owner_id = u.id)) order by 1;
select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and 12 = any (select q.id from posts q where q.owner_id = u.id)) order by 1;
