/**
[env]
namespace = true
database = true
timeout = 30000
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a column of the outermost query read inside an EXISTS inside an EXISTS, a NOT EXISTS inside a NOT EXISTS, and an EXISTS whose select list holds a scalar subquery that reads it, is the column of the row the outermost query is on"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"
*/

select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.owner_id = u.id and q.id = p.id and q.id > 10)) order by 1;
select u.id from users u where not exists (select 1 from posts p where p.owner_id = u.id and not exists (select 1 from posts q where q.owner_id = u.id and q.id = p.id and q.id > 10)) order by 1;
select u.id from users u where exists (select (select u.id from posts q where q.id = p.id) from posts p where p.owner_id = u.id) order by 1;
