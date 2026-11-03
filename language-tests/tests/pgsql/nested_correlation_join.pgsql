/**
[env]
namespace = true
database = true
timeout = 30000
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a column of a joined relation of the outermost query, and of its first relation, read two subqueries down, is the column of the row of the join"

[[test.results]]
value = "[{ id: 1, xid: 11 }, { id: 2, xid: 12 }]"

[[test.results]]
value = "[{ id: 1, xid: 10 }]"
*/

select u.id, x.id as xid from users u join posts x on x.owner_id = u.id where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.owner_id = u.id and q.id = x.id and q.id > 10)) order by 1, 2;
select u.id, x.id as xid from users u join posts x on x.owner_id = u.id where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.owner_id = u.id and q.id = p.id and x.id = 10)) order by 1, 2;
