/**
[env]
namespace = true
database = true
timeout = 30000
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a subquery reads the columns of the query directly above it, however deep it stands: one level, a subquery in a subquery that reads the one above it, three levels each reading the one above, and a column of the middle query"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1, n: 2 }, { id: 2, n: 1 }, { id: 3, n: 0 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"
*/

select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and p.id > 10) order by 1;
select u.id, (select count(*) from posts p where p.owner_id = u.id) as n from users u order by 1;
select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.id = p.id and q.id > 10)) order by 1;
select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.id = p.id and exists (select 1 from posts r where r.id = q.id and r.id > 10))) order by 1;
select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.owner_id = p.owner_id and q.id > 10)) order by 1;
