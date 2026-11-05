/**
[env]
namespace = true
database = true
timeout = 30000
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a column of the outermost query read in the HAVING and in the ORDER BY of a subquery two levels down is the column of the row the outermost query is on"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"
*/

select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q group by q.owner_id having q.owner_id = u.id)) order by 1;
select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.owner_id = u.id order by u.id)) order by 1;
