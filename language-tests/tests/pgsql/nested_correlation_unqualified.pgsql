/**
[env]
namespace = true
database = true
imports = ["pgsql/_nested_correlation_declared.surql"]

[test]
reason = "an unqualified column that no table of the two subqueries above it has is a column of the outermost query, however many subqueries stand between"

[[test.results]]
value = "[{ id: 1 }]"
*/

select u.id from users u where exists (select 1 from posts p where p.owner_id = u.id and exists (select 1 from posts q where q.id = p.id and name = 'ann')) order by 1;
