/**
[env]
namespace = true
database = true
imports = ["pgsql/_nested_correlation.surql"]

[test]
reason = "a column of the outermost query read inside a scalar subquery inside a scalar subquery, in a condition and in the select list, is the column of the row the outermost query is on"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1, x: 1 }, { id: 2, x: 2 }, { id: 3, x: 3 }]"
*/

select u.id from users u where u.id = (select (select u.id from posts q where q.id = p.id) from posts p where p.id = 10) order by 1;
select u.id, (select (select u.id)) as x from users u order by 1;
