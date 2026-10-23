/**
[env]
namespace = true
database = true
imports = ["pgsql/_link_fields.surql"]

[test]
reason = "A table named with the public schema is the table of the same name, so its link columns are written and searched by key as they are when the table is named bare: INSERT, UPDATE and DELETE by the key of a link, a predicate, and a join on the id of the linked table."

[[test.results]]
value = "[{ dept: dept:2, id: 5 }]"

[[test.results]]
value = "[{ dept: dept:ops, id: 5 }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ dname: 'Operations', name: 'Bob' }, { dname: 'Operations', name: 'Kim' }]"

[[test.results]]
value = "[{ id: 4 }, { id: 5 }]"
*/

INSERT INTO public.emp (id, name, dept) VALUES (5, 'Kim', 2) RETURNING id, dept;

UPDATE public.emp SET dept = 'ops' WHERE id = 5 RETURNING id, dept;

SELECT name FROM public.emp WHERE dept = 1 ORDER BY id;

SELECT e.name, d.dname FROM public.emp e JOIN public.dept d ON e.dept = d.id WHERE e.dept = 'ops' ORDER BY e.id;

DELETE FROM public.emp WHERE dept = 'ops' RETURNING id;
