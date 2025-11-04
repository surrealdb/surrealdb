/**
[test]

[[test.results]]
value = "[{ id: a:1 }, { id: a:2 }]"

[[test.results]]
value = "[{ id: e:1, in: a:1, out: a:2 }]"

[[test.results]]
value = "[[e:1]]"

[[test.results]]
value = "[e:1]"

[[test.results]]
value = "[[{ id: e:1, in: a:1, out: a:2 }]]"

[[test.results]]
value = "[[{ id: e:1, in: a:1, out: a:2 }]]"

[[test.results]]
value = "[e:1]"

[[test.results]]
value = "e:1"

*/
create a:1, a:2;
relate a:1->e:1->a:2;

select value <-e from a:2;
select value <-e[0] from a:2;
select value <-e.* from a:2;
select value <-e.*.* from a:2;

a:2<-e;
a:2<-e[0];

