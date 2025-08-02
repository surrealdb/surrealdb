/**
[test]

[[test.results]]
value = "{ id: person:tobie, name: 'tobie' }"

[[test.results]]
value = "{ id: person:tobie, name: 'tobie' }"

[[test.results]]
value = "{ id: person:tobie, name: 'tobie' }"

[[test.results]]
value = "{ id: person:tobie, name: 'tobie' }"

[[test.results]]
value = "[person:tobie, 'tobie']"

[[test.results]]
value = "{ id: person:tobie, name: 'tobie' }"

# TODO: This is a behavior change, discuss with others.
[[test.results]]
value = "[person:tobie, 'tobie']"

[[test.results]]
value = "[{ id: person:tobie, name: 'tobie' }, 'tobie']"

[[test.results]]
value = "person:tobie"

[[test.results]]
value = "{ id: person:tobie, name: 'tobie' }"

*/

create only person:tobie set name = 'tobie';

select * from ONLY person:tobie;

select * from ONLY person:tobie.*;   -- this
select * from ONLY (person:tobie.*); -- does this
(select * from ONLY person:tobie).*; -- not this

select * from ONLY { id: person:tobie, name: 'tobie' };
select * from { id: person:tobie, name: 'tobie' }.*;
select * from person:tobie, 'tobie';
return person:tobie;
return person:tobie.*;
