/**
[test]

[[test.results]]
value = "[{ id: post:1, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ id: post:1, tags: 'a' }, { id: post:1, tags: 'b' }]"

[[test.results]]
value = "[{ id: post:1, tags: 'a' }, { id: post:1, tags: 'b' }]"

*/
create post:1 set tags = ['a', 'b'];
select * from post split tags;
select * from post split tags ORDER BY id;