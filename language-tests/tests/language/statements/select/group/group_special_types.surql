/**
[test]
reason = "Test GROUP BY with special data types (UUID, geometry, etc.)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 2, point: (1.23f, 4.56f), total: 25f }, { count: 2, point: (7.89f, 10.11f), total: 45f }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ activity_count: 3, user_id: u'550e8400-e29b-41d4-a716-446655440000' }, { activity_count: 1, user_id: u'6ba7b810-9dad-11d1-80b4-00c04fd430c8' }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ hash: b"41414141", total_size: 250f }, { hash: b"42424242", total_size: 200f }]'

*/

-- Setup data
{
	CREATE location:1 SET point = (1.23, 4.56), region = 'north', visits = 10;
	CREATE location:2 SET point = (1.23, 4.56), region = 'north', visits = 15;
	CREATE location:3 SET point = (7.89, 10.11), region = 'south', visits = 20;
	CREATE location:4 SET point = (7.89, 10.11), region = 'south', visits = 25;
	RETURN "OK";
};

-- Test grouping by geometry point
SELECT point, COUNT() as count, math::sum(visits) as total FROM location GROUP BY point;

-- Test with UUID
{
	CREATE session:1 SET user_id = u'550e8400-e29b-41d4-a716-446655440000', activity = 'login';
	CREATE session:2 SET user_id = u'550e8400-e29b-41d4-a716-446655440000', activity = 'view';
	CREATE session:3 SET user_id = u'6ba7b810-9dad-11d1-80b4-00c04fd430c8', activity = 'login';
	CREATE session:4 SET user_id = u'550e8400-e29b-41d4-a716-446655440000', activity = 'logout';
	RETURN "OK";
};

SELECT user_id, COUNT() as activity_count FROM session GROUP BY user_id;

-- Test grouping by bytes
{
	CREATE binary:1 SET hash = <bytes>"AAAA", size = 100;
	CREATE binary:2 SET hash = <bytes>"AAAA", size = 150;
	CREATE binary:3 SET hash = <bytes>"BBBB", size = 200;
	RETURN "OK";
};

SELECT hash, math::sum(size) as total_size FROM binary GROUP BY hash;
