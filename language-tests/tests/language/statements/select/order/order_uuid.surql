/**
[test]
reason = "Test ORDER BY with UUID values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: u'00000000-0000-0000-0000-000000000001' }, { id: item:2, value: u'11111111-1111-1111-1111-111111111111' }, { id: item:3, value: u'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' }, { id: item:4, value: u'ffffffff-ffff-ffff-ffff-ffffffffffff' }]"

[[test.results]]
value = "[{ id: item:4, value: u'ffffffff-ffff-ffff-ffff-ffffffffffff' }, { id: item:3, value: u'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' }, { id: item:2, value: u'11111111-1111-1111-1111-111111111111' }, { id: item:1, value: u'00000000-0000-0000-0000-000000000001' }]"
*/

-- Setup: Create records with UUID values
{
	CREATE item:1 SET value = u"00000000-0000-0000-0000-000000000001";
	CREATE item:2 SET value = u"11111111-1111-1111-1111-111111111111";
	CREATE item:3 SET value = u"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
	CREATE item:4 SET value = u"ffffffff-ffff-ffff-ffff-ffffffffffff";
	RETURN "OK";
};

-- Test ASC: lexicographic ordering of UUIDs
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
