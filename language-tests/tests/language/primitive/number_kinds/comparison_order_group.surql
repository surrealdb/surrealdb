/**
[test]

[[test.results]]
value = "[true, true, true, true, true, true, true, true, false, false]"

[[test.results]]
value = "[true, true, true, true, true, true, true, true]"

[[test.results]]
value = "[false, true, true, true, true, true, true]"

[[test.results]]
value = "{5u8,}"

[[test.results]]
value = "[5u8, 6u8]"

[[test.results]]
value = "[{ id: item:1, n: 100u16, tag: 'u16' }]"

[[test.results]]
value = "[{ id: item:2, n: -50i8, tag: 'i8' }]"

[[test.results]]
value = "[{ id: item:3, n: 5u8, tag: 'u8' }]"

[[test.results]]
value = "[{ id: item:4, n: 5i32, tag: 'i32' }]"

[[test.results]]
value = "[{ id: item:5, n: 5f16, tag: 'f16' }]"

[[test.results]]
value = "[{ id: item:6, n: 1.5f32, tag: 'f32' }]"

[[test.results]]
value = "[{ id: item:7, n: 18446744073709551615u64, tag: 'u64' }]"

[[test.results]]
value = "[{ id: item:2, n: -50i8, tag: 'i8' }, { id: item:6, n: 1.5f32, tag: 'f32' }, { id: item:3, n: 5u8, tag: 'u8' }, { id: item:4, n: 5i32, tag: 'i32' }, { id: item:5, n: 5f16, tag: 'f16' }, { id: item:1, n: 100u16, tag: 'u16' }, { id: item:7, n: 18446744073709551615u64, tag: 'u64' }]"

[[test.results]]
value = "[{ c: 1, n: -50i8 }, { c: 1, n: 1.5f32 }, { c: 3, n: 5u8 }, { c: 1, n: 100u16 }, { c: 1, n: 18446744073709551615u64 }]"
*/

-- Value equality `=` across kinds vs exact equality `==`
[
	5u8 = 5,
	5i8 = 5u64,
	5u16 = 5.0f16,
	5i32 = 5.0f32,
	5u32 = 5.0dec,
	0.5f16 = 0.5f32,
	0.5f16 = 0.5f,
	0.5f16 = 0.5dec,
	0.1f16 = 0.1f,
	0.1f32 = 0.1f,
];

[
	5u8 == 5,
	5u8 == 5u8,
	5i64 == 5,
	0.5f16 == 0.5f,
	0.5f16 == 0.5f16,
	1.5f64 == 1.5f,
	0f16 == -0f16,
	0f32 == -0f32,
];

[
	5u8 != 5,
	-1i8 < 0u8,
	255u8 < 256u16,
	9223372036854775807 < 9223372036854775808u64,
	9223372036854775808u64 < 18446744073709551615u64,
	-128i8 < -100f16,
	1.5f16 <= 1.5f32,
];

-- Sets deduplicate by value equality across number kinds
{5u8, 5i16, 5, 5.0f16, 5.0f32, 5.0f, 5.0dec};
array::distinct([5u8, 5i16, 5, 5.0f16, 5.0f32, 5.0f, 5.0dec, 6u8, 6]);

-- ORDER BY and GROUP BY across mixed number kinds
CREATE item:1 SET n = 100u16, tag = 'u16';
CREATE item:2 SET n = -50i8, tag = 'i8';
CREATE item:3 SET n = 5u8, tag = 'u8';
CREATE item:4 SET n = 5i32, tag = 'i32';
CREATE item:5 SET n = 5.0f16, tag = 'f16';
CREATE item:6 SET n = 1.5f32, tag = 'f32';
CREATE item:7 SET n = 18446744073709551615u64, tag = 'u64';

SELECT id, n, tag FROM item ORDER BY n ASC, id ASC;
SELECT n, count() AS c FROM item GROUP BY n ORDER BY n ASC;
