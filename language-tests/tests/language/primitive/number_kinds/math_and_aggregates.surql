/**
[test]

[[test.results]]
value = "[42i8, 'i8', 42u8, 'u8', 1.5f16, 'f16', 1.5f32, 'f32']"

[[test.results]]
error = 'Failed to compute: "math::abs(-128i8)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "math::abs(-32768i16)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "math::abs(-2147483648i32)", as the operation results in an arithmetic overflow.'

[[test.results]]
value = "[2f16, 'f16', 1f16, 'f16', 2f16, 'f16', 2f32, 'f32', 1f32, 'f32', 2f32, 'f32', 5u8, 'u8']"

[[test.results]]
value = "[-1i8, 'i8', 1u8, 'u8', -1f16, 'f16', 1f32, 'f32', 5u8, 'u8', 5u8, 'u8']"

[[test.results]]
value = "[350, 'int', 4f, 'float', 4f, 'float', 3, 'int', 5i16, 'i16', 20u32, 'u32', 2f, 2f]"

[[test.results]]
value = "[{ f: 1.5f16, g: 'a', id: m:1, u: 200u8 }]"

[[test.results]]
value = "[{ f: 2.5f16, g: 'a', id: m:2, u: 100u8 }]"

[[test.results]]
value = "[{ f: 0.5f16, g: 'b', id: m:3, u: 50u8 }]"

[[test.results]]
value = "[{ max_u: 200u8, max_u_type: 'u8', mean_u: 116.66666666666667f, min_u: 50u8, min_u_type: 'u8', sum_f: 4.5f, sum_u: 350, sum_u_type: 'int' }]"

[[test.results]]
value = "[{ g: 'a', max_u: 200u8, mean_f: 2f, min_u: 100u8, sum_u: 300 }, { g: 'b', max_u: 50u8, mean_f: 0.5f, min_u: 50u8, sum_u: 50 }]"
*/

-- Math functions preserving kind where applicable
[
	math::abs(-42i8),
	type::of(math::abs(-42i8)),
	math::abs(42u8),
	type::of(math::abs(42u8)),
	math::abs(-1.5f16),
	type::of(math::abs(-1.5f16)),
	math::abs(-1.5f32),
	type::of(math::abs(-1.5f32)),
];
math::abs(-128i8);
math::abs(-32768i16);
math::abs(-2147483648i32);

[
	math::ceil(1.2f16),
	type::of(math::ceil(1.2f16)),
	math::floor(1.8f16),
	type::of(math::floor(1.8f16)),
	math::round(1.5f16),
	type::of(math::round(1.5f16)),
	math::ceil(1.2f32),
	type::of(math::ceil(1.2f32)),
	math::floor(1.8f32),
	type::of(math::floor(1.8f32)),
	math::round(1.5f32),
	type::of(math::round(1.5f32)),
	math::ceil(5u8),
	type::of(math::ceil(5u8)),
];

[
	math::sign(-5i8),
	type::of(math::sign(-5i8)),
	math::sign(5u8),
	type::of(math::sign(5u8)),
	math::sign(-1.5f16),
	type::of(math::sign(-1.5f16)),
	math::sign(1.5f32),
	type::of(math::sign(1.5f32)),
	math::clamp(10u8, 0u8, 5u8),
	type::of(math::clamp(10u8, 0u8, 5u8)),
	math::clamp(10u8, 0i16, 5u8),
	type::of(math::clamp(10u8, 0i16, 5u8)),
];

-- Array math functions
[
	math::sum([100u8, 200u8, 50u8]),
	type::of(math::sum([100u8, 200u8, 50u8])),
	math::sum([1.5f16, 2.5f16]),
	type::of(math::sum([1.5f16, 2.5f16])),
	math::sum([1.5f32, 2.5f32]),
	type::of(math::sum([1.5f32, 2.5f32])),
	math::sum([1u64, 2i8]),
	type::of(math::sum([1u64, 2i8])),
	math::min([10u8, 5i16, 20u32]),
	type::of(math::min([10u8, 5i16, 20u32])),
	math::max([10u8, 5i16, 20u32]),
	type::of(math::max([10u8, 5i16, 20u32])),
	math::mean([1u8, 2u8, 3u8]),
	math::median([1u8, 2u16, 10i32]),
];

-- Query aggregates (GROUP ALL and GROUP BY)
CREATE m:1 SET g = 'a', u = 200u8, f = 1.5f16;
CREATE m:2 SET g = 'a', u = 100u8, f = 2.5f16;
CREATE m:3 SET g = 'b', u = 50u8, f = 0.5f16;

SELECT
	math::sum(u) AS sum_u,
	type::of(math::sum(u)) AS sum_u_type,
	math::min(u) AS min_u,
	type::of(math::min(u)) AS min_u_type,
	math::max(u) AS max_u,
	type::of(math::max(u)) AS max_u_type,
	math::mean(u) AS mean_u,
	math::sum(f) AS sum_f
FROM m GROUP ALL;

SELECT
	g,
	math::sum(u) AS sum_u,
	math::min(u) AS min_u,
	math::max(u) AS max_u,
	math::mean(f) AS mean_f
FROM m GROUP BY g ORDER BY g;
