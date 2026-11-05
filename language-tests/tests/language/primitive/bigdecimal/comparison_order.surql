/**
[test]
reason = "A bigdecimal compares with every number kind by exact value, and ORDER BY places it among them"

[[test.results]]
value = "[true, true, true, true, true, false, true, true]"

[[test.results]]
value = "[{ id: n:1, v: 10000000000000000000000000000000000000000bigdec }]"

[[test.results]]
value = "[{ id: n:2, v: 2 }]"

[[test.results]]
value = "[{ id: n:3, v: 1.5dec }]"

[[test.results]]
value = "[{ id: n:4, v: -10000000000000000000000000000000000000000bigdec }]"

[[test.results]]
value = "[{ id: n:5, v: 1.4999999999999999999999999999999999999bigdec }]"

[[test.results]]
value = "[{ id: n:6, v: 1.25f }]"

[[test.results]]
value = "[{ id: n:7, v: 7u8 }]"

[[test.results]]
value = "[n:4, n:6, n:5, n:3, n:2, n:7, n:1]"

[[test.results]]
value = "[n:1, n:7, n:2, n:3, n:5, n:6, n:4]"

[[test.results]]
value = "[n:5, n:3, n:2, n:7, n:1]"

[[test.results]]
value = "[{ id: g:1, k: 1.50bigdec, x: 1 }]"

[[test.results]]
value = "[{ id: g:2, k: 1.5bigdec, x: 2 }]"

[[test.results]]
value = "[{ id: g:3, k: 1.5dec, x: 3 }]"

[[test.results]]
value = "[{ c: 3, k: 1.50bigdec, s: 6 }]"

*/

[
	1bigdec < 1.0000000000000000000000000000000000001bigdec,
	1e40bigdec > 79228162514264337593543950335dec,
	1e40bigdec > 18446744073709551615u64,
	1e40bigdec < 1e41f,
	0.1bigdec == 0.1dec,
	0.1bigdec == 0.1f,
	-1e40bigdec < -9223372036854775808,
	1bigdec < NaN,
];

CREATE n:1 SET v = 1e40bigdec;
CREATE n:2 SET v = 2;
CREATE n:3 SET v = 1.5dec;
CREATE n:4 SET v = -1e40bigdec;
CREATE n:5 SET v = 1.4999999999999999999999999999999999999bigdec;
CREATE n:6 SET v = 1.25f;
CREATE n:7 SET v = 7u8;

SELECT VALUE id FROM n ORDER BY v;
SELECT VALUE id FROM n ORDER BY v DESC;
SELECT VALUE id FROM n WHERE v > 1.4999999999999999999999999999999999998bigdec ORDER BY v;

-- A grouping key compares by value, so 1.50 and 1.5 form one group.
CREATE g:1 SET k = 1.50bigdec, x = 1;
CREATE g:2 SET k = 1.5bigdec, x = 2;
CREATE g:3 SET k = 1.5dec, x = 3;
SELECT k, count() AS c, math::sum(x) AS s FROM g GROUP BY k;
