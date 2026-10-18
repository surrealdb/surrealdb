/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = "a float fold answers the serial fold's bits, whatever width the scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 149.95f, s: 449850f, sd: 86.616972932561f, v: 7502.500000000005f }]"

[[test.results]]
value = "[{ g: 0, m: 149.5f, s: 44850f, sd: 86.74675786448736f, v: 7525f }, { g: 1, m: 149.59999999999954f, s: 44879.99999999986f, sd: 86.74675786448736f, v: 7525f }, { g: 2, m: 149.69999999999985f, s: 44909.999999999956f, sd: 86.74675786448736f, v: 7525f }, { g: 3, m: 149.80000000000015f, s: 44940.000000000044f, sd: 86.74675786448736f, v: 7525f }, { g: 4, m: 149.90000000000046f, s: 44970.00000000014f, sd: 86.74675786448736f, v: 7525f }, { g: 5, m: 150f, s: 45000f, sd: 86.74675786448736f, v: 7525f }, { g: 6, m: 150.0999999999995f, s: 45029.999999999854f, sd: 86.74675786448736f, v: 7525f }, { g: 7, m: 150.19999999999982f, s: 45059.99999999995f, sd: 86.74675786448736f, v: 7525f }, { g: 8, m: 150.30000000000018f, s: 45090.00000000005f, sd: 86.74675786448736f, v: 7525f }, { g: 9, m: 150.4000000000005f, s: 45120.000000000146f, sd: 86.74675786448736f, v: 7525f }]"

[[test.results]]
value = "[{ g: 0, s: 8616700f }, { g: 1, s: 8634681.999999996f }, { g: 2, s: 8642668.000000006f }, { g: 3, s: 8650658.000000004f }, { g: 4, s: 8658651.999999996f }, { g: 5, s: 8666650f }, { g: 6, s: 8674651.999999998f }, { g: 7, s: 8682658.000000002f }, { g: 8, s: 8690668.000000002f }, { g: 9, s: 8698681.999999996f }]"
*/

FOR $i IN 0..3000 { CREATE type::record('t', $i) SET f = 0.1 * $i, g = $i % 10 };
SELECT math::sum(f) AS s, math::mean(f) AS m, math::stddev(f) AS sd, math::variance(f) AS v FROM t GROUP ALL;
SELECT g, math::sum(f) AS s, math::mean(f) AS m, math::stddev(f) AS sd, math::variance(f) AS v FROM t GROUP BY g;
SELECT g, math::sum(f * f) AS s FROM t WHERE f > 100 GROUP BY g;
