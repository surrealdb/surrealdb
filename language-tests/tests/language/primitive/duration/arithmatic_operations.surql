/**
[test]

[[test.results]]
value = "NaN"

[[test.results]]
value = "NaN"

[[test.results]]
value = "NaN"

[[test.results]]
value = "1m"

[[test.results]]
value = "30s"

[[test.results]]
value = "NaN"

[[test.results]]
value = "2s"

[[test.results]]
value = "1h1s"

[[test.results]]
value = "59m59s"

[[test.results]]
value = "0ns"

[[test.results]]
error = 'Failed to compute: "0ns - 1ns", as the operation results in a negative value.'

[[test.results]]
error = 'Failed to compute: "584942417355y3w5d7h15s999ms999µs999ns + 1ns", as the operation results in an arithmetic overflow.'

[[test.results]]
value = "20s"

[[test.results]]
error = "Cannot perform multiplication with '1s' and '2s'"

[[test.results]]
error = "Cannot perform remainder with '1h1s' and '1s'"

[[test.results]]
error = "Cannot raise the value '1s' with '2s'"

[[test.results]]
value = "1s..2s"

[[test.results]]
error = "Cannot negate the value '1s'"

[[test.results]]
value = "1s"

[[test.results]]
error = 'Failed to compute: "584942417355y3w5d7h15s999ms999µs999ns * 2", as the operation results in an arithmetic overflow.'

[[test.results]]
value = "292471208677y27w6d15h30m7s999ms999µs999ns"

[[test.results]]
value = "10m"

[[test.results]]
value = "24m"

[[test.results]]
value = "24m"

[[test.results]]
value = "5h"

[[test.results]]
value = "5h30m"

[[test.results]]
value = "5h30m"
*/

1h / 1s;
1m / 1s;
1s / 1s;
1m / 1;
1m / 2;
1 / 1m;

1s + 1s;
1h + 1s;
1h - 1s;
1s - 1s;
0s - 1ns;
18446744073709551615s999999999ns + 1ns;

1s * 20;
1s * 2s;

1h1s % 1s;

1s ** 2s;

1s..2s;

-(1s);
+(1s);

duration::max * 2;
duration::max / 2;

1h / 6;
1h / 2.5;
1h / 2.5dec;

1h * 5;
1h * 5.5;
1h * 5.5dec;