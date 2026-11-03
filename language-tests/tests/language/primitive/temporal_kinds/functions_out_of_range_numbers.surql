/**
[test]
reason = "A count no decimal can hold, or whose micros overflow one, is refused by interval::make and the time constructors instead of panicking or reading as zero"

[[test.results]]
error = 'Failed to compute: "interval::make", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "interval::make", as the operation results in an arithmetic overflow.'

[[test.results]]
value = "ivl'PT1H30M'"

[[test.results]]
error = "Incorrect arguments for function time::make_time(). The seconds are out of range."

[[test.results]]
value = "pt'12:00:30.500'"

*/

interval::make(0, 0, 0, 0, 79228162514264337593543950335dec);
interval::make(0, 0, 0, 0, 0, 0, 100000000000000000000000000000000bigdec);
interval::make(0, 0, 0, 0, 1.5);
time::make_time(12, 0, 100000000000000000000000000000000bigdec);
time::make_time(12, 0, 30.5);
