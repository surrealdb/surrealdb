/**
[test]
reason = "A decimal literal whose value the decimal type holds exactly is accepted in plain and exponent notation, at the largest scale and magnitude, including fractional zeros past the maximum scale"

[[test.results]]
value = "0.0000000000000000000000000001dec"

[[test.results]]
value = "0.0000000000000000000000000001dec"

[[test.results]]
value = "10000000000000000000000000000dec"

[[test.results]]
value = "1.5dec"

[[test.results]]
value = "79228162514264337593543950335dec"

[[test.results]]
value = "1.5dec"
*/

RETURN 0.0000000000000000000000000001dec;
RETURN 1e-28dec;
RETURN 1e28dec;
RETURN 1.500000000000000000000000000000dec;
RETURN 79228162514264337593543950335dec;
RETURN 1.50e0dec;
