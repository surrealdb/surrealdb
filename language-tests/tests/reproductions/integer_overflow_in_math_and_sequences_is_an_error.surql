/**
[env]
namespace = true
database = true

[test]
reason = "Integer overflow is an error, never a silent wrap, in math:: folds, negation and sequence allocation; math::sign treats either float zero as zero"

[[test.results]]
error = "Cannot perform multiplication with '9223372036854775807' and '2'"

[[test.results]]
value = "12"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '1'"

[[test.results]]
error = "Cannot perform subtraction with '9223372036854775807' and '-9223372036854775808'"

[[test.results]]
error = 'Failed to compute: "math::abs(-9223372036854775808)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = "Cannot raise the value '9223372036854775807' with '2'"

[[test.results]]
value = "4611686018427387904"

[[test.results]]
error = "Cannot negate the value '-9223372036854775808'"

[[test.results]]
value = "4611686018427388000f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0dec"

[[test.results]]
value = "-1f"

[[test.results]]
value = "1f"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9223372036854775806"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9223372036854775800"

[[test.results]]
value = "9223372036854775801"
*/
RETURN math::product([9223372036854775807, 2]);
RETURN math::product([3, 4]);
RETURN math::sum([9223372036854775807, 1]);
RETURN math::spread([-9223372036854775807 - 1, 9223372036854775807]);
RETURN math::abs(-9223372036854775807 - 1);
RETURN math::pow(9223372036854775807, 2);
RETURN math::pow(2, 62);
RETURN -(-9223372036854775807 - 1);
RETURN math::mean([9223372036854775807, 1]);
RETURN math::sign(-0.0);
RETURN math::sign(0.0);
RETURN math::sign(-0.0dec);
RETURN math::sign(-2.5);
RETURN math::sign(2.5);
DEFINE SEQUENCE s1 BATCH 1 START 9223372036854775807;
RETURN sequence::nextval('s1');
DEFINE SEQUENCE s2 BATCH 1 START 9223372036854775806;
RETURN sequence::nextval('s2');
RETURN sequence::nextval('s2');
DEFINE SEQUENCE s3 BATCH 100 START 9223372036854775800;
RETURN sequence::nextval('s3');
RETURN sequence::nextval('s3');
