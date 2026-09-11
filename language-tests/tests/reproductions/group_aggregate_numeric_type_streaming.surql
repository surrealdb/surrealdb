/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as
`group_aggregate_numeric_type_legacy.surql`, pinned to the streaming planner;
the two files' expected results are the divergence.

An aggregate over integers does not agree on its result type: the streaming
engine keeps `math::sum` an integer, legacy widens it to a float. The
disagreement is confined to that one aggregate — `math::min` and `math::max`
return integers on both engines, `math::mean` is a float on both, and
`count()` an integer on both.

On its own the type is invisible in most uses, but it decides the result as
soon as the value reaches integer division: `(math::sum(a) + math::sum(b)) / 4`
over sums of 3 and 6 is `9 / 4` here and `9f / 4` on the legacy side, so the
answers are `2` and `2.25f`. Neither is a rounding difference — they disagree
because one operand is an integer and the other a float.

Two controls settle which side is correct, so this is a legacy defect rather
than the open language question it first looks like. `math::sum` over a list of
integers is an integer in scalar form on both engines, so the streaming
aggregate agrees with the function's own contract and the legacy aggregate
disagrees with it.

The result type cannot be asserted by writing the expected number down. The
harness compares numbers across variants, so an expectation written `3` is
satisfied by `3f` and `3dec` alike, and every numeric expectation in this file
would hold whichever type came back. The type is therefore pinned through
`type::is_int`, whose boolean the harness does compare exactly; without it the
divergence this file exists for is observable only indirectly, through the
integer-division and large-integer controls. And the widening is lossy: `9007199254740993` summed with
`0` comes back as `9007199254740992f` here, an off-by-one from the f64
mantissa, while the scalar call on the same input is exact on both engines. An
integer aggregate that silently rounds above 2^53 cannot be the intended
behaviour.

Scalar `9 / 4` truncates to `2` on both engines, so integer division is settled
and is not what differs — the divergence is entirely the aggregate's result
type.

The first statement is the control that separates the two: division itself does
not diverge.
"""

[[test.results]]
value = "[{ a: 1, b: 2, id: m:1, t: 1 }]"

[[test.results]]
value = "[{ a: 2, b: 4, id: m:2, t: 1 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[{ sa: 3, sb: 6, t: 1 }]"

[[test.results]]
value = "[{ mn: 1, mx: 2, t: 1 }]"

[[test.results]]
value = "[{ av: 1.5f, t: 1 }]"

[[test.results]]
value = "[{ n: 2, t: 1 }]"

[[test.results]]
value = "[{ num: 9, t: 1 }]"

[[test.results]]
value = "[{ avg_sum: 2, t: 1 }]"

[[test.results]]
value = "3"

[[test.results]]
value = "[{ a: 9007199254740993, b: 0, id: m:3, t: 2 }]"

[[test.results]]
value = "[{ sa: 9007199254740993, t: 2 }]"

[[test.results]]
value = "9007199254740993"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ mean_is_float: true, min_is_int: true, sum_is_int: true, t: 1 }]"













*/

CREATE m:1 SET a = 1, b = 2, t = 1;
CREATE m:2 SET a = 2, b = 4, t = 1;

-- Control: scalar integer division truncates identically on both engines, so
-- the divergence below is the aggregate's result type, not the operator.
RETURN 9 / 4;

-- The one aggregate whose result type diverges.
SELECT math::sum(a) AS sa, math::sum(b) AS sb, t FROM m GROUP BY t;

-- Controls: every neighbouring aggregate agrees. min/max stay integers on
-- both engines, mean is a float either way, count an integer.
SELECT math::min(a) AS mn, math::max(a) AS mx, t FROM m GROUP BY t;
SELECT math::mean(a) AS av, t FROM m GROUP BY t;
SELECT count() AS n, t FROM m GROUP BY t;

-- The type propagates through arithmetic, and integer division is where it
-- stops being invisible.
SELECT math::sum(a) + math::sum(b) AS num, t FROM m GROUP BY t;
SELECT (math::sum(a) + math::sum(b)) / 4 AS avg_sum, t FROM m GROUP BY t;

-- The scalar form is the oracle: `math::sum` over a list of integers is an
-- integer on both engines, so the streaming result type is the one that agrees
-- with the function's own contract.
RETURN math::sum([1, 2]);

-- And the widening is lossy rather than merely wider: an integer above 2^53
-- does not survive the round trip through f64.
CREATE m:3 SET a = 9007199254740993, b = 0, t = 2;
SELECT math::sum(a) AS sa, t FROM m WHERE t = 2 GROUP BY t;
RETURN math::sum([9007199254740993, 0]);

-- The type claim, asserted type-sensitively. The harness compares numbers
-- across variants, so an expectation written `3` is satisfied by `3f` and
-- `3dec` alike; only a boolean derived from the type can pin a result type.
-- The scalar oracle is `true` on both engines, so integer is the function's
-- own contract, and the grouped form is where the two disagree.
RETURN type::is_int(math::sum([1, 2]));
SELECT type::is_int(math::sum(a)) AS sum_is_int, type::is_int(math::min(a)) AS min_is_int, type::is_float(math::mean(a)) AS mean_is_float, t FROM m WHERE t = 1 GROUP BY t;
