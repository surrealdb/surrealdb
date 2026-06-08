/**
[test]
reason = "Validate the initial-population path of materialized aggregate views for every aggregate handled in DefineTableStatement::initialize_aggregate_view. Regressions: (1) time::min looked up the wrong required-value index (DatetimeMax) and pushed the wrong stat (TimeMax), panicking when only time::min was selected and returning the max value when both time::min and time::max were selected; (2) the PowSum select expression used `count(x**2)` instead of `math::sum(x**2)`, producing nonsensical stddev/variance values on initial population."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tgt:1, num: 1, optional: 10, time: d'2020-01-01T00:00:01Z' }]"

[[test.results]]
value = "[{ id: tgt:2, num: 2, time: d'2020-01-01T00:00:02Z' }]"

[[test.results]]
value = "[{ id: tgt:3, num: 3, optional: 20, time: d'2020-01-01T00:00:03Z' }]"

[[test.results]]
value = "[{ id: tgt:4, num: 4, time: d'2020-01-01T00:00:04Z' }]"

[[test.results]]
value = "[{ id: tgt:5, num: 5, optional: 30, time: d'2020-01-01T00:00:05Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: stats:[], num_max: 5, num_mean: 3f, num_min: 1, num_stddev: 1.5811388300841898f, num_sum: 15f, num_variance: 2.5f, opt_count: 3, time_max: d'2020-01-01T00:00:05Z', time_min: d'2020-01-01T00:00:01Z', total: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: only_time_min:[], t: d'2020-01-01T00:00:01Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: only_num_min:[], n: 1 }]"

*/

DEFINE TABLE tgt SCHEMAFULL;
DEFINE FIELD num ON tgt TYPE int;
DEFINE FIELD time ON tgt TYPE datetime;
DEFINE FIELD optional ON tgt TYPE option<int>;

CREATE tgt:1 SET num = 1, time = d'2020-01-01T00:00:01Z', optional = 10;
CREATE tgt:2 SET num = 2, time = d'2020-01-01T00:00:02Z';
CREATE tgt:3 SET num = 3, time = d'2020-01-01T00:00:03Z', optional = 20;
CREATE tgt:4 SET num = 4, time = d'2020-01-01T00:00:04Z';
CREATE tgt:5 SET num = 5, time = d'2020-01-01T00:00:05Z', optional = 30;

-- Defining the view AFTER the source rows exist exercises the
-- initialize_aggregate_view path (the inserts above don't trigger it).
DEFINE TABLE stats AS
    SELECT
        count() AS total,
        count(optional) AS opt_count,
        math::max(num) AS num_max,
        math::min(num) AS num_min,
        math::sum(num) AS num_sum,
        math::mean(num) AS num_mean,
        time::max(time) AS time_max,
        time::min(time) AS time_min,
        math::stddev(num) AS num_stddev,
        math::variance(num) AS num_variance
    FROM tgt
    GROUP ALL;

SELECT * FROM stats;

-- Standalone time::min view: before the fix this panicked because
-- the lookup keyed on DatetimeMax which was never inserted into
-- required_values when only time::min was requested.
DEFINE TABLE only_time_min AS
    SELECT time::min(time) AS t FROM tgt GROUP ALL;

SELECT * FROM only_time_min;

-- Standalone math::min view, mirrored for parity.
DEFINE TABLE only_num_min AS
    SELECT math::min(num) AS n FROM tgt GROUP ALL;

SELECT * FROM only_num_min;
