/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ country: 'GBP', id: temperature:1, time: d'2020-01-01T08:00:00Z', year: 2020 }, { country: 'GBP', id: temperature:2, time: d'2020-02-01T08:00:00Z', year: 2020 }, { country: 'GBP', id: temperature:3, time: d'2020-03-01T08:00:00Z', year: 2020 }, { country: 'GBP', id: temperature:4, time: d'2021-01-01T08:00:00Z', year: 2021 }, { country: 'GBP', id: temperature:5, time: d'2021-01-01T08:00:00Z', year: 2021 }, { country: 'EUR', id: temperature:6, time: d'2021-01-01T08:00:00Z', year: 2021 }, { country: 'USD', id: temperature:7, time: d'2021-01-01T08:00:00Z', year: 2021 }, { country: 'AUD', id: temperature:8, time: d'2021-01-01T08:00:00Z', year: 2021 }, { country: 'CHF', id: temperature:9, time: d'2023-01-01T08:00:00Z', year: 2023 }]"

[[test.results]]
value = "[{ count: 1, country: 'AUD', max: d'2021-01-01T08:00:00Z', min: d'2021-01-01T08:00:00Z', year: 2021 }, { count: 1, country: 'CHF', max: d'2023-01-01T08:00:00Z', min: d'2023-01-01T08:00:00Z', year: 2023 }, { count: 1, country: 'EUR', max: d'2021-01-01T08:00:00Z', min: d'2021-01-01T08:00:00Z', year: 2021 }, { count: 3, country: 'GBP', max: d'2020-03-01T08:00:00Z', min: d'2020-01-01T08:00:00Z', year: 2020 }, { count: 2, country: 'GBP', max: d'2021-01-01T08:00:00Z', min: d'2021-01-01T08:00:00Z', year: 2021 }, { count: 1, country: 'USD', max: d'2021-01-01T08:00:00Z', min: d'2021-01-01T08:00:00Z', year: 2021 }]"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'temperature' }, operation: 'Iterate Table' }, { detail: { "Aggregate expressions": { expr0: 'time' }, Aggregations: { _a0: 'Count', _a1: 'DatetimeMin(expr0)', _a2: 'DatetimeMax(expr0)' }, "Group expressions": { _g0: 'country', _g1: 'time::year(time)' }, "Select expression": { count: '_a0', country: '_g0', max: '_a2', min: '_a1', year: '_g1' }, type: 'Group' }, operation: 'Collector' }]'''

*/
{
    CREATE temperature:1 SET country = 'GBP', time = d'2020-01-01T08:00:00Z';
    CREATE temperature:2 SET country = 'GBP', time = d'2020-02-01T08:00:00Z';
    CREATE temperature:3 SET country = 'GBP', time = d'2020-03-01T08:00:00Z';
    CREATE temperature:4 SET country = 'GBP', time = d'2021-01-01T08:00:00Z';
    CREATE temperature:5 SET country = 'GBP', time = d'2021-01-01T08:00:00Z';
    CREATE temperature:6 SET country = 'EUR', time = d'2021-01-01T08:00:00Z';
    CREATE temperature:7 SET country = 'USD', time = d'2021-01-01T08:00:00Z';
    CREATE temperature:8 SET country = 'AUD', time = d'2021-01-01T08:00:00Z';
    CREATE temperature:9 SET country = 'CHF', time = d'2023-01-01T08:00:00Z';
    RETURN "OK";
};
SELECT *, time::year(time) AS year FROM temperature;
SELECT count(), time::min(time) as min, time::max(time) as max, time::year(time) AS year, country FROM temperature GROUP BY country, year;
SELECT count(), time::min(time) as min, time::max(time) as max, time::year(time) AS year, country FROM temperature GROUP BY country, year EXPLAIN;

