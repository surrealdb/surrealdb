/**
[test]
run = false
*/

DEFINE TABLE ivq_catalog SCHEMAFULL;
DEFINE FIELD ds ON ivq_catalog TYPE option<interval> SQL TYPE POSTGRESQL 'interval day to second(3)';
DEFINE FIELD ym ON ivq_catalog TYPE option<interval> SQL TYPE POSTGRESQL 'interval year to month';
DEFINE FIELD h ON ivq_catalog TYPE option<interval> SQL TYPE POSTGRESQL 'interval hour';
DEFINE FIELD p ON ivq_catalog TYPE option<interval> SQL TYPE POSTGRESQL 'interval(2)';
DEFINE FIELD f ON ivq_catalog TYPE option<interval> SQL TYPE POSTGRESQL 'interval';
